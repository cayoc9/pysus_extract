import os
import logging
import pandas as pd
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import yaml

# Carregar variáveis de ambiente
load_dotenv()

# Configurar o log
logging.basicConfig(
    filename=os.path.join('log', 'upload_manager.log'),
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

# Carregar configurações do config.yml
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)

# Construir a URL de conexão a partir das variáveis de ambiente
DATABASE_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
              f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# Criar o engine do SQLAlchemy
engine = create_engine(DATABASE_URL)

def get_existing_columns(table_name):
    """
    Retorna uma lista das colunas existentes na tabela.
    """
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    return [column['name'] for column in columns]

def map_dtype(pandas_dtype):
    """
    Mapeia tipos de dados do Pandas para tipos do SQLAlchemy.
    """
    if pandas_dtype in ['int', 'integer']:
        return 'INTEGER'
    elif pandas_dtype in ['float', 'floating']:
        return 'FLOAT'
    elif pandas_dtype == 'boolean':
        return 'BOOLEAN'
    elif pandas_dtype.startswith('datetime'):
        return 'TIMESTAMP'
    elif pandas_dtype.startswith('object') or pandas_dtype.startswith('string'):
        return 'TEXT'
    else:
        return None

def add_missing_columns(table_name, df):
    """
    Adiciona colunas que estão no DataFrame mas não existem na tabela.
    """
    existing_columns = get_existing_columns(table_name)
    missing_columns = set(df.columns) - set(existing_columns)
    
    if missing_columns:
        with engine.connect() as connection:
            for column in missing_columns:
                # Determinar o tipo de dados adequado
                dtype = pd.api.types.infer_dtype(df[column], skipna=True)
                sqlalchemy_type = map_dtype(dtype)
                
                if sqlalchemy_type:
                    try:
                        alter_query = text(f'ALTER TABLE "{table_name}" ADD COLUMN "{column}" {sqlalchemy_type}')
                        connection.execute(alter_query)
                        logging.info(f'Coluna "{column}" adicionada à tabela "{table_name}".')
                    except SQLAlchemyError as e:
                        logging.error(f'Erro ao adicionar a coluna "{column}" à tabela "{table_name}": {e}')
                else:
                    logging.warning(f'Tipo de dados não mapeado para a coluna "{column}".')
    else:
        logging.info(f'Nenhuma coluna faltante encontrada para a tabela "{table_name}".')

def partition_table_by_state(table_name):
    """
    Particiona a tabela por estado usando a coluna SP_UF.
    """
    with engine.connect() as connection:
        # Verificar se a tabela já está particionada
        partition_check = text("""
            SELECT inhrelid::regclass::text AS child
            FROM pg_inherits
            WHERE inhparent = :table::regclass;
        """)
        result = connection.execute(partition_check, table=table_name)
        partitions = result.fetchall()
        
        if partitions:
            logging.info(f'Tabela "{table_name}" já está particionada.')
            return
        
        # Alterar a tabela para ser particionada
        try:
            partition_query = text(f"""
                ALTER TABLE "{table_name}"
                PARTITION BY LIST (SP_UF);
            """)
            connection.execute(partition_query)
            logging.info(f'Tabela "{table_name}" alterada para particionamento por "SP_UF".')
        except SQLAlchemyError as e:
            logging.error(f'Erro ao particionar a tabela "{table_name}": {e}')
            return
        
        # Obter todos os estados a partir da configuração
        states = config['download']['states']
        
        for state in states:
            try:
                # Criar a partição para cada estado
                create_partition = text(f"""
                    CREATE TABLE "{table_name}_{state}" PARTITION OF "{table_name}"
                    FOR VALUES IN ('{state}');
                """)
                connection.execute(create_partition)
                logging.info(f'Partição "{table_name}_{state}" criada para o estado "{state}".')
            except SQLAlchemyError as e:
                logging.error(f'Erro ao criar partição para o estado "{state}": {e}')

def insert_data(table_name, df):
    """
    Insere os dados do DataFrame na tabela usando o comando COPY para eficiência.
    """
    # Converter o DataFrame para CSV em memória
    csv_data = df.to_csv(index=False, header=False, sep=',')
    csv_buffer = io.StringIO(csv_data)
    
    try:
        with engine.raw_connection() as connection:
            cursor = connection.cursor()
            cursor.copy_expert(sql=f"COPY \"{table_name}\" FROM STDIN WITH CSV", file=csv_buffer)
            connection.commit()
        logging.info(f'Dados inseridos com sucesso na tabela "{table_name}".')
    except SQLAlchemyError as e:
        logging.error(f'Erro ao inserir dados na tabela "{table_name}": {e}')

def create_index(table_name, column_name):
    """
    Cria um índice na coluna especificada da tabela.
    """
    index_name = f"{table_name}_{column_name}_idx"
    with engine.connect() as connection:
        try:
            create_index_query = text(f"""
                CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" ("{column_name}");
            """)
            connection.execute(create_index_query)
            logging.info(f'Índice "{index_name}" criado na tabela "{table_name}" para a coluna "{column_name}".')
        except SQLAlchemyError as e:
            logging.error(f'Erro ao criar índice "{index_name}" na tabela "{table_name}": {e}')

def process_arquivo(tabela, arquivo_path):
    """
    Processa um único arquivo Parquet: adiciona colunas, particiona a tabela, insere os dados e cria índices.
    """
    logging.info(f'Processando arquivo: {arquivo_path}')
    
    try:
        # Carregar os dados do Parquet em um DataFrame
        df = pd.read_parquet(arquivo_path)
        
        # Tratar as colunas e adicionar se faltarem
        add_missing_columns(tabela, df)
        
        # Particionar a tabela por estado (se ainda não estiver particionada)
        partition_table_by_state(tabela)
        
        # Inserir os dados na tabela
        insert_data(tabela, df)
        
        # Criar índices nas colunas mais importantes
        create_index(tabela, 'SP_UF')  # Exemplo: índice na coluna de estado
        
    except Exception as e:
        logging.error(f'Erro ao processar o arquivo "{arquivo_path}": {e}', exc_info=True)

def upload_all_data_parallel():
    """
    Processo principal para iterar sobre cada base, grupo e realizar o upload dos dados de forma paralela.
    """
    # Iterar sobre cada base de dados
    bases = ['SIH_SI_SP', 'SIH_SI_RJ', 'SIH_ER', 'SIH_RD']  # Adicione outras bases conforme necessário
    
    with ThreadPoolExecutor(max_workers=config['parameters']['max_threads']) as executor:
        futures = []
        
        for base in bases:
            base_path = os.path.join('parquet_files', base)
            
            if not os.path.exists(base_path):
                logging.warning(f'Diretório para a base "{base}" não existe: {base_path}')
                continue
            
            # Iterar sobre cada grupo dentro da base
            grupos = config['download']['groups']['SIA'] if base.startswith('SIH_SI') else config['download']['groups']['SIA']  # Ajuste conforme a estrutura dos grupos
            
            for grupo_code, grupo_nome in grupos.items():
                # Pular grupos vazios
                if not grupo_nome:
                    logging.info(f'Grupo "{grupo_code}" está vazio. Pulando.')
                    continue
                
                # Nome da tabela
                tabela = f"{base}_{grupo_code}"
                
                # Caminho para os arquivos Parquet do grupo
                grupo_path = os.path.join(base_path, grupo_code)
                
                if not os.path.exists(grupo_path):
                    logging.warning(f'Diretório para o grupo "{grupo_code}" não existe: {grupo_path}')
                    continue
                
                # Listar todos os arquivos Parquet no diretório do grupo
                arquivos = [f for f in os.listdir(grupo_path) if f.endswith('.parquet')]
                
                if not arquivos:
                    logging.info(f'Nenhum arquivo Parquet encontrado para o grupo "{grupo_code}".')
                    continue
                
                # Iterar sobre cada arquivo Parquet e preparar para upload
                for arquivo in arquivos:
                    arquivo_path = os.path.join(grupo_path, arquivo)
                    futures.append(executor.submit(process_arquivo, tabela, arquivo_path))
        
        # Aguardar a conclusão de todas as tarefas
        for future in as_completed(futures):
            pass  # Os logs já estão sendo tratados nas funções
