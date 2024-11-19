import os
import glob
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from urllib.parse import quote_plus
import logging


# Garantir que o diretório 'logs' existe
if not os.path.exists('logs'):
    os.makedirs('logs')

    
# Configuração do logging
logging.basicConfig(
    filename='logs/upload.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

def upload_parquet_files_to_db():
    # Carregar variáveis de ambiente
    load_dotenv()

    # Configurações do banco de dados
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT', '5432')  # Porta padrão do PostgreSQL
    DB_NAME = os.getenv('DB_NAME')

    # Verificar se todas as variáveis de ambiente estão definidas
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
        logging.error("Variáveis de ambiente do banco de dados não estão completamente definidas.")
        return

    # Codificar a senha (caso tenha caracteres especiais)
    DB_PASSWORD_ENCODED = quote_plus(DB_PASSWORD)

    # Cria a engine de conexão com o PostgreSQL
    DATABASE_URL = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(DATABASE_URL)

    # Diretório onde estão os arquivos Parquet
    parquet_dir = os.path.expanduser('~/pysus/2023')

    # Padrão dos arquivos a serem lidos
    parquet_files_pattern = os.path.join(parquet_dir, 'SPSP23[0-1][0-9].parquet')

    # Listar todos os arquivos Parquet correspondentes
    parquet_files = sorted(glob.glob(parquet_files_pattern))

    # Filtrar apenas os arquivos de SPSP2301.parquet a SPSP2312.parquet
    parquet_files = [f for f in parquet_files if 'SPSP2300.parquet' < os.path.basename(f) <= 'SPSP2312.parquet']

    if not parquet_files:
        logging.info("Nenhum arquivo Parquet encontrado para upload.")
        return

    # Nome da tabela no banco de dados
    table_name = 'SIH_Serviços_Profissionais' 

    # Ler e inserir cada arquivo Parquet no banco de dados
    for file in parquet_files:
        try:
            logging.info(f"Lendo o arquivo {file}")
            df = pd.read_parquet(file)

            # Opcional: Realizar transformações nos dados, se necessário
            # Por exemplo, converter tipos de dados, renomear colunas, etc.

            # Inserir os dados no banco de dados
            df.to_sql(table_name, engine, if_exists='append', index=False)
            logging.info(f"Dados do arquivo {file} inseridos com sucesso na tabela '{table_name}'.")
        except Exception as e:
            logging.error(f"Erro ao inserir dados do arquivo {file}: {e}")

if __name__ == "__main__":
    upload_parquet_files_to_db()
