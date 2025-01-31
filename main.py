import logging
import os
import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv
import duckdb
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
# Removido temporariamente:
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
# from jose import JWTError, jwt
from pydantic import BaseModel, field_validator
from datetime import datetime

# Dicionário de mapeamento para nomes de grupos
grupos_dict = {
    'AB': 'APAC_de_Cirurgia_Bariatrica',
    'ABO': 'APAC_de_Acompanhamento_Pos_Cirurgia_Bariatrica',
    'ACF': 'APAC_de_Confeccao_de_Fistula',
    'AD': 'APAC_de_Laudos_Diversos',
    'AM': 'APAC_de_Medicamentos',
    'AMP': 'APAC_de_Acompanhamento_Multiprofissional',
    'AN': 'APAC_de_Nefrologia',
    'AQ': 'APAC_de_Quimioterapia',
    'AR': 'APAC_de_Radioterapia',
    'ATD': 'APAC_de_Tratamento_Dialitico',
    'BI': 'Boletim_de_Producao_Ambulatorial_individualizado',
    'IMPBO': 'IMPBO',
    'PA': 'Producao_Ambulatorial',
    'PAM': 'PAM',
    'PAR': 'PAR',
    'PAS': 'PAS',
    'PS': 'RAAS_Psicossocial',
    'SAD': 'RAAS_de_Atencao_Domiciliar',
    'RD': 'AIH_Reduzida',
    'RJ': 'AIH_Rejeitada',
    'ER': 'AIH_Rejeitada_com_erro',
    'SP': 'Servicos_Profissionais',
    'CH': 'Cadastro_Hospitalar',
    'CM': 'CM',
    'DC': 'Dados_Complementares',
    'EE': 'Estabelecimento_de_Ensino',
    'EF': 'Estabelecimento_Filantropico',
    'EP': 'Equipes',
    'EQ': 'Equipamentos',
    'GM': 'Gestao_Metas',
    'HB': 'Habilitacao',
    'IN': 'Incentivos',
    'LT': 'Leitos',
    'PF': 'Profissional',
    'RC': 'Regra_Contratual',
    'SR': 'Servico_Especializado',
    'ST': 'Estabelecimentos'
}

CAMPOS_CNES = {
    "AB": ["ap_cnspcn"],
    "ABO": ["ap_cnspcn"],
    "ACF": ["ap_cnspcn"],
    "AD": ["ap_cnspcn"],
    "AM": ["ap_cnspcn"],
    "AMP": ["ap_cnspcn"],
    "AN": ["ap_cnspcn"],
    "AQ": ["ap_cnspcn"],
    "AR": ["ap_cnspcn"],
    "ATD": ["ap_cnspcn"],
    "BI": ["cns_pac", "cnsprof"],
    "PA": ["PA_CODUNI"],
    "RD": ["CNES"],
    "RJ": ["cnes"],
    "ER": ["CNES"],
    "SP": ["sp_cnes"]
}




# ---------------------------------------------------------------------------
# Configurações básicas
# ---------------------------------------------------------------------------
load_dotenv()

# Diretório de logs
if not os.path.exists('logs'):
    os.makedirs('logs')

# Configurando logging para arquivo
logging.basicConfig(
    filename='logs/app.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Adicionando um handler para exibir logs no console (terminal)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_format = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
console_handler.setFormatter(console_format)
logging.getLogger().addHandler(console_handler)

# Configurações do JWT - Removido temporariamente
# SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key")
# ALGORITHM = "HS256"
# security = HTTPBearer()

# Configurações do banco de dados
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_PASSWORD_ENCODED = quote_plus(DB_PASSWORD)

# ---------------------------------------------------------------------------
# Modelo de dados para parâmetros da consulta
# ---------------------------------------------------------------------------
class QueryParams(BaseModel):
    base: str
    grupo: str
    cnes_list: List[str]
    campos_agrupamento: List[str]
    competencia_inicio: str
    competencia_fim: str
    table_name: str | None = None  # Campo opcional para nome da tabela
    
    @field_validator('base')
    def validate_base(cls, v):
        if v not in ['SIH', 'SIA']:
            raise ValueError('Base deve ser SIH ou SIA')
        return v

    @field_validator('grupo')
    def validate_grupo(cls, v):
        # Caso deseje restringir a um subconjunto fixo, pode usar v not in [...],
        # mas no momento alguns grupos foram listados:
        grupos_validos = [
            'RD','RJ','ER','SP','PA','AB','ABO','ACF','AD','AM','AMP','AN','AQ','AR','ATD',
            'BI','IMPBO','PAM','PAR','PAS','PS','SAD','CH','CM','DC','EE','EF','EP','EQ',
            'GM','HB','IN','LT','PF','RC','SR','ST'
        ]
        if v not in grupos_validos:
            raise ValueError('Grupo inválido')
        return v

    @field_validator('competencia_inicio', 'competencia_fim')
    def validate_competencia(cls, v):
        """
        Verifica se a competência está no formato MM/YYYY.
        """
        try:
            datetime.strptime(v, '%m/%Y')
        except ValueError:
            raise ValueError('Formato de competência deve ser MM/YYYY')
        return v

# ---------------------------------------------------------------------------
# Função de coleta de arquivos parquet
# ---------------------------------------------------------------------------
def log_execution(message: str, is_start: bool = True) -> None:
    marker = ">>>" if is_start else "<<<"
    logging.info(f"{marker} {message}")

def get_parquet_files(base: str, grupo: str, comp_inicio: str, comp_fim: str) -> List[str]:
    log_execution("Iniciando busca de arquivos Parquet")
    import glob
    
    # Lista de UFs do Brasil
    ufs = [
        'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
        'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'
    ]
    
    logging.info(
        f"[get_parquet_files] Iniciando coleta de arquivos: base={base}, grupo={grupo}, "
        f"intervalo={comp_inicio} até {comp_fim}"
    )

    files = []
    start_date = datetime.strptime(comp_inicio, '%m/%Y')
    end_date = datetime.strptime(comp_fim, '%m/%Y')
    
    current_date = start_date
    while current_date <= end_date:
        year_suffix = current_date.strftime('%y')  # ex: 22 para 2022
        month = current_date.strftime('%m')        # ex: 01 para janeiro
        
        for uf in ufs:
            pattern = (
                f"parquet_files/{base}/{grupo}/"
                f"{grupo}{uf}{year_suffix}{month}.parquet/*.parquet"
            )
            logging.debug(f"[get_parquet_files] Buscando arquivos com pattern: {pattern}")
            
            matching_files = glob.glob(pattern)
            if matching_files:
                logging.info(
                    f"[get_parquet_files] Encontrados {len(matching_files)} arquivos "
                    f"para {uf} em {month}/{year_suffix}."
                )
                files.extend(matching_files)
        
        # Avança para o próximo mês
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
    
    total_files = len(files)
    if total_files > 0:
        logging.info(f"[get_parquet_files] Total de arquivos encontrados: {total_files}")
    else:
        logging.warning("[get_parquet_files] Nenhum arquivo encontrado para o período.")
    
    log_execution("Finalizada busca de arquivos Parquet", False)
    return files

# ---------------------------------------------------------------------------
# Processa os arquivos usando DuckDB
# ---------------------------------------------------------------------------
def get_cnes_column(grupo: str) -> str:
    """Retorna o nome da coluna CNES para o grupo específico"""
    if (grupo not in CAMPOS_CNES):
        return "CNES"  # coluna padrão se não encontrar mapeamento
    return CAMPOS_CNES[grupo][0]  # pega primeiro item da lista

def process_parquet_files(files: List[str], params: QueryParams) -> pd.DataFrame:
    log_execution("Iniciando processamento de arquivos")
    logging.info(f"[process_parquet_files] Iniciando processamento de {len(files)} arquivo(s).")
    
    con = duckdb.connect()
    cnes_str = ",".join([f"'{cnes}'" for cnes in params.cnes_list])
    
    # Obtém nome correto da coluna CNES para o grupo
    cnes_column = get_cnes_column(params.grupo)
    
    # Adiciona coluna CNES aos campos de agrupamento se não existir
    campos_agrupamento = params.campos_agrupamento.copy()
    if not any(col.lower() == cnes_column.lower() for col in campos_agrupamento):
        campos_agrupamento.insert(0, cnes_column)
    
    group_by_cols = ",".join(campos_agrupamento)
    
    results = []
    for file_path in files:
        logging.info(f"[process_parquet_files] Processando arquivo: {file_path}")
        
        query = f"""
        SELECT {group_by_cols}
        FROM read_parquet('{file_path}')
        WHERE LOWER({cnes_column}) IN (SELECT LOWER(unnest(ARRAY[{cnes_str}])))
        """
        try:
            df = con.execute(query).df()
            logging.info(f"[process_parquet_files] OK. Linhas retornadas: {len(df)}.")
            results.append(df)
        except Exception as e:
            logging.error(f"[process_parquet_files] Erro ao processar arquivo {file_path}: {e}")
    
    con.close()
    
    if not results:
        logging.info("[process_parquet_files] Nenhum resultado encontrado.")
        return pd.DataFrame()
    
    final_df = pd.concat(results, ignore_index=True)
    logging.info(f"[process_parquet_files] Concat final OK. Linhas totais: {len(final_df)}.")
    log_execution("Finalizado processamento de arquivos", False)
    return final_df

# ---------------------------------------------------------------------------
# Função de salvamento de resultados
# ---------------------------------------------------------------------------
def save_results(df: pd.DataFrame, table_name: str) -> None:
    logging.info(f"[save_results] Iniciando salvamento. Tabela destino: {table_name}")
    
    try:
        # Força recarregamento das variáveis de ambiente
        load_dotenv(override=True)  # Adicionar parâmetro override
        
        # Criar string de conexão com valores atualizados
        conn_str = (
            f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
            f"{quote_plus(os.getenv('DB_PASSWORD'))}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
            f"{os.getenv('DB_NAME')}"
        )
        
        engine = create_engine(conn_str)
        
        # Testar conexão antes de prosseguir
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
            logging.info("Conexão com banco testada com sucesso")
        
        # Salva no PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"[save_results] Tabela '{table_name}' salva/recriada no PostgreSQL (replace).")

        # Se tiver menos de 10M linhas, salva em CSV
        row_count = len(df)
        if row_count < 10_000_000:
            csv_dir = 'consultas'
            os.makedirs(csv_dir, exist_ok=True)
            csv_path = f'{csv_dir}/{table_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            df.to_csv(csv_path, index=False)
            logging.info(f"[save_results] DataFrame com {row_count} linha(s) salvo em CSV: {csv_path}")
        else:
            logging.info(f"[save_results] DataFrame com {row_count} linha(s). Não será salvo em CSV.")
    except Exception as e:
        logging.error(f"[save_results] Erro ao salvar resultados: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao salvar resultados: {e}")

# ---------------------------------------------------------------------------
# Autenticação - Removido temporariamente
# ---------------------------------------------------------------------------
# def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
#     try:
#         token = credentials.credentials
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         return payload
#     except JWTError:
#         raise HTTPException(status_code=401, detail="Token inválido")

# ---------------------------------------------------------------------------
# Criação da aplicação FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(title="DataSUS API")

# Configuração CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Rota principal para query
# ---------------------------------------------------------------------------
@app.post("/query")
async def query_data(params: QueryParams) -> Dict[str, Any]:
    log_execution("Iniciando processamento de requisição")
    """
    Endpoint para consulta de dados do DataSUS.
    
    Exemplo de requisição:
    ```
    POST /query
    
    {
        "base": "SIH",
        "grupo": "RD",
        "cnes_list": ["2077485", "2077493"],
        "campos_agrupamento": ["CNES", "ANO_CMPT", "MES_CMPT"],
        "competencia_inicio": "01/2022",
        "competencia_fim": "12/2022"
    }
    ```
    
    Outro exemplo com diferentes parâmetros:
    ```
    POST /query
    
    {
        "base": "SIA",
        "grupo": "PA",
        "cnes_list": ["2077485"],
        "campos_agrupamento": ["CNES", "PROC_ID"],
        "competencia_inicio": "06/2022",
        "competencia_fim": "06/2022"
    }
    ```
    """
    logging.info(
        "[query_data] Recebendo requisição. "
        f"Base={params.base}, Grupo={params.grupo}, Competências={params.competencia_inicio} a {params.competencia_fim}."
    )
    try:
        # 1) Localiza arquivos .parquet
        files = get_parquet_files(
            params.base,
            params.grupo,
            params.competencia_inicio,
            params.competencia_fim
        )
        
        if not files:
            msg = "Nenhum arquivo encontrado para os critérios especificados."
            logging.warning(f"[query_data] {msg}")
            raise HTTPException(status_code=404, detail=msg)
        
        # 2) Processa os arquivos
        result_df = process_parquet_files(files, params)
        
        if result_df.empty:
            msg = "Nenhum dado encontrado para os critérios especificados."
            logging.warning(f"[query_data] {msg}")
            raise HTTPException(status_code=404, detail=msg)
        
        # 3) Mapeia o grupo para um nome descritivo e define nome da tabela
        grupo_mapped = grupos_dict.get(params.grupo, params.grupo)
        # Usa o nome da tabela fornecido ou gera um nome padrão
        table_name = params.table_name if params.table_name else f"{params.base.lower()}_{grupo_mapped.lower()}"
        
        # 4) Salva os resultados
        save_results(result_df, table_name)
        
        logging.info("[query_data] Consulta processada com sucesso.")
        
        # 5) Retorna a resposta com os dados e metadados
        response = {
            "status": "success",
            "message": "Consulta processada com sucesso",
            "dados": result_df.to_dict(orient='records'),
            "total_registros": len(result_df),
            "colunas": result_df.columns.tolist(),
            "table_name": table_name
        }
        
    except Exception as e:
        logging.error(f"[query_data] Erro ao processar consulta: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    log_execution("Finalizado processamento de requisição", False)
    return response

# ---------------------------------------------------------------------------
# Verifica conexão com banco de dados
# ---------------------------------------------------------------------------
def verify_db_connection():
    try:
        with create_engine(
            f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        ).connect() as connection:
            connection.execute(text('SELECT 1'))
            logging.info("Conexão com banco de dados verificada com sucesso")
    except Exception as e:
        logging.error(f"Erro na conexão com banco de dados: {str(e)}")
        raise

# ---------------------------------------------------------------------------
# Execução local (uvicorn)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
