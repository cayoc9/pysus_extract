# Módulo para funções compartilhadas
import os
import logging
import pandas as pd
import unicodedata
import re
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Configurações globais
load_dotenv()
DATABASE_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# Funções de Normalização
def normalizar_nome(nome):
    nome = nome.lower()
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('ASCII')
    nome = re.sub(r'\W+', '_', nome)
    return nome.strip('_')

# Funções de Banco de Dados
def get_db_engine():
    return create_engine(DATABASE_URL, pool_size=10, max_overflow=20)

def executar_query(query, params=None):
    try:
        with get_db_engine().connect() as conn:
            return conn.execute(text(query), params or {})
    except SQLAlchemyError as e:
        logging.error(f"Erro na query: {query}\nErro: {str(e)}")
        raise

# Funções de Arquivos
def carregar_parquet(caminho):
    if os.path.isdir(caminho):
        arquivos = [os.path.join(caminho, f) for f in os.listdir(caminho) if f.endswith('.parquet')]
        return pd.concat((pd.read_parquet(f) for f in arquivos), ignore_index=True)
    return pd.read_parquet(caminho)

# Funções de Log
def configurar_logging(nome_script):
    logger = logging.getLogger(nome_script)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # File handler
    log_path = f"logs/{nome_script}.log"
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    fh = logging.FileHandler(log_path)
    fh.setFormatter(formatter)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

# Funções de Análise
def analisar_coluna(df, coluna):
    return {
        'tipo': str(df[coluna].dtype),
        'unicos': df[coluna].nunique(),
        'nulos': df[coluna].isnull().sum(),
        'exemplo': df[coluna].dropna().sample(3).tolist()
    } 