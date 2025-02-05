from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_engine():
    """
    Cria e retorna uma conexão com o banco de dados usando variáveis de ambiente
    """
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
        pool_size=10,
        max_overflow=20
    )

def executar_query(query: str, params: dict = None):
    """
    Executa uma query SQL de forma segura com tratamento de erros
    
    Args:
        query (str): Query SQL a ser executada
        params (dict): Parâmetros para a query
    
    Returns:
        ResultProxy: Resultado da execução
    """
    try:
        with get_db_engine().connect() as conn:
            return conn.execute(text(query), params or {})
    except SQLAlchemyError as e:
        raise RuntimeError(f"Erro na query: {query}\nErro: {str(e)}") from e

def tabela_existe(tabela: str, schema: str = 'public') -> bool:
    """
    Verifica se uma tabela existe no banco de dados
    
    Args:
        tabela (str): Nome da tabela
        schema (str): Nome do schema
    
    Returns:
        bool: True se a tabela existe
    """
    query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = :schema 
            AND table_name = :tabela
        )
    """
    return executar_query(query, {'schema': schema, 'tabela': tabela}).scalar() 