# database_utils.py

import pandas as pd
from sqlalchemy import inspect, text  # Importar 'text' do SQLAlchemy
from sqlalchemy.types import VARCHAR, INTEGER, FLOAT, DATE, DateTime
import logging

def get_last_date(engine, table_name):
    """
    Obtém a data mais recente dos dados na tabela especificada.
    """
    with engine.connect() as connection:
        inspector = inspect(engine)
        if not inspector.has_table(table_name):
            logging.info(f"Tabela '{table_name}' não existe no banco de dados.")
            return None
        else:
            query = text(f"SELECT MAX(data_atendimento) FROM {table_name}")
            result = connection.execute(query).fetchone()
            return result[0]

def update_database(engine, data, table_name):
    """
    Atualiza o banco de dados com o DataFrame fornecido.
    """
    try:
        # Insere os dados no banco
        data.to_sql(table_name, engine, if_exists='append', index=False)
        logging.info(f"Dados inseridos na tabela '{table_name}' com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao atualizar o banco de dados: {e}")

def adjust_table_schema(engine, data, table_name):
    """
    Ajusta o esquema da tabela no banco de dados para corresponder ao DataFrame.
    """
    inspector = inspect(engine)
    with engine.connect() as connection:
        if not inspector.has_table(table_name):
            # Cria a tabela se não existir
            data.head(0).to_sql(table_name, engine, if_exists='replace', index=False)
            logging.info(f"Tabela '{table_name}' criada com sucesso.")
        else:
            # Verifica colunas existentes
            db_columns = [col['name'] for col in inspector.get_columns(table_name)]
            for column in data.columns:
                if column not in db_columns:
                    # Adiciona coluna que está faltando
                    col_type = get_sqlalchemy_type(data[column].dtype)
                    alter_stmt = text(f'ALTER TABLE {table_name} ADD COLUMN "{column}" {col_type}')
                    connection.execute(alter_stmt)
                    logging.info(f"Coluna '{column}' adicionada à tabela '{table_name}'.")
                        
def get_sqlalchemy_type(dtype):
    """
    Mapeia o tipo de dados do pandas para o tipo SQLAlchemy correspondente.
    """
    if pd.api.types.is_integer_dtype(dtype):
        return INTEGER()
    elif pd.api.types.is_float_dtype(dtype):
        return FLOAT()
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return DateTime()
    else:
        return VARCHAR()
