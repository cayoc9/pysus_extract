# modulos/db_utils.py

import os
import psycopg2
import logging
from dotenv import load_dotenv

def get_db_connection():
    load_dotenv()
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
        logging.error("Variáveis de ambiente do banco de dados não definidas.")
        raise EnvironmentError("Variáveis de ambiente do banco de dados não definidas.")
    
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            sslmode='disable'
        )
        cursor = conn.cursor()
        logging.info("Conexão com o banco de dados estabelecida com sucesso.")
        return conn, cursor
    except Exception as e:
        logging.error(f"Erro ao conectar ao banco: {e}", exc_info=True)
        raise e

def close_db_connection(conn, cursor):
    try:
        cursor.close()
        conn.close()
        logging.info("Conexão com o banco de dados fechada.")
    except Exception as e:
        logging.error(f"Erro ao fechar conexão com o banco: {e}", exc_info=True)
