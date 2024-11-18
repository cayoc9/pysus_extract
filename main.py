# main.py

import logging
import os
import datetime
from dotenv import load_dotenv
from data_fetcher import fetch_data
from database_utils import get_last_date, update_database, adjust_table_schema
from sqlalchemy import create_engine
from urllib.parse import quote_plus

def main():
    # Garantir que o diretório 'logs' existe
    if not os.path.exists('logs'):
        os.makedirs('logs')

    # Configuração do logging
    logging.basicConfig(
        filename='logs/app.log',
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s'
    )
    
    logging.info("Iniciando o processo de atualização do banco de dados.")
    
    # Carregar variáveis de ambiente
    load_dotenv()
    
    # Configurações do banco de dados
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    
    # Verificar se todas as variáveis de ambiente estão definidas
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
        logging.error("Variáveis de ambiente do banco de dados não estão completamente definidas.")
        return
    
    # Codificar a senha (caso tenha caracteres especiais)
    DB_PASSWORD_ENCODED = quote_plus(DB_PASSWORD)
    
    # Cria a engine de conexão com o PostgreSQL
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    # Nome da tabela no banco de dados
    table_name = 'sih_data'
    
    try:
        # Verifica a data mais recente no banco
        last_date = get_last_date(engine, table_name)
        logging.info(f"Última data registrada no banco: {last_date}")
        
        if last_date is None:
            # Se não houver dados, baixa os últimos 10 anos
            end_year = datetime.datetime.now().year
            start_year = end_year - 10
            logging.info(f"Baixando dados de {start_year} até {end_year}")
        else:
            # Se houver dados, baixa desde a última data até a atual
            start_year = last_date.year
            end_year = datetime.datetime.now().year
            logging.info(f"Baixando dados de {start_year} até {end_year}")
        
        # Baixar os dados
        data = fetch_data(start_year, end_year)
        
        if data.empty:
            logging.info("Nenhum dado novo para atualizar.")
            return
        
        # Ajustar o esquema da tabela
        adjust_table_schema(engine, data, table_name)
        
        # Atualizar o banco de dados
        update_database(engine, data, table_name)
        
        logging.info("Banco de dados atualizado com sucesso.")
        
    except Exception as e:
        logging.error(f"Ocorreu um erro durante a atualização do banco de dados: {e}")
        return

if __name__ == '__main__':
    main()
