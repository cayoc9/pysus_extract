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
    table_name = 'SIH_Serviços_Profissionais'  # Nome da tabela conforme solicitado

    try:
        # Definir o intervalo de anos para baixar
        start_year = 1992
        end_year = 2024  # Inclusive

        # Definir os meses (1 a 12)
        months = list(range(1, 13))
        
        # Definir o grupo SIH (apenas 'SP' conforme solicitado)
        groups = ['SP']
        
        estado = 'SP'  # Estado de São Paulo

        for year in range(start_year, end_year + 1):
            logging.info(f"Baixando dados do ano {year} para o estado {estado}")

            # Baixar os dados para o ano especificado
            data = fetch_data([year], months, groups, estado=estado)
            
            if data.empty:
                logging.info(f"Nenhum dado para o ano {year}")
                continue  # Passa para o próximo ano
            
            # Ajustar o esquema da tabela conforme os dados
            adjust_table_schema(engine, data, table_name)
            
            # Atualizar o banco de dados com os novos dados
            update_database(engine, data, table_name)
            
            logging.info(f"Dados do ano {year} atualizados com sucesso no banco de dados.")
        
        logging.info("Processo de atualização concluído com sucesso.")
        
    except Exception as e:
        logging.error(f"Ocorreu um erro durante a atualização do banco de dados: {e}")
        return

if __name__ == '__main__':
    main()
