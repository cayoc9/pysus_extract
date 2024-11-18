# test_db_connection.py

from sqlalchemy import create_engine, text  # Importar 'text' do SQLAlchemy
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

# Verificar se todas as variáveis foram carregadas
if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    print("Erro: Uma ou mais variáveis de ambiente não foram carregadas.")
    exit(1)

# Codificar a senha
DB_PASSWORD_ENCODED = quote_plus(DB_PASSWORD)

# Criar a engine de conexão com o PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

try:
    with engine.connect() as connection:
        # Usar 'text()' ao executar comandos SQL
        result = connection.execute(text("SELECT 1"))
        print("Conexão com o banco de dados bem-sucedida.")
except Exception as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
