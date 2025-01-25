import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

def verificar_variaveis_ambiente():
    """Verifica e exibe as variáveis de ambiente"""
    load_dotenv()
    
    variaveis = {
        'DB_USER': os.getenv('DB_USER'),
        'DB_PASSWORD': os.getenv('DB_PASSWORD'),  # Mascara a senha por segurança
        'DB_HOST': os.getenv('DB_HOST'),
        'DB_PORT': os.getenv('DB_PORT'),
        'DB_NAME': os.getenv('DB_NAME')
    }
    
    print("\n=== Verificação de Variáveis de Ambiente ===")
    for key, value in variaveis.items():
        print(f"{key}: {value}")

def testar_conexao_banco():
    """Testa a conexão com o banco de dados"""
    try:
        db_password = quote_plus(os.getenv('DB_PASSWORD'))
        engine = create_engine(
            f'postgresql+psycopg2://{os.getenv("DB_USER")}:{db_password}@'
            f'{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}'
        )
        
        with engine.connect() as conn:
            result = conn.execute(text('SELECT version();')).fetchone()
            print("\n=== Teste de Conexão ===")
            print("Conexão estabelecida com sucesso!")
            print(f"Versão do PostgreSQL: {result[0]}")
            print(f"senha usada: {db_password}")
            
    except Exception as e:
        print("\n=== Erro de Conexão ===")
        print(f"Erro ao conectar: {str(e)}")

if __name__ == "__main__":
    verificar_variaveis_ambiente()
    testar_conexao_banco()