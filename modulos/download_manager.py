import os
import time
import logging
from dotenv import load_dotenv
from pysus.ftp.databases.sia import SIA
from tqdm import tqdm
import yaml

# Funções auxiliares
def load_processed_data(controle_path):
    processed_data = set()
    if os.path.exists(controle_path):
        with open(controle_path, 'r') as f:
            for line in f:
                processed_data.add(line.strip())
    return processed_data

def save_progress(controle_path, data_key):
    with open(controle_path, 'a') as f:
        f.write(f"{data_key}\n")

def clear_download_dir(directory):
    for f in os.listdir(directory):
        file_path = os.path.join(directory, f)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
        except Exception as e:
            logging.error(f"Erro ao remover o arquivo {file_path}: {e}", exc_info=True)

def download_sia_pa_data(config, processed_data):
    group_code = "PA"  # Grupo PA (Produção Ambulatorial)
    group_description = "Produção Ambulatorial"
    logging.info(f"Processando grupo {group_code}: {group_description}")
    print(f"Processando grupo {group_code}: {group_description}")

    # Definir o diretório de download para os arquivos
    download_dir = os.path.join(config['paths']['parquet_files'], 'SIA', group_code)
    os.makedirs(download_dir, exist_ok=True)

    sia = SIA().load()

    # Loop pelos anos e meses
    for year in tqdm(range(config['download']['years'][0], config['download']['years'][1] + 1), desc=f"Processando Anos para grupo {group_code}"):
        for month in range(1, 13):
            data_key = f"{group_code}-{year}-{month}"

            try:
                # Verificar se já foi processado
                if data_key in processed_data:
                    continue  # Já processado, pular para o próximo

                # Limpar o diretório de download
                # clear_download_dir(download_dir)

                # Obter a lista de arquivos
                files = sia.get_files(group=group_code, uf=config['download']['states'], year=year, month=month)

                if not files:
                    logging.info(f"{data_key}: Sem dados disponíveis.")
                    continue  # Pular se não houver arquivos

                # Baixar os arquivos para o diretório especificado
                sia.download(group=group_code, uf=config['download']['states'], year=year, month=month, path=download_dir)

                # Verificar se há arquivos baixados
                downloaded_files = [os.path.join(download_dir, f) for f in os.listdir(download_dir) if f.endswith('.dbc') or f.endswith('.parquet')]

                if not downloaded_files:
                    logging.warning(f"{data_key}: Nenhum arquivo foi baixado.")
                    continue

                # Registrar como processado
                processed_data.add(data_key)
                save_progress(config['paths']['progress_file'], data_key)
                logging.info(f"{data_key}: Dados baixados com sucesso.")

            except Exception as e:
                logging.error(f"{data_key}: Erro ao baixar dados: {e}", exc_info=True)
                time.sleep(config['parameters']['retry_delay'])  # Esperar antes de continuar

def download_data():
    # Configurações manuais para este script
    config = {
        'paths': {
            'parquet_files': 'parquet_files',  # Diretório onde os dados serão armazenados
            'logs': 'log',                    # Diretório de logs
            'progress_file': 'progresso_sia_pa.txt'  # Arquivo de progresso
        },
        'download': {
            'groups': {'SIA': {'PA': 'Produção Ambulatorial'}},  # Apenas o grupo PA
            'states': ['MG', 'PR', 'SP'],                       # Estados: Minas Gerais, Paraná, São Paulo
            'years': [2018, 2024]                               # Intervalo de anos
        },
        'parameters': {
            'retry_delay': 10,       # Tempo em segundos para re-tentativas
            'max_retries': 3,        # Máximo de tentativas por operação
            'backoff_factor': 2      # Fator de backoff exponencial
        }
    }

    # Configuração do log
    log_dir = config['paths']['logs']
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logging.basicConfig(
        filename=os.path.join(log_dir, 'download_sia_pa.log'),
        level=logging.INFO,
        format='%(asctime)s:%(levelname)s:%(message)s'
    )

    # Inicialização
    print("Iniciando o script de download de dados...")
    logging.info("Iniciando o script de download de dados.")

    # Carregar as variáveis de ambiente do arquivo .env
    load_dotenv()

    # Configurar conjunto de dados já processados
    processed_data = load_processed_data(config['paths']['progress_file'])

    # Processar o grupo PA
    download_sia_pa_data(config, processed_data)

    logging.info("Download de dados concluído.")
    print("Download de dados concluído.")


# Executar o script
if __name__ == '__main__':
    download_data()
