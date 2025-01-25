import os
import logging
from pysus.ftp.databases.sia import SIA
import re

# Configurar logging
logging.basicConfig(
    filename='download_sia.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Lista de grupos
Grupos = [#'ABO', 'ACF', 'AD', 'AM', 'AMP', 'AN', 'AQ', 'AR', 'ATD', 'PA', 'BI', 'PS', 'SAD'
    ]

# Variáveis de configuração
estados = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", 
           "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", 
           "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
anos = range(1997, 2025)  # Anos desejados

# Inicializa o SIA
sia = SIA().load()

# Caminho do arquivo de progresso
progresso_file = "progress_sia.txt"

# Função para carregar arquivos já baixados
def carregar_arquivos_baixados(progresso_file):
    if os.path.exists(progresso_file):
        with open(progresso_file, 'r') as f:
            return set(line.strip() for line in f)
    return set()

# Função para registrar progresso
def registrar_progresso(progresso_file, arquivo):
    with open(progresso_file, 'a') as f:
        f.write(f"{arquivo}\n")

# Função para mapear .dbc para pasta .parquet
def mapear_dbc_para_parquet(arquivo_dbc):
    """
    Mapeia o nome do arquivo .dbc para o nome da pasta .parquet correspondente.
    Exemplo: 'PASP1801a.dbc' -> 'PASP1801a.parquet'
    """
    base_name = os.path.splitext(arquivo_dbc)[0]
    return f"{base_name}.parquet"

# Função para obter o nome do arquivo a partir do objeto File
def obter_nome_arquivo(file):
    """
    Tenta obter o nome do arquivo a partir do objeto File.
    Prioriza 'filename', mas tenta 'name' e 'path' se necessário.
    """
    if hasattr(file, 'filename'):
        return file.filename
    elif hasattr(file, 'name'):
        return file.name
    elif hasattr(file, 'path'):
        return os.path.basename(file.path)
    else:
        raise AttributeError("O objeto 'File' não possui atributos 'filename', 'name' ou 'path'.")

# Carregar progresso existente
arquivos_baixados = carregar_arquivos_baixados(progresso_file)

for Grupo in Grupos:
    try:
        # Diretório local onde os arquivos serão baixados
        local_dir = os.path.expanduser(os.path.join("./parquet_files/SIA", Grupo))
        os.makedirs(local_dir, exist_ok=True)  # Cria o diretório, se não existir
        print(f"Processando o grupo: {Grupo}")
        logging.info(f"Processando o grupo: {Grupo}")

        # Obtém os arquivos para o grupo atual
        files = sia.get_files([Grupo], uf=estados, year=anos)

        # Filtra arquivos já baixados com base no progresso
        arquivos_para_download = []
        for file in files:
            try:
                arquivo_nome = obter_nome_arquivo(file)
                if arquivo_nome not in arquivos_baixados:
                    arquivos_para_download.append(file)
            except AttributeError as ae:
                logging.error(f"Erro ao obter nome do arquivo: {ae}")
                print(f"Erro ao obter nome do arquivo: {ae}")

        if arquivos_para_download:
            # Faz o download apenas dos arquivos que ainda não foram baixados
            for file in arquivos_para_download:
                try:
                    # Mapeia o arquivo .dbc para a pasta .parquet
                    arquivo_nome = obter_nome_arquivo(file)
                    parquet_folder = mapear_dbc_para_parquet(arquivo_nome)
                    parquet_path = os.path.join(local_dir, parquet_folder)

                    # Verifica se a pasta .parquet já existe
                    if os.path.exists(parquet_path):
                        print(f"Pasta '{parquet_folder}' já existe. Pulando download.")
                        logging.info(f"Pasta '{parquet_folder}' já existe. Pulando download.")
                        registrar_progresso(progresso_file, arquivo_nome)
                        continue  # Pula para o próximo arquivo

                    # Faz o download do arquivo .dbc
                    sia.download([file], local_dir=local_dir)
                    logging.info(f"Arquivo '{arquivo_nome}' baixado com sucesso.")

                    # Registrar progresso
                    registrar_progresso(progresso_file, arquivo_nome)

                    # Verificar se a pasta .parquet foi criada após o download
                    if os.path.exists(parquet_path):
                        print(f"Pasta '{parquet_folder}' criada com sucesso.")
                        logging.info(f"Pasta '{parquet_folder}' criada com sucesso.")
                    else:
                        print(f"Aviso: A pasta '{parquet_folder}' não foi encontrada após o download.")
                        logging.warning(f"Aviso: A pasta '{parquet_folder}' não foi encontrada após o download.")

                except Exception as e:
                    logging.error(f"Erro ao baixar o arquivo '{arquivo_nome}': {e}")
                    print(f"Erro ao baixar o arquivo '{arquivo_nome}': {e}")

            print(f"Arquivos para o grupo '{Grupo}' baixados com sucesso em: {local_dir}")
            logging.info(f"Arquivos para o grupo '{Grupo}' baixados com sucesso em: {local_dir}")
        else:
            print(f"Nenhum arquivo novo para o grupo '{Grupo}'. Todos já foram baixados.")
            logging.info(f"Nenhum arquivo novo para o grupo '{Grupo}'. Todos já foram baixados.")

    except Exception as e:
        logging.error(f"Erro ao processar o grupo '{Grupo}': {e}")
        print(f"Erro ao processar o grupo '{Grupo}': {e}")
