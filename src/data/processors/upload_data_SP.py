import os
import logging
import pandas as pd
import io
import psutil
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from utils import (
    data_utils,
    db_utils,
    log_utils
)

# Configuração do ambiente
load_dotenv()

# Configuração do logger
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logger = log_utils.configurar_logging('upload_data_SP')

# Configuração do banco de dados
DATABASE_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)

BASE_PATH = "parquet_files/SIH/SP"
GRUPOS = ["SP"]
ESTADOS = [
    "PR"
]
ANOS = range(2018, 2025)
TABELA = "sih_servicos_profissionais"

ESTADOS_VALIDOS = {
    "12": "AC", "27": "AL", "16": "AP", "13": "AM", "29": "BA", "23": "CE",
    "53": "DF", "32": "ES", "52": "GO", "21": "MA", "51": "MT", "50": "MS",
    "31": "MG", "15": "PA", "25": "PB", "41": "PR", "26": "PE", "22": "PI",
    "33": "RJ", "24": "RN", "43": "RS", "11": "RO", "14": "RR", "42": "SC",
    "35": "SP", "28": "SE", "17": "TO"
}

# Ordem das colunas conforme a tabela (incluindo 'id_log')
COLUNAS_TABELA = [
    "sp_uf", "sp_procrea", "sp_gestor",
    "sp_aa", "sp_mm", "sp_cnes", "sp_naih", "sp_dtinter",
    "sp_dtsaida", "sp_num_pr", "sp_tipo", "sp_cpfcgc", "sp_atoprof",
    "sp_tp_ato", "sp_qtd_ato", "sp_ptsp", "sp_nf", "sp_valato",
    "sp_m_hosp", "sp_m_pac", "sp_des_hos", "sp_des_pac", "sp_complex",
    "sp_financ", "sp_co_faec", "sp_pf_cbo", "sp_pf_doc", "sp_pj_doc",
    "in_tp_val", "sequencia", "remessa", "serv_cla", "sp_cidpri",
    "sp_cidsec", "sp_qt_proc", "sp_u_aih", "id_log"
]

def monitorar_memoria():
    """
    Monitora o uso de memória e loga o estado atual.
    """
    mem = psutil.virtual_memory()
    logger.info(f"Memória disponível: {mem.available / (1024 ** 2):.2f} MB, Usada: {mem.percent}%")

def verificar_ultimo_arquivo_procesado():
    """
    Verifica no banco de dados qual foi o último arquivo e registro processado com base em 'id_log'.
    Retorna uma lista de arquivos processados.
    """
    try:
        with engine.connect() as connection:
            query = f"""
            SELECT DISTINCT substring(id_log FROM '^(.*)_\\d+$') AS arquivo_processado
            FROM {TABELA}
            """
            resultado = connection.execute(query).fetchall()
            arquivos_processados = [row[0] for row in resultado]
            logger.info(f"Arquivos já processados: {len(arquivos_processados)} encontrados.")
            return set(arquivos_processados)
    except Exception as e:
        logger.error(f"Erro ao verificar arquivos processados: {e}")
        return set()

def obter_pastas_de_arquivos():
    """
    Retorna a lista de pastas de arquivos válidas encontradas no diretório base.
    Cada pasta representa um conjunto de arquivos .parquet.
    """
    pastas_de_arquivos = []
    for grupo in GRUPOS:
        for estado in ESTADOS:
            for ano in ANOS:
                for mes in range(1, 13):
                    nome_pasta = f"{grupo}{estado}{str(ano)[-2:]}{str(mes).zfill(2)}.parquet"
                    ## logger.info(f"Caminho analisado: {nome_pasta}")
                    caminho_pasta = os.path.join(BASE_PATH, nome_pasta)
                    if os.path.isdir(caminho_pasta):
                        pastas_de_arquivos.append(caminho_pasta)
    logger.info(f"Pastas de arquivos encontradas: {len(pastas_de_arquivos)}")
    return pastas_de_arquivos

def obter_arquivos_parquet(pasta_de_arquivos):
    """
    Retorna a lista de arquivos .parquet válidos encontrados dentro de uma pasta específica.
    """
    arquivos_parquet = []
    for arquivo in os.listdir(pasta_de_arquivos):
        if arquivo.endswith(".parquet"):
            caminho_arquivo = os.path.join(pasta_de_arquivos, arquivo)
            if os.path.isfile(caminho_arquivo):
                arquivos_parquet.append(caminho_arquivo)
    logger.info(f"Arquivos .parquet encontrados na pasta {pasta_de_arquivos}: {len(arquivos_parquet)}")
    return arquivos_parquet

def normalizar_colunas(df):
    """
    Normaliza os nomes das colunas para corresponder ao esquema do banco e
    converte os códigos de estado numéricos para suas abreviações.
    Adiciona colunas ausentes como valores nulos, exceto 'id_log'.
    """
    colunas_corretas = COLUNAS_TABELA.copy()
    colunas_corretas.remove('id_log')  # Remove 'id_log' da lista de colunas a serem verificadas
    
    colunas_mapeadas = {col.lower(): col for col in colunas_corretas}
    df.columns = [colunas_mapeadas.get(col.lower(), col) for col in df.columns]

    # Adicionar colunas ausentes, exceto 'id_log'
    for coluna in colunas_corretas:
        if coluna not in df.columns:
            logger.warning(f"Adicionando coluna ausente: {coluna}")
            df[coluna] = None  # Ou algum valor padrão, se aplicável

    # Converter 'SP_UF' de código numérico para abreviação
    if 'sp_uf' in df.columns:
        df['sp_uf'] = df['sp_uf'].astype(str).str.zfill(2)  # Garantir que tenha 2 dígitos
        df['sp_uf'] = df['sp_uf'].map(ESTADOS_VALIDOS)
        # Verificar se houve mapeamento falho
        if df['sp_uf'].isnull().any():
            estados_invalidos = df[df['sp_uf'].isnull()]['sp_uf'].unique()
            logger.error(f"Estados inválidos encontrados: {estados_invalidos}")
            df = df.dropna(subset=['sp_uf'])  # Opcional: remover linhas com estados inválidos
    else:
        logger.error("A coluna 'sp_uf' não está presente no DataFrame.")

    return df

def ajustar_ordem_colunas(df):
    """
    Ajusta a ordem das colunas do DataFrame para o esquema do banco, ignorando 'id'.
    """
    colunas_ordenadas = COLUNAS_TABELA.copy()

    colunas_faltantes = set(colunas_ordenadas) - set(df.columns)
    if colunas_faltantes:
        # Adicionar colunas ausentes com valores padrão
        for coluna in colunas_faltantes:
            logger.warning(f"Adicionando coluna ausente: {coluna}")
            df[coluna] = None
    return df[colunas_ordenadas]

def carregar_dados_em_lotes(pastas_de_arquivos, tamanho_lote=10000):
    """
    Carrega os arquivos .parquet em lotes pequenos e gera a coluna id_log.
    """
    arquivos_processados = verificar_ultimo_arquivo_procesado()
    
    for pasta in pastas_de_arquivos:
        arquivos = obter_arquivos_parquet(pasta)
        for arquivo in arquivos:
            nome_pasta = os.path.basename(pasta)
            nome_arquivo = os.path.basename(arquivo)
            id_arquivo = f"{nome_pasta}_{nome_arquivo}"
            
            if id_arquivo in arquivos_processados:
                logger.info(f"PULANDO arquivo já processado: {id_arquivo}")
                continue
            
            try:
                logger.info(f"Carregando arquivo: {arquivo}")
                monitorar_memoria()
                df = pd.read_parquet(arquivo)
                df = normalizar_colunas(df)
                
                # Adicionar a coluna 'id_log' com base no nome da pasta e arquivo e índice
                df['id_log'] = [f"{id_arquivo}_{i}" for i in range(len(df))]
                
                for inicio in range(0, len(df), tamanho_lote):
                    yield df.iloc[inicio:inicio + tamanho_lote]
            except Exception as e:
                logger.error(f"Erro ao carregar arquivo {arquivo}: {e}")

def inserir_dados_em_lotes(df_lote):
    """
    Insere os dados no banco por lotes.
    """
    try:
        csv_buffer = io.StringIO()
        df_lote.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)
        with engine.connect() as connection:
            with connection.connection.cursor() as cursor:
                cursor.copy_expert(f"COPY {TABELA} ({', '.join(COLUNAS_TABELA)}) FROM STDIN WITH CSV", csv_buffer)
            connection.connection.commit()
        logger.info(f"Lote de {len(df_lote)} registros inserido com sucesso.")
    except Exception as e:
        logger.critical(f"Erro ao inserir dados: {e}", exc_info=True)

def processar_dados():
    """
    Fluxo principal do script.
    """
    try:
        pastas_de_arquivos = obter_pastas_de_arquivos()
        if not pastas_de_arquivos:
            logger.warning("Nenhuma pasta de arquivos .parquet encontrada para processamento.")
            return
        for df_lote in carregar_dados_em_lotes(pastas_de_arquivos):
            df_lote = ajustar_ordem_colunas(df_lote)
            inserir_dados_em_lotes(df_lote)
        logger.info("Processo concluído.")
    except Exception as e:
        logger.critical(f"Erro crítico no processamento: {e}", exc_info=True)

if __name__ == "__main__":
    processar_dados()
