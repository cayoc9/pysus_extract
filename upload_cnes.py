import os
import re
import logging
import pandas as pd
import io
import psutil
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Configuração do ambiente
load_dotenv()

# Configuração do logger
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler(os.path.join(LOG_DIR, f"upload_CNES_{os.getpid()}.log"))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Configuração do banco de dados
DATABASE_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)

BASE_PATH = "parquet_files/CNES/"

# GRUPOS_INFO fornecido
GRUPOS_INFO = {
    "HB": {
        "tabela": "cnes_habilitacao",
        "colunas": {
            "cnes": "TEXT",
            "codufmun": "INTEGER",
            "regsaude": "TEXT",
            "micr_reg": "TEXT",
            "distrsan": "TEXT",
            "distradm": "TEXT",
            "tpgestao": "CHAR(1)",
            "pf_pj": "SMALLINT",
            "cpf_cnpj": "TEXT",
            "niv_dep": "SMALLINT",
            "cnpj_man": "TEXT",
            "esfera_a": "TEXT",
            "retencao": "CHAR(2)",
            "atividad": "TEXT",
            "natureza": "TEXT",
            "clientel": "TEXT",
            "tp_unid": "TEXT",
            "turno_at": "TEXT",
            "niv_hier": "TEXT",
            "terceiro": "TEXT",
            "cod_cep": "TEXT",
            "vinc_sus": "BOOLEAN",
            "tp_prest": "SMALLINT",
            "sgruphab": "TEXT",
            "cmpt_ini": "INTEGER",
            "cmpt_fim": "INTEGER",
            "dtportar": "TEXT",
            "portaria": "TEXT",
            "maportar": "INTEGER",
            "nuleitos": "SMALLINT",
            "competen": "INTEGER",
            "nat_jur": "SMALLINT",
            "id_log": "VARCHAR(255)",
            "uf": "CHAR(2)"
        }
    },
    "EQ": {
        "tabela": "cnes_equipamentos",
        "colunas": {
            "cnes": "TEXT",
            "codufmun": "INTEGER",
            "regsaude": "TEXT",
            "micr_reg": "TEXT",
            "distrsan": "TEXT",
            "distradm": "TEXT",
            "tpgestao": "CHAR(1)",
            "pf_pj": "SMALLINT",
            "cpf_cnpj": "TEXT",
            "niv_dep": "SMALLINT",
            "cnpj_man": "TEXT",
            "esfera_a": "TEXT",
            "atividad": "TEXT",
            "retencao": "CHAR(2)",
            "natureza": "TEXT",
            "clientel": "TEXT",
            "tp_unid": "TEXT",
            "turno_at": "TEXT",
            "niv_hier": "TEXT",
            "terceiro": "TEXT",
            "tipequip": "SMALLINT",
            "codequip": "TEXT",
            "qt_exist": "SMALLINT",
            "qt_uso": "SMALLINT",
            "ind_sus": "BOOLEAN",
            "ind_nsus": "BOOLEAN",
            "competen": "INTEGER",
            "nat_jur": "SMALLINT",
            "id_log": "VARCHAR(255)",
            "uf": "CHAR(2)"
        }
    },
    # ... Demais grupos omitidos para brevidade, utilizar exatamente o dicionário fornecido ...
    "DC": {
        "tabela": "cnes_dados_complementares",
        "colunas": {
            "cnes": "TEXT",
            "codufmun": "INTEGER",
            "cpf_cnpj": "TEXT",
            "pf_pj": "TEXT",
            "niv_dep": "TEXT",
            "cnpj_man": "TEXT",
            "cod_ir": "TEXT",
            "regsaude": "TEXT",
            "micr_reg": "TEXT",
            "distrsan": "TEXT",
            "vinc_sus": "BOOLEAN",
            "distradm": "TEXT",
            "tpgestao": "CHAR(1)",
            "esfera_a": "TEXT",
            "retencao": "TEXT",
            "atividad": "TEXT",
            "natureza": "TEXT",
            "clientel": "TEXT",
            "tp_unid": "TEXT",
            "turno_at": "TEXT",
            "niv_hier": "TEXT",
            "tp_prest": "CHAR(2)",
            "s_hbsagp": "SMALLINT",
            "s_hbsagn": "SMALLINT",
            "s_dpi": "SMALLINT",
            "s_dpac": "SMALLINT",
            "s_reagp": "SMALLINT",
            "s_reagn": "SMALLINT",
            "s_rehcv": "SMALLINT",
            "maq_prop": "SMALLINT",
            "maq_outr": "SMALLINT",
            "f_areia": "TEXT",
            "f_carvao": "TEXT",
            "abrandad": "TEXT",
            "deioniza": "TEXT",
            "osmose_r": "TEXT",
            "out_trat": "TEXT",
            "cns_nefr": "TEXT",
            "dialise": "TEXT",
            "simul_rd": "SMALLINT",
            "planj_rd": "SMALLINT",
            "armaz_ft": "SMALLINT",
            "conf_mas": "SMALLINT",
            "sala_mol": "SMALLINT",
            "blocoper": "SMALLINT",
            "s_armaze": "SMALLINT",
            "s_prepar": "SMALLINT",
            "s_qcdura": "SMALLINT",
            "s_qldura": "SMALLINT",
            "s_cpflux": "SMALLINT",
            "s_simula": "SMALLINT",
            "s_acell6": "SMALLINT",
            "s_alseme": "SMALLINT",
            "s_alcome": "SMALLINT",
            "ortv1050": "SMALLINT",
            "orv50150": "BOOLEAN",
            "ov150500": "SMALLINT",
            "un_cobal": "SMALLINT",
            "eqbrbaix": "SMALLINT",
            "eqbrmedi": "SMALLINT",
            "eqbralta": "SMALLINT",
            "eq_marea": "SMALLINT",
            "eq_mindi": "SMALLINT",
            "eqsispln": "SMALLINT",
            "eqdoscli": "SMALLINT",
            "eqfonsel": "SMALLINT",
            "cns_adm": "TEXT",
            "cns_oped": "TEXT",
            "cns_conc": "TEXT",
            "cns_oclin": "TEXT",
            "cns_mrad": "TEXT",
            "cns_fnuc": "TEXT",
            "quimradi": "TEXT",
            "s_recepc": "SMALLINT",
            "s_trihmt": "SMALLINT",
            "s_tricli": "SMALLINT",
            "s_coleta": "SMALLINT",
            "s_aferes": "SMALLINT",
            "s_preest": "SMALLINT",
            "s_proces": "SMALLINT",
            "s_estoqu": "SMALLINT",
            "s_distri": "SMALLINT",
            "s_sorolo": "SMALLINT",
            "s_imunoh": "SMALLINT",
            "s_pretra": "SMALLINT",
            "s_hemost": "SMALLINT",
            "s_contrq": "SMALLINT",
            "s_biomol": "SMALLINT",
            "s_imunfe": "SMALLINT",
            "s_transf": "SMALLINT",
            "s_sgdoad": "SMALLINT",
            "qt_cadre": "SMALLINT",
            "qt_cenre": "SMALLINT",
            "qt_refsa": "SMALLINT",
            "qt_conra": "SMALLINT",
            "qt_extpl": "SMALLINT",
            "qt_fre18": "SMALLINT",
            "qt_fre30": "SMALLINT",
            "qt_agipl": "SMALLINT",
            "qt_selad": "SMALLINT",
            "qt_irrhe": "SMALLINT",
            "qt_agltn": "SMALLINT",
            "qt_maqaf": "SMALLINT",
            "qt_refre": "SMALLINT",
            "qt_refas": "SMALLINT",
            "qt_capfl": "SMALLINT",
            "cns_hmtr": "TEXT",
            "cns_hmtl": "TEXT",
            "cns_cres": "TEXT",
            "cns_rtec": "TEXT",
            "hemotera": "TEXT",
            "ap01cv01": "TEXT",
            "ap01cv02": "TEXT",
            "ap01cv05": "CHAR(1)",
            "ap01cv06": "CHAR(1)",
            "ap01cv03": "TEXT",
            "ap01cv04": "TEXT",
            "ap02cv01": "TEXT",
            "ap02cv02": "TEXT",
            "ap02cv05": "CHAR(1)",
            "ap02cv06": "CHAR(1)",
            "ap02cv03": "TEXT",
            "ap02cv04": "TEXT",
            "ap03cv01": "TEXT",
            "ap03cv02": "TEXT",
            "ap03cv05": "CHAR(1)",
            "ap03cv06": "CHAR(1)",
            "ap03cv03": "TEXT",
            "ap03cv04": "TEXT",
            "ap04cv01": "TEXT",
            "ap04cv02": "TEXT",
            "ap04cv05": "CHAR(1)",
            "ap04cv06": "CHAR(1)",
            "ap04cv03": "TEXT",
            "ap04cv04": "TEXT",
            "ap05cv01": "TEXT",
            "ap05cv02": "TEXT",
            "ap05cv05": "CHAR(1)",
            "ap05cv06": "CHAR(1)",
            "ap05cv03": "TEXT",
            "ap05cv04": "TEXT",
            "ap06cv01": "TEXT",
            "ap06cv02": "TEXT",
            "ap06cv05": "CHAR(1)",
            "ap06cv06": "CHAR(1)",
            "ap06cv03": "TEXT",
            "ap06cv04": "TEXT",
            "ap07cv01": "TEXT",
            "ap07cv02": "TEXT",
            "ap07cv05": "CHAR(1)",
            "ap07cv06": "CHAR(1)",
            "ap07cv03": "TEXT",
            "ap07cv04": "TEXT",
            "atend_pr": "TEXT",
            "gesprg3e": "TEXT",
            "gesprg3m": "TEXT",
            "gesprg4e": "TEXT",
            "gesprg4m": "TEXT",
            "gesprg6e": "CHAR(1)",
            "gesprg6m": "CHAR(1)",
            "nivate_a": "CHAR(1)",
            "nivate_h": "CHAR(1)",
            "competen": "TEXT",
            "ap01cv07": "CHAR(1)",
            "ap02cv07": "CHAR(1)",
            "ap03cv07": "CHAR(1)",
            "ap04cv07": "CHAR(1)",
            "ap05cv07": "CHAR(1)",
            "ap06cv07": "CHAR(1)",
            "ap07cv07": "CHAR(1)",
            "nat_jur": "SMALLINT",
            "id_log": "VARCHAR(255)",
            "uf": "CHAR(2)"
        }
    }
}

ESTADOS = [
    "AC",  "AL",  "AP",  "AM",
    "BA",  "CE",  "DF",   "ES",   "GO",
    "MA",  "MT",   "MS",   "MG",
    "PA",  "PB",  "PR",
    "PE",  "PI",  "RJ",   "RN",   "RS",
    "RO",  "RR",  "SC",
    "SP",  "SE",  "TO"
] # Estados que devem ser processados
ANOS = range(1997, 2025)


def monitorar_memoria():
    mem = psutil.virtual_memory()
    logger.info(f"Memória disponível: {mem.available / (1024 ** 2):.2f} MB, Usada: {mem.percent}%")

def verificar_ultimo_arquivo_processado(tabela):
    try:
        with engine.connect() as connection:
            query = text(f"""
                SELECT DISTINCT substring(id_log FROM '^(.*)_\\d+$') AS arquivo_processado
                FROM {tabela}
            """)
            resultado = connection.execute(query).mappings().all()
            arquivos_processados = [row['arquivo_processado'] for row in resultado]
            logger.info(f"[{tabela}] Arquivos já processados: {len(arquivos_processados)} encontrados.")
            return set(arquivos_processados)
    except Exception as e:
        logger.error(f"[{tabela}] Erro ao verificar arquivos processados: {e}")
        return set()

def obter_pastas_de_arquivos(grupo):
    pastas_de_arquivos = []
    caminho_base_grupo = os.path.join(BASE_PATH, grupo)

    if not os.path.exists(caminho_base_grupo):
        logger.warning(f"O caminho base para o grupo {grupo} não existe: {caminho_base_grupo}")
        return pastas_de_arquivos

    padrao_pasta = re.compile(rf"^{grupo}([A-Z]{{2}}).+\.parquet$", re.IGNORECASE)

    for nome_pasta in os.listdir(caminho_base_grupo):
        caminho_pasta = os.path.join(caminho_base_grupo, nome_pasta)
        if os.path.isdir(caminho_pasta) and padrao_pasta.match(nome_pasta):
            pastas_de_arquivos.append(caminho_pasta)

    logger.info(f"[{grupo}] Pastas de arquivos encontradas: {len(pastas_de_arquivos)}")
    return pastas_de_arquivos

def obter_arquivos_parquet(pasta_de_arquivos):
    arquivos_parquet = []
    for arquivo in os.listdir(pasta_de_arquivos):
        if arquivo.endswith(".parquet"):
            caminho_arquivo = os.path.join(pasta_de_arquivos, arquivo)
            if os.path.isfile(caminho_arquivo):
                arquivos_parquet.append(caminho_arquivo)
    logger.info(f"Arquivos .parquet encontrados na pasta {pasta_de_arquivos}: {len(arquivos_parquet)}")
    return arquivos_parquet

def mapear_tipo_postgres(tipo):
    tipo = tipo.strip().upper()
    mapeamento = {
        'SMALLINT': 'SMALLINT',
        'INTEGER': 'INTEGER',
        'BIGINT': 'BIGINT',
        'NUMERIC': 'NUMERIC',
        'BOOLEAN': 'BOOLEAN',
        'DATE': 'DATE',
        'TIMESTAMP': 'TIMESTAMP',
        'TEXT': 'TEXT',
        'SERIAL': 'SERIAL',
        'VARCHAR': 'VARCHAR',
        'CHAR': 'CHAR'
    }
    if tipo.startswith('NUMERIC'):
        return tipo
    match = re.match(r'(CHAR|VARCHAR)\((\d+)\)', tipo)
    if match:
        return f"{match.group(1)}({match.group(2)})"
    if tipo in mapeamento:
        return mapeamento[tipo]
    else:
        logger.warning(f"Aviso: Tipo de dado desconhecido '{tipo}'. Usando 'TEXT'.")
        return 'TEXT'

def converter_tipos(df, mapeamento_tipos):
    for col, tipo in mapeamento_tipos.items():
        col_lower = col.lower()
        if col_lower not in df.columns:
            continue
        try:
            pg_tipo = mapear_tipo_postgres(tipo)
            if pg_tipo.startswith("VARCHAR") or pg_tipo.startswith("CHAR") or pg_tipo == "TEXT":
                # Aplica strip apenas se o valor for string, preservando NaN
                df[col_lower] = df[col_lower].apply(lambda x: x.strip() if isinstance(x, str) else x)
            elif pg_tipo in ["INTEGER", "BIGINT", "SMALLINT"]:
                # Conversão numérica com preservação de NaN
                if pg_tipo == "SMALLINT":
                    df[col_lower] = pd.to_numeric(df[col_lower], errors='coerce').astype('Int16')
                elif pg_tipo == "INTEGER":
                    df[col_lower] = pd.to_numeric(df[col_lower], errors='coerce').astype('Int32')
                elif pg_tipo == "BIGINT":
                    df[col_lower] = pd.to_numeric(df[col_lower], errors='coerce').astype('Int64')
            elif pg_tipo.startswith("NUMERIC"):
                df[col_lower] = pd.to_numeric(df[col_lower], errors='coerce')
            elif pg_tipo == "DATE":
                df[col_lower] = pd.to_datetime(df[col_lower], errors='coerce').dt.date
            elif pg_tipo == "BOOLEAN":
                df[col_lower] = df[col_lower].map({
                    True: True, False: False, 
                    'True': True, 'False': False, 
                    '1': True, '0': False
                })
        except Exception as e:
            raise TypeError(f"Erro ao converter coluna '{col_lower}' para o tipo '{tipo}': {e}")
    return df

def normalizar_colunas(df, colunas_esperadas, mapeamento_tipos):
    # Remover colunas id, uf, id_log se existirem
    df = df.drop(columns=['id', 'uf', 'id_log'], errors='ignore')

    # Normalizar nomes das colunas do DF
    df.columns = [c.lower() for c in df.columns]

    # Adicionar colunas ausentes
    colunas_para_normalizar = [col for col in colunas_esperadas if col not in ('uf', 'id_log')]
    for coluna in colunas_para_normalizar:
        if coluna.lower() not in df.columns:
            logger.warning(f"Adicionando coluna ausente: {coluna.lower()}")
            df[coluna.lower()] = None

    return df

def ajustar_ordem_colunas(df, colunas_esperadas):
    colunas_ordenadas = [col.lower() for col in colunas_esperadas]
    return df[colunas_ordenadas]

def extrair_uf(nome, grupo):
    padrao = rf"^{grupo}(?P<uf>[A-Z]{{2}})"
    match = re.match(padrao, nome, re.IGNORECASE)
    if match:
        return match.group('uf').upper()
    else:
        return None

def carregar_dados_em_lotes(grupo, pastas_de_arquivos, tamanho_lote=10000):
    tabela = GRUPOS_INFO[grupo]["tabela"]
    arquivos_processados = verificar_ultimo_arquivo_processado(tabela)
    colunas_esperadas = list(GRUPOS_INFO[grupo]["colunas"].keys())
    mapeamento_tipos = GRUPOS_INFO[grupo]["colunas"]

    colunas_para_insercao = colunas_esperadas  # já inclui uf e id_log

    for pasta in pastas_de_arquivos:
        arquivos = obter_arquivos_parquet(pasta)
        nome_pasta = os.path.basename(pasta)
        uf_pasta = extrair_uf(nome_pasta, grupo)

        for arquivo in arquivos:
            nome_arquivo = os.path.basename(arquivo)
            id_arquivo = f"{nome_pasta}_{nome_arquivo}"

            if id_arquivo in arquivos_processados:
                logger.info(f"[{grupo}] PULANDO arquivo já processado: {id_arquivo}")
                continue

            try:
                logger.info(f"[{grupo}] Carregando arquivo: {arquivo}")
                monitorar_memoria()
                df = pd.read_parquet(arquivo)
                df = normalizar_colunas(df, colunas_esperadas, mapeamento_tipos)
                df = converter_tipos(df, mapeamento_tipos)

                # Extrair UF do nome do arquivo ou da pasta
                uf = extrair_uf(nome_arquivo, grupo) or uf_pasta
                if not uf:
                    logger.warning(f"[{grupo}] Não foi possível extrair UF do arquivo {arquivo}")
                    continue

                df['uf'] = uf.upper()
                df['id_log'] = [f"{id_arquivo}_{i}" for i in range(len(df))]

                # Ajustar a ordem das colunas
                df = ajustar_ordem_colunas(df, colunas_para_insercao)

                for inicio in range(0, len(df), tamanho_lote):
                    yield df.iloc[inicio:inicio + tamanho_lote]
            except Exception as e:
                logger.error(f"[{grupo}] Erro ao carregar arquivo {arquivo}: {e}")

def inserir_dados_em_lotes(tabela, df_lote, colunas_ajustadas):
    try:
        df_lote = df_lote.reindex(columns=[c.lower() for c in colunas_ajustadas])

        logger.debug(f"[{tabela}] Número de registros no lote: {len(df_lote)}")

        if len(df_lote.columns) != len(colunas_ajustadas):
            logger.error(f"[{tabela}] Número de colunas no DataFrame ({len(df_lote.columns)}) "
                         f"não corresponde ao esperado ({len(colunas_ajustadas)})")
            colunas_faltantes = set(colunas_ajustadas) - set(df_lote.columns)
            colunas_extras = set(df_lote.columns) - set(colunas_ajustadas)
            if colunas_faltantes:
                logger.error(f"[{tabela}] Colunas faltantes no DataFrame: {colunas_faltantes}")
            if colunas_extras:
                logger.error(f"[{tabela}] Colunas extras no DataFrame: {colunas_extras}")
            return

        csv_buffer = io.StringIO()
        df_lote.to_csv(csv_buffer, index=False, header=False, na_rep='')  # Adicionado na_rep=''
        csv_buffer.seek(0)
        with engine.connect() as connection:
            with connection.connection.cursor() as cursor:
                cursor.copy_expert(
                    f"COPY {tabela} ({', '.join([col.lower() for col in colunas_ajustadas])}) FROM STDIN WITH CSV",
                    csv_buffer
                )
            connection.connection.commit()
        logger.info(f"[{tabela}] Lote de {len(df_lote)} registros inserido com sucesso.")
    except Exception as e:
        logger.critical(f"[{tabela}] Erro ao inserir dados: {e}", exc_info=True)

def processar_dados():
    try:
        for grupo, info in GRUPOS_INFO.items():
            tabela = info["tabela"]
            colunas_esperadas = list(info["colunas"].keys())
            logger.info(f"[{grupo}] Iniciando processamento para a tabela {tabela}")

            logger.info(f"[{tabela}] Verificando arquivos já processados...")
            arquivos_processados = verificar_ultimo_arquivo_processado(tabela)
            logger.info(f"[{tabela}] Total de arquivos já processados: {len(arquivos_processados)}")

            pastas_de_arquivos = obter_pastas_de_arquivos(grupo)
            if not pastas_de_arquivos:
                logger.warning(f"[{grupo}] Nenhuma pasta de arquivos .parquet encontrada para processamento.")
                continue
            logger.info(f"[{grupo}] Iniciando processamento das pastas de arquivos...")

            for df_lote in carregar_dados_em_lotes(grupo, pastas_de_arquivos):
                inserir_dados_em_lotes(tabela, df_lote, colunas_esperadas)
        logger.info("Processo concluído.")
    except Exception as e:
        logger.critical(f"Erro crítico no processamento: {e}", exc_info=True)

if __name__ == "__main__":
    processar_dados()
