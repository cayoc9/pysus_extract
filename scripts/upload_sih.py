import os
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

file_handler = logging.FileHandler(os.path.join(LOG_DIR, f"upload_sih_{os.getpid()}.log"))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Configuração do banco de dados
DATABASE_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)

BASE_PATH = "parquet_files/SIH/"
GRUPOS_INFO = {
    "RD": {
        "tabela": "sih_aih_reduzida",
        "colunas":  [
            "VAL_SADTSR", "VAL_TRANSP", "VAL_OBSANG", "VAL_PED1AC", "VAL_TOT", 
            "GESTOR_DT", "VAL_UTI", "US_TOT", "INFEHOSP", "DT_INTER", "DT_SAIDA", 
            "UTI_MES_IN", "UTI_MES_AN", "UTI_MES_AL", "UTI_MES_TO", "MES_CMPT", 
            "UTI_INT_IN", "SEQUENCIA", "UTI_INT_AN", "UTI_INT_AL", "UTI_INT_TO", 
            "VAL_SH_FED", "VAL_SP_FED", "VAL_SH_GES", "VAL_SP_GES", "VAL_UCI", "COD_IDADE", 
            "IDADE", "DIAS_PERM", "MORTE", "NACIONAL", "DIAR_ACOM", "QT_DIARIAS", "TOT_PT_SP", 
            "NASC", "HOMONIMO", "NUM_FILHOS", "ANO_CMPT", "VAL_SH", "VAL_SP", "VAL_SADT", "VAL_RN",
            "VAL_ACOMP", "VAL_ORTP", "VAL_SANGUE", "ETNIA", "REMESSA", "AUD_JUST", "SIS_JUST",
            "MARCA_UCI", "DIAGSEC1", "DIAGSEC2", "DIAGSEC3", "DIAGSEC4", "DIAGSEC5", "DIAGSEC6", "DIAGSEC7", 
            "DIAGSEC8", "DIAGSEC9", "TPDISEC1", "TPDISEC2", "TPDISEC3", "TPDISEC4", "TPDISEC5", "TPDISEC6", 
            "TPDISEC7", "TPDISEC8", "TPDISEC9", "id_log", "UF_ZI", "ESPEC", "CGC_HOSP", "N_AIH", "IDENT", 
            "CEP", "MUNIC_RES", "SEXO", "MARCA_UTI", "PROC_SOLIC", "PROC_REA", "DIAG_PRINC", "DIAG_SECUN", 
            "COBRANCA", "NATUREZA", "NAT_JUR", "GESTAO", "RUBRICA", "IND_VDRL", "MUNIC_MOV", "NUM_PROC", 
            "CAR_INT", "CPF_AUT", "INSTRU", "CID_NOTIF", "CONTRACEP1", "CONTRACEP2", "GESTRISCO", "INSC_PN",
            "SEQ_AIH5", "CBOR", "CNAER", "VINCPREV", "GESTOR_COD", "GESTOR_TP", "GESTOR_CPF", "CNES", "CNPJ_MANT",
            "CID_ASSO", "CID_MORTE", "COMPLEX", "FINANC", "FAEC_TP", "REGCT", "RACA_COR"
        ]
    },
    "RJ": {
        "tabela": "sih_aih_rejeitada",
        "colunas": [
            "cnes", "cod_idade", "num_filhos",
            "diar_acom", "n_aih", "gestao", "dias_perm", "qt_diarias", "dt_inter", 
            "gestor_dt", "gestor_tp", "seq_aih5", "gestrisco", 
            "tot_pt_sp", "uf_zi", "us_tot", "uti_int_al", "uti_int_an", 
            "uti_int_in", "uti_int_to", "uti_mes_al", "uti_mes_an", "uti_mes_in", 
            "uti_mes_to", "val_acomp", "val_obsang", "val_ortp", "val_ped1ac", 
            "val_rn", "val_sadt", "val_sadtsr", "val_sangue", "val_sh", "val_sp", 
            "val_tot", "val_transp", "val_uti", "vincprev", "homonimo", 
            "idade", "ident", "ind_vdrl", "infehosp", "dt_saida", "instru", 
            "SEQUENCIA", "mes_cmpt", "morte", "munic_mov", "munic_res", 
            "ano_cmpt", "nasc", "marca_uti", "REMESSA", 
            "id_log", "st_situac", "st_bloq", "st_mot_blo",
            "car_int", "cbor", "cep", "cgc_hosp", "cid_asso", 
            "cid_morte", "cid_notif", "cnaer", "cnpj_mant", 
            "cobranca", "complex", "contracep1", "contracep2", 
            "cpf_aut", "diag_princ", "diag_secun", "espec", 
            "etnia", "faec_tp", "financ", "gestor_cod", 
            "gestor_cpf", "insc_pn", "nacional", "natureza", 
            "nat_jur", "num_proc", "proc_rea",
            "proc_solic", "raca_cor",
            "regct", "rubrica", "sexo"
        ]
    },
    "ER": {
         "tabela": "sih_aih_rejeitada_erro",
         "colunas": [
             "SEQUENCIA", "ANO", "MES", "DT_INTER", 
             "DT_SAIDA", "UF_RES", "CO_ERRO", "id_log", 
             "MUN_MOV", "UF_ZI", "REMESSA", "CNES", "AIH", "MUN_RES"
         ]
    },
    "SP": {
         "tabela": "sih_servicos_profissionais",
         "colunas": [
             "sp_uf", "sp_procrea", "sp_gestor",
             "sp_aa", "sp_mm", "sp_cnes", "sp_naih", "sp_dtinter",
             "sp_dtsaida", "sp_num_pr", "sp_tipo", "sp_cpfcgc", "sp_atoprof",
             "sp_tp_ato", "sp_qtd_ato", "sp_ptsp", "sp_nf", "sp_valato",
             "sp_m_hosp", "sp_m_pac", "sp_des_hos", "sp_des_pac", "sp_complex",
             "sp_financ", "sp_co_faec", "sp_pf_cbo", "sp_pf_doc", "sp_pj_doc",
             "in_tp_val", "sequencia", "remessa", "serv_cla", "sp_cidpri",
             "sp_cidsec", "sp_qt_proc", "sp_u_aih", "id_log"
         ]
     }
}



ESTADOS = ["PR", "SP"]  # Estados que devem ser processados
ANOS = range(2018, 2025)

# Mapeamento dos tipos de dados para cada tabela
tipo_coluna_map = {
    'sih_aih_reduzida': 
    {'uf_zi': 'INTEGER', 'ano_cmpt': 'SMALLINT', 'mes_cmpt': 'SMALLINT', 'espec': 'CHAR(2)', 'cgc_hosp': 'CHAR(14)',
         'n_aih': 'BIGINT', 'ident': 'SMALLINT', 'cep': 'CHAR(8)', 'munic_res': 'INTEGER', 'nasc': 'DATE', 'sexo': 'CHAR(1)', 
         'uti_mes_in': 'SMALLINT', 'uti_mes_an': 'SMALLINT', 'uti_mes_al': 'SMALLINT', 'uti_mes_to': 'SMALLINT', 
         'marca_uti':   'CHAR(2)', 'uti_int_in': 'SMALLINT', 'uti_int_an': 'SMALLINT', 'uti_int_al': 'SMALLINT', 'uti_int_to': 'SMALLINT', 
         'diar_acom': 'SMALLINT', 'qt_diarias': 'SMALLINT', 'proc_solic': 'VARCHAR(20)', 'proc_rea': 'VARCHAR(20)',
         'val_sh': 'NUMERIC(12,2)', 'val_sp': 'NUMERIC(10,2)', 'val_sadt': 'NUMERIC(10,2)', 'val_rn': 'NUMERIC(10,2)', 
         'val_acomp': 'NUMERIC(10,2)', 'val_ortp': 'NUMERIC(10,2)', 'val_sangue': 'NUMERIC(10,2)', 'val_sadtsr': 'NUMERIC(10,2)',
         'val_transp': 'NUMERIC(10,2)', 'val_obsang': 'NUMERIC(10,2)', 'val_ped1ac': 'NUMERIC(10,2)', 'val_tot': 'NUMERIC(14,2)', 
         'val_uti': 'NUMERIC(15,2)', 'us_tot': 'NUMERIC(10,2)', 'dt_inter': 'DATE', 'dt_saida': 'DATE', 'diag_princ': 'VARCHAR(10)',
         'diag_secun': 'VARCHAR(4)', 'cobranca': 'VARCHAR(2)', 'natureza': 'CHAR(2)', 'nat_jur': 'VARCHAR(4)', 'gestao': 'SMALLINT',
         'rubrica': 'CHAR(4)', 'ind_vdrl': 'BOOLEAN', 'munic_mov': 'INTEGER', 'cod_idade': 'SMALLINT', 'idade': 'SMALLINT',
         'dias_perm': 'SMALLINT', 'morte': 'BOOLEAN', 'nacional': 'VARCHAR(3)', 'num_proc': 'VARCHAR(4)', 'car_int': 'CHAR(2)',
         'tot_pt_sp': 'NUMERIC(10,2)', 'cpf_aut': 'CHAR(11)', 'homonimo': 'BOOLEAN', 'num_filhos': 'SMALLINT', 
         'instru': 'SMALLINT', 'cid_notif': 'VARCHAR(4)', 'contracep1': 'CHAR(2)', 'contracep2': 'CHAR(2)',
         'gestrisco': 'BOOLEAN', 'insc_pn': 'CHAR(12)', 'seq_aih5': 'SMALLINT', 'cbor': 'CHAR(6)', 'cnaer': 'CHAR(3)', 'vincprev': 'SMALLINT', 
         'gestor_cod': 'CHAR(5)', 'gestor_tp': 'SMALLINT', 'gestor_cpf': 'CHAR(15)', 'gestor_dt': 'DATE', 'cnes': 'INTEGER', 'cnpj_mant': 'CHAR(14)', 
         'infehosp': 'BOOLEAN', 'cid_asso': 'VARCHAR(4)', 'cid_morte': 'VARCHAR(4)', 'complex': 'CHAR(2)', 'financ': 'CHAR(2)', 'faec_tp': 'CHAR(6)', 
         'regct': 'CHAR(4)', 'raca_cor': 'CHAR(2)', 'etnia': 'CHAR(4)', 'sequencia': 'BIGINT', 'remessa': 'VARCHAR(50)', 'aud_just': 'TEXT', 
         'sis_just': 'TEXT', 'val_sh_fed': 'NUMERIC(12,2)', 'val_sp_fed': 'NUMERIC(12,2)', 'val_sh_ges': 'NUMERIC(12,2)', 'val_sp_ges': 'NUMERIC(12,2)', 
         'val_uci': 'NUMERIC(15,2)', 'marca_uci': 'CHAR(2)', 'diagsec1': 'VARCHAR(4)', 'diagsec2': 'VARCHAR(4)', 'diagsec3': 'VARCHAR(4)', 
         'diagsec4': 'VARCHAR(4)', 'diagsec5': 'VARCHAR(4)', 'diagsec6': 'VARCHAR(4)', 'diagsec7': 'VARCHAR(4)', 'diagsec8': 'VARCHAR(4)',
         'diagsec9': 'VARCHAR(4)', 'tpdisec1': 'SMALLINT', 'tpdisec2': 'SMALLINT', 'tpdisec3': 'SMALLINT', 'tpdisec4': 'SMALLINT',
         'tpdisec5': 'SMALLINT', 'tpdisec6': 'SMALLINT', 'tpdisec7': 'SMALLINT', 'tpdisec8': 'SMALLINT', 'tpdisec9': 'SMALLINT'}, 
    'sih_aih_rejeitada': 
    {'cnes':  "VARCHAR(7)", 
      'cod_idade':  "SMALLINT", 
      'num_filhos':  "SMALLINT", 
      'diar_acom':  "SMALLINT", 
      'n_aih':  "BIGINT", 
      'gestao':  "SMALLINT", 
      'dias_perm':  "SMALLINT", 
      'qt_diarias':  "SMALLINT", 
      'dt_inter':  "DATE", 
      'gestor_dt':  "DATE", 
      'gestor_tp':  "SMALLINT", 
      'seq_aih5':  "VARCHAR(3)", 
      'gestrisco':  "SMALLINT", 
      'tot_pt_sp':  "SMALLINT", 
      'uf_zi':  "VARCHAR(6)", 
      'us_tot':  "NUMERIC(12,2)", 
      'uti_int_al':  "SMALLINT", 
      'uti_int_an':  "SMALLINT", 
      'uti_int_in':  "SMALLINT", 
      'uti_int_to':  "SMALLINT", 
      'uti_mes_al':  "SMALLINT", 
      'uti_mes_an':  "SMALLINT", 
      'uti_mes_in':  "SMALLINT", 
      'uti_mes_to':  "SMALLINT", 
      'val_acomp':  "NUMERIC(12,2)", 
      'val_obsang':  "NUMERIC(12,2)", 
      'val_ortp':  "NUMERIC(12,2)", 
      'val_ped1ac':  "NUMERIC(12,2)", 
      'val_rn':  "NUMERIC(12,2)", 
      'val_sadt':  "NUMERIC(12,2)", 
      'val_sadtsr':  "NUMERIC(12,2)", 
      'val_sangue':  "NUMERIC(12,2)", 
      'val_sh':  "NUMERIC(12,2)", 
      'val_sp':  "NUMERIC(12,2)", 
      'val_tot':  "NUMERIC(15,2)", 
      'val_transp':  "NUMERIC(12,2)", 
      'val_uti':  "NUMERIC(12,2)", 
      'vincprev':  "SMALLINT", 
      'homonimo':  "SMALLINT", 
      'idade':  "INTEGER", 
      'ident':  "SMALLINT", 
      'ind_vdrl':  "SMALLINT", 
      'infehosp':  "VARCHAR(1)", 
      'dt_saida':  "DATE", 
      'instru':  "SMALLINT", 
      'sequencia':  "INTEGER", 
      'mes_cmpt':  "SMALLINT", 
      'morte':  "SMALLINT", 
      'munic_mov':  "VARCHAR(6)", 
      'munic_res':  "VARCHAR(6)", 
      'ano_cmpt':  "INTEGER", 
      'nasc':  "DATE", 
      'marca_uti':  "SMALLINT", 
      'remessa':  "VARCHAR(20)",
      'id_log': "VARCHAR(255)", 
      'st_situac':  "SMALLINT", 
      'st_bloq':  "SMALLINT", 
      'st_mot_blo':  "VARCHAR(2)", 
      'car_int':  "VARCHAR(2)", 
      'cbor':  "VARCHAR(6)", 
      'cep':  "VARCHAR(8)", 
      'cgc_hosp':  "VARCHAR(14)", 
      'cid_asso':  "VARCHAR(4)", 
      'cid_morte':  "VARCHAR(4)", 
      'cid_notif':  "VARCHAR(4)", 
      'cnaer':  "VARCHAR(3)", 
      'cnpj_mant':  "VARCHAR(14)", 
      'cobranca':  "SMALLINT", 
      'complex':  "VARCHAR(2)", 
      'contracep1':  "VARCHAR(2)", 
      'contracep2':  "VARCHAR(2)", 
      'cpf_aut':  "VARCHAR(11)", 
      'diag_princ':  "VARCHAR(4)", 
      'diag_secun':  "VARCHAR(4)", 
      'espec':  "SMALLINT", 
      'etnia':  "VARCHAR(4)", 
      'faec_tp':  "VARCHAR(6)", 
      'financ':  "VARCHAR(2)", 
      'gestor_cod':  "VARCHAR(5)", 
      'gestor_cpf':  "VARCHAR(15)", 
      'insc_pn':  "VARCHAR(12)", 
      'nacional':  "VARCHAR(3)", 
      'natureza':  "VARCHAR(2)", 
      'nat_jur':  "VARCHAR(4)", 
      'num_proc':  "VARCHAR(4)", 
      'proc_rea':  "VARCHAR(10)", 
      'proc_solic':  "VARCHAR(10)", 
      'raca_cor':  "VARCHAR(2)", 
      'regct':  "VARCHAR(4)", 
      'rubrica':  "SMALLINT", 
      'sexo':  "SMALLINT"
      },
    'sih_aih_rejeitada_erro': 
    {'sequencia': 'INTEGER', 'remessa': 'VARCHAR(50)', 'cnes': 'INTEGER', 'aih': 'BIGINT', 'ano': 'SMALLINT', 'mes': 'SMALLINT', 
     'dt_inter': 'DATE', 'dt_saida': 'DATE', 'mun_mov': 'INTEGER', 'uf_zi': 'INTEGER', 'mun_res': 'INTEGER', 'uf_res': 'CHAR(2)',
       'co_erro': 'VARCHAR(10)'},
}





def monitorar_memoria():
    """
    Monitora o uso de memória e loga o estado atual.
    """
    mem = psutil.virtual_memory()
    logger.info(f"Memória disponível: {mem.available / (1024 ** 2):.2f} MB, Usada: {mem.percent}%")

def verificar_ultimo_arquivo_processado(tabela):
    """
    Verifica no banco de dados qual foi o último arquivo e registro processado com base em 'id_log'.
    Retorna um conjunto de arquivos processados para a tabela específica.
    """
    try:
        with engine.connect() as connection:
            query = text(f"""
                SELECT DISTINCT substring(id_log FROM '^(.*)_\\d+$') AS arquivo_processado
                FROM {tabela}
            """)
            resultado = connection.execute(query).fetchall()
            arquivos_processados = [row['arquivo_processado'] for row in resultado]
            logger.info(f"[{tabela}] Arquivos já processados: {len(arquivos_processados)} encontrados.")
            return set(arquivos_processados)
    except Exception as e:
        logger.error(f"[{tabela}] Erro ao verificar arquivos processados: {e}")
        return set()

def obter_pastas_de_arquivos(grupo):
    """
    Retorna a lista de pastas de arquivos válidas encontradas no diretório base para um grupo específico.
    """
    pastas_de_arquivos = []
    for estado in ESTADOS:
        for ano in ANOS:
            for mes in range(1, 13):
                nome_pasta = f"{grupo}{estado}{str(ano)[-2:]}{str(mes).zfill(2)}.parquet"
                caminho_pasta = os.path.join(BASE_PATH, grupo, nome_pasta)
                if os.path.isdir(caminho_pasta):
                    pastas_de_arquivos.append(caminho_pasta)
    logger.info(f"[{grupo}] Pastas de arquivos encontradas: {len(pastas_de_arquivos)}")
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

def converter_tipos(df, mapeamento_tipos):
    """
    Converte os tipos de dados das colunas do DataFrame com base no mapeamento fornecido.
    
    Args:
        df (pd.DataFrame): DataFrame a ser convertido.
        mapeamento_tipos (dict): Dicionário mapeando colunas para tipos de dados.
    
    Returns:
        pd.DataFrame: DataFrame com tipos de dados convertidos.
    
    Raises:
        TypeError: Se ocorrer erro durante a conversão.
    """
    for col, tipo in mapeamento_tipos.items():
        if col not in df.columns:
            continue  # Ignorar colunas ausentes já verificadas
        try:
            if tipo.startswith("VARCHAR") or tipo.startswith("CHAR"):
                # Remover espaços em branco
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].astype(str)
            elif tipo in ["INTEGER", "BIGINT", "SMALLINT"]:
                if tipo == "SMALLINT":
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int16')
                elif tipo == "INTEGER":
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int32')
                elif tipo == "BIGINT":
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif tipo.startswith("NUMERIC"):
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif tipo == "DATE":
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            elif tipo == "BOOLEAN":
                # Garantir que valores válidos sejam mapeados corretamente
                df[col] = df[col].map({True: True, False: False, 'True': True, 'False': False, '1': True, '0': False})
            elif tipo == "TEXT":
                df[col] = df[col].astype(str).str.strip()
            else:
                logger.warning(f"Tipo de dado não mapeado para a coluna '{col}': '{tipo}'. Mantendo o tipo original.")
        except Exception as e:
            raise TypeError(f"Erro ao converter coluna '{col}' para o tipo '{tipo}': {e}")
    return df

def normalizar_colunas(df, colunas_esperadas, mapeamento_tipos):
    """
    Normaliza os nomes das colunas para corresponder ao esquema do banco,
    remove espaços em branco das colunas de texto e adiciona colunas ausentes como valores nulos.
    
    Args:
        df (pd.DataFrame): DataFrame a ser normalizado.
        colunas_esperadas (list): Lista de colunas esperadas conforme a tabela.
        mapeamento_tipos (dict): Dicionário mapeando colunas para tipos de dados.
    
    Returns:
        pd.DataFrame: DataFrame com colunas normalizadas e adicionadas.
    """
    # Convertendo colunas esperadas para minúsculas
    colunas_corretas = [col.lower() for col in colunas_esperadas]
    colunas_mapeadas = {col.lower(): col.lower() for col in colunas_corretas}
    
    # Convertendo as colunas do DataFrame para minúsculas
    df.columns = [colunas_mapeadas.get(col.lower(), col.lower()) for col in df.columns]
    
    # Remover espaços em branco para colunas de texto
    for col in df.columns:
        tipo = mapeamento_tipos.get(col, '').upper()
        if tipo.startswith("VARCHAR") or tipo.startswith("CHAR") or tipo == "TEXT":
            df[col] = df[col].astype(str).str.strip()
    
    # Adicionar colunas ausentes
    for coluna in colunas_corretas:
        if coluna not in df.columns:
            logger.warning(f"Adicionando coluna ausente: {coluna}")
            df[coluna] = None  # Ou algum valor padrão, se aplicável
    
    return df

def ajustar_ordem_colunas(df, colunas_esperadas):
    """
    Ajusta a ordem das colunas do DataFrame para o esquema do banco, ignorando 'id_log'.
    
    Args:
        df (pd.DataFrame): DataFrame a ser ajustado.
        colunas_esperadas (list): Lista de colunas esperadas conforme a tabela.
    
    Returns:
        pd.DataFrame: DataFrame com colunas na ordem correta.
    """
    colunas_ordenadas = [col.lower() for col in colunas_esperadas]
    return df[colunas_ordenadas]

def carregar_dados_em_lotes(grupo, pastas_de_arquivos, tamanho_lote=10000):
    """
    Carrega os arquivos .parquet em lotes pequenos e gera a coluna id_log.
    
    Args:
        grupo (str): Nome do grupo (e.g., "RD", "RJ", "ER").
        pastas_de_arquivos (list): Lista de caminhos para as pastas de arquivos.
        tamanho_lote (int): Número de registros por lote.
    
    Yields:
        pd.DataFrame: Lote de dados a ser inserido no banco.
    """
    tabela = GRUPOS_INFO[grupo]["tabela"]
    arquivos_processados = verificar_ultimo_arquivo_processado(tabela)
    colunas_esperadas = GRUPOS_INFO[grupo]["colunas"]
    mapeamento_tipos = tipo_coluna_map.get(tabela, {})

    for pasta in pastas_de_arquivos:
        arquivos = obter_arquivos_parquet(pasta)
        for arquivo in arquivos:
            nome_pasta = os.path.basename(pasta)
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
                
                # Adicionar a coluna 'id_log' com base no nome da pasta e arquivo e índice
                df['id_log'] = [f"{id_arquivo}_{i}" for i in range(len(df))]
                
                # Ajustar a ordem das colunas
                df = ajustar_ordem_colunas(df, colunas_esperadas)
                
                for inicio in range(0, len(df), tamanho_lote):
                    yield df.iloc[inicio:inicio + tamanho_lote]
            except Exception as e:
                logger.error(f"[{grupo}] Erro ao carregar arquivo {arquivo}: {e}")

def inserir_dados_em_lotes(tabela, df_lote, colunas_esperadas):
    """
    Insere os dados no banco por lotes.
    
    Args:
        tabela (str): Nome da tabela no banco de dados.
        df_lote (pd.DataFrame): Lote de dados a ser inserido.
        colunas_esperadas (list): Lista de colunas esperadas conforme a tabela.
    """
    try:
        csv_buffer = io.StringIO()
        df_lote.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)
        with engine.connect() as connection:
            with connection.connection.cursor() as cursor:
                cursor.copy_expert(
                    f"COPY {tabela} ({', '.join([col.lower() for col in colunas_esperadas])}) FROM STDIN WITH CSV",
                    csv_buffer
                )
            connection.connection.commit()
        logger.info(f"[{tabela}] Lote de {len(df_lote)} registros inserido com sucesso.")
    except Exception as e:
        logger.critical(f"[{tabela}] Erro ao inserir dados: {e}", exc_info=True)

def processar_dados():
    """
    Fluxo principal do script.
    """
    try:
        for grupo, info in GRUPOS_INFO.items():
            tabela = info["tabela"]
            colunas_esperadas = info["colunas"]
            pastas_de_arquivos = obter_pastas_de_arquivos(grupo)
            if not pastas_de_arquivos:
                logger.warning(f"[{grupo}] Nenhuma pasta de arquivos .parquet encontrada para processamento.")
                continue
            for df_lote in carregar_dados_em_lotes(grupo, pastas_de_arquivos):
                inserir_dados_em_lotes(tabela, df_lote, colunas_esperadas)
        logger.info("Processo concluído.")
    except Exception as e:
        logger.critical(f"Erro crítico no processamento: {e}", exc_info=True)

if __name__ == "__main__":
    processar_dados()
