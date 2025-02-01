import logging
import os
import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv
import duckdb
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
# Removido temporariamente:
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
# from jose import JWTError, jwt
from pydantic import BaseModel, field_validator
from datetime import datetime
import logging.handlers

# Dicionário de mapeamento para nomes de grupos
grupos_dict = {
    'AB': 'APAC_de_Cirurgia_Bariatrica',
    'ABO': 'APAC_de_Acompanhamento_Pos_Cirurgia_Bariatrica',
    'ACF': 'APAC_de_Confeccao_de_Fistula',
    'AD': 'APAC_de_Laudos_Diversos',
    'AM': 'APAC_de_Medicamentos',
    'AMP': 'APAC_de_Acompanhamento_Multiprofissional',
    'AN': 'APAC_de_Nefrologia',
    'AQ': 'APAC_de_Quimioterapia',
    'AR': 'APAC_de_Radioterapia',
    'ATD': 'APAC_de_Tratamento_Dialitico',
    'BI': 'Boletim_de_Producao_Ambulatorial_individualizado',
    'IMPBO': 'IMPBO',
    'PA': 'Producao_Ambulatorial',
    'PAM': 'PAM',
    'PAR': 'PAR',
    'PAS': 'PAS',
    'PS': 'RAAS_Psicossocial',
    'SAD': 'RAAS_de_Atencao_Domiciliar',
    'RD': 'AIH_Reduzida',
    'RJ': 'AIH_Rejeitada',
    'ER': 'AIH_Rejeitada_com_erro',
    'SP': 'Servicos_Profissionais',
    'CH': 'Cadastro_Hospitalar',
    'CM': 'CM',
    'DC': 'Dados_Complementares',
    'EE': 'Estabelecimento_de_Ensino',
    'EF': 'Estabelecimento_Filantropico',
    'EP': 'Equipes',
    'EQ': 'Equipamentos',
    'GM': 'Gestao_Metas',
    'HB': 'Habilitacao',
    'IN': 'Incentivos',
    'LT': 'Leitos',
    'PF': 'Profissional',
    'RC': 'Regra_Contratual',
    'SR': 'Servico_Especializado',
    'ST': 'Estabelecimentos'
}

CAMPOS_CNES = {
    "AB": ["ap_cnspcn"],
    "ABO": ["ap_cnspcn"],
    "ACF": ["ap_cnspcn"],
    "AD": ["ap_cnspcn"],
    "AM": ["ap_cnspcn"],
    "AMP": ["ap_cnspcn"],
    "AN": ["ap_cnspcn"],
    "AQ": ["ap_cnspcn"],
    "AR": ["ap_cnspcn"],
    "ATD": ["ap_cnspcn"],
    "BI": ["cns_pac", "cnsprof"],
    "PA": ["PA_CODUNI"],
    "RD": ["CNES"],
    "RJ": ["cnes"],
    "ER": ["CNES"],
    "SP": ["sp_cnes"]
}

GRUPOS_INFO = {
    "RD": {
        "tabela": "sih_aih_reduzida",
        "colunas": {
            "uf_zi": "TEXT",  # Alterado para TEXT (código de 2 caracteres)
            "ano_cmpt": "INTEGER",  # Aumentado para INTEGER (4 dígitos)
            "mes_cmpt": "SMALLINT",
            "n_aih": "TEXT",  # Alterado para TEXT (13 caracteres)
            "ident": "TEXT",  # Alterado para TEXT (código de 1 caractere)
            "nasc": "DATE",
            "sexo": "TEXT",
            "uti_mes_in": "SMALLINT",
            "uti_mes_an": "SMALLINT",
            "uti_mes_al": "SMALLINT",
            "uti_mes_to": "SMALLINT",
            "marca_uti": "TEXT",
            "uti_int_in": "SMALLINT",
            "uti_int_an": "SMALLINT",
            "uti_int_al": "SMALLINT",
            "uti_int_to": "SMALLINT",
            "diar_acom": "SMALLINT",
            "qt_diarias": "SMALLINT",
            "proc_solic": "TEXT",
            "proc_rea": "TEXT",
            "val_sh": "NUMERIC(15,2)",  # Aumentada precisão
            "val_sp": "NUMERIC(15,2)",
            "val_sadt": "NUMERIC(15,2)",
            "val_rn": "NUMERIC(15,2)",
            "val_acomp": "NUMERIC(15,2)",
            "val_ortp": "NUMERIC(15,2)",
            "val_sangue": "NUMERIC(15,2)",
            "val_sadtsr": "NUMERIC(15,2)",
            "val_transp": "NUMERIC(15,2)",
            "val_obsang": "NUMERIC(15,2)",
            "val_ped1ac": "NUMERIC(15,2)",
            "val_tot": "NUMERIC(15,2)",  # Padronizado precisão
            "val_uti": "NUMERIC(15,2)",
            "us_tot": "NUMERIC(15,2)",  # Aumentada precisão
            "dt_inter": "DATE",
            "dt_saida": "DATE",
            "diag_princ": "TEXT",
            "diag_secun": "TEXT",
            "ind_vdrl": "TEXT",  # Alterado para TEXT (indicador)
            "cod_idade": "TEXT",  # Alterado para TEXT (código de 2 caracteres)
            "idade": "SMALLINT",
            "dias_perm": "SMALLINT",
            "morte": "TEXT",  # Alterado para TEXT (indicador)
            "car_int": "TEXT",
            "tot_pt_sp": "NUMERIC(15,2)",  # Aumentada precisão
            "cnes": "TEXT",  # Alterado para TEXT (código de 7 caracteres)
            "cid_asso": "TEXT",
            "cid_morte": "TEXT",
            "complex": "TEXT",
            "faec_tp": "TEXT",
            "aud_just": "TEXT",
            "val_uci": "NUMERIC(15,2)",
            "diagsec1": "TEXT",
            "espec": "TEXT",
            "cgc_hosp": "TEXT",
            "cep": "TEXT",
            "munic_res": "INTEGER",
            "cobranca": "TEXT",
            "natureza": "TEXT",
            "nat_jur": "TEXT",
            "gestao": "SMALLINT",
            "rubrica": "TEXT",
            "munic_mov": "INTEGER",
            "nacional": "TEXT",
            "num_proc": "TEXT",
            "cpf_aut": "TEXT",
            "homonimo": "BOOLEAN",
            "num_filhos": "SMALLINT",
            "instru": "SMALLINT",
            "cid_notif": "TEXT",
            "contracep1": "TEXT",
            "contracep2": "TEXT",
            "gestrisco": "BOOLEAN",
            "insc_pn": "TEXT",
            "seq_aih5": "SMALLINT",
            "cbor": "TEXT",
            "cnaer": "TEXT",
            "vincprev": "SMALLINT",
            "gestor_cod": "TEXT",
            "gestor_tp": "SMALLINT",
            "gestor_cpf": "TEXT",
            "gestor_dt": "DATE",
            "cnpj_mant": "TEXT",
            "infehosp": "BOOLEAN",
            "financ": "TEXT",
            "regct": "TEXT",
            "raca_cor": "TEXT",
            "etnia": "TEXT",
            "sequencia": "BIGINT",
            "remessa": "TEXT",
            "sis_just": "TEXT",
            "val_sh_fed": "NUMERIC(12,2)",
            "val_sp_fed": "NUMERIC(12,2)",
            "val_sh_ges": "NUMERIC(12,2)",
            "val_sp_ges": "NUMERIC(12,2)",
            "marca_uci": "TEXT",
            "diagsec2": "TEXT",
            "diagsec3": "TEXT",
            "diagsec4": "TEXT",
            "diagsec5": "TEXT",
            "diagsec6": "TEXT",
            "diagsec7": "TEXT",
            "diagsec8": "TEXT",
            "diagsec9": "TEXT",
            "tpdisec1": "SMALLINT",
            "tpdisec2": "SMALLINT",
            "tpdisec3": "SMALLINT",
            "tpdisec4": "SMALLINT",
            "tpdisec5": "SMALLINT",
            "tpdisec6": "SMALLINT",
            "tpdisec7": "SMALLINT",
            "tpdisec8": "SMALLINT",
            "tpdisec9": "SMALLINT"
        }
    },
    "RJ": {
        "tabela": "sih_aih_rejeitada",
        "colunas": {
            "cnes": "TEXT",
            "cod_idade": "SMALLINT",
            "num_filhos": "SMALLINT",
            "diar_acom": "SMALLINT",
            "n_aih": "BIGINT",
            "gestao": "SMALLINT",
            "dias_perm": "SMALLINT",
            "qt_diarias": "SMALLINT",
            "dt_inter": "DATE",
            "gestor_dt": "DATE",
            "gestor_tp": "SMALLINT",
            "seq_aih5": "TEXT",
            "gestrisco": "SMALLINT",
            "tot_pt_sp": "SMALLINT",
            "uf_zi": "TEXT",
            "us_tot": "NUMERIC(12,2)",
            "uti_int_al": "SMALLINT",
            "uti_int_an": "SMALLINT",
            "uti_int_in": "SMALLINT",
            "uti_int_to": "SMALLINT",
            "uti_mes_al": "SMALLINT",
            "uti_mes_an": "SMALLINT",
            "uti_mes_in": "SMALLINT",
            "uti_mes_to": "SMALLINT",
            "val_acomp": "NUMERIC(12,2)",
            "val_obsang": "NUMERIC(12,2)",
            "val_ortp": "NUMERIC(12,2)",
            "val_ped1ac": "NUMERIC(12,2)",
            "val_rn": "NUMERIC(12,2)",
            "val_sadt": "NUMERIC(12,2)",
            "val_sadtsr": "NUMERIC(12,2)",
            "val_sangue": "NUMERIC(12,2)",
            "val_sh": "NUMERIC(12,2)",
            "val_sp": "NUMERIC(12,2)",
            "val_tot": "NUMERIC(15,2)",
            "val_transp": "NUMERIC(12,2)",
            "val_uti": "NUMERIC(12,2)",
            "vincprev": "SMALLINT",
            "homonimo": "SMALLINT",
            "idade": "INTEGER",
            "ident": "SMALLINT",
            "ind_vdrl": "SMALLINT",
            "infehosp": "TEXT",
            "dt_saida": "DATE",
            "instru": "SMALLINT",
            "sequencia": "INTEGER",
            "mes_cmpt": "SMALLINT",
            "morte": "SMALLINT",
            "munic_mov": "TEXT",
            "munic_res": "TEXT",
            "ano_cmpt": "INTEGER",
            "nasc": "DATE",
            "marca_uti": "SMALLINT",
            "remessa": "TEXT",
            "id_log": "TEXT",
            "st_situac": "SMALLINT",
            "st_bloq": "SMALLINT",
            "st_mot_blo": "TEXT",
            "car_int": "TEXT",
            "cbor": "TEXT",
            "cep": "TEXT",
            "cgc_hosp": "TEXT",
            "cid_asso": "TEXT",
            "cid_morte": "TEXT",
            "cid_notif": "TEXT",
            "cnaer": "TEXT",
            "cnpj_mant": "TEXT",
            "cobranca": "SMALLINT",
            "complex": "TEXT",
            "contracep1": "TEXT",
            "contracep2": "TEXT",
            "cpf_aut": "TEXT",
            "diag_princ": "TEXT",
            "diag_secun": "TEXT",
            "espec": "SMALLINT",
            "etnia": "TEXT",
            "faec_tp": "TEXT",
            "financ": "TEXT",
            "gestor_cod": "TEXT",
            "gestor_cpf": "TEXT",
            "insc_pn": "TEXT",
            "nacional": "TEXT",
            "natureza": "TEXT",
            "nat_jur": "TEXT",
            "num_proc": "TEXT",
            "proc_rea": "TEXT",
            "proc_solic": "TEXT",
            "raca_cor": "TEXT",
            "regct": "TEXT",
            "rubrica": "SMALLINT",
            "sexo": "SMALLINT"
        }
    },
    "ER": {
        "tabela": "sih_aih_rejeitada_erro",
        "colunas": {
            "sequencia": "INTEGER",
            "remessa": "TEXT",
            "cnes": "INTEGER",
            "aih": "BIGINT",
            "ano": "SMALLINT",
            "mes": "SMALLINT",
            "dt_inter": "DATE",
            "dt_saida": "DATE",
            "mun_mov": "INTEGER",
            "uf_zi": "INTEGER",
            "mun_res": "INTEGER",
            "uf_res": "TEXT",
            "co_erro": "TEXT"
        }
    },
    "PA": {
        "tabela": "sia_producao_ambulatorial",
        "colunas": {
            "id_log": "TEXT",
            "idademax": "TEXT",
            "idademin": "TEXT",
            "nu_pa_tot": "TEXT",
            "nu_vpa_tot": "NUMERIC(15,2)",
            "pa_alta": "BOOLEAN",
            "pa_autoriz": "TEXT",
            "pa_catend": "TEXT",
            "pa_cbocod": "TEXT",
            "pa_cidcas": "TEXT",
            "pa_cidpri": "TEXT",
            "pa_cidsec": "TEXT",
            "pa_cmp": "INTEGER",
            "pa_cnpj_cc": "TEXT",
            "pa_cnpjcpf": "TEXT",
            "pa_cnpjmnt": "TEXT",
            "pa_cnsmed": "TEXT",
            "pa_codesp": "TEXT",
            "pa_codoco": "TEXT",
            "pa_codpro": "TEXT",
            "pa_coduni": "TEXT",  # Mantido como TEXT pois é código de 7 caracteres
            "pa_condic": "TEXT",
            "pa_datpr": "INTEGER",
            "pa_datref": "INTEGER",
            "pa_dif_val": "TEXT",
            "pa_docorig": "TEXT",
            "pa_encerr": "BOOLEAN",
            "pa_etnia": "TEXT",
            "pa_fler": "BOOLEAN",
            "pa_flidade": "SMALLINT",
            "pa_flqt": "TEXT",
            "pa_fxetar": "TEXT",
            "pa_gestao": "TEXT",
            "pa_idade": "TEXT",
            "pa_incout": "TEXT",
            "pa_incurg": "TEXT",
            "pa_indica": "SMALLINT",
            "pa_ine": "TEXT",
            "pa_mn_ind": "TEXT",
            "pa_mndif": "SMALLINT",
            "pa_morfol": "TEXT",
            "pa_motsai": "TEXT",
            "pa_munat": "INTEGER",
            "pa_munpcn": "INTEGER",
            "pa_mvm": "DATE",
            "pa_nat_jur": "TEXT",
            "pa_nh": "TEXT",
            "pa_nivcpl": "SMALLINT",
            "pa_numapa": "TEXT",
            "pa_obito": "BOOLEAN",
            "pa_perman": "BOOLEAN",
            "pa_proc_id": "TEXT",
            "pa_qtdapr": "INTEGER",
            "pa_qtdpro": "INTEGER",
            "pa_racacor": "TEXT",
            "pa_rcb": "TEXT",
            "pa_rcbdf": "SMALLINT",
            "pa_regct": "TEXT",
            "pa_sexo": "TEXT",
            "pa_srv_c": "TEXT",
            "pa_subfin": "TEXT",
            "pa_tipate": "TEXT",
            "pa_tippre": "TEXT",
            "pa_tippro": "TEXT",
            "pa_tpfin": "TEXT",
            "pa_tpups": "TEXT",
            "pa_transf": "BOOLEAN",
            "pa_ufdif": "SMALLINT",
            "pa_ufmun": "TEXT",
            "pa_valapr": "NUMERIC(15,2)",
            "pa_valpro": "NUMERIC(15,2)",
            "pa_vl_cf": "NUMERIC(15,2)",
            "pa_vl_cl": "NUMERIC(15,2)",
            "pa_vl_inc": "NUMERIC(15,2)",
            "uf": "TEXT"
        }
    }
}

# ---------------------------------------------------------------------------
# Configurações básicas
# ---------------------------------------------------------------------------

load_dotenv()

# Diretório de logs
if not os.path.exists('logs'):
    os.makedirs('logs')

# Remover configuração básica anterior
logging.root.handlers = []

# Configurar logging com rotação diária
log_file = f"logs/app_{datetime.now().strftime('%Y-%m-%d')}.log"
file_handler = logging.handlers.TimedRotatingFileHandler(
    filename='logs/app.log',
    when='midnight',
    interval=1,
    backupCount=7,
    encoding='utf-8'
)
file_handler.suffix = "%Y-%m-%d.log"
file_handler.namer = lambda name: name.replace(".log", "") + ".log"

# Formato dos logs
formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
file_handler.setFormatter(formatter)

# Configurar níveis e handlers
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        file_handler,
        logging.StreamHandler()
    ]
)

# Manter handler do console com formatação
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s:%(message)s'))
logging.getLogger().addHandler(console_handler)

# Configurações do JWT - Removido temporariamente
# SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key")
# ALGORITHM = "HS256"
# security = HTTPBearer()

# Configurações do banco de dados
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_PASSWORD_ENCODED = quote_plus(DB_PASSWORD)

# ---------------------------------------------------------------------------
# Modelo de dados para parâmetros da consulta
# ---------------------------------------------------------------------------
class QueryParams(BaseModel):
    base: str
    grupo: str
    cnes_list: List[str]
    campos_agrupamento: List[str]
    competencia_inicio: str
    competencia_fim: str
    table_name: str | None = None  # Campo opcional para nome da tabela
    
    @field_validator('base')
    def validate_base(cls, v):
        if v not in ['SIH', 'SIA']:
            raise ValueError('Base deve ser SIH ou SIA')
        return v

    @field_validator('grupo')
    def validate_grupo(cls, v):
        grupos_validos = [
            'RD','RJ','ER','SP','PA','AB','ABO','ACF','AD','AM','AMP','AN','AQ','AR','ATD',
            'BI','IMPBO','PAM','PAR','PAS','PS','SAD','CH','CM','DC','EE','EF','EP','EQ',
            'GM','HB','IN','LT','PF','RC','SR','ST'
        ]
        if v not in grupos_validos:
            raise ValueError('Grupo inválido')
        return v

    @field_validator('competencia_inicio', 'competencia_fim')
    def validate_competencia(cls, v):
        try:
            datetime.strptime(v, '%m/%Y')
        except ValueError:
            raise ValueError('Formato de competência deve ser MM/YYYY')
        return v

# ---------------------------------------------------------------------------
# Função de coleta de arquivos parquet
# ---------------------------------------------------------------------------
def log_execution(message: str, is_start: bool = True) -> None:
    marker = ">>>" if is_start else "<<<"
    logging.info(f"{marker} {message}")

def get_parquet_files(base: str, grupo: str, comp_inicio: str, comp_fim: str) -> List[str]:
    log_execution("Iniciando busca de arquivos Parquet")
    import glob
    ufs = [
        'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
        'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'
    ]
    logging.info(
        f"[get_parquet_files] Iniciando coleta de arquivos: base={base}, grupo={grupo}, "
        f"intervalo={comp_inicio} até {comp_fim}"
    )
    files = []
    start_date = datetime.strptime(comp_inicio, '%m/%Y')
    end_date = datetime.strptime(comp_fim, '%m/%Y')
    current_date = start_date
    while current_date <= end_date:
        year_suffix = current_date.strftime('%y')
        month = current_date.strftime('%m')
        for uf in ufs:
            pattern = (
                f"parquet_files/{base}/{grupo}/"
                f"{grupo}{uf}{year_suffix}{month}.parquet/*.parquet"
            )
            logging.debug(f"[get_parquet_files] Buscando arquivos com pattern: {pattern}")
            matching_files = glob.glob(pattern)
            if matching_files:
                logging.info(
                    f"[get_parquet_files] Encontrados {len(matching_files)} arquivos "
                    f"para {uf} em {month}/{year_suffix}."
                )
                files.extend(matching_files)
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
    total_files = len(files)
    if total_files > 0:
        logging.info(f"[get_parquet_files] Total de arquivos encontrados: {total_files}")
    else:
        logging.warning("[get_parquet_files] Nenhum arquivo encontrado para o período.")
    log_execution("Finalizada busca de arquivos Parquet", False)
    return files

# ---------------------------------------------------------------------------
# Conversão e tratamento de tipos de dados
# ---------------------------------------------------------------------------
def get_schema_info(base: str, grupo: str) -> Dict[str, str]:
    try:
        if grupo in GRUPOS_INFO:
            return GRUPOS_INFO[grupo]['colunas']
        logging.warning(f"Grupo {grupo} não encontrado na base {base}")
        return {}
    except KeyError as e:
        logging.error(f"Erro ao acessar schema do grupo {grupo}: {str(e)}")
        return {}

def convert_datatypes(df: pd.DataFrame, base: str, grupo: str) -> pd.DataFrame:
    """
    Converte tipos do DataFrame conforme schema do grupo.
    Se a coluna for do tipo DATE e os valores tiverem 6 dígitos (YYYYMM),
    converte utilizando o formato '%Y%m' (assumindo dia 01).
    """
    schema = get_schema_info(base, grupo)
    if not schema:
        logging.warning("Schema não encontrado, retornando DataFrame original")
        return df
        
    logging.info(f"Iniciando conversão de tipos para {len(df.columns)} colunas")
    
    type_mapping = {
        'INTEGER': 'Int64',
        'SMALLINT': 'Int32',
        'BIGINT': 'Int64',
        'NUMERIC': 'float',
        'DATE': 'datetime64[ns]',
        'BOOLEAN': 'boolean',
        'TEXT': 'string'
    }
    
    for col in df.columns:
        col_lower = col.lower()
        matched = False
        for schema_col, schema_type in schema.items():
            if schema_col.lower() == col_lower:
                sql_type = schema_type.split('(')[0].upper()
                pd_type = type_mapping.get(sql_type, 'object')
                if sql_type == 'DATE':
                    # Verifica se o primeiro valor não nulo possui 6 dígitos (YYYYMM)
                    non_null = df[col].dropna()
                    if not non_null.empty:
                        sample = str(non_null.iloc[0]).strip()
                        if len(sample) == 6 and sample.isdigit():
                            try:
                                df[col] = pd.to_datetime(df[col], format='%Y%m', errors='coerce')
                            except Exception as e:
                                logging.error(f"Erro ao converter coluna {col} para data (YYYYMM): {str(e)}")
                        else:
                            try:
                                df[col] = pd.to_datetime(df[col], errors='coerce')
                            except Exception as e:
                                logging.error(f"Erro ao converter coluna {col} para data: {str(e)}")
                    else:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    matched = True
                else:
                    try:
                        df[col] = df[col].astype(pd_type)
                        matched = True
                    except Exception as e:
                        logging.error(f"Erro ao converter {col} para {pd_type}: {str(e)}")
                        matched = True
                break
        if not matched:
            logging.warning(f"Coluna {col} não mapeada no schema - mantendo tipo original")
    return df

def get_cnes_column(grupo: str) -> str:
    if grupo not in CAMPOS_CNES:
        return "CNES"
    return CAMPOS_CNES[grupo][0]

def process_parquet_files(files: List[str], params: QueryParams) -> pd.DataFrame:
    log_execution("Iniciando processamento de arquivos")
    logging.info(f"[process_parquet_files] Iniciando processamento de {len(files)} arquivo(s).")
    con = duckdb.connect()
    cnes_str = ",".join([f"'{cnes}'" for cnes in params.cnes_list])
    cnes_column = get_cnes_column(params.grupo)
    campos_agrupamento = params.campos_agrupamento.copy()
    if not any(col.lower() == cnes_column.lower() for col in campos_agrupamento):
        campos_agrupamento.insert(0, cnes_column)
    group_by_cols = ",".join(campos_agrupamento)
    results = []
    total_rows = 0
    for file_path in files:
        logging.info(f"[process_parquet_files] Processando arquivo: {file_path}")
        query = f"""
        SELECT {group_by_cols}
        FROM read_parquet('{file_path}')
        WHERE LOWER({cnes_column}) IN (SELECT LOWER(unnest(ARRAY[{cnes_str}])))
        """
        try:
            df = con.execute(query).df()
            rows = len(df)
            total_rows += rows
            logging.info(f"[process_parquet_files] OK. Linhas retornadas: {rows}.")
            results.append(df)
        except Exception as e:
            logging.error(f"[process_parquet_files] Erro ao processar arquivo {file_path}: {e}")
    con.close()
    if not results:
        logging.info("[process_parquet_files] Nenhum resultado encontrado.")
        return pd.DataFrame()
    logging.info(f"[process_parquet_files] Concatenando {len(results)} DataFrames...")
    final_df = pd.concat(results, ignore_index=True)
    final_df.columns = final_df.columns.str.lower()
    logging.info("[process_parquet_files] Colunas convertidas para minúsculo")
    logging.info("[process_parquet_files] Iniciando conversão de tipos...")
    try:
        final_df = convert_datatypes(final_df, params.base, params.grupo)
        logging.info("[process_parquet_files] Conversão de tipos concluída com sucesso")
    except Exception as e:
        logging.error(f"[process_parquet_files] Erro na conversão de tipos: {e}")
    null_info = final_df.isnull().sum()
    if null_info.any():
        logging.warning("Colunas com valores nulos após processamento:")
        for col, count in null_info[null_info > 0].items():
            logging.warning(f"  - {col}: {count} valores nulos")
    log_execution("Finalizado processamento de arquivos", False)
    return final_df

def save_results(df: pd.DataFrame, table_name: str) -> None:
    logging.info(f"[save_results] Iniciando salvamento. Tabela destino: {table_name}")
    # Forçar tipos específicos antes de salvar
    type_mapping = {
        'pa_mvm': 'datetime64[ns]',
        'pa_qtdpro': 'Int64',
        'pa_qtdapr': 'Int64',
        'pa_valpro': 'float64',
        'pa_valapr': 'float64',
        'pa_vl_cf': 'float64',
        'pa_vl_cl': 'float64',
        'pa_vl_inc': 'float64'
    }
    for col, dtype in type_mapping.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                logging.error(f"Erro ao converter coluna {col}: {str(e)}")
    try:
        load_dotenv(override=True)
        conn_str = (
            f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
            f"{quote_plus(os.getenv('DB_PASSWORD'))}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
            f"{os.getenv('DB_NAME')}"
        )
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
            logging.info("Conexão com banco testada com sucesso")
        df.columns = df.columns.str.lower()
        # Definindo tipos específicos para o PostgreSQL
        if table_name.startswith("sia_pa"):
            from sqlalchemy.types import Integer, Numeric, Date, Text
            dtype_mapping = {
                'pa_coduni': Text(),
                'pa_gestao': Text(),
                'pa_mvm': Date(),
                'pa_proc_id': Text(),
                'pa_docorig': Text(),
                'pa_sexo': Text(),
                'pa_qtdpro': Integer(),
                'pa_qtdapr': Integer(),
                'pa_valpro': Numeric(15, 2),
                'pa_valapr': Numeric(15, 2),
                'pa_vl_cf': Numeric(15, 2),
                'pa_vl_cl': Numeric(15, 2),
                'pa_vl_inc': Numeric(15, 2)
            }
        else:
            dtype_mapping = None
        df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype_mapping)
        logging.info(f"[save_results] Tabela '{table_name}' salva/recriada no PostgreSQL (replace).")
        # Se tiver menos de 10M linhas, salva em CSV (comentado temporariamente)
        row_count = len(df)
        # if row_count < 10_000_000:
        #     csv_dir = 'consultas'
        #     os.makedirs(csv_dir, exist_ok=True)
        #     csv_path = f'{csv_dir}/{table_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        #     df.to_csv(csv_path, index=False)
        #     logging.info(f"[save_results] DataFrame com {row_count} linha(s) salvo em CSV: {csv_path}")
        # else:
        #     logging.info(f"[save_results] DataFrame com {row_count} linha(s). Não será salvo em CSV.")
        
        logging.info(f"[save_results] Salvamento em CSV temporariamente desativado. Linhas processadas: {row_count}")
    except Exception as e:
        logging.error(f"[save_results] Erro ao salvar resultados: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao salvar resultados: {e}")

# ---------------------------------------------------------------------------
# Autenticação - Removido temporariamente
# ---------------------------------------------------------------------------
# def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
#     try:
#         token = credentials.credentials
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         return payload
#     except JWTError:
#         raise HTTPException(status_code=401, detail="Token inválido")

# ---------------------------------------------------------------------------
# Criação da aplicação FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(title="DataSUS API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Rota principal para query
# ---------------------------------------------------------------------------
@app.post("/query")
async def query_data(params: QueryParams) -> Dict[str, Any]:
    log_execution("Iniciando processamento de requisição")
    logging.info(
        "[query_data] Recebendo requisição. "
        f"Base={params.base}, Grupo={params.grupo}, Competências={params.competencia_inicio} a {params.competencia_fim}."
    )
    try:
        files = get_parquet_files(
            params.base,
            params.grupo,
            params.competencia_inicio,
            params.competencia_fim
        )
        if not files:
            msg = "Nenhum arquivo encontrado para os critérios especificados."
            logging.warning(f"[query_data] {msg}")
            raise HTTPException(status_code=404, detail=msg)
        result_df = process_parquet_files(files, params)
        if result_df.empty:
            msg = "Nenhum dado encontrado para os critérios especificados."
            logging.warning(f"[query_data] {msg}")
            raise HTTPException(status_code=404, detail=msg)
        grupo_mapped = grupos_dict.get(params.grupo, params.grupo)
        table_name = params.table_name if params.table_name else f"{params.base.lower()}_{grupo_mapped.lower()}"
        save_results(result_df, table_name)
        logging.info("[query_data] Consulta processada com sucesso.")
        response = {
            "status": "success",
            "message": "Consulta processada com sucesso",
            "dados": result_df.to_dict(orient='records'),
            "total_registros": len(result_df),
            "colunas": result_df.columns.tolist(),
            "table_name": table_name
        }
    except Exception as e:
        logging.error(f"[query_data] Erro ao processar consulta: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    log_execution("Finalizado processamento de requisição", False)
    return response

# ---------------------------------------------------------------------------
# Verifica conexão com banco de dados
# ---------------------------------------------------------------------------
def verify_db_connection():
    try:
        with create_engine(
            f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        ).connect() as connection:
            connection.execute(text('SELECT 1'))
            logging.info("Conexão com banco de dados verificada com sucesso")
    except Exception as e:
        logging.error(f"Erro na conexão com banco de dados: {str(e)}")
        raise

# ---------------------------------------------------------------------------
# Execução local (uvicorn)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
