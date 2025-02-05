import logging
import os
import datetime
from typing import List, Dict, Any, Optional, Tuple
from dotenv import load_dotenv
import duckdb
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
import logging.handlers
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4
import psutil
import time
import json
from io import StringIO
import traceback
import threading
from queue import PriorityQueue
from asyncio import Semaphore
import gc
from fastapi.responses import JSONResponse
import csv
import numpy as np
from collections import defaultdict
import re
import logging.config
import psycopg2
import tempfile
import shutil
from sqlalchemy.pool import NullPool

# -----------------------------------------------------------------------------
# Configurações iniciais e carregamento do ambiente
# -----------------------------------------------------------------------------
load_dotenv(override=True)
os.environ.update(os.environ)

# Configurar logging (arquivo + console)
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "app.log")),
        logging.StreamHandler()
    ]
)

# -----------------------------------------------------------------------------
# Variáveis de ambiente para o PostgreSQL
# -----------------------------------------------------------------------------
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_PASSWORD_ENCODED = quote_plus(DB_PASSWORD)

# Cria engine sem pool persistente
engine = create_engine(
    f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}',
    poolclass=NullPool,
    connect_args={'options': '-c statement_timeout=15000'}  # 15s timeout
)

# -----------------------------------------------------------------------------
# Dicionários de configuração (ex.: mapeamento de grupos, schemas, etc.)
# -----------------------------------------------------------------------------
# (Aqui você insere os seus dicionários de grupos e mapeamento de colunas, conforme seu código.)
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
    "SP": "SP_CNES",
    "RD": "CNES",
    "PA": "PA_CODUNI",
    "AB": "AP_CNSPCN",
    "ABO": "AP_CNSPCN",
    "ACF": "AP_CNSPCN",
    "AD": "AP_CNSPCN",
    "AM": "AP_CNSPCN",
    "AMP": "AP_CNSPCN",
    "AN": "AP_CNSPCN",
    "AQ": "AP_CNSPCN",
    "AR": "AP_CNSPCN",
    "ATD": "AP_CNSPCN",
    "BI": "CNS_PAC",
    "RJ": "CNES",
    "ER": "CNES"
}

# Exemplo: GRUPOS_INFO – adicione aqui o seu dicionário completo de schemas
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
            "cnes": "NUMERIC(7,0)",  # Numérico 7 dígitos (CNES)
            "uf_zi": "SMALLINT",  # Numérico 2 dígitos (Código UF)
            "ano_cmpt": "SMALLINT",  # Numérico 4 dígitos (ano)
            "mes_cmpt": "SMALLINT",  # Numérico 2 dígitos (01-12)
            "espec": "SMALLINT",  # Numérico 2 dígitos (especialidade)
            "cgc_hosp": "NUMERIC(14,0)",  # Numérico 14 dígitos (CNPJ)
            "n_aih": "NUMERIC(13,0)",  # Numérico 13 dígitos
            "ident": "SMALLINT",  # Numérico 1 dígito (tipo AIH)
            "cep": "NUMERIC(8,0)",  # Numérico 8 dígitos
            "munic_res": "INTEGER",  # Numérico 6 dígitos (código IBGE)
            "nasc": "DATE",  # Data AAAAMMDD
            "sexo": "SMALLINT",  # Numérico 1 dígito
            "uti_mes_in": "SMALLINT",  # Numérico 2 dígitos
            "uti_mes_an": "SMALLINT",  # Numérico 2 dígitos
            "uti_mes_al": "SMALLINT",  # Numérico 2 dígitos
            "uti_mes_to": "SMALLINT",  # Numérico 2 dígitos
            "marca_uti": "SMALLINT",  # Numérico 1 dígito
            "uti_int_in": "SMALLINT",  # Numérico 2 dígitos
            "uti_int_an": "SMALLINT",  # Numérico 2 dígitos
            "uti_int_al": "SMALLINT",  # Numérico 2 dígitos
            "uti_int_to": "SMALLINT",  # Numérico 2 dígitos
            "diar_acom": "SMALLINT",  # Numérico 2 dígitos
            "qt_diarias": "SMALLINT",  # Numérico 3 dígitos
            "proc_solic": "NUMERIC(10,0)",  # Numérico 10 dígitos
            "proc_rea": "NUMERIC(10,0)",  # Numérico 10 dígitos
            "val_sh": "NUMERIC(10,2)",  # Numérico 10,2 decimais
            "val_sp": "NUMERIC(10,2)",  # Numérico 10,2 decimais
            "val_sadt": "NUMERIC(10,2)",  # Numérico 10,2 decimais
            "val_rn": "NUMERIC(10,2)",  # Numérico 10,2 decimais
            "val_acomp": "NUMERIC(10,2)",  # Numérico 10,2 decimais
            "val_ortp": "NUMERIC(10,2)",  # Numérico 10,2 decimais
            "val_sangue": "NUMERIC(10,2)",  # Numérico 10,2 decimais
            "val_tot": "NUMERIC(10,2)",  # Numérico 10,2 decimais
            "dt_inter": "DATE",  # Data AAAAMMDD
            "dt_saida": "DATE",  # Data AAAAMMDD
            "diag_princ": "TEXT",  # Texto 4 caracteres (CID-10)
            "diag_secun": "TEXT",  # Texto 4 caracteres (CID-10)
            "morte": "SMALLINT",  # Numérico 1 dígito
            "gestao": "SMALLINT",  # Numérico 1 dígito
            "cnpj_mant": "NUMERIC(14,0)",  # Numérico 14 dígitos
            "cid_asso": "TEXT",  # Texto 4 caracteres
            "cid_morte": "TEXT",  # Texto 4 caracteres
            "complex": "SMALLINT",  # Numérico 2 dígitos
            "financ": "NUMERIC(2,0)",  # Numérico 2 dígitos
            "faec_tp": "SMALLINT",  # Numérico 2 dígitos
            "regct": "NUMERIC(2,0)",  # Numérico 2 dígitos
            "raca_cor": "SMALLINT",  # Numérico 1 dígito
            "etnia": "SMALLINT",  # Numérico 2 dígitos
            "st_situac": "SMALLINT",  # Numérico 1 dígito
            "st_bloq": "SMALLINT",  # Numérico 1 dígito
            "st_mot_blo": "SMALLINT",  # Numérico 2 dígitos
            "sequencia": "NUMERIC(6,0)",  # Numérico 6 dígitos
            "remessa": "TEXT",  # Texto 10 caracteres
            # Campos mantidos conforme schema original
            "cod_idade": "SMALLINT",
            "num_filhos": "SMALLINT",
            "dias_perm": "SMALLINT",
            "gestor_dt": "DATE",
            "gestor_tp": "SMALLINT",
            "seq_aih5": "TEXT",
            "gestrisco": "SMALLINT",
            "tot_pt_sp": "SMALLINT",
            "us_tot": "NUMERIC(12,2)",
            "val_obsang": "NUMERIC(12,2)",
            "val_ped1ac": "NUMERIC(12,2)",
            "val_sadtsr": "NUMERIC(12,2)",
            "val_transp": "NUMERIC(12,2)",
            "val_uti": "NUMERIC(12,2)",
            "vincprev": "SMALLINT",
            "homonimo": "SMALLINT",
            "idade": "INTEGER",
            "ind_vdrl": "SMALLINT",
            "infehosp": "TEXT",
            "instru": "SMALLINT",
            "munic_mov": "TEXT",
            "id_log": "TEXT",
            "car_int": "TEXT",
            "cbor": "TEXT",
            "cid_notif": "TEXT",
            "cnaer": "TEXT",
            "cobranca": "SMALLINT",
            "contracep1": "TEXT",
            "contracep2": "TEXT",
            "cpf_aut": "TEXT",
            "gestor_cod": "TEXT",
            "gestor_cpf": "TEXT",
            "insc_pn": "TEXT",
            "nacional": "TEXT",
            "natureza": "TEXT",
            "nat_jur": "TEXT",
            "num_proc": "TEXT",
            "rubrica": "SMALLINT"
        }
    },
    "ER": {
        "tabela": "sih_aih_rejeitada_erro",
        "colunas": {
            "sequencia": "NUMERIC(6,0)",  # Numérico 6 dígitos conforme schema
            "remessa": "TEXT",            # Texto 10 caracteres (mantido TEXT pois é adequado para strings)
            "cnes": "NUMERIC(7,0)",       # Numérico 7 dígitos (CNES)
            "n_aih": "NUMERIC(13,0)",     # Numérico 13 dígitos (N_AIH)
            "ano": "NUMERIC(4,0)",        # Numérico 4 dígitos (ano)
            "mes": "NUMERIC(2,0)",        # Numérico 2 dígitos (01-12)
            "dt_inter": "DATE",           # Data no formato DATE (AAAAMMDD)
            "dt_saida": "DATE",           # Data no formato DATE (AAAAMMDD)
            "mun_mov": "NUMERIC(6,0)",    # Código IBGE 6 dígitos
            "uf_zi": "NUMERIC(2,0)",      # Código UF 2 dígitos
            "mun_res": "NUMERIC(6,0)",    # Código IBGE 6 dígitos
            "uf_res": "NUMERIC(2,0)",     # Código UF 2 dígitos (corrigido de TEXT para numérico)
            "co_erro": "NUMERIC(2,0)"     # Código de erro 2 dígitos (corrigido de TEXT para numérico)
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
    },
    "SP": {
        "tabela": "sih_servicos_profissionais",
        "colunas": {
            "sp_gestor": "NUMERIC(6,0)",
            "sp_uf": "NUMERIC(2,0)",
            "sp_aa": "NUMERIC(4,0)",
            "sp_mm": "NUMERIC(2,0)",
            "sp_cnes": "NUMERIC(7,0)",
            "sp_naih": "NUMERIC(13,0)",
            "sp_procrea": "NUMERIC(10,0)",
            "sp_dtinter": "DATE",
            "sp_dtsaida": "DATE",
            "sp_num_pr": "TEXT",
            "sp_tipo": "NUMERIC(2,0)",
            "sp_cpfcgc": "NUMERIC(14,0)",
            "sp_atoprof": "NUMERIC(10,0)",
            "sp_tp_ato": "NUMERIC(2,0)",
            "sp_qtd_ato": "NUMERIC(5,0)",
            "sp_ptsp": "NUMERIC(2,0)",
            "sp_nf": "NUMERIC(12,0)",
            "sp_valato": "NUMERIC(10,2)",
            "sp_m_hosp": "NUMERIC(10,2)",
            "sp_m_pac": "NUMERIC(10,2)",
            "sp_des_hos": "NUMERIC(10,2)",
            "sp_des_pac": "NUMERIC(10,2)",
            "sp_complex": "NUMERIC(2,0)",
            "sp_financ": "NUMERIC(2,0)",
            "sp_co_faec": "NUMERIC(10,0)",
            "sp_pf_cbo": "NUMERIC(6,0)",
            "sp_pf_doc": "NUMERIC(15,0)",
            "sp_pj_doc": "NUMERIC(14,0)",
            "in_tp_val": "NUMERIC(1,0)",
            "sequencia": "NUMERIC(5,0)",
            "remessa": "TEXT",
            "serv_cla": "NUMERIC(4,0)",
            "sp_cidpri": "TEXT",
            "sp_cidsec": "TEXT",
            "sp_qt_proc": "NUMERIC(5,0)",
            "sp_u_aih": "NUMERIC(2,0)"
        }
    }
}
# -----------------------------------------------------------------------------
# Modelo de Dados para Parâmetros da Consulta
# -----------------------------------------------------------------------------
class QueryParams(BaseModel):
    base: str
    grupo: str
    cnes_list: List[str] = Field(..., min_length=1)
    campos_agrupamento: List[str] = Field(..., min_length=1)
    competencia_inicio: str
    competencia_fim: str
    table_name: Optional[str] = None
    consulta_personalizada: Optional[str] = None
    
    @field_validator('base')
    def validate_base(cls, v):
        if v not in ['SIH', 'SIA']:
            raise ValueError('Base deve ser SIH ou SIA')
        return v.upper()

    @field_validator('cnes_list')
    def validate_cnes(cls, v):
        if v == ["*"]:
            return v
        for cnes in v:
            if len(cnes) != 7 or not cnes.isdigit():
                raise ValueError('CNES deve conter exatamente 7 dígitos ou "*" para todos')
        return v

    @field_validator('competencia_inicio', 'competencia_fim')
    def validate_competencia(cls, v):
        try:
            datetime.strptime(v, '%m/%Y')
        except ValueError:
            raise ValueError('Formato inválido. Use MM/YYYY')
        return v

# -----------------------------------------------------------------------------
# Funções de Utilidade para Processamento e Conversão
# -----------------------------------------------------------------------------
def log_execution(message: str, is_start: bool = True) -> None:
    marker = ">>>" if is_start else "<<<"
    logging.info(f"{marker} {message}")

def get_parquet_files(base: str, grupo: str, comp_inicio: str, comp_fim: str) -> List[str]:
    log_execution("Iniciando busca de arquivos Parquet")
    import glob
    ufs = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
           'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']
    logging.info(f"[get_parquet_files] Parâmetros: base={base}, grupo={grupo}, intervalo={comp_inicio} a {comp_fim}")
    
    files = []
    start_date = datetime.strptime(comp_inicio, '%m/%Y')
    end_date = datetime.strptime(comp_fim, '%m/%Y')
    current_date = start_date
    while current_date <= end_date:
        periodo = current_date.strftime('%y%m')
        for uf in ufs:
            nome_pasta = f"{grupo}{uf}{periodo}.parquet"
            caminho = os.path.join("parquet_files", base, grupo, nome_pasta)
            if os.path.isdir(caminho):
                found = glob.glob(os.path.join(caminho, "*.parquet"))
                if found:
                    logging.info(f"Encontrados {len(found)} arquivos em {nome_pasta}")
                    files.extend(found)
                else:
                    logging.debug(f"Pasta {nome_pasta} vazia")
            else:
                logging.debug(f"Pasta não encontrada: {caminho}")
        # Avança para o próximo mês
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
    logging.info(f"Total de arquivos coletados: {len(files)}")
    log_execution("Finalizada busca de arquivos Parquet", False)
    return files

def get_schema_info(grupo: str) -> dict:
    grupo = grupo.strip().upper()
    if grupo in GRUPOS_INFO:
        return GRUPOS_INFO[grupo]
    for key in GRUPOS_INFO:
        if key.startswith(grupo):
            return GRUPOS_INFO[key]
    logging.warning(f"Schema padrão utilizado para {grupo}")
    return {}

def create_error_columns(df: pd.DataFrame, grupo: str) -> pd.DataFrame:
    schema = get_schema_info(grupo)
    for col in schema.get('colunas', {}):
        df[f"new_{col}"] = df[col]  # Cria uma nova coluna com os valores originais
    return df

def convert_datatypes(df: pd.DataFrame, grupo: str) -> pd.DataFrame:
    """
    Converte os tipos de dados de um DataFrame de acordo com o schema definido para o grupo.
    
    Args:
        df (pd.DataFrame): DataFrame com os dados brutos a serem convertidos
        grupo (str): Grupo/tabela para obter o schema de conversão
        
    Returns:
        pd.DataFrame: DataFrame com tipos de dados convertidos e colunas de erro
        
    Funcionamento:
        1. Obtém o schema de tipos do grupo usando get_schema_info
        2. Para cada coluna no schema:
           - Converte valores numéricos (NUMERIC/INT) tratando valores inválidos
           - Converte datas com múltiplos formatos e cria coluna de backup
           - Converte booleanos tratando variações comuns
           - Trata strings removendo espaços em branco
        3. Preserva valores originais em colunas new_{col} quando há erros
        4. Registra erros e gera logs detalhados
    """
    schema = get_schema_info(grupo)
    error_stats = defaultdict(int)
    df = df.apply(lambda col: col.astype(str).str.strip() if col.dtype == 'object' else col)
    
    for col, dtype in schema.get('colunas', {}).items():
        if col not in df.columns:
            continue
        dtype = dtype.upper()
        original = df[col].copy()
        try:
            if any(nt in dtype for nt in ['NUMERIC', 'INT']):
                df[col] = pd.to_numeric(df[col].replace({'': pd.NA, ' ': pd.NA}), errors='coerce')
                error_mask = df[col].isna() & original.notna()
                if error_mask.sum() > 0:
                    df[f"new_{col}"] = original
                    df[col] = pd.NA
            elif 'DATE' in dtype:
                df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce', exact=False)
                error_mask = df[col].isna() & original.notna()
                if error_mask.sum() > 0:
                    df[f"new_{col}"] = original
                    df[col] = pd.NaT
            elif 'BOOLEAN' in dtype:
                df[col] = df[col].apply(lambda x: False if str(x).lower() in ['0', 'false', 'f'] else True)
            else:
                df[col] = df[col].astype('string')
        except Exception as e:
            logging.error(f"Erro na conversão da coluna {col}: {str(e)}")
            raise
    logging.info(f"Conversão de tipos concluída.")
    return df

def validate_data_for_postgres(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida o DataFrame para garantir compatibilidade com o PostgreSQL.
    
    Args:
        df (pd.DataFrame): DataFrame a ser validado
        
    Returns:
        pd.DataFrame: DataFrame validado (inalterado se válido)
        
    Funcionamento:
        1. Verifica caracteres não-ASCII em colunas de texto
        2. Valida comprimento máximo de strings (255 caracteres)
        3. Ignora colunas de erro (prefixo 'ERRO_')
        4. Levanta exceção com detalhes dos problemas encontrados
        
    Exceptions:
        ValueError: Se encontrar dados incompatíveis com o PostgreSQL
    """
    for col in df.columns:
        if col.startswith('ERRO_'):
            continue
        if df[col].dtype == 'object':
            invalid = df[col].str.contains(r'[^\x00-\x7F]', na=False)
            if invalid.any():
                raise ValueError(f"Caracteres não-ASCII na coluna {col}")
    max_lengths = df.select_dtypes(include='object').apply(lambda x: x.str.len().max())
    for col, length in max_lengths.items():
        if length and length > 255:
            raise ValueError(f"Coluna {col} excede 255 caracteres")
    return df

def apply_filters(df: pd.DataFrame, params: QueryParams) -> pd.DataFrame:
    if params.cnes_list != ["*"]:
        cnes_column = get_cnes_column(params.grupo).upper()
        df.columns = df.columns.str.upper()
        if cnes_column not in df.columns:
            raise KeyError(f"Coluna {cnes_column} não encontrada no DataFrame")
        return df[df[cnes_column].isin(params.cnes_list)]
    return df

# -----------------------------------------------------------------------------
# Função para dividir um CSV grande em chunks menores
# -----------------------------------------------------------------------------
def split_csv(file_path: str, lines_per_chunk: int, output_dir: str) -> List[str]:
    os.makedirs(output_dir, exist_ok=True)
    chunk_files = []
    with open(file_path, 'r', encoding='utf-8') as f:
        header = f.readline()
        chunk_num = 0
        lines = []
        for line in f:
            lines.append(line)
            if len(lines) >= lines_per_chunk:
                chunk_filename = os.path.join(output_dir, f"chunk_{chunk_num}.csv")
                with open(chunk_filename, 'w', encoding='utf-8') as chunk_file:
                    chunk_file.write(header)
                    chunk_file.writelines(lines)
                chunk_files.append(chunk_filename)
                chunk_num += 1
                lines = []
        if lines:
            chunk_filename = os.path.join(output_dir, f"chunk_{chunk_num}.csv")
            with open(chunk_filename, 'w', encoding='utf-8') as chunk_file:
                chunk_file.write(header)
                chunk_file.writelines(lines)
            chunk_files.append(chunk_filename)
    return chunk_files

# -----------------------------------------------------------------------------
# Função de salvamento otimizado utilizando COPY e chunks
# -----------------------------------------------------------------------------
def save_results(df: pd.DataFrame, table_name: str, params: QueryParams) -> None:
    """Processo completo de salvamento com validação"""
    try:
        # 1. Exportar schema do DuckDB
        schema_sql = export_schema(df, table_name)
        
        # 2. Criar tabela no PostgreSQL
        with engine.connect() as conn:
            conn.execute(text(schema_sql))
            logging.info(f"Tabela {table_name} criada com sucesso")
        
        # 3. Gerar CSV formatado
        temp_dir = tempfile.mkdtemp()
        temp_csv = os.path.join(temp_dir, 'data.csv')
        
        df.to_csv(
            temp_csv,
            index=False,
            header=True,
            na_rep='\\N',
            quoting=csv.QUOTE_MINIMAL,
            escapechar='\\',
            date_format='%Y-%m-%d',
            encoding='utf-8'
        )
        
        # 4. Validar amostra
        if not validate_csv_sample(temp_csv, table_name):
            raise ValueError("Falha na validação do CSV")
        
        # 5. Fazer upload completo
        with engine.connect() as conn:
            with conn.connection.cursor() as cursor:
                with open(temp_csv, 'r') as f:
                    cursor.copy_expert(
                        f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '\\N')",
                        f
                    )
            conn.commit()
        
        logging.info(f"Dados carregados com sucesso na tabela {table_name}")
        
    except Exception as e:
        logging.error(f"Erro no processo de salvamento: {str(e)}")
        raise
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

# -----------------------------------------------------------------------------
# Endpoints FastAPI
# -----------------------------------------------------------------------------
app = FastAPI(title="DataSUS API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Variáveis globais para gerenciamento de tarefas
# -----------------------------------------------------------------------------
task_lock = threading.Lock()
task_queue = PriorityQueue()
async_jobs = {}

# -----------------------------------------------------------------------------
# Endpoint principal para consulta
# -----------------------------------------------------------------------------
@app.post("/query", tags=["Main"])
async def query_data(params: QueryParams, background_tasks: BackgroundTasks):
    logging.info(f"Nova requisição: {params.model_dump()}")
    files = get_parquet_files(params.base, params.grupo, params.competencia_inicio, params.competencia_fim)
    if not files:
        logging.error("Nenhum arquivo encontrado")
        raise HTTPException(status_code=404, detail="Nenhum arquivo encontrado")
    start_time = datetime.now().isoformat()
    logging.info(f"Iniciando processamento de {len(files)} arquivos às {start_time}")
    background_tasks.add_task(lambda: process_with_logging(files, params, start_time))
    return {
        "status": "processing",
        "start_time": start_time,
        "total_files": len(files),
        "message": "Processamento iniciado. Verifique os logs para detalhes."
    }

def process_with_logging(files: List[str], params: QueryParams, start_time: str):
    try:
        adaptive_processing(files, params)
        logging.info("Processamento concluído com sucesso")
    except Exception as e:
        logging.critical(f"Erro catastrófico: {str(e)}\n{traceback.format_exc()}")
        raise

@app.post("/query/async", tags=["Async Operations"])
async def async_query(params: QueryParams, background_tasks: BackgroundTasks) -> Dict[str, str]:
    async with Semaphore(1):
        job_id = str(uuid4())
        async_jobs[job_id] = {
            "status": "processing",
            "start_time": datetime.now().isoformat(),
            "progress": 0
        }
        def task_processor():
            try:
                files = get_parquet_files(params.base, params.grupo, params.competencia_inicio, params.competencia_fim)
                result_df = process_parquet_files(files, params)
                table_name = params.table_name if params.table_name else GRUPOS_INFO[params.grupo]['tabela']
                save_results(result_df, table_name, params)
                async_jobs[job_id].update({
                    "status": "completed",
                    "end_time": datetime.now().isoformat(),
                    "result_size": len(result_df),
                    "table_name": table_name
                })
            except Exception as e:
                async_jobs[job_id].update({
                    "status": "error",
                    "error": str(e),
                    "end_time": datetime.now().isoformat()
                })
        background_tasks.add_task(task_processor)
        return {"job_id": job_id, "status_url": f"/query/jobs/{job_id}"}

@app.get("/query/jobs/{job_id}", tags=["Async Operations"])
async def get_job_status(job_id: str):
    if job_id not in async_jobs:
        raise HTTPException(status_code=404, detail=f"Job {job_id} não encontrado")
    return async_jobs[job_id]

# -----------------------------------------------------------------------------
# Middleware de Monitoramento de Performance
# -----------------------------------------------------------------------------
@app.middleware("http")
async def resource_guard(request: Request, call_next):
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent()
    if mem.percent > 75 or cpu > 85:
        return JSONResponse(
            status_code=503,
            content={
                "error": "high_load",
                "message": "Servidor sob alta carga",
                "metrics": {
                    "memory_used": f"{mem.used/1024**3:.1f}GB",
                    "cpu_usage": f"{cpu}%"
                }
            }
        )
    try:
        response = await call_next(request)
        return response
    except MemoryError:
        return JSONResponse(
            status_code=500,
            content={"error": "out_of_memory", "message": "Memória insuficiente"}
        )

# -----------------------------------------------------------------------------
# Verifica Conexão com o PostgreSQL
# -----------------------------------------------------------------------------
def verify_db_connection():
    try:
        with engine.connect() as connection:
            connection.execute(text('SELECT 1'))
            logging.info("Conexão com o PostgreSQL verificada com sucesso")
    except Exception as e:
        logging.error(f"Erro na conexão com o PostgreSQL: {str(e)}")
        raise

# -----------------------------------------------------------------------------
# Execução local com Uvicorn
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

# -----------------------------------------------------------------------------
# Funções auxiliares de tarefas e processamento adaptativo (ATUALIZADA)
# -----------------------------------------------------------------------------
def adaptive_processing(files: List[str], params: QueryParams) -> None:
    chunk_size = 50  # Tamanho inicial do chunk
    total_files = len(files)
    processed = 0
    logging.info(f"Iniciando processamento adaptativo de {total_files} arquivos")
    
    for i in range(0, total_files, chunk_size):
        chunk = files[i:i+chunk_size]
        logging.info(f"Processando chunk {i//chunk_size + 1} com {len(chunk)} arquivos")
        
        try:
            # Processa o chunk atual
            process_chunk_with_duckdb(chunk, params)
            processed += len(chunk)
            
            # Calcula e loga o progresso
            progress = (processed / total_files) * 100
            logging.info(f"Progresso: {processed}/{total_files} arquivos ({progress:.1f}%) - Memória: {psutil.virtual_memory().percent}%")
            
            # Ajuste dinâmico do tamanho do chunk baseado no uso de memória
            mem = psutil.virtual_memory()
            if mem.percent > 70:
                chunk_size = max(10, chunk_size // 2)
                logging.warning(f"Reduzindo chunk size para {chunk_size} (Memória: {mem.percent}%)")
            elif mem.percent < 30:
                chunk_size = min(100, chunk_size * 2)
                logging.info(f"Aumentando chunk size para {chunk_size} (Memória: {mem.percent}%)")
                
        except Exception as e:
            logging.error(f"Erro no chunk {i//chunk_size + 1}: {str(e)}")
            raise

# -----------------------------------------------------------------------------
# Função para construção de query com tratamento de erros (ATUALIZADA)
# -----------------------------------------------------------------------------
def build_conversion_query(grupo: str) -> str:
    """Constrói query de conversão com sintaxe corrigida para DuckDB"""
    schema = GRUPOS_INFO[grupo]['colunas']
    selects = []
    
    for col, dtype in schema.items():
        if 'NUMERIC' in dtype:
            # Sintaxe corrigida usando TRY_CAST padrão
            selects.append(
                f"TRY_CAST({col} AS {dtype}) AS {col}, "
                f"CASE WHEN {col} IS NOT NULL AND TRY_CAST({col} AS {dtype}) IS NULL "
                f"THEN 'ERRO_NUMERICO' ELSE NULL END AS new_{col}_error"
            )
        elif 'DATE' in dtype:
            selects.append(
                f"COALESCE("
                f"TRY_CAST(strptime({col}, '%Y%m%d') AS DATE), "
                f"TRY_CAST(strptime({col}, '%d/%m/%Y') AS DATE)"
                f") AS {col}, "
                f"CASE WHEN {col} IS NOT NULL THEN 'FORMATO_INVALIDO' ELSE NULL END AS new_{col}_error"
            )
        elif 'BOOLEAN' in dtype:
            selects.append(
                f"CASE WHEN UPPER({col}) IN ('1', 'T', 'TRUE', 'V') THEN TRUE "
                f"WHEN UPPER({col}) IN ('0', 'F', 'FALSE') THEN FALSE "
                f"ELSE NULL END AS {col}, "
                f"CASE WHEN {col} IS NOT NULL AND "
                f"UPPER({col}) NOT IN ('1', 'T', 'TRUE', 'V', '0', 'F', 'FALSE') "
                f"THEN 'VALOR_BOOLEANO_INVALIDO' ELSE NULL END AS new_{col}_error"
            )
        else:
            selects.append(
                f"SUBSTR({col}, 1, 255) AS {col}, "
                f"CASE WHEN LENGTH({col}) > 255 THEN 'TAMANHO_EXCEDIDO' "
                f"WHEN {col} ~ '[^\\x00-\\x7F]' THEN 'CARACTERE_INVALIDO' "
                f"ELSE NULL END AS new_{col}_error"
            )
    
    return ", ".join(selects)

def get_cnes_column(grupo: str) -> str:
    """Obtém o nome da coluna CNES correspondente ao grupo"""
    grupo = grupo.upper()
    if grupo not in CAMPOS_CNES:
        raise ValueError(f"Grupo {grupo} não possui mapeamento de CNES")
    return CAMPOS_CNES[grupo]

def process_parquet_files(files: List[str], params: QueryParams) -> pd.DataFrame:
    """Processa arquivos Parquet com tratamento de erros detalhado"""
    try:
        # Constrói query de conversão
        conversion_query = build_conversion_query(params.grupo)
        
        # Executa query no DuckDB
        query = f"""
            SELECT {conversion_query}
            FROM read_parquet({files})
        """
        
        # Executa e converte para DataFrame
        df = duckdb.query(query).to_df()
        
        # Aplica filtros de CNES
        return apply_filters(df, params)
        
    except Exception as e:
        logging.error(f"Falha no processamento: {str(e)}")
        # Extrair nome da coluna do erro
        if "Erro na coluna" in str(e):
            col_error = str(e).split(":")[0].replace("Erro na coluna ", "")
            logging.error(f"COLUNA PROBLEMÁTICA: {col_error}")
        raise

def process_chunk_with_duckdb(chunk_files: List[str], params: QueryParams) -> pd.DataFrame:
    """Processa chunk com log detalhado de erros"""
    try:
        # Constrói query de conversão para o grupo
        conversion_query = build_conversion_query(params.grupo)
        
        # Executa query no DuckDB
        query = f"""
            SELECT {conversion_query}
            FROM read_parquet({chunk_files})
        """
        
        # Executa e retorna DataFrame processado
        return duckdb.query(query).to_df()
        
    except Exception as e:
        # Capturar detalhes do erro do DuckDB
        error_msg = str(e)
        if "Conversion Error" in error_msg:
            col_match = re.search(r'column "(.*?)"', error_msg)
            if col_match:
                col_name = col_match.group(1)
                logging.error(f"ERRO DE CONVERSÃO NA COLUNA: {col_name}")
        logging.error(f"Detalhes do erro: {error_msg}")
        raise

def export_schema(df: pd.DataFrame, table_name: str) -> str:
    """Exporta o schema do DataFrame para SQL PostgreSQL"""
    type_mapping = {
        'object': 'TEXT',
        'int64': 'BIGINT',
        'float64': 'DOUBLE PRECISION',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'DATE'
    }
    
    columns = []
    for col, dtype in df.dtypes.items():
        pg_type = type_mapping.get(str(dtype), 'TEXT')
        columns.append(f'"{col}" {pg_type}')
    
    schema_sql = f"DROP TABLE IF EXISTS {table_name};\n"
    schema_sql += f"CREATE TABLE {table_name} (\n"
    schema_sql += ",\n".join(columns)
    schema_sql += "\n);"
    
    schema_file = f"{table_name}_schema.sql"
    with open(schema_file, 'w') as f:
        f.write(schema_sql)
    
    return schema_sql

def validate_csv_sample(csv_path: str, table_name: str) -> bool:
    """Valida uma amostra do CSV contra o schema do PostgreSQL"""
    try:
        with engine.connect() as conn:
            # Criar tabela temporária
            temp_table = f"temp_{uuid4().hex[:8]}"
            conn.execute(text(f"CREATE TEMP TABLE {temp_table} (LIKE {table_name})"))
            
            # Copiar amostra
            sample_size = 1000
            conn.execute(text(
                f"COPY {temp_table} FROM PROGRAM 'head -n {sample_size} {csv_path}' "
                "WITH (FORMAT CSV, HEADER TRUE, NULL '\\N')"
            ))
            
            # Verificar consistência
            result = conn.execute(text(
                f"SELECT COUNT(*) AS errors FROM {temp_table} "
                "WHERE " + " OR ".join([f"{col} IS NULL AND new_{col}_error IS NULL" for col in GRUPOS_INFO['colunas']])
            )).fetchone()
            
            if result[0] > 0:
                logging.error(f"Erros de validação encontrados: {result[0]}")
                return False
            return True
            
    except Exception as e:
        logging.error(f"Falha na validação: {str(e)}")
        return False
