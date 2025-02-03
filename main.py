import logging
import os
import datetime
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import duckdb
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
# Removido temporariamente:
# from fastapi.security i\mport HTTPBearer, HTTPAuthorizationCredentials
# from jose import JWTError, jwt
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
import logging.handlers
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import BackgroundTasks, Request
from uuid import uuid4
import psutil
import time
import json
from io import StringIO
import traceback
from resource import getrusage, RUSAGE_SELF
import threading
from queue import PriorityQueue
from asyncio import Semaphore
import gc
from fastapi.responses import JSONResponse
import io
import csv
import numpy as np
from collections import defaultdict

# Adicionar no início com outras declarações globais
task_lock = threading.Lock()

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
            "sp_pf_doc": "NUMERIC(11,0)",
            "sp_pj_doc": "NUMERIC(14,0)",
            "in_tp_val": "NUMERIC(1,0)",
            "sequencia": "NUMERIC(5,0)",
            "remessa": "NUMERIC(6,0)",
            "serv_cla": "NUMERIC(4,0)",
            "sp_cidpri": "TEXT",
            "sp_cidsec": "TEXT",
            "sp_qt_proc": "NUMERIC(5,0)",
            "sp_u_aih": "NUMERIC(2,0)"
        }
    }
}

# ---------------------------------------------------------------------------
# Configurações básicas
# ---------------------------------------------------------------------------
load_dotenv()

# Configurações de logging atualizadas
def setup_logging():
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    base_filename = os.path.join(log_dir, 'app.log')
    
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=base_filename,
        when='midnight',
        interval=1,
        backupCount=7,
        encoding='utf-8'
    )
    file_handler.suffix = "%Y-%m-%d.log"
    file_handler.namer = lambda name: name.replace(".log", "") + ".log"
    
    formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    
    # Mover a configuração para dentro da função
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, logging.StreamHandler()],
        force=True
    )

# Chamar a configuração no início da execução
setup_logging()

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
    cnes_list: List[str] = Field(..., min_length=1)
    campos_agrupamento: List[str] = Field(..., min_length=1)
    competencia_inicio: str
    competencia_fim: str
    table_name: Optional[str] = None
    consulta_personalizada: Optional[str] = Field(
        None,
        description="Consulta SQL personalizada a ser aplicada após o processamento inicial"
    )
    
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
        
        current_year = datetime.now().year
        month, year = map(int, v.split('/'))
        if year < 1990 or year > current_year:
            raise ValueError(f'Ano deve estar entre 1990 e {current_year}')
        
        if month < 1 or month > 12:
            raise ValueError('Mês deve estar entre 01 e 12')
        
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
def get_schema_info(grupo: str) -> dict:
    """Busca schema com tratamento de case e fallback"""
    grupo = grupo.strip().upper()
    
    # Tentativa 1: Match exato
    if grupo in GRUPOS_INFO:
        return GRUPOS_INFO[grupo]
    
    # Tentativa 2: Match parcial
    for key in GRUPOS_INFO:
        if key.startswith(grupo):
            return GRUPOS_INFO[key]
    
    # Fallback e logging
    logging.warning(f"Schema padrão utilizado para {grupo}")
    return {}

def create_error_columns(df, grupo):
    schema = get_schema_info(grupo)
    error_cols = {}
    
    for col in schema['colunas']:
        error_col = f'erro_{col}'
        error_cols[error_col] = pd.Series(dtype='object')
    
    return df.assign(**error_cols)

def convert_datatypes(df: pd.DataFrame, grupo: str) -> pd.DataFrame:
    schema = get_schema_info(grupo)
    error_stats = defaultdict(int)
    
    # Log inicial do DataFrame
    logging.debug(f"[convert_datatypes] Shape inicial: {df.shape}")
    logging.debug(f"[convert_datatypes] Colunas: {df.columns.tolist()}")
    
    # Touchpoint 1: Antes da conversão
    for col in df.columns:
        if col.startswith('erro_'):
            continue
            
        schema_type = schema['colunas'].get(col, '').upper()
        logging.debug(f"[convert_datatypes] Processando coluna {col} (tipo: {schema_type})")
        
        # Log de amostra antes da conversão
        sample_before = df[col].head().tolist()
        logging.debug(f"[convert_datatypes] Amostra antes ({col}): {sample_before}")
        
        if any(nt in schema_type for nt in ['NUMERIC', 'INTEGER', 'SMALLINT']):
            # Touchpoint 2: Limpeza de whitespace
            df[col] = df[col].replace(r'^\s*$', pd.NA, regex=True)
            logging.debug(f"[convert_datatypes] Após limpeza whitespace ({col}): {df[col].head().tolist()}")
            
            # Touchpoint 3: Conversão numérica
            original = df[col].copy()
            df[col] = pd.to_numeric(df[col], errors='coerce')
            logging.debug(f"[convert_datatypes] Após conversão numérica ({col}): {df[col].head().tolist()}")
            
            # Touchpoint 4: Registro de erros
            mask = df[col].isna() & original.notna()
            if mask.any():
                error_col = f'erro_{col}'
                df[error_col] = np.where(mask, original.astype(str), pd.NA)
                error_count = mask.sum()
                error_stats[col] += error_count
                logging.debug(f"[convert_datatypes] Erros encontrados em {col}: {error_count}")
                logging.debug(f"[convert_datatypes] Exemplos de valores com erro: {original[mask].head().tolist()}")

    # Log final de estatísticas
    logging.info(f"[convert_datatypes] Estatísticas de conversão: {dict(error_stats)}")
    return df

def get_cnes_column(grupo: str) -> str:
    if isinstance(grupo, (list, tuple)):
        grupo = grupo[0] if len(grupo) > 0 else "CNES"
    
    grupo_str = str(grupo).strip().upper()
    return CAMPOS_CNES.get(grupo_str, "CNES")

# ===========================================================================
# Seção 0: Controle de Performance
# ===========================================================================
# Configurações de paralelismo
MAX_CONCURRENT_SYNC = 1
MAX_CONCURRENT_ASYNC = 2
sync_semaphore = Semaphore(MAX_CONCURRENT_SYNC)
async_semaphore = Semaphore(MAX_CONCURRENT_ASYNC)

# Na Seção 0: Controle de Performance
MAX_MEMORY = 12  # Total de RAM disponível em GB
SAFE_THRESHOLD = 0.7  # 70% de uso seguro
CRITICAL_THRESHOLD = 0.9  # 90% para modo emergencial

MAX_THREADS = 4  # Baseado nos 12GB RAM e 40GHz CPU

def get_system_load():
    mem = psutil.virtual_memory()
    return {
        'memory_percent': mem.percent,
        'memory_free_gb': mem.available / (1024**3),
        'cpu_percent': psutil.cpu_percent(interval=1)
    }

def calculate_max_workers():
    mem = psutil.virtual_memory().available / (1024**3)  # Memória livre em GB
    safe_workers = min(
        int(mem // 0.5),  # 0.5GB por worker
        int(40 * 0.7 // 2.5),  # 2.5GHz por worker
        MAX_THREADS
    )
    return max(1, safe_workers)

# Fila prioritária de tarefas
task_queue = PriorityQueue()

def process_worker():
    while True:
        with task_lock:
            if not task_queue.empty():
                priority, task = task_queue.get()
                execute_task(task)
        time.sleep(1)

# Iniciar workers
for _ in range(calculate_max_workers()):
    threading.Thread(target=process_worker, daemon=True).start()

# ===========================================================================
# Seção 2: Processamento paralelo de arquivos
# ===========================================================================
def adjust_chunk_size(total_files: int) -> int:
    mem = psutil.virtual_memory()
    free_mem_gb = mem.available / (1024**3)
    
    # Estimativa conservadora (200MB por arquivo)
    safe_size = int((free_mem_gb * 0.8) / 0.2)  # 80% da memória livre
    
    return max(50, min(
        safe_size,
        200,  # Máximo absoluto
        total_files // 10  # Mínimo 10% dos arquivos
    ))

def process_parquet_files(files: List[str], params: QueryParams) -> pd.DataFrame:
    chunk_size = adjust_chunk_size(len(files))
    results = []
    
    for i in range(0, len(files), chunk_size):
        check_memory_usage(0.65)
        chunk = files[i:i+chunk_size]
        
        # Processar chunk com DuckDB
        df = process_chunk_with_duckdb(chunk, params)
        results.append(df)
        
        cleanup_memory(df)
        log_resource_usage()
    
    # Filtrar resultados válidos
    valid_results = [df for df in results if not df.empty]
    
    if not valid_results:
        return pd.DataFrame()
    
    return pd.concat(valid_results, ignore_index=True)

def process_chunk_with_duckdb(files: List[str], params: QueryParams) -> pd.DataFrame:
    try:
        # Touchpoint 9: Antes do processamento
        logging.debug(f"[process_chunk] Processando {len(files)} arquivos")
        
        con = duckdb.connect()
        cnes_column = get_cnes_column(params.grupo).upper()
        
        # Corrigir formatação dos arquivos
        formatted_files = ', '.join([f"'{f}'" for f in files])
        
        query = f"""
        SELECT *
        FROM read_parquet([{formatted_files}])
        {f"WHERE {cnes_column} IN {tuple(params.cnes_list)}" if params.cnes_list != ["*"] else ""}
        """
        
        # Executar query e normalizar colunas
        df = con.execute(query).df()
        df.columns = df.columns.str.lower()  # Conversão para minúsculas
        
        # Touchpoint 10: Após processamento DuckDB
        logging.debug(f"[process_chunk] Resultado DuckDB - Shape: {df.shape}")
        logging.debug(f"[process_chunk] Colunas: {df.columns.tolist()}")
        logging.debug(f"[process_chunk] Tipos de dados: {df.dtypes.to_dict()}")
        
        return optimize_data_types(df, params.grupo, params)
    
    except Exception as e:
        logging.error(f"[process_chunk] Erro no processamento: {str(e)}", exc_info=True)
        raise
    finally:
        con.close()
    
def cleanup_memory(df: pd.DataFrame):
    del df
    gc.collect()
    time.sleep(0.1)

def process_chunk(files: List[str], params: QueryParams) -> pd.DataFrame:
    try:
        max_workers = min(4, len(files))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_single_file, file, params) for file in files]
            return pd.concat([future.result() for future in as_completed(futures)])
    except Exception as e:
        logging.error(f"Erro no processamento do chunk: {str(e)}")
        return pd.DataFrame()
    
def check_memory_usage(threshold=0.7):
    mem = psutil.virtual_memory()
    if mem.percent > threshold * 100:
        logging.warning(f"Uso de memória: {mem.percent}%")
        gc.collect()
        time.sleep(1)
        mem = psutil.virtual_memory()
        if mem.percent > threshold * 100:
            raise MemoryError(f"Memória insuficiente ({mem.used/1024**3:.1f}GB usado)")

def process_single_file(file_path: str, params: QueryParams) -> pd.DataFrame:
    try:
        check_memory_usage()
        
        # Conexão DuckDB
        con = duckdb.connect()
        cnes_column = get_cnes_column(params.grupo).upper()  # Garantir maiúsculas
        
        # Query otimizada
        query = f"""
        SELECT *
        FROM read_parquet('{file_path}', hive_partitioning=1)
        WHERE {cnes_column} IN ({','.join([f"'{cnes}'" for cnes in params.cnes_list])})
        """ if params.cnes_list != ["*"] else f"""
        SELECT * FROM read_parquet('{file_path}', hive_partitioning=1)
        """
        
        # Execução com controle de memória
        df = con.execute(query).df()
        con.close()
        
        # Normalizar nomes de colunas
        df.columns = df.columns.str.upper()
        
        # Nova etapa de limpeza
        df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)
        df = optimize_data_types(df, params.grupo, params)
        
        return df
        
    except duckdb.CatalogException as e:
        logging.error(f"Coluna {cnes_column} não encontrada em {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Erro em {file_path}: {str(e)}")
        return pd.DataFrame()

def optimize_data_types(df: pd.DataFrame, grupo: str, params: QueryParams) -> pd.DataFrame:
    """Processa apenas colunas relevantes"""
    relevant_cols = {
        col.lower() 
        for col in params.campos_agrupamento + [get_cnes_column(grupo)]
    }
    
    for col in df.columns:
        col_lower = col.lower()
        if col_lower not in relevant_cols:
            continue
        
        # ... existing conversion logic with error columns ...
    
    return df

def apply_filters(df: pd.DataFrame, params: QueryParams) -> pd.DataFrame:
    if params.cnes_list != ["*"]:
        cnes_column = get_cnes_column(params.grupo).upper()  # Forçar maiúsculas
        df.columns = df.columns.str.upper()  # Normalizar colunas
        
        if cnes_column not in df.columns:
            raise KeyError(f"Coluna {cnes_column} não encontrada no DataFrame")
        
        return df[df[cnes_column].isin(params.cnes_list)]
    return df

# ===========================================================================
# Seção 3: Endpoints assíncronos e paginação
# ===========================================================================
async_jobs = {}

# Crie a instância app primeiro
app = FastAPI(title="DataSUS API")

def format_response(df: pd.DataFrame) -> dict:
    """Formata a resposta da API com metadados"""
    return {
        "data": df.to_dict(orient='records'),
        "metadata": {
            "row_count": len(df),
            "columns": list(df.columns),
            "dtypes": str(df.dtypes.to_dict()),
            "generated_at": datetime.now().isoformat()
        }
    }

@app.post("/query", tags=["Main"])
async def query_data(params: QueryParams, background_tasks: BackgroundTasks):
    files = get_parquet_files(params.base, params.grupo, params.competencia_inicio, params.competencia_fim)
    
    if not files:
        raise HTTPException(404, "Nenhum arquivo encontrado")
    
    if len(files) > 1000:  # Processamento assíncrono para grandes volumes
        task_id = str(uuid4())
        task_manager.add_task((files, params), priority=1)
        return {"task_id": task_id, "status": "queued"}
    
    # Processamento síncrono para pequenos volumes
    result = process_parquet_files(files, params)
    return format_response(result)

@app.post("/query/async", tags=["Async Operations"])
async def async_query(
    params: QueryParams,
    background_tasks: BackgroundTasks
) -> Dict[str, str]:
    async with async_semaphore:
        load = get_system_load()
        priority = 1 if load['memory_free_gb'] < 6 else 5
        add_task_with_priority((params, background_tasks), priority)
        
        job_id = str(uuid4())
        async_jobs[job_id] = {
            "status": "processing", 
            "start_time": datetime.now().isoformat(),
            "progress": 0
        }
        
        def task_processor():
            try:
                files = get_parquet_files(
                    params.base,
                    params.grupo,
                    params.competencia_inicio,
                    params.competencia_fim
                )
                
                total_files = len(files)
                async_jobs[job_id]["total_files"] = total_files
                
                result_df = process_parquet_files(files, params)
                
                grupo_mapped = grupos_dict.get(params.grupo, params.grupo)
                table_name = params.table_name if params.table_name else f"{params.base.lower()}_{grupo_mapped.lower()}"
                
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

# ===========================================================================
# Seção 4: Otimização do salvamento no PostgreSQL
# ===========================================================================
def memory_safe_operation(func):
    def wrapper(*args, **kwargs):
        soft_limit = MAX_MEMORY * SAFE_THRESHOLD
        hard_limit = MAX_MEMORY * CRITICAL_THRESHOLD
        
        current_mem = getrusage(RUSAGE_SELF).ru_maxrss / (1024**2)
        
        if current_mem > soft_limit:
            gc.collect()
            if current_mem > hard_limit:
                raise MemoryError("Limite crítico de memória atingido")
        
        return func(*args, **kwargs)
    return wrapper

# Decorar funções críticas
@memory_safe_operation
def save_results(df: pd.DataFrame, table_name: str, params: QueryParams):
    try:
        # Log inicial com amostra dos dados
        logging.info(f"Iniciando salvamento na tabela {table_name}")
        logging.info(f"Total de registros: {len(df)}")
        logging.info(f"Amostra de dados (primeiras 3 linhas):\n{df.head(3).to_dict('records')}")
        
        # Validação e limpeza dos dados antes do COPY
        df = validate_data_for_postgres(df)
        
        # Preparar dados para COPY
        csv_buffer = io.StringIO()
        
        # Adicionar mais padrões de limpeza
        df = df.replace({
            'nan': pd.NA,
            'None': pd.NA,
            'null': pd.NA,
            '\\N': pd.NA,
            'NULL': pd.NA,
            '': pd.NA,
            '        ': pd.NA,
            '00000000': pd.NA
        })
        
        # Configuração especial para to_csv para lidar com nulos
        df.to_csv(
            csv_buffer,
            index=False,
            header=False,
            na_rep='\\N',  # Formato PostgreSQL para NULL
            quoting=csv.QUOTE_MINIMAL,
            escapechar='\\',
            date_format='%Y-%m-%d'  # Formato PostgreSQL para datas
        )
        
        csv_buffer.seek(0)
        
        # Conexão e COPY
        engine = create_engine(
            f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        )
        
        with engine.connect() as connection:
            with connection.connection.cursor() as cursor:
                # Verificar se a tabela existe
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = %s
                    )
                """, (table_name,))
                
                table_exists = cursor.fetchone()[0]
                
                if table_exists:
                    # Nova etapa: Limpar dados existentes
                    logging.info(f"Limpando dados existentes na tabela {table_name}")
                    cursor.execute(f"TRUNCATE TABLE {table_name}")
                    connection.connection.commit()
                else:
                    logging.info(f"Tabela {table_name} não existe. Criando...")
                    
                    grupo = params.grupo.upper()
                    schema_info = GRUPOS_INFO.get(grupo, {}).get('colunas', {})
                    
                    if not schema_info:
                        raise ValueError(f"Schema não encontrado para o grupo {grupo}")
                    
                    columns_def = []
                    for col in df.columns:
                        # Usar nome original mantendo o case do schema
                        original_col = col.upper()  # Ajuste para match com GRUPOS_INFO
                        col_type = schema_info.get(original_col, 'TEXT')
                        
                        # Adicionar escaping para nomes reservados
                        columns_def.append(f'"{col}" {col_type}')  # Aspas duplas para case-sensitive

                    # Query de criação com IF NOT EXISTS
                    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {','.join(columns_def)}
                    )
                    """
                    
                    cursor.execute(create_table_sql)
                    connection.connection.commit()
                    logging.info(f"Tabela {table_name} criada com sucesso")

                # Comando COPY com tratamento de nulos (existente)
                # ... (restante do código existente)

    except Exception as e:
        logging.error(f"Erro crítico no save_results: {str(e)}", exc_info=True)
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()

def validate_data_for_postgres(df: pd.DataFrame) -> pd.DataFrame:
    """Valida e limpa dados antes do COPY"""
    
    logging.info(f"Iniciando validação de dados para PostgreSQL")
    logging.info(f"Shape inicial: {df.shape}")
    
    # Primeiro passo: Limpeza inicial de valores problemáticos
    df = df.replace({
        'nan': pd.NA,
        'None': pd.NA,
        'null': pd.NA,
        '\\N': pd.NA,
        'NULL': pd.NA,
        '': pd.NA,
        '        ': pd.NA,
        '00000000': pd.NA
    })
    
    # Mapeamento de tipos baseado no schema do SIH
    type_mapping = {
        'sp_gestor': 'Int64',      # Numérico 6 dígitos
        'sp_uf': 'Int64',          # Numérico 2 dígitos
        'sp_aa': 'Int64',          # Ano (4 dígitos)
        'sp_mm': 'Int64',          # Mês (2 dígitos)
        'sp_cnes': 'Int64',        # CNES (7 dígitos)
        'sp_naih': 'Int64',        # AIH (13 dígitos)
        'sp_procrea': 'string',    # Código procedimento
        'sp_dtinter': 'datetime64[ns]',
        'sp_dtsaida': 'datetime64[ns]',
        'sp_num_pr': 'string',
        'sp_tipo': 'string',
        'sp_cpfcgc': 'string',     # CPF/CNPJ
        'sp_atoprof': 'string',    # Código ato
        'sp_tp_ato': 'string',
        'sp_qtd_ato': 'Int64',     # Quantidade
        'sp_ptsp': 'Int64',
        'sp_nf': 'string',
        'sp_valato': 'float64',    # Valor monetário
        'sp_m_hosp': 'Int64',      # Município
        'sp_m_pac': 'Int64',       # Município
        'sp_des_hos': 'Int64',
        'sp_des_pac': 'Int64',
        'sp_complex': 'string',
        'sp_financ': 'string',
        'sp_co_faec': 'string',
        'sp_pf_cbo': 'string',     # CBO
        'sp_pf_doc': 'string',     # Documento
        'sp_pj_doc': 'string',     # CNPJ
        'in_tp_val': 'Int64',
        'sequencia': 'Int64',
        'remessa': 'string',
        'serv_cla': 'string',
        'sp_cidpri': 'string',     # CID
        'sp_cidsec': 'string',     # CID
        'sp_qt_proc': 'Int64',
        'sp_u_aih': 'Int64'
    }
    
    # Converter tipos com tratamento de erros
    for col, dtype in type_mapping.items():
        if col not in df.columns:
            continue
            
        try:
            if dtype == 'datetime64[ns]':
                # Tratamento especial para datas
                df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
            elif dtype in ['Int64', 'float64']:
                # Limpar strings antes da conversão numérica
                df[col] = df[col].str.strip() if isinstance(df[col], pd.Series) else df[col]
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if dtype == 'Int64':
                    df[col] = df[col].astype('Int64')  # Tipo nullable integer
            else:
                # Strings: remover espaços e padronizar nulos
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({'nan': pd.NA, 'None': pd.NA})
                
            # Log apenas se houver valores nulos
            null_count = df[col].isna().sum()
            if null_count > 0:
                logging.info(f"Coluna {col}: {null_count} valores nulos após conversão para {dtype}")
                
        except Exception as e:
            logging.error(f"Erro ao converter coluna {col} para {dtype}: {str(e)}")
            # Manter coluna original em caso de erro
            continue
    
    logging.info(f"Validação concluída. Shape final: {df.shape}")
    return df

# ===========================================================================
# Seção 5: Middleware de monitoramento de performance
# ===========================================================================
class StructuredMessage:
    def __init__(self, message, **kwargs):
        self.message = message
        self.kwargs = kwargs
    
    def __str__(self):
        return json.dumps({**self.kwargs, "message": self.message})

def s_logger():
    return StructuredMessage

slog = s_logger()

# Middleware de monitoramento de performance
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
        return await call_next(request)
    except MemoryError:
        return JSONResponse(
            status_code=500,
            content={"error": "out_of_memory", "message": "Memória insuficiente"}
        )

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

def add_task_with_priority(task, priority=5):
    with task_lock:
        task_queue.put((priority, task))

def execute_task(task):
    try:
        params, background_tasks = task
        # Implementação do processamento assíncrono
    except Exception as e:
        logging.error(f"Erro na execução da tarefa: {e}")

def adaptive_processing(files, params):
    try:
        return process_parquet_files(files, params)
    except MemoryError:
        logging.warning("Memória insuficiente, ativando modo seguro")
        return process_in_chunks(files, params, chunk_size=1000)

def process_in_chunks(files, params, chunk_size):
    for i in range(0, len(files), chunk_size):
        chunk = files[i:i+chunk_size]
        yield process_parquet_files(chunk, params)
        gc.collect()

class TaskManager:
    def __init__(self):
        self.queue = PriorityQueue()
        self.lock = threading.Lock()
        self.active = False
    
    def start_workers(self):
        self.active = True
        for _ in range(4):  # 4 workers fixos
            threading.Thread(target=self.process_queue, daemon=True).start()
    
    def add_task(self, task, priority=5):
        with self.lock:
            self.queue.put((priority, task))
    
    def process_queue(self):
        while self.active:
            with self.lock:
                if not self.queue.empty():
                    priority, task = self.queue.get()
                    self.execute_task(task)
            time.sleep(0.5)
    
    def execute_task(self, task):
        try:
            files, params = task
            process_parquet_files(files, params)
        except Exception as e:
            logging.error(f"Task failed: {str(e)}")

task_manager = TaskManager()
task_manager.start_workers()

def log_resource_usage():
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent()
    logging.info(
        f"Resource Usage - Memory: {mem.percent}% ({mem.used/1024**3:.1f}GB/"
        f"{mem.total/1024**3:.1f}GB), CPU: {cpu}%"
    )

def check_memory(threshold=0.7):
    mem = psutil.virtual_memory()
    if mem.percent > threshold * 100:
        logging.warning("Limite de memória excedido, iniciando coleta de lixo")
        gc.collect()
        if mem.percent > threshold * 100:
            raise MemoryError(f"Uso de memória acima de {threshold*100}%")

# Configuração do logger
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Definir file_handler antes de usar
file_handler = logging.FileHandler(os.path.join(LOG_DIR, "api.log"))
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())

def validate_columns(df: pd.DataFrame, schema: dict):
    schema_cols = set(col.lower() for col in schema.get('colunas', {}))
    df_cols = set(df.columns.str.lower())
    
    missing = schema_cols - df_cols
    if missing:
        logging.warning(f"Colunas faltando no DataFrame: {missing}")
    
    extra = df_cols - schema_cols
    if extra:
        logging.info(f"Colunas extras detectadas: {extra}")

def validate_numeric_columns(df: pd.DataFrame, schema: dict):
    for col, dtype in schema['colunas'].items():
        if 'NUMERIC' in dtype:
            invalid = df[col].apply(lambda x: not str(x).strip().isdigit() if pd.notna(x) else False)
            if invalid.any():
                logging.warning(f"Valores não numéricos na coluna {col}: {df[col][invalid].tolist()}")

def get_numeric_columns(grupo: str) -> list:
    schema = GRUPOS_INFO[grupo]["colunas"]
    return [
        col.lower() for col, dtype in schema.items()
        if any(nt in dtype.upper() for nt in ['NUMERIC', 'INT', 'SMALLINT', 'BIGINT'])
    ]
