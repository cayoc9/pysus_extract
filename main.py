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
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
# from jose import JWTError, jwt
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
import logging.handlers
from concurrent.futures import ThreadPoolExecutor
from fastapi import BackgroundTasks, Request
from uuid import uuid4
import psutil
import time
import json
from io import StringIO
import traceback

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
            "sp_num_pr": "NUMERIC(3,0)",
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
    
    # Nome do arquivo base
    base_filename = os.path.join(log_dir, 'app.log')
    
    # Configurar rotação diária
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=base_filename,
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
    
    # Configurar handlers
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
                        try:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                        except Exception as e:
                            logging.error(f"Erro ao converter coluna {col} para data: {str(e)}")
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

# ===========================================================================
# Seção 2: Processamento paralelo de arquivos
# ===========================================================================
def process_single_file(file_path: str, params: QueryParams) -> pd.DataFrame:
    """Processa um único arquivo Parquet"""
    try:
        con = duckdb.connect()
        cnes_column = get_cnes_column(params.grupo)
        
        # Construir cláusula WHERE
        if params.cnes_list == ["*"]:
            where_clause = "1=1"  # Seleciona todos os registros
        else:
            cnes_str = ",".join([f"'{cnes}'" for cnes in params.cnes_list])
            where_clause = f"LOWER({cnes_column}) IN (SELECT LOWER(unnest(ARRAY[{cnes_str}])))"
        
        campos_agrupamento = params.campos_agrupamento.copy()
        
        if not any(col.lower() == cnes_column.lower() for col in campos_agrupamento):
            campos_agrupamento.insert(0, cnes_column)
        
        group_by_cols = ",".join(campos_agrupamento)
        
        query = f"""
        SELECT {group_by_cols}
        FROM read_parquet('{file_path}')
        WHERE {where_clause}
        """
        
        df = con.execute(query).df()
        con.close()
        return df
        
    except Exception as e:
        logging.error(slog(
            "Erro no processamento de arquivo",
            file_path=file_path,
            error=str(e)
        ))
        return pd.DataFrame()
    
def process_parquet_files(files: List[str], params: QueryParams) -> pd.DataFrame:
    """Processa arquivos em paralelo com ThreadPool"""
    max_workers = max(psutil.cpu_count() - 1, 1)
    logging.info(slog(
        "Iniciando processamento paralelo",
        total_files=len(files),
        max_workers=max_workers
    ))
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_single_file, file, params) for file in files]
        results = []
        
        for future in futures:
            try:
                result = future.result()
                if not result.empty:
                    results.append(result)
            except Exception as e:
                logging.error(slog(
                    "Erro no processamento paralelo",
                    error=str(e)
                ))
    
    if not results:
        return pd.DataFrame()
    
    final_df = pd.concat(results, ignore_index=True)
    final_df.columns = final_df.columns.str.lower()
    
    # Aplicar consulta personalizada se fornecida
    if params.consulta_personalizada:
        logging.info("Aplicando consulta personalizada")
        
        try:
            con = duckdb.connect()
            # Registra o DataFrame como uma view temporária
            con.register('temp_df', final_df)
            
            # Executa a consulta personalizada
            result = con.execute(params.consulta_personalizada).df()
            
            # Verifica se o resultado não está vazio
            if not result.empty:
                final_df = result
            else:
                logging.warning("Consulta personalizada retornou resultados vazios")
            
            con.close()
            
        except Exception as e:
            logging.error(f"Erro na execução da consulta personalizada: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail=f"Erro na consulta personalizada: {str(e)}"
            )
    
    return final_df

# ===========================================================================
# Seção 3: Endpoints assíncronos e paginação
# ===========================================================================
async_jobs = {}

# Crie a instância app primeiro
app = FastAPI(title="DataSUS API")

@app.post("/query/async", tags=["Async Operations"])
async def async_query(
    params: QueryParams,
    background_tasks: BackgroundTasks
) -> Dict[str, str]:
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
            
            save_results(result_df, table_name)
            
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
    return async_jobs.get(job_id, {"error": "Job não encontrado"})

@app.post("/query", tags=["Main"])
async def query_data(
    params: QueryParams,
    page: int = Query(1, ge=1),
    page_size: int = Query(1000, ge=1, le=10000),
    response_model=Dict[str, Any]
):
    try:
        start_time = time.monotonic()
        
        files = get_parquet_files(
            params.base,
            params.grupo,
            params.competencia_inicio,
            params.competencia_fim
        )
        
        if not files:
            raise HTTPException(status_code=404, detail="Nenhum arquivo encontrado")
        
        result_df = process_parquet_files(files, params)
        
        if result_df.empty:
            raise HTTPException(status_code=404, detail="Nenhum dado encontrado")
        
        grupo_mapped = grupos_dict.get(params.grupo, params.grupo)
        table_name = params.table_name or f"{params.base.lower()}_{grupo_mapped.lower()}"
        save_results(result_df, table_name)
        
        total_records = len(result_df)
        total_pages = (total_records + page_size - 1) // page_size
        paginated_data = result_df[(page-1)*page_size : page*page_size].to_dict(orient='records')
        
        logging.info(slog(
            "Consulta concluída",
            duration=time.monotonic() - start_time,
            total_records=total_records,
            page=page,
            page_size=page_size
        ))
        
        return {
            "data": paginated_data,
            "pagination": {
                "total": total_records,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages
            },
            "metadata": {
                "table_name": table_name,
                "columns": list(result_df.columns),
                "schema": GRUPOS_INFO.get(params.grupo, {}).get("colunas", {})
            }
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logging.error(slog(
            "Erro na consulta",
            error=str(e),
            stack_trace=traceback.format_exc()
        ))
        raise HTTPException(status_code=500, detail="Erro interno no processamento")

# ===========================================================================
# Seção 4: Otimização do salvamento no PostgreSQL
# ===========================================================================
def save_results(df: pd.DataFrame, table_name: str) -> None:
    """Salva resultados usando COPY para melhor performance"""
    try:
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        
        engine = create_engine(
            f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        )
        
        with engine.connect() as conn:
            # Criar tabela se não existir
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join([f'{col} {dtype}' for col, dtype in GRUPOS_INFO[params.grupo]['colunas'].items()])}
            )
            """
            conn.execute(text(create_table_sql))
            
            # Copiar dados
            copy_sql = f"""
            COPY {table_name} FROM STDIN WITH (
                FORMAT CSV,
                HEADER true,
                DELIMITER ','
            )
            """
            conn.execute(text(copy_sql), {"data": buffer.getvalue()})
            
        logging.info(slog(
            "Dados salvos com sucesso",
            table_name=table_name,
            row_count=len(df)
        ))
        
    except Exception as e:
        logging.error(slog(
            "Erro ao salvar dados",
            error=str(e),
            table_name=table_name
        ))
        raise HTTPException(status_code=500, detail=f"Erro ao salvar dados: {str(e)}")

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
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    logging.info(slog(
        "Request processed",
        url=str(request.url),
        method=request.method,
        process_time=process_time,
        status_code=response.status_code
    ))
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
