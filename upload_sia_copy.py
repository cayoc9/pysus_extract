import os
import re
import logging
import pandas as pd
import io
import psutil
from datetime import datetime
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

file_handler = logging.FileHandler(os.path.join(LOG_DIR, f"upload_sia_{os.getpid()}.log"))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Configuração do banco de dados
DATABASE_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)

BASE_PATH = "parquet_files/SIA/"
GRUPOS_INFO = {
    # "AB": {
    #     "tabela": "sia_apac_cirurgia_bariatrica",
    #     "colunas":  {
    #         "ab_anoacom": "TEXT",
    #         "ab_dtcirg2": "DATE",
    #         "ab_dtcirur": "DATE",
    #         "ab_imc": "TEXT",
    #         "ab_mesacom": "TEXT",
    #         "ab_numaih": "TEXT",
    #         "ab_numaih2": "CHAR(13)",
    #         "ab_pontbar": "CHAR(1)",
    #         "ab_prcaih2": "CHAR(10)",
    #         "ab_prcaih3": "CHAR(10)",
    #         "ab_procaih": "CHAR(10)",
    #         "ab_tabbarr": "CHAR(1)",
    #         "ap_alta": "BOOLEAN",
    #         "ap_apacant": "TEXT",
    #         "ap_autoriz": "CHAR(13)",
    #         "ap_catend": "TEXT",
    #         "ap_ceppcn": "CHAR(8)",
    #         "ap_cidcas": "TEXT",
    #         "ap_cidpri": "TEXT",
    #         "ap_cidsec": "TEXT",
    #         "ap_cmp": "CHAR(6)",
    #         "ap_cnpjcpf": "CHAR(14)",
    #         "ap_cnpjmnt": "CHAR(14)",
    #         "ap_cnspcn": "CHAR(15)",
    #         "ap_codemi": "CHAR(10)",
    #         "ap_coduni": "TEXT",
    #         "ap_coidade": "CHAR(1)",
    #         "ap_condic": "CHAR(2)",
    #         "ap_dtaut": "DATE",
    #         "ap_dtfim": "DATE",
    #         "ap_dtinic": "DATE",
    #         "ap_dtocor": "DATE",
    #         "ap_dtsolic": "DATE",
    #         "ap_encerr": "BOOLEAN",
    #         "ap_etnia": "CHAR(4)",
    #         "ap_gestao": "CHAR(6)",
    #         "ap_mn_ind": "CHAR(1)",
    #         "ap_mndif": "BOOLEAN",
    #         "ap_motsai": "CHAR(2)",
    #         "ap_munpcn": "CHAR(6)",
    #         "ap_mvm": "CHAR(6)",
    #         "ap_natjur": "CHAR(4)",
    #         "ap_nuidade": "CHAR(2)",
    #         "ap_obito": "BOOLEAN",
    #         "ap_perman": "BOOLEAN",
    #         "ap_pripal": "TEXT",
    #         "ap_racacor": "TEXT",
    #         "ap_sexo": "CHAR(1)",
    #         "ap_tippre": "TEXT",
    #         "ap_tpapac": "CHAR(1)",
    #         "ap_tpaten": "TEXT",
    #         "ap_tpups": "TEXT",
    #         "ap_transf": "BOOLEAN",
    #         "ap_ufdif": "BOOLEAN",
    #         "ap_ufmun": "CHAR(6)",
    #         "ap_ufnacio": "TEXT",
    #         "ap_unisol": "TEXT",
    #         "ap_vl_ap": "CHAR(20)",
    #         "id_log": "VARCHAR(255)",
    #         "new_ab_dtcirg2": "TEXT",
    #         "uf": "CHAR(2)"
    #     }
    # },
    # "ABO": {
    #     "tabela": "sia_apac_acompanhamento_pos_cirurgia_bariatrica",
    #     "colunas": {
    #         "ab_anoacom": "TEXT",
    #         "ab_dtcirg2": "DATE",
    #         "ab_dtcirur": "DATE",
    #         "ab_imc": "CHAR(3)",
    #         "ab_mesacom": "TEXT",
    #         "ab_numaih": "CHAR(13)",
    #         "ab_numaih2": "CHAR(13)",
    #         "ab_pontbar": "CHAR(1)",
    #         "ab_prcaih2": "CHAR(10)",
    #         "ab_prcaih3": "CHAR(10)",
    #         "ab_prcaih4": "CHAR(10)",
    #         "ab_prcaih5": "CHAR(10)",
    #         "ab_prcaih6": "CHAR(10)",
    #         "ab_procaih": "TEXT",
    #         "ab_t_prc2": "CHAR(3)",
    #         "ab_t_prc3": "CHAR(3)",
    #         "ab_t_prc4": "CHAR(3)",
    #         "ab_t_prc5": "CHAR(3)",
    #         "ab_t_prc6": "CHAR(3)",
    #         "ab_tabbarr": "CHAR(1)",
    #         "ap_adesao": "BOOLEAN",
    #         "ap_alta": "BOOLEAN",
    #         "ap_apacan": "TEXT",
    #         "ap_atv_fis": "BOOLEAN",
    #         "ap_autoriz": "CHAR(13)",
    #         "ap_catend": "TEXT",
    #         "ap_ceppcn": "TEXT",
    #         "ap_cid_c1": "CHAR(4)",
    #         "ap_cid_c2": "CHAR(4)",
    #         "ap_cid_c3": "CHAR(4)",
    #         "ap_cid_c4": "CHAR(4)",
    #         "ap_cid_c5": "CHAR(4)",
    #         "ap_cid_co": "CHAR(4)",
    #         "ap_cidcas": "TEXT",
    #         "ap_cmp": "CHAR(6)",
    #         "ap_cnpjcpf": "TEXT",
    #         "ap_cnpjmnt": "CHAR(14)",
    #         "ap_cnspcn": "CHAR(15)",
    #         "ap_codemi": "CHAR(10)",
    #         "ap_coduni": "CHAR(7)",
    #         "ap_coidade": "CHAR(1)",
    #         "ap_comorb": "CHAR(1)",
    #         "ap_condic": "CHAR(2)",
    #         "ap_dtaut": "DATE",
    #         "ap_dtfim": "DATE",
    #         "ap_dtinic": "DATE",
    #         "ap_dtoocor": "DATE",
    #         "ap_dtsolic": "DATE",
    #         "ap_encerr": "BOOLEAN",
    #         "ap_etnia": "CHAR(4)",
    #         "ap_gestao": "CHAR(6)",
    #         "ap_medicam": "BOOLEAN",
    #         "ap_mn_ind": "CHAR(1)",
    #         "ap_mndif": "BOOLEAN",
    #         "ap_motsai": "CHAR(2)",
    #         "ap_munpcn": "CHAR(6)",
    #         "ap_mvm": "CHAR(6)",
    #         "ap_natjur": "CHAR(4)",
    #         "ap_nuidade": "CHAR(2)",
    #         "ap_obito": "BOOLEAN",
    #         "ap_perman": "BOOLEAN",
    #         "ap_polivit": "BOOLEAN",
    #         "ap_pripal": "TEXT",
    #         "ap_racacor": "TEXT",
    #         "ap_reg_pes": "BOOLEAN",
    #         "ap_sexo": "CHAR(1)",
    #         "ap_tpapac": "CHAR(1)",
    #         "ap_tpatend": "TEXT",
    #         "ap_tppre": "TEXT",
    #         "ap_tpups": "TEXT",
    #         "ap_transf": "BOOLEAN",
    #         "ap_ufdif": "BOOLEAN",
    #         "ap_ufmun": "CHAR(6)",
    #         "ap_ufnacio": "TEXT",
    #         "ap_unisol": "TEXT",
    #         "ap_vl_ap": "CHAR(20)",
    #         "co_cidprim": "TEXT",
    #         "co_cidsec": "TEXT",
    #         "id_log": "VARCHAR(255)",
    #         "uf": "CHAR(2)"
    #     }
    # },
    # "ACF": {
    #     "tabela": "sia_apac_confeccao_de_fistula",
    #     "colunas": {
    #         "acf_artdia": "TEXT",
    #         "acf_duplex": "CHAR(1)",
    #         "acf_flebit": "CHAR(1)",
    #         "acf_fremit": "CHAR(1)",
    #         "acf_hemato": "CHAR(1)",
    #         "acf_prefav": "CHAR(1)",
    #         "acf_pulso": "CHAR(1)",
    #         "acf_usocat": "CHAR(1)",
    #         "acf_veiavi": "CHAR(1)",
    #         "acf_veidia": "TEXT",
    #         "ap_alta": "BOOLEAN",
    #         "ap_apacant": "TEXT",
    #         "ap_autoriz": "CHAR(13)",
    #         "ap_catend": "TEXT",
    #         "ap_ceppcn": "TEXT",
    #         "ap_cidcas": "TEXT",
    #         "ap_cidpri": "TEXT",
    #         "ap_cidsec": "TEXT",
    #         "ap_cmp": "CHAR(6)",
    #         "ap_cnpjcpf": "TEXT",
    #         "ap_cnpjmnt": "CHAR(14)",
    #         "ap_cnspcn": "CHAR(15)",
    #         "ap_codemi": "CHAR(10)",
    #         "ap_coduni": "CHAR(7)",
    #         "ap_coidade": "CHAR(1)",
    #         "ap_condic": "CHAR(2)",
    #         "ap_dtaut": "DATE",
    #         "ap_dtfim": "DATE",
    #         "ap_dtinic": "DATE",
    #         "ap_dtocor": "DATE",
    #         "ap_dtsolic": "DATE",
    #         "ap_encerr": "BOOLEAN",
    #         "ap_etnia": "CHAR(4)",
    #         "ap_gestao": "CHAR(6)",
    #         "ap_mn_ind": "CHAR(1)",
    #         "ap_mndif": "BOOLEAN",
    #         "ap_motsai": "CHAR(2)",
    #         "ap_munpcn": "CHAR(6)",
    #         "ap_mvm": "CHAR(6)",
    #         "ap_natjur": "CHAR(4)",
    #         "ap_nuidade": "CHAR(2)",
    #         "ap_obito": "BOOLEAN",
    #         "ap_perman": "BOOLEAN",
    #         "ap_pripal": "TEXT",
    #         "ap_racacor": "TEXT",
    #         "ap_sexo": "CHAR(1)",
    #         "ap_tippre": "TEXT",
    #         "ap_tpapac": "CHAR(1)",
    #         "ap_tpaten": "CHAR(2)",
    #         "ap_tpups": "TEXT",
    #         "ap_transf": "BOOLEAN",
    #         "ap_ufdif": "BOOLEAN",
    #         "ap_ufmun": "CHAR(6)",
    #         "ap_ufnacio": "TEXT",
    #         "ap_unisol": "CHAR(7)",
    #         "ap_vl_ap": "CHAR(20)",
    #         "id_log": "VARCHAR(255)",
    #         "new_ap_dtaut": "TEXT",
    #         "uf": "CHAR(2)"
    #     }
    # },

    "AD": {
        "tabela": "sia_apac_laudos_diversos",
        "colunas": {
            "ap_alta": "BOOLEAN",
            "ap_apacant": "TEXT",
            "ap_autoriz": "BIGINT",
            "ap_catend": "TEXT",
            "ap_ceppcn": "TEXT",
            "ap_cidcas": "TEXT",
            "ap_cidpri": "TEXT",
            "ap_cidsec": "TEXT",
            "ap_cmp": "INTEGER",
            "ap_cnpjcpf": "TEXT",
            "ap_cnpjmnt": "TEXT",
            "ap_cnspcn": "CHAR(15)",
            "ap_codemi": "CHAR(10)",
            "ap_coduni": "TEXT",
            "ap_coidade": "SMALLINT",
            "ap_condic": "CHAR(2)",
            "ap_dtaut": "DATE",
            "new_ap_dtaut": "TEXT",
            "ap_dtfim": "DATE",
            "new_ap_dtfim": "TEXT",
            "ap_dtinic": "DATE",
            "new_ap_dtinic": "TEXT",
            "ap_dtocor": "DATE",
            "new_ap_dtocor": "TEXT",
            "ap_dtsolic": "DATE",
            "new_ap_dtsolic": "TEXT",
            "ap_encerr": "BOOLEAN",
            "ap_etnia": "TEXT",
            "ap_gestao": "INTEGER",
            "ap_mn_ind": "CHAR(1)",
            "ap_mndif": "BOOLEAN",
            "ap_motsai": "SMALLINT",
            "ap_munpcn": "INTEGER",
            "ap_mvm": "INTEGER",
            "ap_natjur": "CHAR(4)",
            "ap_nuidade": "TEXT",
            "ap_obito": "BOOLEAN",
            "ap_perman": "BOOLEAN",
            "ap_pripal": "TEXT",
            "ap_racacor": "TEXT",
            "ap_sexo": "CHAR(1)",
            "ap_tippre": "TEXT",
            "ap_tpapac": "SMALLINT",
            "ap_tpaten": "TEXT",
            "ap_tpups": "TEXT",
            "ap_transf": "BOOLEAN",
            "ap_ufdif": "BOOLEAN",
            "ap_ufmun": "INTEGER",
            "ap_ufnacio": "TEXT",
            "ap_unisol": "TEXT",
            "ap_vl_ap": "TEXT",
            "id_log": "VARCHAR(255)",
            "uf": "CHAR(2)"
        }
    },
    "AM": {
        "tabela": "sia_apac_medicamentos",
        "colunas": {
            "am_altura": "TEXT",
            "am_gestant": "CHAR(1)",
            "am_peso": "TEXT",
            "am_qtdtran": "TEXT",
            "am_transpl": "CHAR(1)",
            "ap_alta": "BOOLEAN",
            "ap_apacant": "TEXT",
            "ap_autoriz": "BIGINT",
            "ap_catend": "TEXT",
            "ap_ceppcn": "TEXT",
            "ap_cidcas": "TEXT",
            "ap_cidpri": "CHAR(4)",
            "ap_cidsec": "TEXT",
            "ap_cmp": "INTEGER",
            "ap_cnpjcpf": "TEXT",
            "ap_cnpjmnt": "TEXT",
            "ap_cnspcn": "CHAR(15)",
            "ap_codemi": "CHAR(10)",
            "ap_coduni": "TEXT",
            "ap_coidade": "SMALLINT",
            "ap_condic": "CHAR(2)",
            "ap_dtaut": "DATE",
            "new_ap_dtaut": "TEXT",
            "ap_dtfim": "DATE",
            "new_ap_dtfim": "TEXT",
            "ap_dtinic": "DATE",
            "new_ap_dtinic": "TEXT",
            "ap_dtocor": "DATE",
            "new_ap_dtocor": "TEXT",
            "ap_dtsolic": "DATE",
            "new_ap_dtsolic": "TEXT",
            "ap_encerr": "BOOLEAN",
            "ap_etnia": "TEXT",
            "ap_gestao": "INTEGER",
            "ap_mn_ind": "CHAR(1)",
            "ap_mndif": "BOOLEAN",
            "ap_motsai": "SMALLINT",
            "ap_munpcn": "INTEGER",
            "ap_mvm": "INTEGER",
            "ap_natjur": "TEXT",
            "ap_nuidade": "TEXT",
            "ap_obito": "BOOLEAN",
            "ap_perman": "BOOLEAN",
            "ap_pripal": "TEXT",
            "ap_racacor": "TEXT",
            "ap_sexo": "CHAR(1)",
            "ap_tippre": "TEXT",
            "ap_tpapac": "SMALLINT",
            "ap_tpaten": "TEXT",
            "ap_tpups": "TEXT",
            "ap_transf": "BOOLEAN",
            "ap_ufdif": "BOOLEAN",
            "ap_ufmun": "INTEGER",
            "ap_ufnacio": "TEXT",
            "ap_unisol": "TEXT",
            "ap_vl_ap": "NUMERIC",
            "id_log": "VARCHAR(255)",
            "uf": "CHAR(2)"
        }
    },
    # "AMP": {
    #     "tabela": "sia_apac_acompanhamento_multiprofissional",
    #     "colunas": {
    #         "amp_acevas": "CHAR(1)",
    #         "amp_albumi": "TEXT",
    #         "amp_caract": "CHAR(1)",
    #         "amp_dtcli": "DATE",
    #         "amp_dtini": "DATE",
    #         "amp_fosfor": "CHAR(4)",
    #         "amp_hb": "CHAR(4)",
    #         "amp_hbsag": "CHAR(1)",
    #         "amp_hcv": "CHAR(1)",
    #         "amp_hiv": "CHAR(1)",
    #         "amp_interc": "CHAR(1)",
    #         "amp_ktvsem": "TEXT",
    #         "amp_maisne": "CHAR(1)",
    #         "amp_pth": "TEXT",
    #         "amp_seapto": "CHAR(1)",
    #         "amp_seperi": "CHAR(1)",
    #         "amp_sitini": "CHAR(1)",
    #         "amp_sittra": "CHAR(1)",
    #         "amp_tru": "TEXT",
    #         "ap_alta": "BOOLEAN",
    #         "ap_apacant": "TEXT",
    #         "ap_autoriz": "CHAR(13)",
    #         "ap_catend": "TEXT",
    #         "ap_ceppcn": "CHAR(8)",
    #         "ap_cidcas": "TEXT",
    #         "ap_cidpri": "TEXT",
    #         "ap_cidsec": "TEXT",
    #         "ap_cmp": "CHAR(6)",
    #         "ap_cnpjcpf": "TEXT",
    #         "ap_cnpjmnt": "CHAR(14)",
    #         "ap_cnspcn": "CHAR(15)",
    #         "ap_codemi": "CHAR(10)",
    #         "ap_coduni": "CHAR(7)",
    #         "ap_coidade": "CHAR(1)",
    #         "ap_condic": "CHAR(2)",
    #         "ap_dtaut": "DATE",
    #         "ap_dtfim": "DATE",
    #         "ap_dtinic": "DATE",
    #         "ap_dtocor": "DATE",
    #         "ap_dtsolic": "DATE",
    #         "ap_encerr": "BOOLEAN",
    #         "ap_etnia": "CHAR(4)",
    #         "ap_gestao": "CHAR(6)",
    #         "ap_mn_ind": "CHAR(1)",
    #         "ap_mndif": "BOOLEAN",
    #         "ap_motsai": "CHAR(2)",
    #         "ap_munpcn": "CHAR(6)",
    #         "ap_mvm": "CHAR(6)",
    #         "ap_natjur": "CHAR(4)",
    #         "ap_nuidade": "CHAR(2)",
    #         "ap_obito": "BOOLEAN",
    #         "ap_perman": "BOOLEAN",
    #         "ap_pripal": "TEXT",
    #         "ap_racacor": "TEXT",
    #         "ap_sexo": "CHAR(1)",
    #         "ap_tippre": "TEXT",
    #         "ap_tpapac": "CHAR(1)",
    #         "ap_tpaten": "CHAR(2)",
    #         "ap_tpups": "TEXT",
    #         "ap_transf": "BOOLEAN",
    #         "ap_ufdif": "BOOLEAN",
    #         "ap_ufmun": "CHAR(6)",
    #         "ap_ufnacio": "TEXT",
    #         "ap_unisol": "TEXT",
    #         "ap_vl_ap": "CHAR(20)",
    #         "id_log": "VARCHAR(255)",
    #         "new_amp_dtcli": "TEXT",
    #         "new_amp_dtini": "TEXT",
    #         "uf": "CHAR(2)"
    #     }},
    # "AN": {
    #     "tabela": "sia_apac_nefrologia",
    #     "colunas": {
    #         "an_acevas": "CHAR(1)",
    #         "an_albumi": "TEXT",
    #         "an_altura": "TEXT",
    #         "an_cncdo": "CHAR(1)",
    #         "an_diures": "TEXT",
    #         "an_dtpdr": "DATE",
    #         "an_glicos": "TEXT",
    #         "an_hb": "TEXT",
    #         "an_hbsag": "CHAR(1)",
    #         "an_hcv": "CHAR(1)",
    #         "an_hiv": "CHAR(1)",
    #         "an_intfis": "TEXT",
    #         "an_peso": "TEXT",
    #         "an_tru": "TEXT",
    #         "an_ulsoab": "CHAR(1)",
    #         "ap_alta": "BOOLEAN",
    #         "ap_apacant": "TEXT",
    #         "ap_autoriz": "CHAR(13)",
    #         "ap_catend": "TEXT",
    #         "ap_ceppcn": "TEXT",
    #         "ap_cidcas": "TEXT",
    #         "ap_cidpri": "CHAR(4)",
    #         "ap_cidsec": "TEXT",
    #         "ap_cmp": "CHAR(6)",
    #         "ap_cnpjcpf": "CHAR(14)",
    #         "ap_cnpjmnt": "TEXT",
    #         "ap_cnspcn": "CHAR(15)",
    #         "ap_codemi": "CHAR(10)",
    #         "ap_coduni": "CHAR(7)",
    #         "ap_coidade": "CHAR(1)",
    #         "ap_condic": "CHAR(2)",
    #         "ap_dtaut": "DATE",
    #         "ap_dtfim": "DATE",
    #         "ap_dtinic": "DATE",
    #         "ap_dtocor": "DATE",
    #         "ap_dtsolic": "DATE",
    #         "ap_encerr": "BOOLEAN",
    #         "ap_etnia": "CHAR(4)",
    #         "ap_gestao": "CHAR(6)",
    #         "ap_mn_ind": "CHAR(1)",
    #         "ap_mndif": "BOOLEAN",
    #         "ap_motsai": "CHAR(2)",
    #         "ap_munpcn": "CHAR(6)",
    #         "ap_mvm": "CHAR(6)",
    #         "ap_nuidade": "TEXT",
    #         "ap_obito": "BOOLEAN",
    #         "ap_perman": "BOOLEAN",
    #         "ap_pripal": "TEXT",
    #         "ap_racacor": "TEXT",
    #         "ap_sexo": "CHAR(1)",
    #         "ap_tippre": "CHAR(2)",
    #         "ap_tpapac": "CHAR(1)",
    #         "ap_tpaten": "TEXT",
    #         "ap_tpups": "TEXT",
    #         "ap_transf": "BOOLEAN",
    #         "ap_ufdif": "BOOLEAN",
    #         "ap_ufmun": "CHAR(6)",
    #         "ap_ufnacio": "TEXT",
    #         "ap_unisol": "CHAR(7)",
    #         "ap_vl_ap": "CHAR(20)",
    #         "id_log": "VARCHAR(255)",
    #         "uf": "CHAR(2)"
    #     }
    # },
    # "AQ": {
    #     "tabela": "sia_apac_quimioterapia",
    #     "colunas": {
    #         "ap_alta": "BOOLEAN",
    #         "ap_apacant": "TEXT",
    #         "ap_autoriz": "CHAR(13)",
    #         "ap_catend": "TEXT",
    #         "ap_ceppcn": "TEXT",
    #         "ap_cidcas": "TEXT",
    #         "ap_cidpri": "CHAR(4)",
    #         "ap_cidsec": "TEXT",
    #         "ap_cmp": "CHAR(6)",
    #         "ap_cnpjcpf": "CHAR(14)",
    #         "ap_cnpjmnt": "CHAR(14)",
    #         "ap_cnspcn": "CHAR(15)",
    #         "ap_codemi": "CHAR(10)",
    #         "ap_coduni": "TEXT",
    #         "ap_coidade": "CHAR(1)",
    #         "ap_condic": "CHAR(2)",
    #         "ap_dtaut": "DATE",
    #         "ap_dtfim": "DATE",
    #         "ap_dtinic": "DATE",
    #         "ap_dtocor": "DATE",
    #         "ap_dtsolic": "DATE",
    #         "ap_encerr": "BOOLEAN",
    #         "ap_etnia": "TEXT",
    #         "ap_gestao": "CHAR(6)",
    #         "ap_mn_ind": "CHAR(1)",
    #         "ap_mndif": "BOOLEAN",
    #         "ap_motsai": "CHAR(2)",
    #         "ap_munpcn": "CHAR(6)",
    #         "ap_mvm": "CHAR(6)",
    #         "ap_natjur": "CHAR(4)",
    #         "ap_nuidade": "CHAR(2)",
    #         "ap_obito": "BOOLEAN",
    #         "ap_perman": "BOOLEAN",
    #         "ap_pripal": "TEXT",
    #         "ap_racacor": "TEXT",
    #         "ap_sexo": "CHAR(1)",
    #         "ap_tippre": "TEXT",
    #         "ap_tpapac": "CHAR(1)",
    #         "ap_tpaten": "TEXT",
    #         "ap_tpups": "TEXT",
    #         "ap_transf": "BOOLEAN",
    #         "ap_ufdif": "BOOLEAN",
    #         "ap_ufmun": "CHAR(6)",
    #         "ap_ufnacio": "TEXT",
    #         "ap_unisol": "TEXT",
    #         "ap_vl_ap": "CHAR(12)",
    #         "aq_cid10": "CHAR(4)",
    #         "aq_cidini1": "CHAR(4)",
    #         "aq_cidini2": "CHAR(4)",
    #         "aq_cidini3": "CHAR(4)",
    #         "aq_conttr": "CHAR(1)",
    #         "aq_dtiden": "DATE",
    #         "aq_dtini1": "DATE",
    #         "aq_dtini2": "DATE",
    #         "aq_dtini3": "DATE",
    #         "aq_dtintr": "DATE",
    #         "aq_esqu_p1": "CHAR(5)",
    #         "aq_esqu_p2": "CHAR(10)",
    #         "aq_estadi": "TEXT",
    #         "aq_grahis": "TEXT",
    #         "aq_linfin": "TEXT",
    #         "aq_med01": "TEXT",
    #         "aq_med02": "TEXT",
    #         "aq_med03": "TEXT",
    #         "aq_med04": "TEXT",
    #         "aq_med05": "TEXT",
    #         "aq_med06": "TEXT",
    #         "aq_med07": "TEXT",
    #         "aq_med08": "TEXT",
    #         "aq_med09": "TEXT",
    #         "aq_med10": "TEXT",
    #         "aq_totmau": "TEXT",
    #         "aq_totmpl": "TEXT",
    #         "aq_trante": "TEXT",
    #         "id_log": "VARCHAR(255)",
    #         "uf": "CHAR(2)"
    #     }
    # },
    # "AR": {
    #     "tabela": "sia_apac_radioterapia",
    #     "colunas": {
    #         "ap_alta": "BOOLEAN",
    #         "ap_apacant": "TEXT",
    #         "ap_autoriz": "CHAR(13)",
    #         "ap_catend": "TEXT",
    #         "ap_ceppcn": "TEXT",
    #         "ap_cidcas": "TEXT",
    #         "ap_cidpri": "CHAR(4)",
    #         "ap_cidsec": "TEXT",
    #         "ap_cmp": "CHAR(6)",
    #         "ap_cnpjcpf": "CHAR(14)",
    #         "ap_cnpjmnt": "CHAR(14)",
    #         "ap_cnspcn": "CHAR(15)",
    #         "ap_codemi": "CHAR(10)",
    #         "ap_coduni": "CHAR(7)",
    #         "ap_coidade": "CHAR(1)",
    #         "ap_condic": "CHAR(2)",
    #         "ap_dtaut": "DATE",
    #         "ap_dtfim": "DATE",
    #         "ap_dtinic": "DATE",
    #         "ap_dtocor": "DATE",
    #         "ap_dtsolic": "DATE",
    #         "ap_encerr": "BOOLEAN",
    #         "ap_etnia": "CHAR(4)",
    #         "ap_gestao": "CHAR(6)",
    #         "ap_mn_ind": "CHAR(1)",
    #         "ap_mndif": "BOOLEAN",
    #         "ap_motsai": "CHAR(2)",
    #         "ap_munpcn": "CHAR(6)",
    #         "ap_mvm": "CHAR(6)",
    #         "ap_natjur": "CHAR(4)",
    #         "ap_nuidade": "CHAR(2)",
    #         "ap_obito": "BOOLEAN",
    #         "ap_perman": "BOOLEAN",
    #         "ap_pripal": "TEXT",
    #         "ap_racacor": "TEXT",
    #         "ap_sexo": "CHAR(1)",
    #         "ap_tippre": "TEXT",
    #         "ap_tpapac": "CHAR(1)",
    #         "ap_tpaten": "TEXT",
    #         "ap_tpups": "TEXT",
    #         "ap_transf": "BOOLEAN",
    #         "ap_ufdif": "BOOLEAN",
    #         "ap_ufmun": "CHAR(6)",
    #         "ap_ufnacio": "TEXT",
    #         "ap_unisol": "TEXT",
    #         "ap_vl_ap": "CHAR(20)",
    #         "ar_cid10": "CHAR(4)",
    #         "ar_cidini1": "CHAR(4)",
    #         "ar_cidini2": "CHAR(4)",
    #         "ar_cidini3": "CHAR(4)",
    #         "ar_cidtr1": "DATE",
    #         "ar_cidtr2": "DATE",
    #         "ar_cidtr3": "DATE",
    #         "ar_conttr": "CHAR(1)",
    #         "ar_dtiden": "DATE",
    #         "ar_dtini1": "DATE",
    #         "ar_dtini2": "DATE",
    #         "ar_dtini3": "DATE",
    #         "ar_dtintr": "DATE",
    #         "ar_estadi": "TEXT",
    #         "ar_fimar1": "CHAR(8)",
    #         "ar_fimar2": "CHAR(8)",
    #         "ar_fimar3": "CHAR(8)",
    #         "ar_finali": "CHAR(1)",
    #         "ar_grahis": "TEXT",
    #         "ar_iniar1": "CHAR(8)",
    #         "ar_iniar2": "CHAR(8)",
    #         "ar_iniar3": "CHAR(8)",
    #         "ar_linfin": "TEXT",
    #         "ar_numc1": "CHAR(3)",
    #         "ar_numc2": "CHAR(3)",
    #         "ar_numc3": "CHAR(3)",
    #         "ar_smrd": "CHAR(3)",
    #         "ar_trante": "CHAR(1)",
    #         "id_log": "VARCHAR(255)",
    #         "new_ap_dtaut": "TEXT",
    #         "new_ap_dtsolic": "TEXT",
    #         "new_ar_cidtr1": "TEXT",
    #         "new_ar_cidtr2": "TEXT",
    #         "new_ar_cidtr3": "TEXT",
    #         "new_ar_dtini1": "TEXT",
    #         "new_ar_dtini2": "TEXT",
    #         "new_ar_dtini3": "TEXT",
    #         "uf": "CHAR(2)"
    #     }
    # },
    
    "ATD": {
        "tabela": "sia_apac_tratamento_dialitico",
        "colunas": {
            "ap_alta": "BOOLEAN",
            "ap_apacant": "TEXT",
            "ap_autoriz": "BIGINT",
            "ap_catend": "TEXT",
            "ap_ceppcn": "TEXT",
            "ap_cidcas": "TEXT",
            "ap_cidpri": "TEXT",
            "ap_cidsec": "TEXT",
            "ap_cmp": "INTEGER",
            "ap_cnpjcpf": "TEXT",
            "ap_cnpjmnt": "TEXT",
            "ap_cnspcn": "CHAR(15)",
            "ap_codemi": "CHAR(10)",
            "ap_coduni": "TEXT",
            "ap_coidade": "SMALLINT",
            "ap_condic": "CHAR(2)",
            "ap_dtaut": "DATE",
            "new_ap_dtaut": "TEXT",
            "ap_dtfim": "DATE",
            "new_ap_dtfim": "TEXT",
            "ap_dtinic": "DATE",
            "new_ap_dtinic": "TEXT",
            "ap_dtocor": "DATE",
            "new_ap_dtocor": "TEXT",
            "ap_dtsolic": "DATE",
            "new_ap_dtsolic": "TEXT",
            "ap_encerr": "BOOLEAN",
            "ap_etnia": "TEXT",
            "ap_gestao": "INTEGER",
            "ap_mn_ind": "CHAR(1)",
            "ap_mndif": "BOOLEAN",
            "ap_motsai": "SMALLINT",
            "ap_munpcn": "INTEGER",
            "ap_mvm": "INTEGER",
            "ap_natjur": "SMALLINT",
            "ap_nuidade": "TEXT",
            "ap_obito": "BOOLEAN",
            "ap_perman": "BOOLEAN",
            "ap_pripal": "TEXT",
            "ap_racacor": "TEXT",
            "ap_sexo": "CHAR(1)",
            "ap_tippre": "TEXT",
            "ap_tpapac": "SMALLINT",
            "ap_tpaten": "SMALLINT",
            "ap_tpups": "TEXT",
            "ap_transf": "BOOLEAN",
            "ap_ufdif": "BOOLEAN",
            "ap_ufmun": "INTEGER",
            "ap_ufnacio": "TEXT",
            "ap_unisol": "TEXT",
            "ap_vl_ap": "NUMERIC",
            "atd_acevas": "TEXT",
            "atd_albumi": "TEXT",
            "atd_caract": "TEXT",
            "atd_dtcli": "DATE",
            "atd_dtpdr": "DATE",
            "atd_fosfor": "TEXT",
            "atd_hb": "TEXT",
            "atd_hbsag": "CHAR(1)",
            "atd_hcv": "CHAR(1)",
            "atd_hiv": "CHAR(1)",
            "atd_interc": "CHAR(1)",
            "atd_ktvsem": "TEXT",
            "atd_maisne": "TEXT",
            "atd_pth": "TEXT",
            "atd_seapto": "TEXT",
            "atd_seperi": "CHAR(1)",
            "atd_sitini": "TEXT",
            "atd_sittra": "TEXT",
            "atd_tru": "TEXT",
            "id_log": "VARCHAR(255)",
            "uf": "CHAR(2)"
        }
    },
    "BI": {
         "tabela": "sia_boletim_producao_ambulatorial_individualizado",
         "colunas": {
            "autoriz": "TEXT",
            "catend": "TEXT",
            "cboprof": "TEXT",
            "cidpri": "TEXT",
            "cnpj_cc": "TEXT",
            "cnpjcpf": "TEXT",
            "cnpjmnt": "TEXT",
            "cns_pac": "CHAR(15)",
            "cnsprof": "TEXT",
            "coduni": "TEXT",
            "complex": "SMALLINT",
            "condic": "CHAR(2)",
            "dt_atend": "TEXT",
            "dt_process": "TEXT",
            "dtnasc": "DATE",
            "new_dtnasc": "TEXT",
            "etnia": "TEXT",
            "gestao": "INTEGER",
            "id_log": "VARCHAR(255)",
            "idadepac": "TEXT",
            "mn_ind": "CHAR(1)",
            "mndif": "SMALLINT",
            "munpac": "INTEGER",
            "nat_jur": "TEXT",
            "proc_id": "TEXT",
            "qt_apres": "SMALLINT",
            "qt_aprov": "SMALLINT",
            "racacor": "TEXT",
            "sexopac": "TEXT",
            "subfin": "TEXT",
            "tippre": "TEXT",
            "tpfin": "TEXT",
            "tpidadepac": "SMALLINT",
            "tpups": "TEXT",
            "uf": "CHAR(2)",
            "ufdif": "SMALLINT",
            "ufmun": "TEXT",
            "vl_apres": "NUMERIC",
            "vl_aprov": "NUMERIC"
        }
    },
    "PS": {
        "tabela": "raas_psicossocial",
        "colunas": {
            "catend": "TEXT",
            "cidassoc": "CHAR(4)",
            "cidpri": "CHAR(4)",
            "cnes_esf": "TEXT",
            "cnes_exec": "TEXT",
            "cnpjcpf": "TEXT",
            "cnpjmnt": "TEXT",
            "cns_pac": "CHAR(15)",
            "cob_esf": "CHAR(1)",
            "condic": "CHAR(2)",
            "destinopac": "TEXT",
            "dt_atend": "TEXT",
            "dt_fim": "DATE",
            "new_dt_fim": "TEXT",
            "dt_inicio": "DATE",
            "new_dt_inicio": "TEXT",
            "dt_motcob": "DATE",
            "new_dt_motcob": "TEXT",
            "dt_process": "TEXT",
            "dtnasc": "DATE",
            "new_dtnasc": "TEXT",
            "etnia": "TEXT",
            "fim": "TEXT",
            "gestao": "INTEGER",
            "id_log": "VARCHAR(255)",
            "idadepac": "TEXT",
            "inicio": "TEXT",
            "loc_realiz": "CHAR(1)",
            "mn_ind": "CHAR(1)",
            "mot_cob": "TEXT",
            "munpac": "TEXT",
            "nacion_pac": "TEXT",
            "nat_jur": "SMALLINT",
            "origem_pac": "TEXT",
            "pa_class_s": "TEXT",
            "pa_proc_id": "TEXT",
            "pa_qtdapr": "SMALLINT",
            "pa_qtdpro": "SMALLINT",
            "pa_srv": "SMALLINT",
            "permanen": "TEXT",
            "qtdate": "BOOLEAN",
            "qtdpcn": "BOOLEAN",
            "racacor": "TEXT",
            "sexopac": "CHAR(1)",
            "sit_rua": "CHAR(3)",
            "tippre": "TEXT",
            "tp_droga": "CHAR(3)",
            "tpidadepac": "SMALLINT",
            "tpups": "TEXT",
            "uf": "CHAR(2)",
            "ufmun": "INTEGER"
        }
    },
    # "PA": {
    #     "tabela": "sia_producao_ambulatorial",
    #     "colunas":{
    #         "id_log": "CHAR(255)",
    #         "idademax": "SMALLINT",
    #         "idademin": "SMALLINT",
    #         "nu_pa_tot": "NUMERIC",
    #         "nu_vpa_tot": "NUMERIC",
    #         "pa_alta": "SMALLINT",
    #         "pa_autoriz": "CHAR(13)",
    #         "pa_catend": "CHAR(2)",
    #         "pa_cbocod": "CHAR(6)",
    #         "pa_cidcas": "CHAR(4)",
    #         "pa_cidpri": "CHAR(4)",
    #         "pa_cidsec": "CHAR(4)",
    #         "pa_cmp": "CHAR(6)",
    #         "pa_cnpj_cc": "CHAR(14)",
    #         "pa_cnpjcpf": "VARCHAR(14)",
    #         "pa_cnpjmnt": "CHAR(14)",
    #         "pa_cnsmed": "CHAR(15)",
    #         "pa_codoco": "SMALLINT",
    #         "pa_coduni": "INTEGER",
    #         "pa_condic": "CHAR(2)",
    #         "pa_dif_val": "NUMERIC",
    #         "pa_docorig": "CHAR(1)",
    #         "pa_encerr": "SMALLINT",
    #         "pa_etnia": "CHAR(4)",
    #         "pa_fler": "SMALLINT",
    #         "pa_flidade": "SMALLINT",
    #         "pa_flqt": "CHAR(1)",
    #         "pa_gestao": "INTEGER",
    #         "pa_idade": "SMALLINT",
    #         "pa_incout": "CHAR(4)",
    #         "pa_incurg": "CHAR(4)",
    #         "pa_indica": "SMALLINT",
    #         "pa_ine": "VARCHAR(10)",
    #         "pa_mn_ind": "CHAR(1)",
    #         "pa_mndif": "SMALLINT",
    #         "pa_motsai": "CHAR(2)",
    #         "pa_munpcn": "CHAR(6)",
    #         "pa_mvm": "CHAR(6)",
    #         "pa_nat_jur": "CHAR(4)",
    #         "pa_nivcpl": "SMALLINT",
    #         "pa_obito": "SMALLINT",
    #         "pa_perman": "SMALLINT",
    #         "pa_proc_id": "CHAR(10)",
    #         "pa_qtdapr": "SMALLINT",
    #         "pa_qtdpro": "INTEGER",
    #         "pa_racacor": "CHAR(2)",
    #         "pa_regct": "CHAR(4)",
    #         "pa_sexo": "CHAR(1)",
    #         "pa_srv_c": "CHAR(6)",
    #         "pa_subfin": "CHAR(4)",
    #         "pa_tippre": "CHAR(2)",
    #         "pa_tpfin": "CHAR(2)",
    #         "pa_tpups": "CHAR(2)",
    #         "pa_transf": "SMALLINT",
    #         "pa_ufdif": "SMALLINT",
    #         "pa_ufmun": "INTEGER",
    #         "pa_valapr": "NUMERIC",
    #         "pa_valpro": "NUMERIC",
    #         "pa_vl_cf": "NUMERIC",
    #         "pa_vl_cl": "NUMERIC",
    #         "pa_vl_inc": "NUMERIC",
    #         "uf": "CHAR(2)"
    #     }
    # }
 
}




ESTADOS = [
    "AC",  "AL",  "AP",  "AM",
    "BA",  "CE",  "DF",   "ES",   "GO",
    "MA",  "MT",   "MS",   "MG",
    "PA",  "PB",  "PR",
    "PE",  "PI",  "RJ",   "RN",   "RS",
    "RO",  "RR",  "SC",
    "SP",  "SE",  "TO"
]

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

def ultimo_dia_mes(ano, mes):
    from calendar import monthrange
    return monthrange(ano, mes)[1]

def tentar_corrigir_data(valor):
    # Valor no formato YYYYMMDD
    # Se "00000000": retorna None indicando para nulificar.
    if valor == "00000000":
        return None, True  # None date, True = precisamos criar new_col

    ano_str = valor[0:4]
    mes_str = valor[4:6]
    dia_str = valor[6:8]

    if not (ano_str.isdigit() and mes_str.isdigit() and dia_str.isdigit()):
        # Não é todos dígitos, retorna None sem corrigir.
        return None, False

    ano = int(ano_str)
    mes = int(mes_str)
    dia = int(dia_str)

    # Tenta converter diretamente
    def data_valida(a, m, d):
        try:
            datetime(a, m, d)
            return True
        except ValueError:
            return False

    if data_valida(ano, mes, dia):
        return datetime(ano, mes, dia).date(), False
    else:
        # Tentamos decrementar o dia até achar uma data válida
        # Se não achar, tentamos ajustar mês, ano etc.
        # Estratégia: diminuir o dia até encontrar data válida.
        # Se dia < 1, retroceder um mês.
        # Se mês < 1, retroceder um ano.
        tentativas = 0
        while tentativas < 5000:  # um limite arbitrário para não loopar infinito
            dia -= 1
            if dia < 1:
                mes -= 1
                if mes < 1:
                    mes = 12
                    ano -= 1
                    if ano < 1900:  # limite
                        break
                dia = ultimo_dia_mes(ano, mes)
            if data_valida(ano, mes, dia):
                return datetime(ano, mes, dia).date(), True
            tentativas += 1

        # Se chegou aqui, não encontrou data válida
        return None, True

def converter_tipos(df, mapeamento_tipos):
    """
    Converte os tipos das colunas do DataFrame com base no mapeamento fornecido.
    Para colunas do tipo DATE, verifica se o conteúdo está no formato YYYYMMDD.
    Se estiver, converte para YYYY-MM-DD. Caso contrário, seta como NULL e salva o valor original na coluna new_{coluna}.
    
    Args:
        df (pd.DataFrame): DataFrame a ser convertido.
        mapeamento_tipos (dict): Mapeamento de colunas para seus tipos desejados.
    
    Returns:
        pd.DataFrame: DataFrame com as colunas convertidas.
    """
    for col, tipo in mapeamento_tipos.items():
        if col not in df.columns:
            continue

        tipo_upper = tipo.strip().upper()

        if tipo_upper == "DATE":
            # Nome da coluna de backup
            new_col = f"new_{col}"
            
            # Inicializa a coluna de backup com None
            df[new_col] = None

            # Expressão regular para verificar formato YYYYMMDD
            pattern = re.compile(r'^\d{8}$')

            # Máscara para valores que correspondem ao padrão YYYYMMDD
            valid_mask = df[col].astype(str).str.match(pattern)

            # Tenta converter os valores válidos para datetime
            df.loc[valid_mask, col] = pd.to_datetime(
                df.loc[valid_mask, col], format='%Y%m%d', errors='coerce'
            ).dt.date

            # Identifica valores inválidos (não correspondem ao padrão ou falharam na conversão)
            invalid_mask = ~valid_mask | df[col].isna()

            # Salva os valores originais que são inválidos na coluna de backup
            df.loc[invalid_mask, new_col] = df.loc[invalid_mask, col].astype(str)

            # Define os valores inválidos na coluna original como NULL
            df.loc[invalid_mask, col] = None

        else:
            # Tratamento para outros tipos de dados
            try:
                if tipo_upper.startswith("VARCHAR") or tipo_upper.startswith("CHAR") or tipo_upper == "TEXT":
                    df[col] = df[col].astype(str).str.strip()
                elif tipo_upper in ["INTEGER", "BIGINT", "SMALLINT"]:
                    if tipo_upper == "SMALLINT":
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int16')
                    elif tipo_upper == "INTEGER":
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int32')
                    elif tipo_upper == "BIGINT":
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                elif tipo_upper.startswith("NUMERIC"):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif tipo_upper == "BOOLEAN":
                    df[col] = df[col].map({
                        True: True, False: False, 
                        'True': True, 'False': False, 
                        '1': True, '0': False
                    })
                else:
                    # Fallback para tipos não especificados
                    df[col] = df[col].astype(str).str.strip()
            except Exception as e:
                raise TypeError(f"Erro ao converter coluna '{col}' para o tipo '{tipo}': {e}")

    return df

def normalizar_colunas(df, colunas_esperadas, mapeamento_tipos):
    """
    Normaliza as colunas do DataFrame:
    - Remove colunas indesejadas.
    - Normaliza os nomes das colunas para minúsculas.
    - Adiciona colunas ausentes com valor None.
    - Adiciona colunas de backup para colunas do tipo DATE.

    Args:
        df (pd.DataFrame): DataFrame a ser normalizado.
        colunas_esperadas (list): Lista de nomes de colunas esperadas.
        mapeamento_tipos (dict): Mapeamento de colunas para seus tipos desejados.

    Returns:
        pd.DataFrame: DataFrame normalizado.
    """
    # Remover colunas id, uf, id_log se existirem
    df = df.drop(columns=['id', 'uf', 'id_log'], errors='ignore')

    # Normalizar nomes das colunas do DF
    df.columns = [c.lower() for c in df.columns]

    # Adicionar colunas ausentes
    colunas_para_normalizar = [col for col in colunas_esperadas if col not in ('uf', 'id_log')]
    for coluna in colunas_para_normalizar:
        col_lower = coluna.lower()
        if col_lower not in df.columns:
            logger.warning(f"Adicionando coluna ausente: {col_lower}")
            df[col_lower] = None
            # Se a coluna for do tipo DATE, adicionar a coluna de backup
            if mapeamento_tipos[coluna].strip().upper() == "DATE":
                new_col = f"new_{col_lower}"
                logger.warning(f"Adicionando coluna de backup: {new_col}")
                df[new_col] = None

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
    """
    Carrega os dados em lotes para upload, tratando colunas do tipo DATE conforme necessário.

    Args:
        grupo (str): Grupo de dados a ser processado.
        pastas_de_arquivos (list): Lista de pastas contendo arquivos .parquet.
        tamanho_lote (int): Tamanho de cada lote para upload.

    Yields:
        pd.DataFrame: Lote de dados a ser inserido no banco.
    """
    tabela = GRUPOS_INFO[grupo]["tabela"]
    arquivos_processados = verificar_ultimo_arquivo_processado(tabela)
    colunas_esperadas_dict = GRUPOS_INFO[grupo]["colunas"]
    colunas_esperadas = list(colunas_esperadas_dict.keys())

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
                df = normalizar_colunas(df, colunas_esperadas, colunas_esperadas_dict)
                df = converter_tipos(df, colunas_esperadas_dict)

                # Extrair UF do nome do arquivo ou da pasta
                uf = extrair_uf(nome_arquivo, grupo) or uf_pasta
                if not uf:
                    logger.warning(f"[{grupo}] Não foi possível extrair UF do arquivo {arquivo}")
                    continue

                df['uf'] = uf.upper()
                df['id_log'] = [f"{id_arquivo}_{i}" for i in range(len(df))]

                # Ajustar a ordem das colunas
                df = ajustar_ordem_colunas(df, colunas_esperadas)

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
        df_lote.to_csv(csv_buffer, index=False, header=False)
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
    """
    Processa os dados para todos os grupos definidos em GRUPOS_INFO.
    Carrega os arquivos .parquet, normaliza e converte os tipos, e insere os dados no banco em lotes.
    
    Returns:
        None
    """
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