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
    "AB": {
        "tabela": "sia_apac_cirurgia_bariatrica",
        "colunas": [
            "ab_anoacom",
            "ab_dtcirg2",
            "ab_dtcirur",
            "ab_imc",
            "ab_mesacom",
            "ab_numaih",
            "ab_numaih2",
            "ab_pontbar",
            "ab_prcaih2",
            "ab_prcaih3",
            "ab_procaih",
            "ab_tabbarr",
            "ap_alta",
            "ap_apacant",
            "ap_autoriz",
            "ap_catend",
            "ap_ceppcn",
            "ap_cidcas",
            "ap_cidpri",
            "ap_cidsec",
            "ap_cmp",
            "ap_cnpjcpf",
            "ap_cnpjmnt",
            "ap_cnspcn",
            "ap_codemi",
            "ap_coduni",
            "ap_coidade",
            "ap_condic",
            "ap_dtaut",
            "ap_dtfim",
            "ap_dtinic",
            "ap_dtocor",
            "ap_dtsolic",
            "ap_encerr",
            "ap_etnia",
            "ap_gestao",
            "ap_mn_ind",
            "ap_mndif",
            "ap_motsai",
            "ap_munpcn",
            "ap_mvm",
            "ap_natjur",
            "ap_nuidade",
            "ap_obito",
            "ap_perman",
            "ap_pripal",
            "ap_racacor",
            "ap_sexo",
            "ap_tippre",
            "ap_tpapac",
            "ap_tpaten",
            "ap_tpups",
            "ap_transf",
            "ap_ufdif",
            "ap_ufmun",
            "ap_ufnacio",
            "ap_unisol",
            "ap_vl_ap",
            "id_log",
            "uf"
        ]
    },
    "ABO": {
        "tabela": "sia_apac_acompanhamento_pos_cirurgia_bariatrica",
        "colunas": [
            "ab_anoacom",
            "ab_dtcirg2",
            "ab_dtcirur",
            "ab_imc",
            "ab_mesacom",
            "ab_numaih",
            "ab_numaih2",
            "ab_pontbar",
            "ab_prcaih2",
            "ab_prcaih3",
            "ab_prcaih4",
            "ab_prcaih5",
            "ab_prcaih6",
            "ab_procaih",
            "ab_t_prc2",
            "ab_t_prc3",
            "ab_t_prc4",
            "ab_t_prc5",
            "ab_t_prc6",
            "ab_tabbarr",
            "ap_adesao",
            "ap_alta",
            "ap_apacan",
            "ap_atv_fis",
            "ap_autoriz",
            "ap_catend",
            "ap_ceppcn",
            "ap_cid_c1",
            "ap_cid_c2",
            "ap_cid_c3",
            "ap_cid_c4",
            "ap_cid_c5",
            "ap_cid_co",
            "ap_cidcas",
            "ap_cmp",
            "ap_cnpjcpf",
            "ap_cnpjmnt",
            "ap_cnspcn",
            "ap_codemi",
            "ap_coduni",
            "ap_coidade",
            "ap_comorb",
            "ap_condic",
            "ap_dtaut",
            "ap_dtfim",
            "ap_dtinic",
            "ap_dtoocor",
            "ap_dtsolic",
            "ap_encerr",
            "ap_etnia",
            "ap_gestao",
            "ap_medicam",
            "ap_mn_ind",
            "ap_mndif",
            "ap_motsai",
            "ap_munpcn",
            "ap_mvm",
            "ap_natjur",
            "ap_nuidade",
            "ap_obito",
            "ap_perman",
            "ap_polivit",
            "ap_pripal",
            "ap_racacor",
            "ap_reg_pes",
            "ap_sexo",
            "ap_tpapac",
            "ap_tpatend",
            "ap_tppre",
            "ap_tpups",
            "ap_transf",
            "ap_ufdif",
            "ap_ufmun",
            "ap_ufnacio",
            "ap_unisol",
            "ap_vl_ap",
            "co_cidprim",
            "co_cidsec",
            "id_log",
            "uf"
        ]
    },
    "ACF": {
        "tabela": "sia_apac_confeccao_de_fistula",
        "colunas": [
            "acf_artdia",
            "acf_duplex",
            "acf_flebit",
            "acf_fremit",
            "acf_hemato",
            "acf_prefav",
            "acf_pulso",
            "acf_usocat",
            "acf_veiavi",
            "acf_veidia",
            "ap_alta",
            "ap_apacant",
            "ap_autoriz",
            "ap_catend",
            "ap_ceppcn",
            "ap_cidcas",
            "ap_cidpri",
            "ap_cidsec",
            "ap_cmp",
            "ap_cnpjcpf",
            "ap_cnpjmnt",
            "ap_cnspcn",
            "ap_codemi",
            "ap_coduni",
            "ap_coidade",
            "ap_condic",
            "ap_dtaut",
            "ap_dtfim",
            "ap_dtinic",
            "ap_dtocor",
            "ap_dtsolic",
            "ap_encerr",
            "ap_etnia",
            "ap_gestao",
            "ap_mn_ind",
            "ap_mndif",
            "ap_motsai",
            "ap_munpcn",
            "ap_mvm",
            "ap_natjur",
            "ap_nuidade",
            "ap_obito",
            "ap_perman",
            "ap_pripal",
            "ap_racacor",
            "ap_sexo",
            "ap_tippre",
            "ap_tpapac",
            "ap_tpaten",
            "ap_tpups",
            "ap_transf",
            "ap_ufdif",
            "ap_ufmun",
            "ap_ufnacio",
            "ap_unisol",
            "ap_vl_ap",
            "id_log",
            "uf"
        ]
    },
    "AD": {
        "tabela": "sia_apac_laudos_diversos",
        "colunas": [
            "ap_alta",
            "ap_apacant",
            "ap_autoriz",
            "ap_catend",
            "ap_ceppcn",
            "ap_cidcas",
            "ap_cidpri",
            "ap_cidsec",
            "ap_cmp",
            "ap_cnpjcpf",
            "ap_cnpjmnt",
            "ap_cnspcn",
            "ap_codemi",
            "ap_coduni",
            "ap_coidade",
            "ap_condic",
            "ap_dtaut",
            "ap_dtfim",
            "ap_dtinic",
            "ap_dtocor",
            "ap_dtsolic",
            "ap_encerr",
            "ap_etnia",
            "ap_gestao",
            "ap_mn_ind",
            "ap_mndif",
            "ap_motsai",
            "ap_munpcn",
            "ap_mvm",
            "ap_natjur",
            "ap_nuidade",
            "ap_obito",
            "ap_perman",
            "ap_pripal",
            "ap_racacor",
            "ap_sexo",
            "ap_tippre",
            "ap_tpapac",
            "ap_tpaten",
            "ap_tpups",
            "ap_transf",
            "ap_ufdif",
            "ap_ufmun",
            "ap_ufnacio",
            "ap_unisol",
            "ap_vl_ap",
            "id_log",
            "uf"
        ]
    },
    "AM": {
        "tabela": "sia_apac_medicamentos",
        "colunas": [
            "am_altura",
            "am_gestant",
            "am_peso",
            "am_qtdtran",
            "am_transpl",
            "ap_alta",
            "ap_apacant",
            "ap_autoriz",
            "ap_catend",
            "ap_ceppcn",
            "ap_cidcas",
            "ap_cidpri",
            "ap_cidsec",
            "ap_cmp",
            "ap_cnpjcpf",
            "ap_cnpjmnt",
            "ap_cnspcn",
            "ap_codemi",
            "ap_coduni",
            "ap_coidade",
            "ap_condic",
            "ap_dtaut",
            "ap_dtfim",
            "ap_dtinic",
            "ap_dtocor",
            "ap_dtsolic",
            "ap_encerr",
            "ap_etnia",
            "ap_gestao",
            "ap_mn_ind",
            "ap_mndif",
            "ap_motsai",
            "ap_munpcn",
            "ap_mvm",
            "ap_natjur",
            "ap_nuidade",
            "ap_obito",
            "ap_perman",
            "ap_pripal",
            "ap_racacor",
            "ap_sexo",
            "ap_tippre",
            "ap_tpapac",
            "ap_tpaten",
            "ap_tpups",
            "ap_transf",
            "ap_ufdif",
            "ap_ufmun",
            "ap_ufnacio",
            "ap_unisol",
            "ap_vl_ap",
            "id_log",
            "uf"
        ]
    },
    "AMP": {
        "tabela": "sia_apac_acompanhamento_multiprofissional",
        "colunas": [
            "amp_acevas",
            "amp_albumi",
            "amp_caract",
            "amp_dtcli",
            "amp_dtini",
            "amp_fosfor",
            "amp_hb",
            "amp_hbsag",
            "amp_hcv",
            "amp_hiv",
            "amp_interc",
            "amp_ktvsem",
            "amp_maisne",
            "amp_pth",
            "amp_seapto",
            "amp_seperi",
            "amp_sitini",
            "amp_sittra",
            "amp_tru",
            "ap_alta",
            "ap_apacant",
            "ap_autoriz",
            "ap_catend",
            "ap_ceppcn",
            "ap_cidcas",
            "ap_cidpri",
            "ap_cidsec",
            "ap_cmp",
            "ap_cnpjcpf",
            "ap_cnpjmnt",
            "ap_cnspcn",
            "ap_codemi",
            "ap_coduni",
            "ap_coidade",
            "ap_condic",
            "ap_dtaut",
            "ap_dtfim",
            "ap_dtinic",
            "ap_dtocor",
            "ap_dtsolic",
            "ap_encerr",
            "ap_etnia",
            "ap_gestao",
            "ap_mn_ind",
            "ap_mndif",
            "ap_motsai",
            "ap_munpcn",
            "ap_mvm",
            "ap_natjur",
            "ap_nuidade",
            "ap_obito",
            "ap_perman",
            "ap_pripal",
            "ap_racacor",
            "ap_sexo",
            "ap_tippre",
            "ap_tpapac",
            "ap_tpaten",
            "ap_tpups",
            "ap_transf",
            "ap_ufdif",
            "ap_ufmun",
            "ap_ufnacio",
            "ap_unisol",
            "ap_vl_ap",
            "id_log",
            "uf"
        ]
    },
    "AN": {
        "tabela": "sia_apac_nefrologia",
        "colunas": [
            "an_acevas",
            "an_albumi",
            "an_altura",
            "an_cncdo",
            "an_diures",
            "an_dtpdr",
            "an_glicos",
            "an_hb",
            "an_hbsag",
            "an_hcv",
            "an_hiv",
            "an_intfis",
            "an_peso",
            "an_tru",
            "an_ulsoab",
            "ap_alta",
            "ap_apacant",
            "ap_autoriz",
            "ap_catend",
            "ap_ceppcn",
            "ap_cidcas",
            "ap_cidpri",
            "ap_cidsec",
            "ap_cmp",
            "ap_cnpjcpf",
            "ap_cnpjmnt",
            "ap_cnspcn",
            "ap_codemi",
            "ap_coduni",
            "ap_coidade",
            "ap_condic",
            "ap_dtaut",
            "ap_dtfim",
            "ap_dtinic",
            "ap_dtocor",
            "ap_dtsolic",
            "ap_encerr",
            "ap_etnia",
            "ap_gestao",
            "ap_mn_ind",
            "ap_mndif",
            "ap_motsai",
            "ap_munpcn",
            "ap_mvm",
            "ap_nuidade",
            "ap_obito",
            "ap_perman",
            "ap_pripal",
            "ap_racacor",
            "ap_sexo",
            "ap_tippre",
            "ap_tpapac",
            "ap_tpaten",
            "ap_tpups",
            "ap_transf",
            "ap_ufdif",
            "ap_ufmun",
            "ap_ufnacio",
            "ap_unisol",
            "ap_vl_ap",
            "id_log",
            "uf"
        ]
    },
    "AQ": {
        "tabela": "sia_apac_quimioterapia",
        "colunas": [
            "ap_alta",
            "ap_apacant",
            "ap_autoriz",
            "ap_catend",
            "ap_ceppcn",
            "ap_cidcas",
            "ap_cidpri",
            "ap_cidsec",
            "ap_cmp",
            "ap_cnpjcpf",
            "ap_cnpjmnt",
            "ap_cnspcn",
            "ap_codemi",
            "ap_coduni",
            "ap_coidade",
            "ap_condic",
            "ap_dtaut",
            "ap_dtfim",
            "ap_dtinic",
            "ap_dtocor",
            "ap_dtsolic",
            "ap_encerr",
            "ap_etnia",
            "ap_gestao",
            "ap_mn_ind",
            "ap_mndif",
            "ap_motsai",
            "ap_munpcn",
            "ap_mvm",
            "ap_natjur",
            "ap_nuidade",
            "ap_obito",
            "ap_perman",
            "ap_pripal",
            "ap_racacor",
            "ap_sexo",
            "ap_tippre",
            "ap_tpapac",
            "ap_tpaten",
            "ap_tpups",
            "ap_transf",
            "ap_ufdif",
            "ap_ufmun",
            "ap_ufnacio",
            "ap_unisol",
            "ap_vl_ap",
            "aq_cid10",
            "aq_cidini1",
            "aq_cidini2",
            "aq_cidini3",
            "aq_conttr",
            "aq_dtiden",
            "aq_dtini1",
            "aq_dtini2",
            "aq_dtini3",
            "aq_dtintr",
            "aq_esqu_p1",
            "aq_esqu_p2",
            "aq_estadi",
            "aq_grahis",
            "aq_linfin",
            "aq_med01",
            "aq_med02",
            "aq_med03",
            "aq_med04",
            "aq_med05",
            "aq_med06",
            "aq_med07",
            "aq_med08",
            "aq_med09",
            "aq_med10",
            "aq_totmau",
            "aq_totmpl",
            "aq_trante",
            "id_log",
            "uf"
        ]
    },
    "AR": {
        "tabela": "sia_apac_radioterapia",
        "colunas": [
            "ap_alta",
            "ap_apacant",
            "ap_autoriz",
            "ap_catend",
            "ap_ceppcn",
            "ap_cidcas",
            "ap_cidpri",
            "ap_cidsec",
            "ap_cmp",
            "ap_cnpjcpf",
            "ap_cnpjmnt",
            "ap_cnspcn",
            "ap_codemi",
            "ap_coduni",
            "ap_coidade",
            "ap_condic",
            "ap_dtaut",
            "ap_dtfim",
            "ap_dtinic",
            "ap_dtocor",
            "ap_dtsolic",
            "ap_encerr",
            "ap_etnia",
            "ap_gestao",
            "ap_mn_ind",
            "ap_mndif",
            "ap_motsai",
            "ap_munpcn",
            "ap_mvm",
            "ap_natjur",
            "ap_nuidade",
            "ap_obito",
            "ap_perman",
            "ap_pripal",
            "ap_racacor",
            "ap_sexo",
            "ap_tippre",
            "ap_tpapac",
            "ap_tpaten",
            "ap_tpups",
            "ap_transf",
            "ap_ufdif",
            "ap_ufmun",
            "ap_ufnacio",
            "ap_unisol",
            "ap_vl_ap",
            "ar_cid10",
            "ar_cidini1",
            "ar_cidini2",
            "ar_cidini3",
            "ar_cidtr1",
            "ar_cidtr2",
            "ar_cidtr3",
            "ar_conttr",
            "ar_dtiden",
            "ar_dtini1",
            "ar_dtini2",
            "ar_dtini3",
            "ar_dtintr",
            "ar_estadi",
            "ar_fimar1",
            "ar_fimar2",
            "ar_fimar3",
            "ar_finali",
            "ar_grahis",
            "ar_iniar1",
            "ar_iniar2",
            "ar_iniar3",
            "ar_linfin",
            "ar_numc1",
            "ar_numc2",
            "ar_numc3",
            "ar_smrd",
            "ar_trante",
            "id_log",
            "uf"
        ]
    },
    "ATD": {
        "tabela": "sia_apac_tratamento_dialitico",
        "colunas": [
            "ap_alta",
            "ap_apacant",
            "ap_autoriz",
            "ap_catend",
            "ap_ceppcn",
            "ap_cidcas",
            "ap_cidpri",
            "ap_cidsec",
            "ap_cmp",
            "ap_cnpjcpf",
            "ap_cnpjmnt",
            "ap_cnspcn",
            "ap_codemi",
            "ap_coduni",
            "ap_coidade",
            "ap_condic",
            "ap_dtaut",
            "ap_dtfim",
            "ap_dtinic",
            "ap_dtocor",
            "ap_dtsolic",
            "ap_encerr",
            "ap_etnia",
            "ap_gestao",
            "ap_mn_ind",
            "ap_mndif",
            "ap_motsai",
            "ap_munpcn",
            "ap_mvm",
            "ap_natjur",
            "ap_nuidade",
            "ap_obito",
            "ap_perman",
            "ap_pripal",
            "ap_racacor",
            "ap_sexo",
            "ap_tippre",
            "ap_tpapac",
            "ap_tpaten",
            "ap_tpups",
            "ap_transf",
            "ap_ufdif",
            "ap_ufmun",
            "ap_ufnacio",
            "ap_unisol",
            "ap_vl_ap",
            "atd_acevas",
            "atd_albumi",
            "atd_caract",
            "atd_dtcli",
            "atd_dtpdr",
            "atd_fosfor",
            "atd_hb",
            "atd_hbsag",
            "atd_hcv",
            "atd_hiv",
            "atd_interc",
            "atd_ktvsem",
            "atd_maisne",
            "atd_pth",
            "atd_seapto",
            "atd_seperi",
            "atd_sitini",
            "atd_sittra",
            "atd_tru",
            "id_log",
            "uf"
        ]
    },
    "BI": {
        "tabela": "sia_boletim_producao_ambulatorial_individualizado",
        "colunas": [
            "autoriz",
            "catend",
            "cboprof",
            "cidpri",
            "cnpj_cc",
            "cnpjcpf",
            "cnpjmnt",
            "cns_pac",
            "cnsprof",
            "coduni",
            "complex",
            "condic",
            "dt_atend",
            "dt_process",
            "dtnasc",
            "etnia",
            "gestao",
            "id_log",
            "idadepac",
            "mn_ind",
            "mndif",
            "munpac",
            "nat_jur",
            "proc_id",
            "qt_apres",
            "qt_aprov",
            "racacor",
            "sexopac",
            "subfin",
            "tippre",
            "tpfin",
            "tpidadepac",
            "tpups",
            "uf",
            "ufdif",
            "ufmun",
            "vl_apres",
            "vl_aprov"
        ]
    },
    "PA": {
        "tabela": "sia_producao_ambulatorial",
        "colunas": {
            "id_log": "VARCHAR(255)",
            "idademax": "TEXT",
            "idademin": "TEXT",
            "nu_pa_tot": "TEXT",
            "nu_vpa_tot": "NUMERIC",
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
            "pa_codoco": "VARCHAR(3)",
            "pa_codpro": "TEXT",
            "pa_coduni": "TEXT",
            "pa_condic": "CHAR(2)",
            "pa_datpr": "INTEGER",
            "pa_datref": "INTEGER",
            "pa_dif_val": "TEXT",
            "pa_docorig": "CHAR(1)",
            "pa_encerr": "BOOLEAN",
            "pa_etnia": "TEXT",
            "pa_fler": "BOOLEAN",
            "pa_flidade": "SMALLINT",
            "pa_flqt": "CHAR(1)",
            "pa_fxetar": "TEXT",
            "pa_gestao": "INTEGER",
            "pa_idade": "TEXT",
            "pa_incout": "TEXT",
            "pa_incurg": "TEXT",
            "pa_indica": "SMALLINT",
            "pa_ine": "TEXT",
            "pa_mn_ind": "CHAR(1)",
            "pa_mndif": "SMALLINT",
            "pa_morfol": "TEXT",
            "pa_motsai": "TEXT",
            "pa_munat": "INTEGER",
            "pa_munpcn": "INTEGER",
            "pa_mvm": "INTEGER",
            "pa_nat_jur": "TEXT",
            "pa_nh": "TEXT",
            "pa_nivcpl": "SMALLINT",
            "pa_numapa": "TEXT",
            "pa_obito": "BOOLEAN",
            "pa_perman": "BOOLEAN",
            "pa_proc_id": "TEXT",
            "pa_qtdapr": "TEXT",
            "pa_qtdpro": "TEXT",
            "pa_racacor": "TEXT",
            "pa_rcb": "CHAR(6)",
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
            "pa_valapr": "TEXT",
            "pa_valpro": "TEXT",
            "pa_vl_cf": "NUMERIC",
            "pa_vl_cl": "NUMERIC",
            "pa_vl_inc": "NUMERIC",
            "uf": "CHAR(2)"
        }
    }
}




ESTADOS = ["SP", "PR", "MG"]  # Estados que devem ser processados
ANOS = range(1997, 2025)

# Mapeamento dos tipos de dados para cada tabela
tipo_coluna_map = {
    "sia_apac_cirurgia_bariatrica": {
        "ab_anoacom": "TEXT",
        "ab_dtcirg2": "TEXT",
        "ab_dtcirur": "CHAR(8)",
        "ab_imc": "TEXT",
        "ab_mesacom": "TEXT",
        "ab_numaih": "TEXT",
        "ab_numaih2": "CHAR(13)",
        "ab_pontbar": "CHAR(1)",
        "ab_prcaih2": "CHAR(10)",
        "ab_prcaih3": "CHAR(10)",
        "ab_procaih": "CHAR(10)",
        "ab_tabbarr": "CHAR(1)",
        "ap_alta": "BOOLEAN",
        "ap_apacant": "TEXT",
        "ap_autoriz": "CHAR(13)",
        "ap_catend": "TEXT",
        "ap_ceppcn": "CHAR(8)",
        "ap_cidcas": "TEXT",
        "ap_cidpri": "TEXT",
        "ap_cidsec": "TEXT",
        "ap_cmp": "CHAR(6)",
        "ap_cnpjcpf": "CHAR(14)",
        "ap_cnpjmnt": "CHAR(14)",
        "ap_cnspcn": "CHAR(15)",
        "ap_codemi": "CHAR(10)",
        "ap_coduni": "TEXT",
        "ap_coidade": "CHAR(1)",
        "ap_condic": "CHAR(2)",
        "ap_dtaut": "CHAR(8)",
        "ap_dtfim": "CHAR(8)",
        "ap_dtinic": "CHAR(8)",
        "ap_dtocor": "TEXT",
        "ap_dtsolic": "CHAR(8)",
        "ap_encerr": "BOOLEAN",
        "ap_etnia": "CHAR(4)",
        "ap_gestao": "CHAR(6)",
        "ap_mn_ind": "CHAR(1)",
        "ap_mndif": "BOOLEAN",
        "ap_motsai": "CHAR(2)",
        "ap_munpcn": "CHAR(6)",
        "ap_mvm": "CHAR(6)",
        "ap_natjur": "CHAR(4)",
        "ap_nuidade": "CHAR(2)",
        "ap_obito": "BOOLEAN",
        "ap_perman": "BOOLEAN",
        "ap_pripal": "TEXT",
        "ap_racacor": "TEXT",
        "ap_sexo": "CHAR(1)",
        "ap_tippre": "TEXT",
        "ap_tpapac": "CHAR(1)",
        "ap_tpaten": "TEXT",
        "ap_tpups": "TEXT",
        "ap_transf": "BOOLEAN",
        "ap_ufdif": "BOOLEAN",
        "ap_ufmun": "CHAR(6)",
        "ap_ufnacio": "TEXT",
        "ap_unisol": "TEXT",
        "ap_vl_ap": "CHAR(20)",
        "id_log": "VARCHAR(255)",
        "uf": "CHAR(2)"
    },
    "sia_apac_acompanhamento_pos_cirurgia_bariatrica": {
        "ab_anoacom": "TEXT",
        "ab_dtcirg2": "CHAR(8)",
        "ab_dtcirur": "CHAR(8)",
        "ab_imc": "CHAR(3)",
        "ab_mesacom": "TEXT",
        "ab_numaih": "CHAR(13)",
        "ab_numaih2": "CHAR(13)",
        "ab_pontbar": "CHAR(1)",
        "ab_prcaih2": "CHAR(10)",
        "ab_prcaih3": "CHAR(10)",
        "ab_prcaih4": "CHAR(10)",
        "ab_prcaih5": "CHAR(10)",
        "ab_prcaih6": "CHAR(10)",
        "ab_procaih": "TEXT",
        "ab_t_prc2": "CHAR(3)",
        "ab_t_prc3": "CHAR(3)",
        "ab_t_prc4": "CHAR(3)",
        "ab_t_prc5": "CHAR(3)",
        "ab_t_prc6": "CHAR(3)",
        "ab_tabbarr": "CHAR(1)",
        "ap_adesao": "BOOLEAN",
        "ap_alta": "BOOLEAN",
        "ap_apacan": "TEXT",
        "ap_atv_fis": "BOOLEAN",
        "ap_autoriz": "CHAR(13)",
        "ap_catend": "TEXT",
        "ap_ceppcn": "TEXT",
        "ap_cid_c1": "CHAR(4)",
        "ap_cid_c2": "CHAR(4)",
        "ap_cid_c3": "CHAR(4)",
        "ap_cid_c4": "CHAR(4)",
        "ap_cid_c5": "CHAR(4)",
        "ap_cid_co": "CHAR(4)",
        "ap_cidcas": "TEXT",
        "ap_cmp": "CHAR(6)",
        "ap_cnpjcpf": "TEXT",
        "ap_cnpjmnt": "CHAR(14)",
        "ap_cnspcn": "CHAR(15)",
        "ap_codemi": "CHAR(10)",
        "ap_coduni": "CHAR(7)",
        "ap_coidade": "CHAR(1)",
        "ap_comorb": "CHAR(1)",
        "ap_condic": "CHAR(2)",
        "ap_dtaut": "CHAR(8)",
        "ap_dtfim": "CHAR(8)",
        "ap_dtinic": "CHAR(8)",
        "ap_dtoocor": "TEXT",
        "ap_dtsolic": "CHAR(8)",
        "ap_encerr": "BOOLEAN",
        "ap_etnia": "CHAR(4)",
        "ap_gestao": "CHAR(6)",
        "ap_medicam": "BOOLEAN",
        "ap_mn_ind": "CHAR(1)",
        "ap_mndif": "BOOLEAN",
        "ap_motsai": "CHAR(2)",
        "ap_munpcn": "CHAR(6)",
        "ap_mvm": "CHAR(6)",
        "ap_natjur": "CHAR(4)",
        "ap_nuidade": "CHAR(2)",
        "ap_obito": "BOOLEAN",
        "ap_perman": "BOOLEAN",
        "ap_polivit": "BOOLEAN",
        "ap_pripal": "TEXT",
        "ap_racacor": "TEXT",
        "ap_reg_pes": "BOOLEAN",
        "ap_sexo": "CHAR(1)",
        "ap_tpapac": "CHAR(1)",
        "ap_tpatend": "TEXT",
        "ap_tppre": "TEXT",
        "ap_tpups": "TEXT",
        "ap_transf": "BOOLEAN",
        "ap_ufdif": "BOOLEAN",
        "ap_ufmun": "CHAR(6)",
        "ap_ufnacio": "TEXT",
        "ap_unisol": "TEXT",
        "ap_vl_ap": "CHAR(20)",
        "co_cidprim": "TEXT",
        "co_cidsec": "TEXT",
        "id_log": "VARCHAR(255)",
        "uf": "CHAR(2)"
    },
    "sia_apac_confeccao_de_fistula": {
        "acf_artdia": "TEXT",
        "acf_duplex": "CHAR(1)",
        "acf_flebit": "CHAR(1)",
        "acf_fremit": "CHAR(1)",
        "acf_hemato": "CHAR(1)",
        "acf_prefav": "CHAR(1)",
        "acf_pulso": "CHAR(1)",
        "acf_usocat": "CHAR(1)",
        "acf_veiavi": "CHAR(1)",
        "acf_veidia": "TEXT",
        "ap_alta": "BOOLEAN",
        "ap_apacant": "TEXT",
        "ap_autoriz": "CHAR(13)",
        "ap_catend": "TEXT",
        "ap_ceppcn": "TEXT",
        "ap_cidcas": "TEXT",
        "ap_cidpri": "TEXT",
        "ap_cidsec": "TEXT",
        "ap_cmp": "CHAR(6)",
        "ap_cnpjcpf": "TEXT",
        "ap_cnpjmnt": "CHAR(14)",
        "ap_cnspcn": "CHAR(15)",
        "ap_codemi": "CHAR(10)",
        "ap_coduni": "CHAR(7)",
        "ap_coidade": "CHAR(1)",
        "ap_condic": "CHAR(2)",
        "ap_dtaut": "CHAR(8)",
        "ap_dtfim": "CHAR(8)",
        "ap_dtinic": "CHAR(8)",
        "ap_dtocor": "CHAR(8)",
        "ap_dtsolic": "TEXT",
        "ap_encerr": "BOOLEAN",
        "ap_etnia": "CHAR(4)",
        "ap_gestao": "CHAR(6)",
        "ap_mn_ind": "CHAR(1)",
        "ap_mndif": "BOOLEAN",
        "ap_motsai": "CHAR(2)",
        "ap_munpcn": "CHAR(6)",
        "ap_mvm": "CHAR(6)",
        "ap_natjur": "CHAR(4)",
        "ap_nuidade": "CHAR(2)",
        "ap_obito": "BOOLEAN",
        "ap_perman": "BOOLEAN",
        "ap_pripal": "TEXT",
        "ap_racacor": "TEXT",
        "ap_sexo": "CHAR(1)",
        "ap_tippre": "TEXT",
        "ap_tpapac": "CHAR(1)",
        "ap_tpaten": "CHAR(2)",
        "ap_tpups": "TEXT",
        "ap_transf": "BOOLEAN",
        "ap_ufdif": "BOOLEAN",
        "ap_ufmun": "CHAR(6)",
        "ap_ufnacio": "TEXT",
        "ap_unisol": "CHAR(7)",
        "ap_vl_ap": "CHAR(20)",
        "id_log": "VARCHAR(255)",
        "uf": "CHAR(2)"
    },
    "sia_apac_laudos_diversos": {
        "ap_alta": "BOOLEAN",
        "ap_apacant": "TEXT",
        "ap_autoriz": "CHAR(13)",
        "ap_catend": "TEXT",
        "ap_ceppcn": "TEXT",
        "ap_cidcas": "TEXT",
        "ap_cidpri": "CHAR(4)",
        "ap_cidsec": "TEXT",
        "ap_cmp": "CHAR(6)",
        "ap_cnpjcpf": "TEXT",
        "ap_cnpjmnt": "CHAR(14)",
        "ap_cnspcn": "CHAR(15)",
        "ap_codemi": "CHAR(10)",
        "ap_coduni": "TEXT",
        "ap_coidade": "CHAR(1)",
        "ap_condic": "CHAR(2)",
        "ap_dtaut": "CHAR(8)",
        "ap_dtfim": "CHAR(8)",
        "ap_dtinic": "CHAR(8)",
        "ap_dtocor": "TEXT",
        "ap_dtsolic": "CHAR(8)",
        "ap_encerr": "BOOLEAN",
        "ap_etnia": "TEXT",
        "ap_gestao": "CHAR(6)",
        "ap_mn_ind": "CHAR(1)",
        "ap_mndif": "BOOLEAN",
        "ap_motsai": "CHAR(2)",
        "ap_munpcn": "CHAR(6)",
        "ap_mvm": "CHAR(6)",
        "ap_natjur": "CHAR(4)",
        "ap_nuidade": "CHAR(2)",
        "ap_obito": "BOOLEAN",
        "ap_perman": "BOOLEAN",
        "ap_pripal": "TEXT",
        "ap_racacor": "TEXT",
        "ap_sexo": "CHAR(1)",
        "ap_tippre": "TEXT",
        "ap_tpapac": "CHAR(1)",
        "ap_tpaten": "TEXT",
        "ap_tpups": "TEXT",
        "ap_transf": "BOOLEAN",
        "ap_ufdif": "BOOLEAN",
        "ap_ufmun": "CHAR(6)",
        "ap_ufnacio": "TEXT",
        "ap_unisol": "TEXT",
        "ap_vl_ap": "CHAR(20)",
        "id_log": "VARCHAR(255)",
        "uf": "CHAR(2)"
    },
    "sia_apac_medicamentos": {
        "am_altura": "CHAR(3)",
        "am_gestant": "CHAR(1)",
        "am_peso": "TEXT",
        "am_qtdtran": "TEXT",
        "am_transpl": "CHAR(1)",
        "ap_alta": "BOOLEAN",
        "ap_apacant": "TEXT",
        "ap_autoriz": "CHAR(13)",
        "ap_catend": "TEXT",
        "ap_ceppcn": "TEXT",
        "ap_cidcas": "TEXT",
        "ap_cidpri": "CHAR(4)",
        "ap_cidsec": "TEXT",
        "ap_cmp": "CHAR(6)",
        "ap_cnpjcpf": "CHAR(14)",
        "ap_cnpjmnt": "CHAR(14)",
        "ap_cnspcn": "CHAR(15)",
        "ap_codemi": "CHAR(10)",
        "ap_coduni": "CHAR(7)",
        "ap_coidade": "CHAR(1)",
        "ap_condic": "CHAR(2)",
        "ap_dtaut": "TEXT",
        "ap_dtfim": "CHAR(8)",
        "ap_dtinic": "CHAR(8)",
        "ap_dtocor": "TEXT",
        "ap_dtsolic": "TEXT",
        "ap_encerr": "BOOLEAN",
        "ap_etnia": "TEXT",
        "ap_gestao": "CHAR(6)",
        "ap_mn_ind": "CHAR(1)",
        "ap_mndif": "BOOLEAN",
        "ap_motsai": "CHAR(2)",
        "ap_munpcn": "CHAR(6)",
        "ap_mvm": "CHAR(6)",
        "ap_natjur": "CHAR(4)",
        "ap_nuidade": "CHAR(2)",
        "ap_obito": "BOOLEAN",
        "ap_perman": "BOOLEAN",
        "ap_pripal": "TEXT",
        "ap_racacor": "TEXT",
        "ap_sexo": "CHAR(1)",
        "ap_tippre": "TEXT",
        "ap_tpapac": "CHAR(1)",
        "ap_tpaten": "TEXT",
        "ap_tpups": "CHAR(2)",
        "ap_transf": "BOOLEAN",
        "ap_ufdif": "BOOLEAN",
        "ap_ufmun": "CHAR(6)",
        "ap_ufnacio": "TEXT",
        "ap_unisol": "TEXT",
        "ap_vl_ap": "CHAR(12)",
        "id_log": "VARCHAR(255)",
        "uf": "CHAR(2)"
    },
    "sia_apac_acompanhamento_multiprofissional": {
        "amp_acevas": "CHAR(1)",
        "amp_albumi": "TEXT",
        "amp_caract": "CHAR(1)",
        "amp_dtcli": "CHAR(8)",
        "amp_dtini": "CHAR(8)",
        "amp_fosfor": "CHAR(4)",
        "amp_hb": "CHAR(4)",
        "amp_hbsag": "CHAR(1)",
        "amp_hcv": "CHAR(1)",
        "amp_hiv": "CHAR(1)",
        "amp_interc": "CHAR(1)",
        "amp_ktvsem": "TEXT",
        "amp_maisne": "CHAR(1)",
        "amp_pth": "TEXT",
        "amp_seapto": "CHAR(1)",
        "amp_seperi": "CHAR(1)",
        "amp_sitini": "CHAR(1)",
        "amp_sittra": "CHAR(1)",
        "amp_tru": "TEXT",
        "ap_alta": "BOOLEAN",
        "ap_apacant": "TEXT",
        "ap_autoriz": "CHAR(13)",
        "ap_catend": "TEXT",
        "ap_ceppcn": "CHAR(8)",
        "ap_cidcas": "TEXT",
        "ap_cidpri": "TEXT",
        "ap_cidsec": "TEXT",
        "ap_cmp": "CHAR(6)",
        "ap_cnpjcpf": "TEXT",
        "ap_cnpjmnt": "CHAR(14)",
        "ap_cnspcn": "CHAR(15)",
        "ap_codemi": "CHAR(10)",
        "ap_coduni": "CHAR(7)",
        "ap_coidade": "CHAR(1)",
        "ap_condic": "CHAR(2)",
        "ap_dtaut": "CHAR(8)",
        "ap_dtfim": "CHAR(8)",
        "ap_dtinic": "CHAR(8)",
        "ap_dtocor": "TEXT",
        "ap_dtsolic": "CHAR(8)",
        "ap_encerr": "BOOLEAN",
        "ap_etnia": "CHAR(4)",
        "ap_gestao": "CHAR(6)",
        "ap_mn_ind": "CHAR(1)",
        "ap_mndif": "BOOLEAN",
        "ap_motsai": "CHAR(2)",
        "ap_munpcn": "CHAR(6)",
        "ap_mvm": "CHAR(6)",
        "ap_natjur": "CHAR(4)",
        "ap_nuidade": "CHAR(2)",
        "ap_obito": "BOOLEAN",
        "ap_perman": "BOOLEAN",
        "ap_pripal": "TEXT",
        "ap_racacor": "TEXT",
        "ap_sexo": "CHAR(1)",
        "ap_tippre": "TEXT",
        "ap_tpapac": "CHAR(1)",
        "ap_tpaten": "CHAR(2)",
        "ap_tpups": "TEXT",
        "ap_transf": "BOOLEAN",
        "ap_ufdif": "BOOLEAN",
        "ap_ufmun": "CHAR(6)",
        "ap_ufnacio": "TEXT",
        "ap_unisol": "TEXT",
        "ap_vl_ap": "CHAR(20)",
        "id_log": "VARCHAR(255)",
        "uf": "CHAR(2)"
    },
    "sia_apac_nefrologia": {
        "an_acevas": "CHAR(1)",
        "an_albumi": "TEXT",
        "an_altura": "TEXT",
        "an_cncdo": "CHAR(1)",
        "an_diures": "TEXT",
        "an_dtpdr": "CHAR(8)",
        "an_glicos": "TEXT",
        "an_hb": "TEXT",
        "an_hbsag": "CHAR(1)",
        "an_hcv": "CHAR(1)",
        "an_hiv": "CHAR(1)",
        "an_intfis": "TEXT",
        "an_peso": "TEXT",
        "an_tru": "TEXT",
        "an_ulsoab": "CHAR(1)",
        "ap_alta": "BOOLEAN",
        "ap_apacant": "TEXT",
        "ap_autoriz": "CHAR(13)",
        "ap_catend": "TEXT",
        "ap_ceppcn": "TEXT",
        "ap_cidcas": "TEXT",
        "ap_cidpri": "CHAR(4)",
        "ap_cidsec": "TEXT",
        "ap_cmp": "CHAR(6)",
        "ap_cnpjcpf": "CHAR(14)",
        "ap_cnpjmnt": "TEXT",
        "ap_cnspcn": "CHAR(15)",
        "ap_codemi": "CHAR(10)",
        "ap_coduni": "CHAR(7)",
        "ap_coidade": "CHAR(1)",
        "ap_condic": "CHAR(2)",
        "ap_dtaut": "CHAR(8)",
        "ap_dtfim": "CHAR(8)",
        "ap_dtinic": "CHAR(8)",
        "ap_dtocor": "TEXT",
        "ap_dtsolic": "CHAR(8)",
        "ap_encerr": "BOOLEAN",
        "ap_etnia": "CHAR(4)",
        "ap_gestao": "CHAR(6)",
        "ap_mn_ind": "CHAR(1)",
        "ap_mndif": "BOOLEAN",
        "ap_motsai": "CHAR(2)",
        "ap_munpcn": "CHAR(6)",
        "ap_mvm": "CHAR(6)",
        "ap_nuidade": "TEXT",
        "ap_obito": "BOOLEAN",
        "ap_perman": "BOOLEAN",
        "ap_pripal": "TEXT",
        "ap_racacor": "TEXT",
        "ap_sexo": "CHAR(1)",
        "ap_tippre": "CHAR(2)",
        "ap_tpapac": "CHAR(1)",
        "ap_tpaten": "TEXT",
        "ap_tpups": "TEXT",
        "ap_transf": "BOOLEAN",
        "ap_ufdif": "BOOLEAN",
        "ap_ufmun": "CHAR(6)",
        "ap_ufnacio": "TEXT",
        "ap_unisol": "CHAR(7)",
        "ap_vl_ap": "CHAR(20)",
        "id_log": "VARCHAR(255)",
        "uf": "CHAR(2)"
    },
    "sia_apac_quimioterapia": {
        "ap_alta": "BOOLEAN",
        "ap_apacant": "TEXT",
        "ap_autoriz": "CHAR(13)",
        "ap_catend": "TEXT",
        "ap_ceppcn": "TEXT",
        "ap_cidcas": "TEXT",
        "ap_cidpri": "CHAR(4)",
        "ap_cidsec": "TEXT",
        "ap_cmp": "CHAR(6)",
        "ap_cnpjcpf": "CHAR(14)",
        "ap_cnpjmnt": "CHAR(14)",
        "ap_cnspcn": "CHAR(15)",
        "ap_codemi": "CHAR(10)",
        "ap_coduni": "TEXT",
        "ap_coidade": "CHAR(1)",
        "ap_condic": "CHAR(2)",
        "ap_dtaut": "CHAR(8)",
        "ap_dtfim": "CHAR(8)",
        "ap_dtinic": "CHAR(8)",
        "ap_dtocor": "TEXT",
        "ap_dtsolic": "CHAR(8)",
        "ap_encerr": "BOOLEAN",
        "ap_etnia": "TEXT",
        "ap_gestao": "CHAR(6)",
        "ap_mn_ind": "CHAR(1)",
        "ap_mndif": "BOOLEAN",
        "ap_motsai": "CHAR(2)",
        "ap_munpcn": "CHAR(6)",
        "ap_mvm": "CHAR(6)",
        "ap_natjur": "CHAR(4)",
        "ap_nuidade": "CHAR(2)",
        "ap_obito": "BOOLEAN",
        "ap_perman": "BOOLEAN",
        "ap_pripal": "TEXT",
        "ap_racacor": "TEXT",
        "ap_sexo": "CHAR(1)",
        "ap_tippre": "TEXT",
        "ap_tpapac": "CHAR(1)",
        "ap_tpaten": "TEXT",
        "ap_tpups": "TEXT",
        "ap_transf": "BOOLEAN",
        "ap_ufdif": "BOOLEAN",
        "ap_ufmun": "CHAR(6)",
        "ap_ufnacio": "TEXT",
        "ap_unisol": "TEXT",
        "ap_vl_ap": "CHAR(12)",
        "aq_cid10": "CHAR(4)",
        "aq_cidini1": "CHAR(4)",
        "aq_cidini2": "CHAR(4)",
        "aq_cidini3": "CHAR(4)",
        "aq_conttr": "CHAR(1)",
        "aq_dtiden": "CHAR(8)",
        "aq_dtini1": "TEXT",
        "aq_dtini2": "TEXT",
        "aq_dtini3": "TEXT",
        "aq_dtintr": "CHAR(8)",
        "aq_esqu_p1": "CHAR(5)",
        "aq_esqu_p2": "CHAR(10)",
        "aq_estadi": "TEXT",
        "aq_grahis": "TEXT",
        "aq_linfin": "TEXT",
        "aq_med01": "TEXT",
        "aq_med02": "TEXT",
        "aq_med03": "TEXT",
        "aq_med04": "TEXT",
        "aq_med05": "TEXT",
        "aq_med06": "TEXT",
        "aq_med07": "TEXT",
        "aq_med08": "TEXT",
        "aq_med09": "TEXT",
        "aq_med10": "TEXT",
        "aq_totmau": "TEXT",
        "aq_totmpl": "TEXT",
        "aq_trante": "TEXT",
        "id_log": "VARCHAR(255)",
        "uf": "CHAR(2)"
    },
    "sia_apac_radioterapia": {
        "ap_alta": "BOOLEAN",
        "ap_apacant": "TEXT",
        "ap_autoriz": "CHAR(13)",
        "ap_catend": "TEXT",
        "ap_ceppcn": "TEXT",
        "ap_cidcas": "TEXT",
        "ap_cidpri": "CHAR(4)",
        "ap_cidsec": "TEXT",
        "ap_cmp": "CHAR(6)",
        "ap_cnpjcpf": "CHAR(14)",
        "ap_cnpjmnt": "CHAR(14)",
        "ap_cnspcn": "CHAR(15)",
        "ap_codemi": "CHAR(10)",
        "ap_coduni": "CHAR(7)",
        "ap_coidade": "CHAR(1)",
        "ap_condic": "CHAR(2)",
        "ap_dtaut": "CHAR(8)",
        "ap_dtfim": "CHAR(8)",
        "ap_dtinic": "CHAR(8)",
        "ap_dtocor": "CHAR(8)",
        "ap_dtsolic": "CHAR(8)",
        "ap_encerr": "BOOLEAN",
        "ap_etnia": "CHAR(4)",
        "ap_gestao": "CHAR(6)",
        "ap_mn_ind": "CHAR(1)",
        "ap_mndif": "BOOLEAN",
        "ap_motsai": "CHAR(2)",
        "ap_munpcn": "CHAR(6)",
        "ap_mvm": "CHAR(6)",
        "ap_natjur": "CHAR(4)",
        "ap_nuidade": "CHAR(2)",
        "ap_obito": "BOOLEAN",
        "ap_perman": "BOOLEAN",
        "ap_pripal": "TEXT",
        "ap_racacor": "TEXT",
        "ap_sexo": "CHAR(1)",
        "ap_tippre": "TEXT",
        "ap_tpapac": "CHAR(1)",
        "ap_tpaten": "TEXT",
        "ap_tpups": "TEXT",
        "ap_transf": "BOOLEAN",
        "ap_ufdif": "BOOLEAN",
        "ap_ufmun": "CHAR(6)",
        "ap_ufnacio": "TEXT",
        "ap_unisol": "TEXT",
        "ap_vl_ap": "CHAR(20)",
        "ar_cid10": "CHAR(4)",
        "ar_cidini1": "CHAR(4)",
        "ar_cidini2": "CHAR(4)",
        "ar_cidini3": "CHAR(4)",
        "ar_cidtr1": "CHAR(4)",
        "ar_cidtr2": "CHAR(4)",
        "ar_cidtr3": "CHAR(4)",
        "ar_conttr": "CHAR(1)",
        "ar_dtiden": "CHAR(8)",
        "ar_dtini1": "TEXT",
        "ar_dtini2": "TEXT",
        "ar_dtini3": "TEXT",
        "ar_dtintr": "CHAR(8)",
        "ar_estadi": "TEXT",
        "ar_fimar1": "CHAR(8)",
        "ar_fimar2": "CHAR(8)",
        "ar_fimar3": "CHAR(8)",
        "ar_finali": "CHAR(1)",
        "ar_grahis": "TEXT",
        "ar_iniar1": "CHAR(8)",
        "ar_iniar2": "CHAR(8)",
        "ar_iniar3": "CHAR(8)",
        "ar_linfin": "TEXT",
        "ar_numc1": "CHAR(3)",
        "ar_numc2": "CHAR(3)",
        "ar_numc3": "CHAR(3)",
        "ar_smrd": "CHAR(3)",
        "ar_trante": "CHAR(1)",
        "id_log": "VARCHAR(255)",
        "uf": "CHAR(2)"
    },
    "sia_apac_tratamento_dialitico": {
        "ap_alta": "BOOLEAN",
        "ap_apacant": "TEXT",
        "ap_autoriz": "CHAR(13)",
        "ap_catend": "TEXT",
        "ap_ceppcn": "TEXT",
        "ap_cidcas": "TEXT",
        "ap_cidpri": "TEXT",
        "ap_cidsec": "TEXT",
        "ap_cmp": "CHAR(6)",
        "ap_cnpjcpf": "TEXT",
        "ap_cnpjmnt": "CHAR(14)",
        "ap_cnspcn": "CHAR(15)",
        "ap_codemi": "CHAR(10)",
        "ap_coduni": "CHAR(7)",
        "ap_coidade": "CHAR(1)",
        "ap_condic": "CHAR(2)",
        "ap_dtaut": "TEXT",
        "ap_dtfim": "CHAR(8)",
        "ap_dtinic": "CHAR(8)",
        "ap_dtocor": "TEXT",
        "ap_dtsolic": "TEXT",
        "ap_encerr": "BOOLEAN",
        "ap_etnia": "TEXT",
        "ap_gestao": "CHAR(6)",
        "ap_mn_ind": "CHAR(1)",
        "ap_mndif": "BOOLEAN",
        "ap_motsai": "CHAR(2)",
        "ap_munpcn": "CHAR(6)",
        "ap_mvm": "CHAR(6)",
        "ap_natjur": "CHAR(4)",
        "ap_nuidade": "TEXT",
        "ap_obito": "BOOLEAN",
        "ap_perman": "BOOLEAN",
        "ap_pripal": "TEXT",
        "ap_racacor": "TEXT",
        "ap_sexo": "CHAR(1)",
        "ap_tippre": "TEXT",
        "ap_tpapac": "CHAR(1)",
        "ap_tpaten": "CHAR(2)",
        "ap_tpups": "TEXT",
        "ap_transf": "BOOLEAN",
        "ap_ufdif": "BOOLEAN",
        "ap_ufmun": "CHAR(6)",
        "ap_ufnacio": "TEXT",
        "ap_unisol": "TEXT",
        "ap_vl_ap": "CHAR(20)",
        "atd_acevas": "CHAR(1)",
        "atd_albumi": "TEXT",
        "atd_caract": "CHAR(1)",
        "atd_dtcli": "CHAR(8)",
        "atd_dtpdr": "CHAR(8)",
        "atd_fosfor": "TEXT",
        "atd_hb": "TEXT",
        "atd_hbsag": "CHAR(1)",
        "atd_hcv": "CHAR(1)",
        "atd_hiv": "CHAR(1)",
        "atd_interc": "CHAR(1)",
        "atd_ktvsem": "TEXT",
        "atd_maisne": "CHAR(1)",
        "atd_pth": "TEXT",
        "atd_seapto": "TEXT",
        "atd_seperi": "CHAR(1)",
        "atd_sitini": "CHAR(1)",
        "atd_sittra": "CHAR(1)",
        "atd_tru": "TEXT",
        "id_log": "VARCHAR(255)",
        "uf": "CHAR(2)"
    },
    "sia_boletim_producao_ambulatorial_individualizado": {
        "autoriz": "TEXT",
        "catend": "TEXT",
        "cboprof": "TEXT",
        "cidpri": "TEXT",
        "cnpj_cc": "TEXT",
        "cnpjcpf": "CHAR(14)",
        "cnpjmnt": "TEXT",
        "cns_pac": "CHAR(15)",
        "cnsprof": "CHAR(15)",
        "coduni": "CHAR(7)",
        "complex": "CHAR(1)",
        "condic": "CHAR(2)",
        "dt_atend": "CHAR(6)",
        "dt_process": "CHAR(6)",
        "dtnasc": "CHAR(8)",
        "etnia": "TEXT",
        "gestao": "CHAR(6)",
        "id_log": "VARCHAR(255)",
        "idadepac": "CHAR(2)",
        "mn_ind": "CHAR(1)",
        "mndif": "BOOLEAN",
        "munpac": "CHAR(6)",
        "nat_jur": "CHAR(4)",
        "proc_id": "TEXT",
        "qt_apres": "CHAR(11)",
        "qt_aprov": "CHAR(11)",
        "racacor": "TEXT",
        "sexopac": "CHAR(1)",
        "subfin": "TEXT",
        "tippre": "TEXT",
        "tpfin": "TEXT",
        "tpidadepac": "CHAR(1)",
        "tpups": "TEXT",
        "uf": "CHAR(2)",
        "ufdif": "BOOLEAN",
        "ufmun": "CHAR(6)",
        "vl_apres": "CHAR(20)",
        "vl_aprov": "CHAR(20)"
    }
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
            resultado = connection.execute(query).mappings().all()
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
    caminho_base_grupo = os.path.join(BASE_PATH, grupo)
    
    if not os.path.exists(caminho_base_grupo):
        logger.warning(f"O caminho base para o grupo {grupo} não existe: {caminho_base_grupo}")
        return pastas_de_arquivos

    # Expressão regular para identificar pastas que começam com o grupo seguido de UF e outros caracteres
    padrao_pasta = re.compile(rf"^{grupo}([A-Z]{{2}}).+\.parquet$", re.IGNORECASE)
    
    for nome_pasta in os.listdir(caminho_base_grupo):
        caminho_pasta = os.path.join(caminho_base_grupo, nome_pasta)
        if os.path.isdir(caminho_pasta) and padrao_pasta.match(nome_pasta):
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
    remove espaços em branco das colunas de texto e adiciona colunas ausentes como valores nulos,
    excluindo 'uf' e 'id_log', que serão adicionadas posteriormente.
    """
    # Excluir 'uf' e 'id_log' das colunas esperadas para normalização
    colunas_para_normalizar = [col for col in colunas_esperadas if col not in ('uf', 'id_log')]

    # Remover as colunas 'uf' e 'id_log' do DataFrame, se existirem
    df = df.drop(columns=['id', 'uf', 'id_log'], errors='ignore')

    # Convertendo colunas esperadas para minúsculas
    colunas_corretas = [col.lower() for col in colunas_para_normalizar]
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

def extrair_uf(nome, grupo):
    """
    Extrai a UF a partir do nome do arquivo ou pasta.
    """
    padrao = rf"^{grupo}(?P<uf>[A-Z]{{2}})"
    match = re.match(padrao, nome, re.IGNORECASE)
    if match:
        return match.group('uf').upper()
    else:
        return None

def carregar_dados_em_lotes(grupo, pastas_de_arquivos, tamanho_lote=10000):
    """
    Carrega os arquivos .parquet em lotes pequenos, adiciona as colunas 'uf' e 'id_log',
    e prepara os dados para inserção no banco de dados.
    """
    tabela = GRUPOS_INFO[grupo]["tabela"]
    arquivos_processados = verificar_ultimo_arquivo_processado(tabela)
    colunas_esperadas = GRUPOS_INFO[grupo]["colunas"]
    mapeamento_tipos = tipo_coluna_map.get(tabela, {})

    # Excluir 'uf' e 'id_log' das colunas esperadas para evitar duplicação
    colunas_para_insercao = [col for col in colunas_esperadas if col not in ('uf', 'id_log')]

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

                # Extrair UF do nome do arquivo, se não foi possível pelo nome da pasta
                uf = extrair_uf(nome_arquivo, grupo) or uf_pasta
                if not uf:
                    logger.warning(f"[{grupo}] Não foi possível extrair UF do arquivo {arquivo}")
                    continue

                # Adicionar a coluna 'uf' ao DataFrame
                df['uf'] = uf.upper()

                # Adicionar a coluna 'id_log' com base no nome da pasta, arquivo e índice
                df['id_log'] = [f"{id_arquivo}_{i}" for i in range(len(df))]

                # Ajustar a ordem das colunas para inserção
                colunas_ajustadas = colunas_para_insercao + ['uf', 'id_log']
                df = df.reindex(columns=colunas_ajustadas)

                for inicio in range(0, len(df), tamanho_lote):
                    yield df.iloc[inicio:inicio + tamanho_lote]
            except Exception as e:
                logger.error(f"[{grupo}] Erro ao carregar arquivo {arquivo}: {e}")
                            
def inserir_dados_em_lotes(tabela, df_lote, colunas_ajustadas):
    """
    Insere os dados no banco por lotes.
    """
    try:
        # Reindexar o DataFrame para garantir que contém apenas as colunas esperadas, na ordem correta
        df_lote = df_lote.reindex(columns=colunas_ajustadas)

        # Log de depuração: imprimir colunas e dimensões
        #logger.debug(f"[{tabela}] Colunas do DataFrame ({len(df_lote.columns)}): {df_lote.columns.tolist()}")
        #logger.debug(f"[{tabela}] Colunas para inserção ({len(colunas_ajustadas)}): {colunas_ajustadas}")
        logger.debug(f"[{tabela}] Número de registros no lote: {len(df_lote)}")

        # Verificar se o número de colunas corresponde
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

        # Opcional: imprimir uma amostra dos dados
        #logger.debug(f"[{tabela}] Amostra dos dados:\n{df_lote.head(1).to_string(index=False)}")

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
    try:
        for grupo, info in GRUPOS_INFO.items():
            tabela = info["tabela"]
            colunas_esperadas = info["colunas"]
            logger.info(f"[{grupo}] Iniciando processamento para a tabela {tabela}")
            
            # Log antes de verificar arquivos processados
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
    #arquivos_processados = verificar_ultimo_arquivo_processado(tabela)
    #print(f"Arquivos processados: {len(arquivos_processados)}")