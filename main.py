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

GRUPOS_INFO_SIA = {
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

GRUPOS_INFO_SIH = {
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
# ---------------------------------------------------------------------------
# Configurações básicas
# ---------------------------------------------------------------------------
load_dotenv()

# Diretório de logs
if not os.path.exists('logs'):
    os.makedirs('logs')

# Configurando logging para arquivo
logging.basicConfig(
    filename='logs/app.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Adicionando um handler para exibir logs no console (terminal)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_format = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
console_handler.setFormatter(console_format)
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
        # Caso deseje restringir a um subconjunto fixo, pode usar v not in [...],
        # mas no momento alguns grupos foram listados:
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
        """
        Verifica se a competência está no formato MM/YYYY.
        """
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
    
    # Lista de UFs do Brasil
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
        year_suffix = current_date.strftime('%y')  # ex: 22 para 2022
        month = current_date.strftime('%m')        # ex: 01 para janeiro
        
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
        
        # Avança para o próximo mês
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
# Processa os arquivos usando DuckDB
# ---------------------------------------------------------------------------
def get_cnes_column(grupo: str) -> str:
    """Retorna o nome da coluna CNES para o grupo específico"""
    if (grupo not in CAMPOS_CNES):
        return "CNES"  # coluna padrão se não encontrar mapeamento
    return CAMPOS_CNES[grupo][0]  # pega primeiro item da lista

def process_parquet_files(files: List[str], params: QueryParams) -> pd.DataFrame:
    log_execution("Iniciando processamento de arquivos")
    logging.info(f"[process_parquet_files] Iniciando processamento de {len(files)} arquivo(s).")
    
    con = duckdb.connect()
    cnes_str = ",".join([f"'{cnes}'" for cnes in params.cnes_list])
    
    # Obtém nome correto da coluna CNES para o grupo
    cnes_column = get_cnes_column(params.grupo)
    
    # Adiciona coluna CNES aos campos de agrupamento se não existir
    campos_agrupamento = params.campos_agrupamento.copy()
    if not any(col.lower() == cnes_column.lower() for col in campos_agrupamento):
        campos_agrupamento.insert(0, cnes_column)
    
    group_by_cols = ",".join(campos_agrupamento)
    
    results = []
    for file_path in files:
        logging.info(f"[process_parquet_files] Processando arquivo: {file_path}")
        
        query = f"""
        SELECT {group_by_cols}
        FROM read_parquet('{file_path}')
        WHERE LOWER({cnes_column}) IN (SELECT LOWER(unnest(ARRAY[{cnes_str}])))
        """
        try:
            df = con.execute(query).df()
            logging.info(f"[process_parquet_files] OK. Linhas retornadas: {len(df)}.")
            results.append(df)
        except Exception as e:
            logging.error(f"[process_parquet_files] Erro ao processar arquivo {file_path}: {e}")
    
    con.close()
    
    if not results:
        logging.info("[process_parquet_files] Nenhum resultado encontrado.")
        return pd.DataFrame()
    
    final_df = pd.concat(results, ignore_index=True)
    logging.info(f"[process_parquet_files] Concat final OK. Linhas totais: {len(final_df)}.")
    log_execution("Finalizado processamento de arquivos", False)
    return final_df

# ---------------------------------------------------------------------------
# Função de salvamento de resultados
# ---------------------------------------------------------------------------
def save_results(df: pd.DataFrame, table_name: str) -> None:
    logging.info(f"[save_results] Iniciando salvamento. Tabela destino: {table_name}")
    
    try:
        # Força recarregamento das variáveis de ambiente
        load_dotenv(override=True)  # Adicionar parâmetro override
        
        # Criar string de conexão com valores atualizados
        conn_str = (
            f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
            f"{quote_plus(os.getenv('DB_PASSWORD'))}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
            f"{os.getenv('DB_NAME')}"
        )
        
        engine = create_engine(conn_str)
        
        # Testar conexão antes de prosseguir
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
            logging.info("Conexão com banco testada com sucesso")
        
        # Salva no PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"[save_results] Tabela '{table_name}' salva/recriada no PostgreSQL (replace).")

        # Se tiver menos de 10M linhas, salva em CSV
        row_count = len(df)
        if row_count < 10_000_000:
            csv_dir = 'consultas'
            os.makedirs(csv_dir, exist_ok=True)
            csv_path = f'{csv_dir}/{table_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            df.to_csv(csv_path, index=False)
            logging.info(f"[save_results] DataFrame com {row_count} linha(s) salvo em CSV: {csv_path}")
        else:
            logging.info(f"[save_results] DataFrame com {row_count} linha(s). Não será salvo em CSV.")
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

# Configuração CORS
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
    """
    Endpoint para consulta de dados do DataSUS.
    
    Exemplo de requisição:
    ```
    POST /query
    
    {
        "base": "SIH",
        "grupo": "RD",
        "cnes_list": ["2077485", "2077493"],
        "campos_agrupamento": ["CNES", "ANO_CMPT", "MES_CMPT"],
        "competencia_inicio": "01/2022",
        "competencia_fim": "12/2022",
        "table_name": "sih_rd_2022"  
    }
    ```
    
    Outro exemplo com diferentes parâmetros:
    ```
    POST /query
    
    {
        "base": "SIA",
        "grupo": "PA",
        "cnes_list": ["2077485"],
        "campos_agrupamento": ["CNES", "PROC_ID"],
        "competencia_inicio": "06/2022",
        "competencia_fim": "06/2022",
        "table_name": "sia_pa_junho_2022" 
    }
    ```
    
    Se o parâmetro table_name não for fornecido, será gerado um nome automático
    no formato: {base}_{grupo}
    """
    log_execution("Iniciando processamento de requisição")
    logging.info(
        "[query_data] Recebendo requisição. "
        f"Base={params.base}, Grupo={params.grupo}, Competências={params.competencia_inicio} a {params.competencia_fim}."
    )
    try:
        # 1) Localiza arquivos .parquet
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
        
        # 2) Processa os arquivos
        result_df = process_parquet_files(files, params)
        
        if result_df.empty:
            msg = "Nenhum dado encontrado para os critérios especificados."
            logging.warning(f"[query_data] {msg}")
            raise HTTPException(status_code=404, detail=msg)
        
        # 3) Mapeia o grupo para um nome descritivo e define nome da tabela
        grupo_mapped = grupos_dict.get(params.grupo, params.grupo)
        # Usa o nome da tabela fornecido ou gera um nome padrão
        table_name = params.table_name if params.table_name else f"{params.base.lower()}_{grupo_mapped.lower()}"
        
        # 4) Salva os resultados
        save_results(result_df, table_name)
        
        logging.info("[query_data] Consulta processada com sucesso.")
        
        # 5) Retorna a resposta com os dados e metadados
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
