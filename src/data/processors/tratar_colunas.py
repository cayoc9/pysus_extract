import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from datetime import datetime
from utils.db_utils import (
    listar_e_renomear_colunas_para_minusculo,
    alterar_tipos_colunas_com_using
)

# Configuração do ambiente
load_dotenv()

DATABASE_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
                f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)

print("Iniciando a execução da função...")

LOG_FILE = "alterar_tipos_colunas_log.txt"


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
      'sexo':  "SMALLINT"},
    'sih_aih_rejeitada_erro': 
    {'sequencia': 'INTEGER', 'remessa': 'VARCHAR(50)', 'cnes': 'INTEGER', 'aih': 'BIGINT', 'ano': 'SMALLINT', 'mes': 'SMALLINT', 
     'dt_inter': 'DATE', 'dt_saida': 'DATE', 'mun_mov': 'INTEGER', 'uf_zi': 'INTEGER', 'mun_res': 'INTEGER', 'uf_res': 'CHAR(2)',
       'co_erro': 'VARCHAR(10)'}
}



def log_result(query, message):
    """
    Salva o resultado de uma query e sua resposta no arquivo de log.
    
    :param query: A query executada
    :param message: A mensagem de retorno (sucesso ou erro)
    """
    with open(LOG_FILE, "a") as log_file:
        log_file.write(f"=== {datetime.now()} ===\n")
        log_file.write(f"Query: {query}\n")
        log_file.write(f"Resultado: {message}\n")
        log_file.write("=" * 50 + "\n")

if __name__ == "__main__":
    #listar_e_renomear_colunas_para_minusculo('sih_aih_rejeitada')
    if os.path.exists(LOG_FILE):
       os.remove(LOG_FILE)

    alterar_tipos_colunas_com_using(tipo_coluna_map)



