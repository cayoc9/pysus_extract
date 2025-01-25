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

file_handler = logging.FileHandler(os.path.join(LOG_DIR, f"upload_sih_new{os.getpid()}.log"))
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
        "tabela": "new_sih_aih_reduzida",
        "colunas": {
            "ano_cmpt": "SMALLINT",
            "aud_just": "TEXT",
            "car_int": "CHAR(2)",
            "cbor": "CHAR(6)",
            "cep": "CHAR(8)",
            "cgc_hosp": "CHAR(14)",
            "cid_asso": "VARCHAR(4)",
            "cid_morte": "VARCHAR(4)",
            "cid_notif": "VARCHAR(4)",
            "cnaer": "CHAR(3)",
            "cnes": "INTEGER",
            "cnpj_mant": "CHAR(14)",
            "cobranca": "VARCHAR(2)",
            "cod_idade": "SMALLINT",
            "complex": "CHAR(2)",
            "contracep1": "CHAR(2)",
            "contracep2": "CHAR(2)",
            "cpf_aut": "CHAR(11)",
            "diag_princ": "VARCHAR(10)",
            "diag_secun": "VARCHAR(4)",
            "diagsec1": "VARCHAR(4)",
            "diagsec2": "VARCHAR(4)",
            "diagsec3": "VARCHAR(4)",
            "diagsec4": "VARCHAR(4)",
            "diagsec5": "VARCHAR(4)",
            "diagsec6": "VARCHAR(4)",
            "diagsec7": "VARCHAR(4)",
            "diagsec8": "VARCHAR(4)",
            "diagsec9": "VARCHAR(4)",
            "diar_acom": "SMALLINT",
            "dias_perm": "SMALLINT",
            "dt_inter": "DATE",
            "dt_saida": "DATE",
            "espec": "CHAR(2)",
            "etnia": "CHAR(4)",
            "faec_tp": "CHAR(6)",
            "financ": "CHAR(2)",
            "gestao": "SMALLINT",
            "gestor_cod": "CHAR(5)",
            "gestor_cpf": "CHAR(15)",
            "gestor_dt": "DATE",
            "gestor_tp": "SMALLINT",
            "gestrisco": "BOOLEAN",
            "homonimo": "BOOLEAN",
            "id_log": "VARCHAR(255)",
            "idade": "SMALLINT",
            "ident": "SMALLINT",
            "ind_vdrl": "BOOLEAN",
            "infehosp": "BOOLEAN",
            "insc_pn": "CHAR(12)",
            "instru": "SMALLINT",
            "marca_uci": "CHAR(2)",
            "marca_uti": "CHAR(2)",
            "mes_cmpt": "SMALLINT",
            "morte": "BOOLEAN",
            "munic_mov": "INTEGER",
            "munic_res": "INTEGER",
            "n_aih": "BIGINT",
            "nacional": "VARCHAR(3)",
            "nasc": "DATE",
            "nat_jur": "VARCHAR(4)",
            "natureza": "CHAR(2)",
            "new_dt_inter": "TEXT",
            "new_dt_saida": "TEXT",
            "new_gestor_dt": "TEXT",
            "new_nasc": "TEXT",
            "num_filhos": "SMALLINT",
            "num_proc": "VARCHAR(4)",
            "proc_rea": "VARCHAR(20)",
            "proc_solic": "VARCHAR(20)",
            "qt_diarias": "SMALLINT",
            "raca_cor": "CHAR(2)",
            "regct": "CHAR(4)",
            "remessa": "VARCHAR(50)",
            "rubrica": "CHAR(4)",
            "seq_aih5": "SMALLINT",
            "sequencia": "BIGINT",
            "sexo": "CHAR(1)",
            "sis_just": "TEXT",
            "tot_pt_sp": "NUMERIC",
            "tpdisec1": "SMALLINT",
            "tpdisec2": "SMALLINT",
            "tpdisec3": "SMALLINT",
            "tpdisec4": "SMALLINT",
            "tpdisec5": "SMALLINT",
            "tpdisec6": "SMALLINT",
            "tpdisec7": "SMALLINT",
            "tpdisec8": "SMALLINT",
            "tpdisec9": "SMALLINT",
            "uf_zi": "INTEGER",
            "us_tot": "NUMERIC",
            "uti_int_al": "SMALLINT",
            "uti_int_an": "SMALLINT",
            "uti_int_in": "SMALLINT",
            "uti_int_to": "SMALLINT",
            "uti_mes_al": "SMALLINT",
            "uti_mes_an": "SMALLINT",
            "uti_mes_in": "SMALLINT",
            "uti_mes_to": "SMALLINT",
            "val_acomp": "NUMERIC",
            "val_obsang": "NUMERIC",
            "val_ortp": "NUMERIC",
            "val_ped1ac": "NUMERIC",
            "val_rn": "NUMERIC",
            "val_sadt": "NUMERIC",
            "val_sadtsr": "NUMERIC",
            "val_sangue": "NUMERIC",
            "val_sh": "NUMERIC",
            "val_sh_fed": "NUMERIC",
            "val_sh_ges": "NUMERIC",
            "val_sp": "NUMERIC",
            "val_sp_fed": "NUMERIC",
            "val_sp_ges": "NUMERIC",
            "val_tot": "NUMERIC",
            "val_transp": "NUMERIC",
            "val_uci": "NUMERIC",
            "val_uti": "NUMERIC",
            "vincprev": "SMALLINT"
        }
    },
    "RJ": {
        "tabela": "new_sih_aih_rejeitada",
        "colunas": {
            "ano_cmpt": "INTEGER",
            "car_int": "VARCHAR(2)",
            "cbor": "VARCHAR(6)",
            "cep": "VARCHAR(8)",
            "cgc_hosp": "VARCHAR(14)",
            "cid_asso": "VARCHAR(4)",
            "cid_morte": "VARCHAR(4)",
            "cid_notif": "VARCHAR(4)",
            "cnaer": "VARCHAR(3)",
            "cnes": "INTEGER",
            "cnpj_mant": "VARCHAR(14)",
            "cobranca": "SMALLINT",
            "cod_idade": "SMALLINT",
            "complex": "VARCHAR(2)",
            "contracep1": "VARCHAR(2)",
            "contracep2": "VARCHAR(2)",
            "cpf_aut": "VARCHAR(11)",
            "diag_princ": "VARCHAR(4)",
            "diag_secun": "VARCHAR(4)",
            "diar_acom": "SMALLINT",
            "dias_perm": "SMALLINT",
            "dt_inter": "DATE",
            "dt_saida": "DATE",
            "espec": "SMALLINT",
            "etnia": "VARCHAR(4)",
            "faec_tp": "VARCHAR(6)",
            "financ": "VARCHAR(2)",
            "gestao": "SMALLINT",
            "gestor_cod": "VARCHAR(5)",
            "gestor_cpf": "VARCHAR(15)",
            "gestor_dt": "DATE",
            "gestor_tp": "SMALLINT",
            "gestrisco": "SMALLINT",
            "homonimo": "SMALLINT",
            "id_log": "VARCHAR(255)",
            "idade": "INTEGER",
            "ident": "SMALLINT",
            "ind_vdrl": "SMALLINT",
            "infehosp": "VARCHAR(1)",
            "insc_pn": "VARCHAR(12)",
            "instru": "SMALLINT",
            "marca_uti": "SMALLINT",
            "mes_cmpt": "SMALLINT",
            "morte": "SMALLINT",
            "munic_mov": "VARCHAR(6)",
            "munic_res": "VARCHAR(6)",
            "n_aih": "BIGINT",
            "nacional": "VARCHAR(3)",
            "nasc": "DATE",
            "nat_jur": "VARCHAR(4)",
            "natureza": "VARCHAR(2)",
            "new_dt_inter": "TEXT",
            "new_dt_saida": "TEXT",
            "new_gestor_dt": "TEXT",
            "new_nasc": "TEXT",
            "num_filhos": "SMALLINT",
            "num_proc": "VARCHAR(4)",
            "proc_rea": "VARCHAR(10)",
            "proc_solic": "VARCHAR(10)",
            "qt_diarias": "SMALLINT",
            "raca_cor": "VARCHAR(2)",
            "regct": "VARCHAR(4)",
            "remessa": "VARCHAR",
            "rubrica": "SMALLINT",
            "seq_aih5": "SMALLINT",
            "sequencia": "INTEGER",
            "sexo": "SMALLINT",
            "st_bloq": "SMALLINT",
            "st_mot_blo": "VARCHAR(2)",
            "st_situac": "SMALLINT",
            "tot_pt_sp": "SMALLINT",
            "uf_zi": "VARCHAR(6)",
            "us_tot": "NUMERIC",
            "uti_int_al": "SMALLINT",
            "uti_int_an": "SMALLINT",
            "uti_int_in": "SMALLINT",
            "uti_int_to": "SMALLINT",
            "uti_mes_al": "SMALLINT",
            "uti_mes_an": "SMALLINT",
            "uti_mes_in": "SMALLINT",
            "uti_mes_to": "SMALLINT",
            "val_acomp": "NUMERIC",
            "val_obsang": "NUMERIC",
            "val_ortp": "NUMERIC",
            "val_ped1ac": "NUMERIC",
            "val_rn": "NUMERIC",
            "val_sadt": "NUMERIC",
            "val_sadtsr": "NUMERIC",
            "val_sangue": "NUMERIC",
            "val_sh": "NUMERIC",
            "val_sp": "NUMERIC",
            "val_tot": "NUMERIC",
            "val_transp": "NUMERIC",
            "val_uti": "NUMERIC",
            "vincprev": "SMALLINT"
        }
    },
    "ER": {
        "tabela": "new_sih_aih_rejeitada_erro",
        "colunas": {
            "aih": "BIGINT",
            "ano": "SMALLINT",
            "cnes": "INTEGER",
            "co_erro": "VARCHAR(10)",
            "dt_inter": "DATE",
            "dt_saida": "DATE",
            "id_log": "VARCHAR(255)",
            "mes": "SMALLINT",
            "mun_mov": "INTEGER",
            "mun_res": "INTEGER",
            "new_dt_inter": "TEXT",
            "new_dt_saida": "TEXT",
            "remessa": "VARCHAR(50)",
            "sequencia": "INTEGER",
            "uf_res": "CHAR(2)",
            "uf_zi": "INTEGER"
        }
    }
}




ESTADOS = [
    "BA", "MG", "PR",  "SP"
]

ANOS = range(2008, 2025)

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
    """
    Tenta converter uma string no formato 'YYYYMMDD' para 'YYYY-MM-DD'.
    Se não estiver nesse formato, tenta 'YYYYDDMM'.
    Se a data for inválida ou o mês for inválido, retorna None e indica a necessidade de criar uma nova coluna.
    
    Args:
        valor (str): Valor a ser convertido.
    
    Returns:
        tuple: (data_convertida, precisa_new_col)
               data_convertida: datetime.date ou None
               precisa_new_col: True se precisar salvar o valor original na coluna de backup
    """
    # Valor no formato YYYYMMDD ou YYYYDDMM
    # Se "00000000": retorna None indicando para nulificar.
    if valor == "00000000":
        return None, True  # None date, True = precisamos criar new_col

    # Função interna para validar e converter data
    def data_valida(a, m, d):
        try:
            return datetime(a, m, d).date()
        except ValueError:
            return None

    # Tenta no formato YYYYMMDD
    if len(valor) == 8 and valor.isdigit():
        ano_str = valor[0:4]
        mes_str = valor[4:6]
        dia_str = valor[6:8]
        ano = int(ano_str)
        mes = int(mes_str)
        dia = int(dia_str)
        
        data = data_valida(ano, mes, dia)
        if data:
            return data, False  # Data válida, não precisa criar new_col

        # Se falhar, tenta no formato YYYYDDMM
        dia_str_alt = valor[4:6]
        mes_str_alt = valor[6:8]
        dia_alt = int(dia_str_alt)
        mes_alt = int(mes_str_alt)
        
        # Verifica se o mês alternativo é válido
        if 1 <= mes_alt <= 12:
            data_alt = data_valida(ano, mes_alt, dia_alt)
            if data_alt:
                # Retorna a data corrigida e indica que a data original precisa ser armazenada
                return data_alt, True
    # Se não estiver em nenhum dos formatos esperados
    return None, True

def converter_tipos(df, mapeamento_tipos):
    """
    Converte os tipos das colunas do DataFrame com base no mapeamento fornecido.
    Para colunas do tipo DATE, verifica e corrige os formatos YYYYMMDD e YYYYDDMM.

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

            # Salvar os valores originais antes de qualquer conversão
            original_values = df[col].astype(str).copy()

            # Inicializar a coluna de backup com None
            df[new_col] = None

            # Aplicar a função de correção de data
            df['temp_date'] = df[col].astype(str).apply(lambda x: tentar_corrigir_data(x))

            # Extrair os dados corrigidos e a flag de necessidade de backup
            df[col] = df['temp_date'].apply(lambda x: x[0] if x else None)
            df[new_col] = df.apply(lambda row: original_values[row.name] if row['temp_date'] and row['temp_date'][1] else None, axis=1)

            # Remover a coluna temporária
            df = df.drop(columns=['temp_date'])

            # Converter para datetime.date
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

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
                    # Mantém TEXT como fallback
                    df[col] = df[col].astype(str).str.strip()
            except Exception as e:
                raise TypeError(f"Erro ao converter coluna '{col}' para o tipo '{tipo}': {e}")

    return df

def normalizar_colunas(df, colunas_esperadas, mapeamento_tipos):
    """
    Normaliza as colunas do DataFrame:
    - Remove colunas indesejadas.
    - Normaliza os nomes das colunas para minúsculas.
    - Adiciona colunas ausentes com valor None ou string vazia conforme o tipo.
    
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
            tipo = mapeamento_tipos[coluna].strip().upper()
            if tipo.startswith("CHAR") or tipo.startswith("VARCHAR") or tipo in ["SMALLINT", "INTEGER", "BIGINT", "BOOLEAN", "DATE", "NUMERIC", "TEXT"]:
                # Definir colunas CHAR(n) e VARCHAR(n) como strings vazias
                if tipo.startswith("CHAR") or tipo.startswith("VARCHAR") or tipo == "TEXT":
                    df[col_lower] = ""
                else:
                    df[col_lower] = None
            else:
                # Fallback para tipos desconhecidos
                df[col_lower] = None

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
    colunas_esperadas_dict = GRUPOS_INFO[grupo]["colunas"]
    colunas_esperadas = list(colunas_esperadas_dict.keys())

    # Lista de CNES a manter
    cnes_cods = [27014, 2129469, 2082527, 4049020, 2688433, 2751038, 2751046, 13838, 2525933, 2510189, 2390043]

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

                uf = extrair_uf(nome_arquivo, grupo) or uf_pasta
                if not uf:
                    logger.warning(f"[{grupo}] Não foi possível extrair UF do arquivo {arquivo}")
                    continue

                # Define a coluna 'uf' se necessário
                df['uf'] = uf.upper()

                # Define a coluna 'id_log'
                df['id_log'] = [f"{id_arquivo}_{i}" for i in range(len(df))]

                df = ajustar_ordem_colunas(df, colunas_esperadas)

                # Filtrar apenas linhas com cnes nos códigos especificados
                if 'cnes' in df.columns:
                    # Certifique-se de que 'cnes' é numérico
                    df['cnes'] = pd.to_numeric(df['cnes'], errors='coerce')
                    df = df[df['cnes'].isin(cnes_cods)]
                else:
                    logger.warning(f"[{grupo}] A coluna 'cnes' não está presente, não será aplicado filtro de CNES.")

                # Se após o filtro o DataFrame estiver vazio, pular
                if df.empty:
                    logger.info(f"[{grupo}] Nenhum registro com CNES desejado em {arquivo}. Pulando...")
                    continue

                for inicio in range(0, len(df), tamanho_lote):
                    yield df.iloc[inicio:inicio + tamanho_lote]
            except Exception as e:
                logger.error(f"[{grupo}] Erro ao carregar arquivo {arquivo}: {e}")

def inserir_dados_em_lotes(tabela, df_lote, colunas_ajustadas, grupo):
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

        # Garantir que valores nas colunas CHAR(n) não excedam o tamanho
        for col in colunas_ajustadas:
            tipo = GRUPOS_INFO[grupo]["colunas"][col].strip().upper()
            if tipo.startswith("CHAR("):
                tamanho = int(re.findall(r'CHAR\((\d+)\)', tipo)[0])
                df_lote[col] = df_lote[col].astype(str).str.slice(0, tamanho)
            elif tipo.startswith("VARCHAR("):
                tamanho = int(re.findall(r'VARCHAR\((\d+)\)', tipo)[0])
                df_lote[col] = df_lote[col].astype(str).str.slice(0, tamanho)

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
                inserir_dados_em_lotes(tabela, df_lote, colunas_esperadas, grupo)  # Passando 'grupo' aqui
        logger.info("Processo concluído.")
    except Exception as e:
        logger.critical(f"Erro crítico no processamento: {e}", exc_info=True)

if __name__ == "__main__":
    processar_dados()