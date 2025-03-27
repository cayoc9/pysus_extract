import logging
import os
import datetime
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
from datetime import datetime
import traceback
import logging
from collections import defaultdict
import re
import duckdb
from sqlalchemy import text
import psutil

# Importações das definições
from api.definitions import (
    grupos_dict, CAMPOS_CNES, GRUPOS_INFO,
    QueryParams, Settings
)

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
    
    # Lista de colunas que contêm códigos com zeros à esquerda que devem ser preservados
    colunas_codigo = [col for col in df.columns if 
                      any(col.lower().startswith(prefix) for prefix in 
                          ['proc_', 'pa_codpro', 'pa_coduni', 'sp_procrea', 'sp_cnes'])]
    
    for col, dtype in schema.get('colunas', {}).items():
        if col not in df.columns:
            continue
        dtype = dtype.upper()
        original = df[col].copy()
        
        # Verifica se a coluna é uma que deve preservar zeros à esquerda
        is_codigo_column = col in colunas_codigo
        
        try:
            if any(nt in dtype for nt in ['NUMERIC', 'INT']) and not is_codigo_column:
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
                # Para colunas de código ou texto, mantenha como string sem modificações adicionais
                df[col] = df[col].astype('string')
                if is_codigo_column:
                    logging.info(f"Preservando zeros à esquerda para a coluna {col}")
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
def save_results(source_table: str, target_table: str, params: QueryParams, engine=None) -> None:
    """Processo de salvamento otimizado com validação em 5 etapas"""
    try:
        # =====================================================================
        # Etapa 1: Preparação e logging
        # =====================================================================
        logging.info("=== INÍCIO DO SALVAMENTO ===")
        logging.info(f"Parâmetros: {params.model_dump()}")
        logging.info(f"Origem: {source_table} → Destino: {target_table}")

        # =====================================================================
        # Etapa 2: Obtenção do schema com verificação de tipos
        # =====================================================================
        logging.info("Obtendo schema da tabela origem...")
        schema_query = f"""
            SELECT 
                column_name AS name,
                data_type AS type,
                CASE 
                    WHEN data_type LIKE 'VARCHAR%' THEN 'TEXT'
                    WHEN data_type LIKE 'DECIMAL%' THEN 'NUMERIC'
                    ELSE UPPER(data_type)
                END AS pg_type
            FROM information_schema.columns 
            WHERE table_name = '{source_table.lower()}'
        """
        schema = duckdb.execute(schema_query).fetchall()
        logging.info(f"Schema detectado ({len(schema)} colunas):\n{pd.DataFrame(schema)}")

        # =====================================================================
        # Etapa 3: Criação da tabela com transação atômica
        # =====================================================================
        logging.info("Criando tabela no PostgreSQL...")
        columns = []
        for col in schema:
            pg_type = GRUPOS_INFO.get(params.grupo, {}).get('colunas', {}).get(col[0].lower(), col[2])
            columns.append(f'"{col[0]}" {pg_type}')
        
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {target_table} ({', '.join(columns)})"
        
        with engine.connect() as conn:
            # Transação explícita com commit garantido
            with conn.begin():
                logging.info(f"Executando DDL:\n{create_table_sql}")
                conn.execute(text(f"DROP TABLE IF EXISTS {target_table} CASCADE"))
                conn.execute(text(create_table_sql))
            
            # Verificação pós-criação
            exists = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = '{target_table.lower()}'
                )
            """)).scalar()
            
            if not exists:
                raise RuntimeError(f"Falha na criação da tabela {target_table}")
            
            logging.info("Tabela criada com sucesso - Verificação OK")

        # =====================================================================
        # Etapa 4: Transferência de dados com conexão persistente
        # =====================================================================
        logging.info("Iniciando transferência via COPY...")
        
        # Obtenção de parâmetros de conexão a partir de settings (precisa ser passado como parâmetro para esta função)
        settings = Settings()
        connection_params = {
            "dbname": settings.db_name,
            "user": settings.db_user,
            "password": settings.db_pass,
            "host": settings.db_host,
            "port": settings.db_port,
            "connect_timeout": 10
        }
        
        try:
            # Conexão explícita com tratamento de erro
            duckdb.execute("INSTALL postgres; LOAD postgres;")
            attach_cmd = f"ATTACH '{' '.join(f'{k}={v}' for k,v in connection_params.items())}' AS pg_db (TYPE POSTGRES)"
            duckdb.execute(attach_cmd)
            
            # Operação de COPY com batch size
            copy_query = f"""
                INSERT INTO pg_db.{target_table} 
                SELECT * FROM {source_table}
            """
            
            result = duckdb.execute(copy_query)
            logging.info(f"Dados transferidos - {result.fetchall()[0][0]} registros")
            
        except Exception as copy_error:
            logging.error("Falha na transferência de dados:")
            logging.error(f"Tipo: {type(copy_error).__name__}")
            logging.error(f"Mensagem: {str(copy_error)}")
            raise

        # =====================================================================
        # Etapa 5: Validação pós-transferência
        # =====================================================================
        with engine.connect() as conn:
            pg_count = conn.execute(text(f"SELECT COUNT(*) FROM {target_table}")).scalar()
            duckdb_count = duckdb.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
            
            if pg_count != duckdb_count:
                raise ValueError(f"Divergência de registros: DuckDB={duckdb_count} vs PG={pg_count}")
            
            logging.info(f"Validação OK - Registros consistentes: {pg_count}")

    except Exception as e:
        logging.error("Falha crítica no processo de salvamento", exc_info=True)
        raise RuntimeError(f"Erro durante o salvamento: {str(e)}") from e

    finally:
        # =====================================================================
        # Limpeza de recursos
        # =====================================================================
        logging.info("Executando limpeza final...")
        try:
            duckdb.execute("DETACH pg_db")
            logging.info("Conexão PostgreSQL liberada")
        except Exception as detach_error:
            logging.warning(f"Erro ao desconectar: {str(detach_error)}")

# -----------------------------------------------------------------------------
# Funções auxiliares de tarefas e processamento adaptativo
# -----------------------------------------------------------------------------
def adaptive_processing(files: List[str], params: QueryParams, engine=None) -> None:
    chunk_size = 50  # Tamanho inicial do chunk
    total_files = len(files)
    processed = 0
    logging.info(f"Iniciando processamento adaptativo de {total_files} arquivos")
    
    for i in range(0, total_files, chunk_size):
        chunk = files[i:i+chunk_size]
        logging.info(f"Processando chunk {i//chunk_size + 1} com {len(chunk)} arquivos")
        
        try:
            # Processar e salvar
            temp_table = process_data(chunk, params)
            table_name = params.table_name if params.table_name else GRUPOS_INFO[params.grupo]['tabela']
            save_results(temp_table, table_name, params, engine)
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
# Função para construção de query com tratamento de erros
# -----------------------------------------------------------------------------
def build_conversion_query(grupo: str, columns: list) -> str:
    """Constrói query de conversão usando tipos do GRUPOS_INFO"""
    if grupo not in GRUPOS_INFO:
        raise ValueError(f"Grupo {grupo} não encontrado no GRUPOS_INFO")
    
    schema = GRUPOS_INFO[grupo]['colunas']
    selects = []
    
    # Lista de prefixos de colunas que devem ser mantidas como TEXT para preservar zeros à esquerda
    colunas_codigo = ['proc_', 'pa_codpro', 'pa_coduni', 'sp_procrea', 'sp_cnes', 'sp_atoprof']
    
    for col in columns:
        col_lower = col.lower()  # Normaliza para minúsculas
        dtype = schema.get(col_lower, 'TEXT')  # Usa TEXT como fallback
        
        # Verifica se a coluna contém códigos que devem preservar zeros à esquerda
        is_codigo_column = any(col_lower.startswith(prefix) for prefix in colunas_codigo)
        
        # Se for coluna de código com zeros à esquerda, força para TEXT
        if is_codigo_column and 'NUMERIC' in dtype:
            logging.info(f"Preservando zeros à esquerda em {col} - usando TEXT ao invés de {dtype}")
            conversion_expr = f"{col}::STRING AS {col}"
        else:
            # Expressão de conversão principal para outros tipos
            conversion_expr = f"TRY_CAST({col} AS {dtype}) AS {col}"
        
        # Expressão de erro
        error_conditions = []
        if 'NUMERIC' in dtype and not is_codigo_column:
            error_conditions.append(f"{col}::STRING ~ '[^0-9]'")  # Caracteres não numéricos
        elif 'DATE' in dtype:
            error_conditions.append(f"strptime({col}, '%Y%m%d') IS NULL")  # Formato inválido
        
        error_condition = " OR ".join(error_conditions) if error_conditions else "FALSE"
        error_expr = f"CASE WHEN {col} IS NOT NULL AND ({error_condition}) THEN 'ERRO_TIPO' ELSE NULL END AS new_{col}_error"
        
        selects.append(f"{conversion_expr}, {error_expr}")
        logging.info(f"Conversão aplicada: {col} -> {dtype if not is_codigo_column else 'TEXT (preservando zeros)'}")

    return ", ".join(selects)

def get_cnes_column(grupo: str) -> str:
    """Obtém o nome da coluna CNES correspondente ao grupo"""
    grupo = grupo.upper()
    if grupo not in CAMPOS_CNES:
        raise ValueError(f"Grupo {grupo} não possui mapeamento de CNES")
    return CAMPOS_CNES[grupo]

def process_data(
    files: List[str], 
    params: QueryParams,
    is_chunk: bool = False
) -> str:
    """
    Processa arquivos Parquet com as seguintes etapas:
    1. Filtragem inicial por CNES e seleção de colunas
    2. Limpeza básica dos dados
    3. Conversão de tipos com registro de erros
    4. Validação e ajustes finais
    """
    try:
        # =====================================================================
        # Passo 1: Filtragem inicial
        # =====================================================================
        cnes_col = get_cnes_column(params.grupo)
        campos = params.campos_agrupamento.copy()
        
        if cnes_col not in campos:
            campos.append(cnes_col)
            logging.info(f"Adicionada coluna CNES: {cnes_col}")

        # Construir cláusula WHERE
        where_clause = ""
        if params.cnes_list != ["*"]:
            cnes_list = ", ".join([f"'{c}'" for c in params.cnes_list])
            where_clause = f"WHERE {cnes_col} IN ({cnes_list})"
            logging.info(f"Filtro CNES aplicado: {len(params.cnes_list)} valores")

        # Criar tabela filtrada
        duckdb.execute(f"""
            CREATE OR REPLACE TABLE temp_filtered AS
            SELECT {', '.join(campos)}
            FROM read_parquet({files})
            {where_clause}
        """)
        
        # Log de amostra após filtragem
        sample = duckdb.execute("SELECT * FROM temp_filtered LIMIT 5").fetchdf()
        logging.info("Amostra pós-filtro (temp_filtered):\n%s", sample)
        logging.info("Tipos originais:\n%s", sample.dtypes)

        # =====================================================================
        # Passo 2: Limpeza dos dados
        # =====================================================================
        clean_operations = []
        columns = duckdb.execute("DESCRIBE temp_filtered").fetchall()
        
        for col in columns:
            col_name, col_type = col[0], col[1]
            if 'VARCHAR' in col_type:
                clean_operations.append(f"TRIM({col_name}) AS {col_name}")
                logging.info(f"Limpeza aplicada em {col_name}: TRIM")
            else:
                clean_operations.append(col_name)

        duckdb.execute(f"""
            CREATE OR REPLACE TABLE temp_cleaned AS
            SELECT {', '.join(clean_operations)}
            FROM temp_filtered
        """)
        
        # Identificar colunas textuais para validação
        text_columns = [
            col[0] for col in duckdb.execute("DESCRIBE temp_cleaned").fetchall()
            if 'VARCHAR' in col[1]
        ]

        # Gerar condições dinamicamente
        validation_conditions = []
        for col in text_columns:
            validation_conditions.append(f"LENGTH({col}) > 0")

        condition = " OR ".join(validation_conditions) if validation_conditions else "1=0"

        # Query de validação atualizada
        validation_query = f"""
            SELECT 
                COUNT(*) AS total_linhas,
                SUM(CASE WHEN ({condition}) THEN 1 ELSE 0 END) AS textos_validos
            FROM temp_cleaned
        """

        sample_clean = duckdb.execute(validation_query).fetchdf()
        logging.info("Estatísticas de limpeza:\n%s", sample_clean)

        # =====================================================================
        # Passo 3: Conversão de tipos
        # =====================================================================
        conversion_query = build_conversion_query(params.grupo, campos)
        
        logging.info("Iniciando conversão de tipos com query:")
        logging.info(conversion_query[:500] + "...")  # Log parcial da query

        duckdb.execute(f"""
            CREATE OR REPLACE TABLE temp_converted AS
            SELECT {conversion_query}
            FROM temp_cleaned
        """)

        # =====================================================================
        # Passo 4: Validação e ajustes
        # =====================================================================
        # Verificar colunas de erro
        describe_df = duckdb.execute("DESCRIBE temp_converted").fetchdf()
        logging.info(f"Colunas disponíveis no DESCRIBE: {describe_df.columns.tolist()}")

        error_cols = [col for col in describe_df['column_name'] 
                      if col.startswith('new_')]
        
        for col in error_cols:
            error_count = duckdb.execute(f"""
                SELECT COUNT(*) 
                FROM temp_converted 
                WHERE {col} IS NOT NULL
            """).fetchone()[0]
            
            if error_count > 0:
                logging.warning(f"Erros detectados em {col}: {error_count} registros")
            else:
                logging.info(f"Coluna {col} sem erros - será removida")
                duckdb.execute(f"""
                    ALTER TABLE temp_converted DROP COLUMN {col};
                """)

        # Log final
        final_sample = duckdb.execute("""
            SELECT 
                column_name AS coluna,
                data_type AS tipo
            FROM information_schema.columns 
            WHERE table_name = 'temp_converted'
        """).fetchdf()
        
        logging.info("Estrutura final da tabela convertida:\n%s", final_sample)

        return "temp_converted"

    except Exception as e:
        logging.error("Falha no processamento de dados", exc_info=True)
        raise

def process_parquet_files(files: List[str], params: QueryParams) -> pd.DataFrame:
    """Wrapper para processamento completo"""
    return process_data(files, params, is_chunk=False)

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

def validate_csv_sample(csv_path: str, table_name: str, engine=None) -> bool:
    """Valida uma amostra do CSV contra o schema do PostgreSQL"""
    try:
        with engine.connect() as conn:
            # Criar tabela temporária
            from uuid import uuid4
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

def process_with_logging(files: List[str], params: QueryParams, start_time: str):
    try:
        adaptive_processing(files, params, engine)
        logging.info("Processamento concluído com sucesso")
    except Exception as e:
        logging.critical(f"Erro catastrófico: {str(e)}\n{traceback.format_exc()}")
        raise

def verify_db_connection(engine):
    try:
        with engine.connect() as connection:
            connection.execute(text('SELECT 1'))
            logging.info("Conexão com o PostgreSQL verificada com sucesso")
    except Exception as e:
        logging.error(f"Erro na conexão com o PostgreSQL: {str(e)}")
        raise 