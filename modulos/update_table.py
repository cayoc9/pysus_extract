import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

def merge_and_partition_tables():
    # Load environment variables
    load_dotenv()
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')

    # Connect to PostgreSQL database
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = False  # Disable autocommit
        cursor = conn.cursor()
        print("Conectado ao banco de dados com sucesso.")
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return

    try:
        # Step 1: Analyze columns
        print("Analisando as colunas...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'sih_serviços_profissionais';
        """)
        columns_table_1 = {row[0].lower() for row in cursor.fetchall()}

        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'SIH_Serviços_Profissionais';
        """)
        columns_table_2 = {row[0].lower() for row in cursor.fetchall()}

        # Common columns between the two tables
        common_columns = sorted(columns_table_1.intersection(columns_table_2))
        print(f"Colunas comuns: {common_columns}")

        # Create a list of column identifiers for SQL query
        columns_identifiers = [sql.Identifier(col) for col in common_columns]

        # Step 2: Check for duplicates between the tables
        print("Verificando duplicados entre as tabelas...")
        query = sql.SQL("""
            SELECT {columns}
            FROM {table1}
            INTERSECT
            SELECT {columns}
            FROM {table2};
        """).format(
            columns=sql.SQL(', ').join(columns_identifiers),
            table1=sql.Identifier('sih_serviços_profissionais'),
            table2=sql.Identifier('SIH_Serviços_Profissionais')
        )
        cursor.execute(query)
        duplicates = cursor.fetchall()
        print(f"Encontrados {len(duplicates)} registros duplicados.")

        # Step 3: Create merged table without duplicates
        print("Fazendo merge das tabelas sem duplicados...")
        query_merge = sql.SQL("""
            CREATE TABLE IF NOT EXISTS SIH_Serviços_Profissionais_Merged AS
            SELECT DISTINCT {columns} FROM (
                SELECT {columns} FROM {table1}
                UNION
                SELECT {columns} FROM {table2}
            ) AS merged_data;
        """).format(
            columns=sql.SQL(', ').join(columns_identifiers),
            table1=sql.Identifier('sih_serviços_profissionais'),
            table2=sql.Identifier('SIH_Serviços_Profissionais')
        )
        cursor.execute(query_merge)
        print("Merge concluído. Tabela temporária criada com os dados mesclados.")

        # Step 4: Rename final table
        print("Renomeando tabela para SIH_Serviços_Profissionais...")
        cursor.execute("""
            DROP TABLE IF EXISTS SIH_Serviços_Profissionais CASCADE;
        """)
        cursor.execute("""
            ALTER TABLE SIH_Serviços_Profissionais_Merged
            RENAME TO SIH_Serviços_Profissionais;
        """)
        print("Tabela renomeada com sucesso.")

        # Step 5: Alter specific data types
        print("Alterando tipos de dados na tabela final...")
        cursor.execute("""
            ALTER TABLE SIH_Serviços_Profissionais
            ALTER COLUMN sp_valato TYPE NUMERIC(10, 2) USING sp_valato::NUMERIC,
            ALTER COLUMN sp_dtinter TYPE DATE USING TO_DATE(sp_dtinter, 'YYYYMMDD');
        """)

        # Step 6: Create partitioned table by state
        print("Criando tabela particionada por estado...")
        # Build the CREATE TABLE statement using sql module
        create_table_query = sql.SQL("""
            CREATE TABLE SIH_Serviços_Profissionais (
                {columns},
                PRIMARY KEY (sp_uf, sequencia)
            ) PARTITION BY LIST (sp_uf);
        """).format(
            columns=sql.SQL(', ').join([
                sql.SQL("{} {}").format(sql.Identifier('sp_cidsec'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_tp_ato'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_nf'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_procrea'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_mm'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_cidpri'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_tipo'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_aa'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_pf_cbo'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_des_pac'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_num_pr'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_pf_doc'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_cnes'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_gestor'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_co_faec'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_naih'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_ptsp'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('remessa'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_m_pac'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sequencia'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_atoprof'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_m_hosp'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_qt_proc'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_complex'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_pj_doc'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_dtsaida'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_cgchosp'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_ptsp_nf'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('serv_cla'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_des_hos'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_qtd_ato'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_financ'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_cpfcgc'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_uf'), sql.SQL('TEXT NOT NULL')),
                sql.SQL("{} {}").format(sql.Identifier('sp_u_aih'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('in_tp_val'), sql.SQL('TEXT')),
                sql.SQL("{} {}").format(sql.Identifier('sp_valato'), sql.SQL('NUMERIC(10, 2)')),
                sql.SQL("{} {}").format(sql.Identifier('sp_dtinter'), sql.SQL('DATE'))
            ])
        )
        cursor.execute(create_table_query)

        # Create partitions for each state
        states = ["AC", "AL", "AP", "AM", "BA", "CE",
                  "DF", "ES", "GO", "MA", "MT", "MS",
                  "MG", "PA", "PB", "PR", "PE", "PI",
                  "RJ", "RN", "RS", "RO", "RR", "SC",
                  "SP", "SE", "TO"]
        for state in states:
            partition_table = f"SIH_Serviços_Profissionais_{state}"
            create_partition_query = sql.SQL("""
                CREATE TABLE {partition_table} PARTITION OF SIH_Serviços_Profissionais
                FOR VALUES IN (%s);
            """).format(partition_table=sql.Identifier(partition_table))
            cursor.execute(create_partition_query, [state])
        print("Partições criadas com sucesso para todos os estados.")

        # Insert data into the partitioned table
        print("Inserindo dados na tabela particionada...")
        insert_query = sql.SQL("""
            INSERT INTO SIH_Serviços_Profissionais ({columns})
            SELECT {columns}
            FROM SIH_Serviços_Profissionais;
        """).format(
            columns=sql.SQL(', ').join(columns_identifiers)
        )
        cursor.execute(insert_query)

        # Commit the transaction
        conn.commit()
        print("Processo concluído com sucesso.")

    except Exception as e:
        conn.rollback()
        print(f"Ocorreu um erro: {e}")
    finally:
        cursor.close()
        conn.close()
        print("Conexão com o banco de dados encerrada.")

if __name__ == "__main__":
    merge_and_partition_tables()
