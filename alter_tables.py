import psycopg2

def optimize_alter_table():
    # Configuração da conexão com o banco de dados
    conn = psycopg2.connect(
        dbname="datasus",
        user="webadmin",
        password="h532947h5g932h",
        host="10.100.60.19",
        port="5432"
    )
    cursor = conn.cursor()

    try:
        # Script SQL otimizado para executar alterações
        sql_script = """
        DO $$
        BEGIN
            -- Alterações na tabela sia_apac_medicamentos
            ALTER TABLE sia_apac_medicamentos ALTER COLUMN ap_dtaut TYPE DATE USING ap_dtaut::DATE;
            ALTER TABLE sia_apac_medicamentos ALTER COLUMN ap_dtfim TYPE DATE USING ap_dtfim::DATE;
            ALTER TABLE sia_apac_medicamentos ALTER COLUMN ap_dtinic TYPE DATE USING ap_dtinic::DATE;
            ALTER TABLE sia_apac_medicamentos ALTER COLUMN ap_dtocor TYPE DATE USING ap_dtocor::DATE;
            ALTER TABLE sia_apac_medicamentos ALTER COLUMN ap_dtsolic TYPE DATE USING ap_dtsolic::DATE;

            -- Alterações na tabela sia_apac_tratamento_dialitico
            ALTER TABLE sia_apac_tratamento_dialitico ALTER COLUMN ap_dtaut TYPE DATE USING ap_dtaut::DATE;
            ALTER TABLE sia_apac_tratamento_dialitico ALTER COLUMN ap_dtfim TYPE DATE USING ap_dtfim::DATE;
            ALTER TABLE sia_apac_tratamento_dialitico ALTER COLUMN ap_dtinic TYPE DATE USING ap_dtinic::DATE;
            ALTER TABLE sia_apac_tratamento_dialitico ALTER COLUMN ap_dtocor TYPE DATE USING ap_dtocor::DATE;
            ALTER TABLE sia_apac_tratamento_dialitico ALTER COLUMN ap_dtsolic TYPE DATE USING ap_dtsolic::DATE;

            -- Alterações na tabela sia_apac_laudos_diversos
            ALTER TABLE sia_apac_laudos_diversos ALTER COLUMN ap_dtsolic TYPE DATE USING ap_dtsolic::DATE;
            ALTER TABLE sia_apac_laudos_diversos ALTER COLUMN ap_dtaut TYPE DATE USING ap_dtaut::DATE;

            -- Alterações na tabela sia_boletim_producao_ambulatorial_individualizado
            ALTER TABLE sia_boletim_producao_ambulatorial_individualizado ALTER COLUMN dtnasc TYPE DATE USING dtnasc::DATE;
        END $$;
        """
        
        # Executar o script SQL
        cursor.execute(sql_script)
        conn.commit()
        print("Alterações aplicadas com sucesso!")

    except Exception as e:
        conn.rollback()
        print("Erro ao aplicar alterações:", e)

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    optimize_alter_table()
