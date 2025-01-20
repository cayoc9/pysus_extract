from pysus.ftp.databases.sih import SIH
import pandas as pd
import os

sih = SIH().load() # Loads the files from DATASUS

print(sih.groups)

# Lista para armazenar os DataFrames
dataframes = []

# Caminho para a pasta contendo os arquivos Parquet
caminho_pasta = '/root/pysus_sih/parquet_files/SIH/SP/SPAC2303.parquet'

# Itera sobre os arquivos na pasta
for arquivo in os.listdir(caminho_pasta):
    if arquivo.endswith('.parquet'):
        caminho_completo = os.path.join(caminho_pasta, arquivo)
        df_temp = pd.read_parquet(caminho_completo)
        dataframes.append(df_temp)

# Concatena todos os DataFrames em um único
df = pd.concat(dataframes, ignore_index=True)


colunas = df.columns #SP
colunas

""" output: 
Index(['SP_GESTOR', 'SP_UF', 'SP_AA', 'SP_MM', 'SP_CNES', 'SP_NAIH',
       'SP_PROCREA', 'SP_DTINTER', 'SP_DTSAIDA', 'SP_NUM_PR', 'SP_TIPO',
       'SP_CPFCGC', 'SP_ATOPROF', 'SP_TP_ATO', 'SP_QTD_ATO', 'SP_PTSP',
       'SP_NF', 'SP_VALATO', 'SP_M_HOSP', 'SP_M_PAC', 'SP_DES_HOS',
       'SP_DES_PAC', 'SP_COMPLEX', 'SP_FINANC', 'SP_CO_FAEC', 'SP_PF_CBO',
       'SP_PF_DOC', 'SP_PJ_DOC', 'IN_TP_VAL', 'SEQUENCIA', 'REMESSA',
       'SERV_CLA', 'SP_CIDPRI', 'SP_CIDSEC', 'SP_QT_PROC', 'SP_U_AIH'],
      dtype='object')
"""

df.SP_UF.unique()

""" output:
array(['12'], dtype=object)
 """