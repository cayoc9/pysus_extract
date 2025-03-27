from pysus.ftp.databases.sia import SIA
from pysus.ftp.databases.sih import SIH
from pysus.ftp.databases.cnes import CNES
from pysus.ftp.databases.sim import SIM
from pysus.ftp.databases.sinasc import SINASC
from pysus.ftp.databases.sinan import SINAN
import pandas as pd
import os
import re
import shutil

# Carregando as bases de dados
sia = SIA().load()
sih = SIH().load()
cnes = CNES().load()
sim = SIM().load()
sinasc = SINASC().load()
sinan = SINAN().load()

# Definindo variáveis de configuração
base = "SIH"
estados = ['ES']
anos = [2021, 2022, 2023]
grupos = ['RD', 'RJ']  # Grupos conforme a base SIH

# Loop para cada grupo definido
for grupo in grupos:
    try:
        print(f"Processando o grupo: {grupo}")
        # Obtém os arquivos (retorna objetos File)
        files = sih.get_files([grupo], uf=estados, year=anos)
        
        # Define a pasta onde serão verificados e excluídos arquivos/parquets antigos
        pasta_base_excluir = f"parquet_files/{base}/{grupo}/"
        
        # Garante que o diretório exista (caso não, cria-o)
        if not os.path.exists(pasta_base_excluir):
            os.makedirs(pasta_base_excluir, exist_ok=True)
            
        # Itera sobre cada arquivo para checar se já existe um parquet correspondente
        for file in files:
            # Usando o atributo 'name' do objeto File para obter o nome do arquivo como string.
            # Por exemplo, se file.name for "dados_exemplo.zip", a função basename retorna "dados_exemplo.zip"
            filename = os.path.basename(file.name)
            nome_sem_extensao = os.path.splitext(filename)[0]
            
            # Define o caminho do parquet correspondente (exemplo: "parquet_files/SIH/RD/dados_exemplo.parquet")
            pasta_parquet = os.path.join(pasta_base_excluir, f"{nome_sem_extensao}.parquet")
            
            # Se o parquet já existir, ele será removido antes do download do arquivo atualizado
            if os.path.exists(pasta_parquet):
                print(f"Excluindo pasta existente: {pasta_parquet}")
                shutil.rmtree(pasta_parquet)
        
        # Define o diretório de destino para o download
        # Aqui, usei o mesmo diretório definido para exclusão, mas pode ser ajustado conforme a necessidade
        local_dir = pasta_base_excluir
        
        # Efetua o download dos arquivos para o diretório especificado
        parquet = sih.download(files, local_dir=local_dir)
        
        print(f"Arquivos para o grupo '{grupo}' baixados com sucesso em: {local_dir}")
    except Exception as e:
        print(f"Erro ao processar o grupo '{grupo}': {e}")
