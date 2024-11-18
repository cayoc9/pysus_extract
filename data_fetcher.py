# data_fetcher.py

import pandas as pd
from pysus.online_data.SIH import download
import logging
import os

def fetch_data(start_year, end_year, uf='SP'):
    """
    Baixa os dados do SIH entre os anos especificados.

    Parâmetros:
    - start_year (int): Ano inicial para download.
    - end_year (int): Ano final para download.
    - uf (str): Código da Unidade Federativa (estado), ex: 'SP'.

    Retorna:
    - pandas.DataFrame: DataFrame contendo os dados baixados.
    """
    # Definir os grupos e meses a serem baixados
    groups = ['RD', 'RJ', 'ER', 'SP', 'CH']  # Grupos válidos para SIH
    months = list(range(1, 13))  # Baixar todos os meses do ano

    # Diretório onde os arquivos serão baixados
    data_dir = '/home/cayo/pysus'  # Ajuste conforme necessário

    # Garantir que o diretório existe
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    dfs = []
    for year in range(start_year, end_year + 1):
        logging.info(f"Baixando dados do ano {year} para o estado {uf}")
        try:
            # Definir o diretório específico para o ano
            year_dir = os.path.join(data_dir, str(year))
            if not os.path.exists(year_dir):
                os.makedirs(year_dir)
            
            # Passar os argumentos corretamente
            files = download(states=uf, years=year, months=months, groups=groups, data_dir=year_dir)
            logging.info(f"{len(files)} arquivos baixados para o ano {year}")
            
            for file in files:
                # Verificar se o arquivo existe antes de tentar ler
                if os.path.exists(file):
                    df = pd.read_parquet(file)
                    dfs.append(df)
                else:
                    logging.warning(f"Arquivo não encontrado: {file}")
                    
        except Exception as e:
            logging.error(f"Erro ao baixar dados do ano {year}: {e}")

    if dfs:
        data = pd.concat(dfs, ignore_index=True)
        # Processamento adicional, se necessário
        if 'DT_INTER' in data.columns:
            data['data_atendimento'] = pd.to_datetime(data['DT_INTER'], errors='coerce')
        else:
            logging.warning("Coluna 'DT_INTER' não encontrada nos dados.")
        return data
    else:
        return pd.DataFrame()  # Retorna um DataFrame vazio se nenhum dado foi baixado
