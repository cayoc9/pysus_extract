import os
import pandas as pd
import json
import numpy as np
import re
import unicodedata
from datetime import datetime
import duckdb
from glob import glob

def normalizar_nome(nome):
    """
    Normaliza o nome:
    - Converte para minúsculas.
    - Remove acentos e caracteres especiais.
    - Remove caracteres não alfanuméricos, exceto o sublinhado '_'.
    """
    nome = nome.lower()
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('ASCII')
    nome = re.sub(r'\W+', '_', nome)
    nome = nome.strip('_')
    return nome

def converter_tipos_para_json(dados):
    """
    Converte tipos incompatíveis do NumPy para tipos nativos do Python,
    garantindo que o objeto seja serializável em JSON.
    """
    if isinstance(dados, dict):
        return {chave: converter_tipos_para_json(valor) for chave, valor in dados.items()}
    elif isinstance(dados, list):
        return [converter_tipos_para_json(item) for item in dados]
    elif isinstance(dados, np.integer):
        return int(dados)
    elif isinstance(dados, np.floating):
        return float(dados)
    elif isinstance(dados, np.ndarray):
        return dados.tolist()
    else:
        return dados

def salvar_analise_em_arquivo(analise, caminho_saida):
    """
    Salva os resultados da análise em um arquivo JSON.
    """
    analise_convertida = converter_tipos_para_json(analise)
    with open(caminho_saida, 'w') as f:
        json.dump(analise_convertida, f, indent=4, ensure_ascii=False)
    print(f"Análise salva em: {caminho_saida}")

def analisar_dataframe(df, analise_global):
    """
    Realiza uma análise detalhada de um DataFrame e atualiza a análise global.

    Args:
        df (pd.DataFrame): DataFrame a ser analisado.
        analise_global (dict): Dicionário que acumula as análises de todos os arquivos.

    Retorna:
        dict: Dicionário atualizado com as análises.
    """
    for coluna in df.columns:
        # Inicializar valores padrão se a coluna ainda não estiver na análise global
        if coluna not in analise_global:
            analise_global[coluna] = {
                'tipo_dado': str(df[coluna].dtype),
                'valores_unicos': set(),
                'valores_nulos': 0,
                'amostra_valores': set(),
                'maior_caractere': 0,
                'menor_caractere': float('inf'),
                'has_leading_zeros': False,
                'has_special_chars': False,
                'has_mixed_types': False
            }
        
        coluna_info = analise_global[coluna]
        
        # Atualizar tipo de dado (simplificado)
        coluna_info['tipo_dado'] = str(df[coluna].dtype)
        
        # Atualizar valores únicos
        valores_unicos = df[coluna].dropna().unique()
        coluna_info['valores_unicos'].update(valores_unicos)
        
        # Atualizar valores nulos
        coluna_info['valores_nulos'] += df[coluna].isnull().sum()
        
        # Atualizar amostra de valores
        amostra_atual = df[coluna].dropna().unique()[:10]
        coluna_info['amostra_valores'].update(amostra_atual)
        
        # Atualizar maior e menor caractere se for string
        if df[coluna].dtype == 'object' or df[coluna].dtype.name == 'string':
            tamanhos = df[coluna].dropna().apply(lambda x: len(x) if isinstance(x, str) else 0)
            tamanhos = tamanhos[tamanhos > 0]
            if not tamanhos.empty:
                coluna_info['maior_caractere'] = max(coluna_info['maior_caractere'], tamanhos.max())
                coluna_info['menor_caractere'] = min(coluna_info['menor_caractere'], tamanhos.min())
            
            # Verificar zeros à esquerda e caracteres especiais na amostra
            for valor in amostra_atual:
                if isinstance(valor, str):
                    valor_str = valor
                    if len(valor_str) > 1 and valor_str.startswith('0') and valor_str.isdigit():
                        coluna_info['has_leading_zeros'] = True
                    if re.search(r"[^\w\s]", valor_str):
                        coluna_info['has_special_chars'] = True
            
            # Verificar tipos mistos na amostra
            tipos_presentes = set()
            for valor in amostra_atual:
                valor_str = str(valor).strip()
                if re.match(r"^-?\d+$", valor_str):
                    tipos_presentes.add('integer')
                elif re.match(r"^-?\d+(\.\d+)?$", valor_str):
                    tipos_presentes.add('float')
                elif re.match(r"^\d{4}$", valor_str):
                    tipos_presentes.add('year')
                else:
                    tipos_presentes.add('string')
            if len(tipos_presentes) > 1:
                coluna_info['has_mixed_types'] = True
    
    return analise_global

def carregar_e_analisar_parquet_iterativo(caminho_pasta, amostra_por_arquivo=1000):
    """
    Carrega e analisa arquivos Parquet iterativamente, coletando amostras para evitar sobrecarga de memória.

    Args:
        caminho_pasta (str): Caminho para a pasta que contém os arquivos Parquet.
        amostra_por_arquivo (int): Número de linhas a serem amostradas de cada arquivo.

    Returns:
        dict: Dicionário com a análise agregada de todas as colunas.
    """
    # Criar um padrão para encontrar todos os arquivos .parquet recursivamente
    pattern = os.path.join(caminho_pasta, '**', '*.parquet')
    all_parquet_files = glob(pattern, recursive=True)

    if not all_parquet_files:
        # Caso não encontre, verificar se o caminho é um arquivo individual
        if os.path.isfile(caminho_pasta) and caminho_pasta.endswith('.parquet'):
            all_parquet_files = [caminho_pasta]

    if not all_parquet_files:
        print("Nenhum arquivo Parquet encontrado no caminho fornecido.")
        return {}

    print(f"Encontrados {len(all_parquet_files)} arquivos .parquet. Processando iterativamente...")

    analise_global = {}
    total_linhas = 0

    # Conexão com DuckDB
    con = duckdb.connect()

    for idx, arquivo in enumerate(all_parquet_files, 1):
        try:
            print(f"Processando arquivo {idx}/{len(all_parquet_files)}: {arquivo}")

            # Utilizar DuckDB para amostrar dados sem carregar tudo na memória
            # Selecionar uma amostra aleatória de linhas
            # Ajustar o TABLESAMPLE para incluir 'PERCENT'
            sampling_percentage = min((amostra_por_arquivo / 100000) * 100, 100)  # Evita exceder 100%
            query = f"""
                SELECT * FROM read_parquet('{arquivo}')
                TABLESAMPLE BERNOULLI({sampling_percentage} PERCENT) LIMIT {amostra_por_arquivo}
            """
            df_arrow = con.execute(query).arrow()
            df = df_arrow.to_pandas()

            if df.empty:
                print(f"Amostra vazia para o arquivo: {arquivo}. Pulando...")
                continue

            total_linhas += len(df)

            # Analisar o DataFrame e atualizar a análise global
            analise_global = analisar_dataframe(df, analise_global)

            # Liberar memória
            del df
            del df_arrow

        except Exception as e:
            print(f"Erro ao processar o arquivo '{arquivo}': {e}")
            continue

    # Pós-processamento para ajustar os conjuntos em listas e contar únicos
    for coluna, dados in analise_global.items():
        dados['valores_unicos'] = len(dados['valores_unicos'])
        dados['amostra_valores'] = list(dados['amostra_valores'])
        if dados['menor_caractere'] == float('inf'):
            dados['menor_caractere'] = 0

    analise_global['__total_linhas__'] = total_linhas
    return analise_global

def main():
    base = "CNES"

    # Defina uma lista de caminhos que você deseja analisar
    caminhos_pastas = [
        # "./parquet_files/SIH/RD",
        # "./parquet_files/SIH/RJ",
        # "./parquet_files/SIH/ER",
        "./parquet_files/SIA/PA",        
    ]  # Adicione ou remova caminhos conforme necessário

    # Diretório base para salvar as análises
    diretorio_saida = "./Analises/amostras/"

    # Assegurar que o diretório de saída existe
    os.makedirs(diretorio_saida, exist_ok=True)

    for caminho_pasta in caminhos_pastas:
        try:
            # Extrair o nome do grupo a partir do caminho
            grupo = os.path.basename(os.path.normpath(caminho_pasta))
            caminho_saida = os.path.join(diretorio_saida, f'amostra_{base}_{grupo}.json')

            # Analisar os dados iterativamente com amostragem
            analise = carregar_e_analisar_parquet_iterativo(caminho_pasta, amostra_por_arquivo=1000)

            if analise:
                # Salvar os resultados da análise em um arquivo JSON
                salvar_analise_em_arquivo(analise, caminho_saida)
                print(f"Análise salva em: {caminho_saida}")
            else:
                print(f"Nenhuma análise foi realizada para o caminho: {caminho_pasta}")

        except Exception as e:
            print(f"Erro ao processar o caminho '{caminho_pasta}': {e}")
            continue  # Continua com o próximo caminho mesmo se houver erro


if __name__ == "__main__":
    main()


