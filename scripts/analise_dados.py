import os
import pandas as pd
import json
import numpy as np
import re
from datetime import datetime

def normalizar_nome(nome):
    """
    Normaliza o nome:
    - Converte para minúsculas.
    - Remove acentos e caracteres especiais.
    - Remove caracteres não alfanuméricos, exceto o sublinhado '_'.
    """
    # Converter para minúsculas
    nome = nome.lower()
    
    # Remover acentos e diacríticos
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('ASCII')
    
    # Substituir caracteres não alfanuméricos por sublinhado
    nome = re.sub(r'\W+', '_', nome)
    
    # Remover sublinhados iniciais ou finais
    nome = nome.strip('_')
    
    return nome

def analisar_dataframe(df):
    """
    Realiza uma análise detalhada de um DataFrame.

    Retorna um dicionário com informações sobre cada coluna:
    - Tipo de dado.
    - Quantidade de valores únicos.
    - Quantidade de valores nulos.
    - Amostra de até 10 valores únicos.
    - Quantidade de caracteres do maior e menor elemento.
    - Presença de valores mistos.
    - Presença de zeros à esquerda.
    - Presença de caracteres especiais.

    :param df: DataFrame a ser analisado.
    :return: Dicionário com a análise.
    """
    analise = {}
    for coluna in df.columns:
        # Inicializar valores padrão
        maior_caractere = 0
        menor_caractere = 0
        has_leading_zeros = False
        has_special_chars = False
        has_mixed_types = False

        # Calcular tamanhos dos caracteres, se os elementos forem strings ou objetos
        if df[coluna].dtype == 'object' or df[coluna].dtype.name == 'string':
            # Aplicar len() apenas a valores que são strings
            tamanhos = df[coluna].dropna().apply(lambda x: len(x) if isinstance(x, str) else 0)
            tamanhos = tamanhos[tamanhos > 0]  # Excluir zeros resultantes de valores não-string
            maior_caractere = tamanhos.max() if not tamanhos.empty else 0
            menor_caractere = tamanhos.min() if not tamanhos.empty else 0

            # Verificar zeros à esquerda e caracteres especiais
            amostra = df[coluna].dropna().unique()
            for valor in amostra[:10]:  # Limitar a uma amostra para eficiência
                if isinstance(valor, str):
                    valor_str = valor
                    if len(valor_str) > 1 and valor_str.startswith('0') and valor_str.isdigit():
                        has_leading_zeros = True
                    # Verificar caracteres especiais
                    if re.search(r"[^\w\s]", valor_str):
                        has_special_chars = True

            # Verificar tipos mistos
            tipos_presentes = set()
            for valor in amostra[:20]:  # Limitar para eficiência
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
                has_mixed_types = True
        else:
            # Se a coluna não for do tipo objeto ou string, os tamanhos permanecem zero
            pass

        analise[coluna] = {
            'tipo_dado': str(df[coluna].dtype),
            'valores_unicos': int(df[coluna].nunique()),
            'valores_nulos': int(df[coluna].isnull().sum()),
            'amostra_valores': df[coluna].dropna().unique()[:10].tolist(),  # Primeiros 10 valores únicos
            'maior_caractere': maior_caractere,
            'menor_caractere': menor_caractere,
            'has_leading_zeros': has_leading_zeros,
            'has_special_chars': has_special_chars,
            'has_mixed_types': has_mixed_types
        }
    return analise

def carregar_e_concatenar_parquet(caminho_pasta):
    """
    Carrega e concatena todos os arquivos Parquet de uma pasta em um único DataFrame.
    """
    # Lista para armazenar os DataFrames temporários
    dataframes = []

    # Verificar se o caminho é um arquivo ou uma pasta
    if os.path.isfile(caminho_pasta):
        # Se for um arquivo único Parquet
        print(f"Carregando arquivo: {caminho_pasta}")
        df_temp = pd.read_parquet(caminho_pasta)
        dataframes.append(df_temp)
    elif os.path.isdir(caminho_pasta):
        # Itera sobre os arquivos na pasta
        for arquivo in os.listdir(caminho_pasta):
            if arquivo.endswith('.parquet'):
                caminho_completo = os.path.join(caminho_pasta, arquivo)
                print(f"Carregando arquivo: {caminho_completo}")
                df_temp = pd.read_parquet(caminho_completo)
                dataframes.append(df_temp)
    else:
        print(f"O caminho {caminho_pasta} não é um arquivo nem uma pasta válida.")
        return pd.DataFrame()

    # Concatena todos os DataFrames em um único
    if dataframes:
        df = pd.concat(dataframes, ignore_index=True)
        print(f"Total de linhas carregadas: {len(df)}")
        return df
    else:
        print("Nenhum arquivo Parquet encontrado no caminho fornecido.")
        return pd.DataFrame()  # Retorna um DataFrame vazio

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
    Salva os resultados da análise em um arquivo JSON para fácil compartilhamento.
    """
    # Converte os tipos de dados antes de salvar
    analise_convertida = converter_tipos_para_json(analise)

    with open(caminho_saida, 'w') as f:
        json.dump(analise_convertida, f, indent=4, ensure_ascii=False)
    print(f"Análise salva em: {caminho_saida}")

def main():
    base = "SIA"

    arquivos =  {
        "AB": "/root/pysus_sih/parquet_files/SIA/AB/ABSP1610.parquet",
        "ABO": "/root/pysus_sih/parquet_files/SIA/ABO/ABOSP2303.parquet",
        "ACF": "/root/pysus_sih/parquet_files/SIA/ACF/ACFSP2303.parquet",
        "AD": "/root/pysus_sih/parquet_files/SIA/AD/ADSP2310.parquet",
        "AM": "/root/pysus_sih/parquet_files/SIA/AM/AMSP2310.parquet",
        "AMP": "/root/pysus_sih/parquet_files/SIA/AMP/AMPSP2310.parquet",
        "AN": "/root/pysus_sih/parquet_files/SIA/AN/ANSP1409.parquet",
        "AQ": "/root/pysus_sih/parquet_files/SIA/AQ/AQSP2310.parquet",
        "AR": "/root/pysus_sih/parquet_files/SIA/AR/ARSP2309.parquet",
        "ATD": "/root/pysus_sih/parquet_files/SIA/ATD/ATDSP2310.parquet",
        "BI": "/root/pysus_sih/parquet_files/SIA/BI/BISP2311_1.parquet",
        "PS": "/root/pysus_sih/parquet_files/SIA/PS/PSSP2409.parquet"
    }

    for grupo, caminho_pasta in arquivos.items():
        try:
            # Caminho para salvar o resultado da análise
            caminho_saida = f'/root/pysus_sih/Analises/amostras/amostra_{base}_{grupo}.json'

            # Carregar e concatenar os arquivos Parquet
            print(f"Processando o caminho: {caminho_pasta}")
            df = carregar_e_concatenar_parquet(caminho_pasta)

            if not df.empty:
                # Analisar as colunas do DataFrame concatenado
                analise = analisar_dataframe(df)

                # Salvar os resultados da análise em um arquivo JSON
                salvar_analise_em_arquivo(analise, caminho_saida)
                print(f"Análise salva em: {caminho_saida}")
            else:
                print(f"Nenhum dado foi processado para o caminho: {caminho_pasta}")
        except Exception as e:
            print(f"Erro ao processar o grupo '{grupo}': {e}")

if __name__ == "__main__":
    main()
