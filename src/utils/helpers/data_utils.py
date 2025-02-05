import os
import pandas as pd
import unicodedata
import re

def normalizar_nome(nome: str) -> str:
    """
    Normaliza nomes para uso em bancos de dados e sistemas de arquivos
    
    Args:
        nome (str): Nome original a ser normalizado
    
    Returns:
        str: Nome normalizado em minúsculas, sem acentos e caracteres especiais
    """
    nome = nome.lower()
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('ASCII')
    nome = re.sub(r'[^a-z0-9_]', '_', nome)
    return nome.strip('_')

def carregar_parquet(caminho: str) -> pd.DataFrame:
    """
    Carrega arquivos Parquet de um diretório ou arquivo único
    
    Args:
        caminho (str): Caminho para arquivo ou diretório
    
    Returns:
        pd.DataFrame: DataFrame concatenado com todos os dados
    """
    if os.path.isdir(caminho):
        arquivos = [os.path.join(caminho, f) 
                   for f in os.listdir(caminho) 
                   if f.endswith('.parquet')]
        return pd.concat((pd.read_parquet(f) for f in arquivos), 
                        ignore_index=True)
    return pd.read_parquet(caminho)

def converter_tipos(df: pd.DataFrame, mapeamento: dict) -> pd.DataFrame:
    """
    Converte tipos de colunas de um DataFrame conforme mapeamento
    
    Args:
        df (pd.DataFrame): DataFrame original
        mapeamento (dict): Dicionário {coluna: tipo}
    
    Returns:
        pd.DataFrame: DataFrame com tipos convertidos
    """
    for col, dtype in mapeamento.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                raise ValueError(f"Erro ao converter {col} para {dtype}: {e}")
    return df 