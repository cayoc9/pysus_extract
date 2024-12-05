# modulos/data_validation.py

import pandas as pd
import logging
from pandas.api.types import is_string_dtype, is_numeric_dtype

def convert_to_native_types(df):
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = df[col].astype(int)
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].astype(float)
    return df

def verificar_coluna_data(coluna):
    if is_string_dtype(coluna) or is_numeric_dtype(coluna):
        # Seleciona uma amostra para verificar
        sample_size = min(100, len(coluna.dropna()))
        if sample_size == 0:
            return False
        sample = coluna.dropna().astype(str).sample(n=sample_size, random_state=1)
        date_like = 0
        for val in sample:
            if len(val) == 8 and val.isalnum():
                try:
                    pd.to_datetime(val, format='%Y%m%d')
                    date_like += 1
                except:
                    pass
        return (date_like / len(sample)) > 0.8  # 80% das amostras são datas
    return False

def preprocess_dataframe(df):
    # Converter tipos NumPy para tipos nativos do Python
    df = convert_to_native_types(df)
    
    # Identificar e converter colunas de data
    date_cols = [col for col in df.columns if verificar_coluna_data(df[col])]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
    
    # Tratar colunas de string com valores nulos
    object_cols = df.select_dtypes(include=['object']).columns
    df[object_cols] = df[object_cols].apply(lambda x: x.str.strip()).replace(r'^\s*$', None, regex=True).fillna('')
    
    # Tratar colunas numéricas com valores nulos
    numeric_cols = df.select_dtypes(include=['number']).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    return df
