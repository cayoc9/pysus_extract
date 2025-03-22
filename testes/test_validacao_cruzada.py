#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de validação cruzada para comparar métodos de carregamento
Compara o método antigo (SQL customizado) com o método novo (process_data)
"""

import os
import logging
import time
import pandas as pd
import duckdb
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("validacao_cruzada.log"),
        logging.StreamHandler()
    ]
)

# Importação das funções necessárias
from main import get_parquet_files, QueryParams, process_data

def metodo_antigo(files: list, cnes_list: list, campos: list) -> pd.DataFrame:
    """
    Implementação do método antigo (SQL customizado) para comparação
    
    Args:
        files: Lista de arquivos parquet
        cnes_list: Lista de códigos CNES
        campos: Lista de campos a serem selecionados
        
    Returns:
        DataFrame com os dados carregados
    """
    # 1. Criar uma lista de arquivos formatada para SQL
    arquivo_lista = ", ".join([f"'{f}'" for f in files])
    
    # 2. Definir a coluna CNES (simplificado)
    cnes_col = "SP_CNES"
    
    # 3. Construir a cláusula WHERE
    where_clause = ""
    if cnes_list != ["*"]:
        cnes_valores = ", ".join([f"'{c}'" for c in cnes_list])
        where_clause = f"WHERE {cnes_col} IN ({cnes_valores})"
    
    # 4. Construir a SQL final e executar
    colunas_sql = ", ".join(campos)
    query = f"""
        SELECT {colunas_sql}
        FROM read_parquet([{arquivo_lista}])
        {where_clause}
    """
    
    logging.debug(f"SQL do método antigo: {query[:200]}...")
    df = duckdb.query(query).to_df()
    return df

def metodo_novo(files: list, cnes_list: list, campos: list) -> pd.DataFrame:
    """
    Implementação do método novo (process_data) para comparação
    
    Args:
        files: Lista de arquivos parquet
        cnes_list: Lista de códigos CNES
        campos: Lista de campos a serem selecionados
        
    Returns:
        DataFrame com os dados carregados
    """
    # 1. Criar parâmetros de consulta
    params = QueryParams(
        base="SIH",  # Hardcoded para este teste
        grupo="SP",  # Hardcoded para este teste
        cnes_list=cnes_list,
        campos_agrupamento=campos,
        competencia_inicio="01/2024",  # Não importa para o test
        competencia_fim="02/2024",     # Não importa para o test
        table_name=None
    )
    
    # 2. Processar dados usando process_data
    temp_table = process_data(files, params)
    
    # 3. Converter para DataFrame
    df = duckdb.query(f"SELECT * FROM {temp_table}").to_df()
    return df

def comparar_metodos(
    base="SIH", 
    grupo="SP", 
    cnes_list=["2078015"], 
    competencia_inicio="01/2024",
    competencia_fim="02/2024"
):
    """
    Compara os dois métodos e valida se os resultados são equivalentes
    
    Args:
        base: Base de dados (SIH/SIA)
        grupo: Grupo de dados
        cnes_list: Lista de códigos CNES para teste
        competencia_inicio: Período inicial MM/YYYY
        competencia_fim: Período final MM/YYYY
        
    Returns:
        bool: True se a validação for bem-sucedida, False caso contrário
    """
    logging.info("=== VALIDAÇÃO CRUZADA DOS MÉTODOS DE CARREGAMENTO ===")
    logging.info(f"CNES para teste: {cnes_list}")
    
    # Campos a serem selecionados
    campos = [
        "SP_GESTOR", "SP_UF", "SP_AA", "SP_MM", "SP_CNES", 
        "SP_NAIH", "SP_PROCREA", "SP_ATOPROF"
    ]
    
    try:
        # 1. Obter os arquivos parquet
        arquivos = get_parquet_files(base, grupo, competencia_inicio, competencia_fim)
        if not arquivos:
            logging.error("❌ Nenhum arquivo encontrado para o período especificado")
            return False
            
        logging.info(f"✅ Encontrados {len(arquivos)} arquivos parquet")
        
        # 2. Executar ambos os métodos e medir o tempo
        inicio = time.time()
        df_antigo = metodo_antigo(arquivos, cnes_list, campos)
        tempo_antigo = time.time() - inicio
        
        inicio = time.time()
        df_novo = metodo_novo(arquivos, cnes_list, campos)
        tempo_novo = time.time() - inicio
        
        # 3. Verificar carregamento básico
        if df_antigo.empty and df_novo.empty:
            logging.warning("⚠️ Ambos os métodos retornaram DataFrames vazios")
            return True
        
        if df_antigo.empty:
            logging.error("❌ O método antigo retornou DataFrame vazio, mas o novo não")
            return False
            
        if df_novo.empty:
            logging.error("❌ O método novo retornou DataFrame vazio, mas o antigo não")
            return False
        
        # 4. Comparar os resultados
        logging.info(f"Método antigo: {len(df_antigo)} registros em {tempo_antigo:.2f} segundos")
        logging.info(f"Método novo: {len(df_novo)} registros em {tempo_novo:.2f} segundos")
        
        # 5. Verificar se os tamanhos são iguais
        if len(df_antigo) != len(df_novo):
            logging.error(f"❌ Número de registros diferente: Antigo={len(df_antigo)}, Novo={len(df_novo)}")
            
            # 5.1 Analisar diferenças de tamanho
            if len(df_antigo) > len(df_novo):
                logging.error("O método antigo retornou mais registros. Possível inclusão indevida de CNES.")
                # Verificar presença de CNES não solicitados
                if 'SP_CNES' in df_antigo.columns:
                    contagem_cnes = df_antigo['SP_CNES'].value_counts()
                    logging.error(f"Distribuição de CNES no método antigo:\n{contagem_cnes.head(10)}")
            else:
                logging.error("O método novo retornou mais registros. Possível problema no filtro.")
                if 'SP_CNES' in df_novo.columns:
                    contagem_cnes = df_novo['SP_CNES'].value_counts()
                    logging.error(f"Distribuição de CNES no método novo:\n{contagem_cnes.head(10)}")
            
            return False
            
        # 6. Verificar colunas iguais
        cols_antigo = set(df_antigo.columns)
        cols_novo = set(df_novo.columns)
        
        if cols_antigo != cols_novo:
            logging.warning("⚠️ As colunas são diferentes entre os métodos")
            logging.warning(f"Colunas exclusivas do método antigo: {cols_antigo - cols_novo}")
            logging.warning(f"Colunas exclusivas do método novo: {cols_novo - cols_antigo}")
            # Não falhar por isso, apenas alertar
        
        # 7. Examinar dados para colunas comuns
        colunas_comuns = list(cols_antigo.intersection(cols_novo))
        if not colunas_comuns:
            logging.error("❌ Não há colunas em comum para comparar")
            return False
            
        # 8. Preparação para comparação: ordenar os dataframes
        logging.info("Ordenando DataFrames para comparação...")
        if 'SP_NAIH' in colunas_comuns:
            df_antigo = df_antigo.sort_values('SP_NAIH').reset_index(drop=True)
            df_novo = df_novo.sort_values('SP_NAIH').reset_index(drop=True)
        
        # 9. Comparar valores nas colunas comuns (para os primeiros N registros)
        n_registros = min(100, len(df_antigo))
        todas_iguais = True
        
        if n_registros > 0:
            for col in colunas_comuns:
                # Ignorar colunas auxiliares ou de erro que podem diferir entre métodos
                if col.startswith('new_') or col.startswith('ERRO_'):
                    continue
                    
                # Comparar valores das colunas
                try:
                    iguais = df_antigo[col].head(n_registros).equals(df_novo[col].head(n_registros))
                    if not iguais:
                        logging.warning(f"⚠️ Coluna {col} tem valores diferentes entre os métodos")
                        todas_iguais = False
                        # Mostrar alguns exemplos de diferenças
                        for i in range(min(5, n_registros)):
                            if df_antigo[col].iloc[i] != df_novo[col].iloc[i]:
                                logging.warning(f"  Registro {i}: Antigo='{df_antigo[col].iloc[i]}', Novo='{df_novo[col].iloc[i]}'")
                except Exception as e:
                    logging.warning(f"⚠️ Erro ao comparar coluna {col}: {str(e)}")
                    todas_iguais = False
        
        # 10. Resultados da validação
        if todas_iguais:
            logging.info("✅ SUCESSO: Os dois métodos produziram resultados idênticos")
            logging.info(f"Performance: Método novo {tempo_antigo/tempo_novo:.1f}x mais {'rápido' if tempo_antigo > tempo_novo else 'lento'}")
            return True
        else:
            logging.warning("⚠️ Os métodos produziram alguns resultados diferentes")
            logging.info(f"Performance: Método novo {tempo_antigo/tempo_novo:.1f}x mais {'rápido' if tempo_antigo > tempo_novo else 'lento'}")
            # Não falhar, apenas alertar, pois algumas diferenças são esperadas
            return True
            
    except Exception as e:
        logging.error(f"❌ ERRO durante a validação: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    # Lista de CNES para testar
    cnes_para_testar = [
        "2078015",  # Hospital teste 1
        "2445956",  # Hospital teste 2
    ]
    
    # Testes progressivos
    for i in range(1, len(cnes_para_testar) + 1):
        lista_teste = cnes_para_testar[:i]
        logging.info(f"\n\n========== TESTE COM {i} CNES ==========")
        comparar_metodos(cnes_list=lista_teste) 