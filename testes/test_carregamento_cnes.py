#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de teste específico para o carregamento e correspondência exata de CNES
Foco em validar se os códigos CNES estão sendo filtrados corretamente
"""

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
        logging.FileHandler("teste_cnes.log"),
        logging.StreamHandler()
    ]
)

# Importação das funções necessárias
from main import get_parquet_files, QueryParams, process_data

def validar_carregamento_cnes(
    base="SIH", 
    grupo="SP", 
    cnes_list=["2078015"], 
    competencia_inicio="01/2024",
    competencia_fim="02/2024"
):
    """
    Testa especificamente o carregamento e correspondência de códigos CNES
    
    Args:
        base: Base de dados (SIH/SIA)
        grupo: Grupo de dados
        cnes_list: Lista de códigos CNES para teste
        competencia_inicio: Período inicial MM/YYYY
        competencia_fim: Período final MM/YYYY
        
    Returns:
        bool: True se o teste for bem-sucedido, False caso contrário
    """
    logging.info("=== TESTE DE CORRESPONDÊNCIA EXATA DE CNES ===")
    logging.info(f"CNES para teste: {cnes_list}")
    
    inicio = time.time()
    
    try:
        # 1. Obter os arquivos parquet
        arquivos = get_parquet_files(base, grupo, competencia_inicio, competencia_fim)
        if not arquivos:
            logging.error("❌ Nenhum arquivo encontrado para o período especificado")
            return False
            
        logging.info(f"✅ Encontrados {len(arquivos)} arquivos parquet")
        
        # 2. Criar parâmetros de consulta
        campos = ["SP_GESTOR", "SP_UF", "SP_AA", "SP_MM", "SP_CNES", "SP_NAIH", "SP_PROCREA"]
        params = QueryParams(
            base=base,
            grupo=grupo,
            cnes_list=cnes_list,
            campos_agrupamento=campos,
            competencia_inicio=competencia_inicio,
            competencia_fim=competencia_fim,
            table_name=None
        )
        
        # 3. Processar dados com DuckDB
        temp_table = process_data(arquivos, params)
        logging.info(f"✅ Tabela temporária criada: {temp_table}")
        
        # 4. Verificar dados na tabela temporária
        cnes_col = None
        for col in duckdb.execute(f"DESCRIBE {temp_table}").fetchdf()['column_name']:
            if 'CNES' in col.upper():
                cnes_col = col
                break
                
        if not cnes_col:
            logging.error("❌ Coluna CNES não encontrada na tabela")
            return False
            
        # 5. Contar registros por CNES
        query_count = f"""
            SELECT {cnes_col}, COUNT(*) as contagem 
            FROM {temp_table}
            GROUP BY {cnes_col}
            ORDER BY contagem DESC
        """
        
        contagem_df = duckdb.execute(query_count).fetchdf()
        logging.info(f"Distribuição de CNES:\n{contagem_df}")
        
        # 6. Normalizar os códigos CNES para comparação
        contagem_df['cnes_normalizado'] = contagem_df[cnes_col].astype(str).str.strip()
        # Preencher com zeros à esquerda
        contagem_df['cnes_normalizado'] = contagem_df['cnes_normalizado'].apply(
            lambda x: x.zfill(7) if len(x) <= 7 else x[-7:]
        )
        
        # 7. Verificar correspondência exata com os CNES da lista
        cnes_normalizados = [c.zfill(7) if len(c) <= 7 else c[-7:] for c in cnes_list]
        encontrados = contagem_df['cnes_normalizado'].isin(cnes_normalizados).sum()
        
        # 8. Verificar CNES duplicados ou inesperados
        if len(contagem_df) > len(cnes_list):
            logging.warning("⚠️ Existem mais CNES nos dados do que foram solicitados!")
            extras = contagem_df[~contagem_df['cnes_normalizado'].isin(cnes_normalizados)]
            logging.warning(f"CNES extras encontrados:\n{extras}")
        
        # 9. Mostrar amostra dos registros
        amostra = duckdb.execute(f"SELECT * FROM {temp_table} LIMIT 5").fetchdf()
        logging.info(f"Amostra dos dados:\n{amostra}")
        
        tempo_total = time.time() - inicio
        
        # 10. Verificar resultado final
        if encontrados == len(cnes_list):
            logging.info(f"✅ SUCESSO: Todos os {len(cnes_list)} CNES foram encontrados exatamente")
            logging.info(f"Teste concluído em {tempo_total:.2f} segundos")
            return True
        else:
            logging.error(f"❌ FALHA: Apenas {encontrados} de {len(cnes_list)} CNES foram encontrados exatamente")
            logging.info(f"Teste concluído em {tempo_total:.2f} segundos")
            return False
            
    except Exception as e:
        logging.error(f"❌ ERRO durante o teste: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    # Lista de CNES para testar
    cnes_para_testar = [
        "2078015",  # Hospital teste 1
        "2445956",  # Hospital teste 2
        "0011800"   # Hospital teste 3
    ]
    
    # Testar uma lista limitada primeiro
    validar_carregamento_cnes(cnes_list=cnes_para_testar[:1])
    
    # Testar a lista completa
    validar_carregamento_cnes(cnes_list=cnes_para_testar) 