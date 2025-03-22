#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de teste para o módulo analise_procedimentos.py
Utiliza um volume reduzido de dados para validar o funcionamento
"""

import os
import logging
import time
import pandas as pd
from datetime import datetime

# Configurar logging mais detalhado
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("teste_analise.log"),
        logging.StreamHandler()
    ]
)

# Importar funções do módulo a ser testado
from analise_procedimentos import (
    carregar_dados_parquet,
    analisar_procedimentos_hospital,
    gerar_relatorio_procedimentos
)

def test_carregar_dados():
    """
    Testa a função de carregamento de dados com volume reduzido
    """
    logging.info("=== TESTE DE CARREGAMENTO DE DADOS ===")
    
    # Parâmetros reduzidos
    base = "SIH"
    grupo = "SP"
    cnes_list = ["2078015"]  # Apenas um CNES para teste
    competencia_inicio = "01/2024"
    competencia_fim = "02/2024"  # Apenas 2 meses
    
    inicio = time.time()
    df = carregar_dados_parquet(
        base=base,
        grupo=grupo,
        cnes_list=cnes_list,
        competencia_inicio=competencia_inicio,
        competencia_fim=competencia_fim
    )
    
    tempo_exec = time.time() - inicio
    
    if df is not None and not df.empty:
        logging.info(f"✅ SUCESSO: Carregados {len(df)} registros em {tempo_exec:.2f} segundos")
        # Exibir cabeçalho para verificação visual
        logging.info(f"Amostra dos dados:\n{df.head(3)}")
        return df
    else:
        logging.error("❌ FALHA: Não foi possível carregar dados")
        return None

def test_analisar_procedimentos(df=None):
    """
    Testa a função de análise de procedimentos
    """
    logging.info("=== TESTE DE ANÁLISE DE PROCEDIMENTOS ===")
    
    if df is None or df.empty:
        logging.warning("DataFrame vazio para teste, tentando carregar dados novamente")
        df = test_carregar_dados()
        if df is None or df.empty:
            logging.error("❌ FALHA: Sem dados para análise")
            return None
    
    # Obter o ano dos dados para análise
    ano = None
    for col in df.columns:
        if 'ANO' in col.upper() or 'AA' in col.upper():
            try:
                anos_unicos = df[col].unique()
                if len(anos_unicos) > 0:
                    ano = int(anos_unicos[0])
                    break
            except:
                pass
    
    if ano is None:
        ano = 2024  # Valor padrão
        logging.warning(f"Não foi possível determinar o ano dos dados, usando {ano}")
    
    inicio = time.time()
    resultado = analisar_procedimentos_hospital(df, ano)
    tempo_exec = time.time() - inicio
    
    if resultado is not None and not resultado.empty:
        logging.info(f"✅ SUCESSO: Analisados {len(resultado)} procedimentos em {tempo_exec:.2f} segundos")
        # Exibir amostra do resultado
        logging.info(f"Amostra da análise:\n{resultado.head(3)}")
        
        # Verificar valores esperados no resultado
        esperados = ['Procedimento', 'Mês 1', 'Mês 2', 'Total', 'Grupo']
        todos_encontrados = all(col in resultado.columns for col in esperados)
        
        if todos_encontrados:
            logging.info("✅ Todas as colunas esperadas estão presentes")
        else:
            logging.warning("⚠️ Algumas colunas esperadas estão ausentes!")
            logging.warning(f"Esperadas: {esperados}")
            logging.warning(f"Encontradas: {resultado.columns.tolist()}")
        
        return resultado
    else:
        logging.error("❌ FALHA: Não foi possível analisar procedimentos")
        return None

def test_gerar_relatorio():
    """
    Testa a geração completa do relatório
    """
    logging.info("=== TESTE DE GERAÇÃO DO RELATÓRIO ===")
    
    # Parâmetros reduzidos
    base = "SIH"
    grupo = "SP"
    cnes_list = ["2078015", "2445956"]  # Dois CNES para teste
    competencia_inicio = "01/2024"
    competencia_fim = "02/2024"  # Apenas 2 meses
    anos = [2024]
    diretorio_saida = "teste_relatorios"
    
    inicio = time.time()
    try:
        gerar_relatorio_procedimentos(
            base=base,
            grupo=grupo,
            cnes_list=cnes_list,
            competencia_inicio=competencia_inicio,
            competencia_fim=competencia_fim,
            anos=anos,
            output_dir=diretorio_saida
        )
        
        tempo_exec = time.time() - inicio
        
        # Verificar se os arquivos foram gerados
        arquivo_esperado = os.path.join(diretorio_saida, f"Procedimentos Aprovados {anos[0]}.xlsx")
        if os.path.exists(arquivo_esperado):
            tamanho = os.path.getsize(arquivo_esperado) / 1024  # KB
            logging.info(f"✅ SUCESSO: Relatório gerado em {tempo_exec:.2f} segundos")
            logging.info(f"Arquivo: {arquivo_esperado} ({tamanho:.2f} KB)")
            return True
        else:
            logging.error(f"❌ FALHA: Arquivo {arquivo_esperado} não foi criado")
            return False
    
    except Exception as e:
        logging.error(f"❌ FALHA: Erro na geração do relatório: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False

def run_all_tests():
    """
    Executa todos os testes em sequência
    """
    logging.info("=== INICIANDO TESTES DO MÓDULO ANALISE_PROCEDIMENTOS ===")
    logging.info(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Executar testes em sequência
    df = test_carregar_dados()
    if df is not None and not df.empty:
        test_analisar_procedimentos(df)
    test_gerar_relatorio()
    
    logging.info("=== TESTES CONCLUÍDOS ===")

if __name__ == "__main__":
    try:
        run_all_tests()
    except Exception as e:
        logging.critical(f"Erro durante a execução dos testes: {str(e)}")
        import traceback
        logging.critical(traceback.format_exc())
    finally:
        logging.info("Fim da execução dos testes") 