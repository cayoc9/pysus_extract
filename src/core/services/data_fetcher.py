# data_fetcher.py

import pysus
import pandas as pd
from utils.log_utils import configurar_logging
from utils.db_utils import get_db_engine, executar_query

# Configurar logging
logger = configurar_logging('data_fetcher')

def fetch_data(years, months, groups, estado='SP'):
    """
    Baixa os dados do SIH para os anos, meses e grupos especificados.

    :param years: Lista de anos para baixar os dados.
    :param months: Lista de meses para baixar os dados.
    :param groups: Lista de grupos SIH para baixar os dados.
    :param estado: Estado para o qual baixar os dados (padrão 'SP').
    :return: DataFrame consolidado com os dados baixados.
    """
    try:
        engine = get_db_engine()
        df_list = []
        grupos_validos = ['RD', 'RJ', 'ER', 'SP', 'CH']  # Grupos válidos

        # Validar grupos
        grupos_invalidos = set(groups) - set(grupos_validos)
        if grupos_invalidos:
            logger.error(f"Unknown SIH Group(s): {grupos_invalidos}")
            return pd.DataFrame()

        for year in years:
            for group in groups:
                for month in months:
                    try:
                        logger.info(f"Baixando dados do grupo {group}, mês {month}, ano {year} para o estado {estado}")
                        df = pysus.download(year=year, month=month, group=group, estado=estado)
                        if not df.empty:
                            df_list.append(df)
                            logger.info(f"Dados do grupo {group}, mês {month}, ano {year} baixados com sucesso.")
                        else:
                            logger.warning(f"Dados do grupo {group}, mês {month}, ano {year} estão vazios.")
                    except Exception as e:
                        logger.error(f"Erro ao baixar dados do grupo {group}, mês {month}, ano {year}: {e}")

        if df_list:
            data_consolidada = pd.concat(df_list, ignore_index=True)
            # Salvar usando utilitários do banco
            data_consolidada.to_sql('tabela_temp', engine, if_exists='append')
            logger.info("Dados salvos com sucesso")
            return data_consolidada
        else:
            logger.warning("Nenhum dado foi baixado.")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro grave: {str(e)}")
        raise
