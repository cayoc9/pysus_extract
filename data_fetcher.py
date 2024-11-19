# data_fetcher.py

import pysus
import pandas as pd
import logging

def fetch_data(years, months, groups, estado='SP'):
    """
    Baixa os dados do SIH para os anos, meses e grupos especificados.

    :param years: Lista de anos para baixar os dados.
    :param months: Lista de meses para baixar os dados.
    :param groups: Lista de grupos SIH para baixar os dados.
    :param estado: Estado para o qual baixar os dados (padrão 'SP').
    :return: DataFrame consolidado com os dados baixados.
    """
    df_list = []
    grupos_validos = ['RD', 'RJ', 'ER', 'SP', 'CH']  # Grupos válidos

    # Validar grupos
    grupos_invalidos = set(groups) - set(grupos_validos)
    if grupos_invalidos:
        logging.error(f"Unknown SIH Group(s): {grupos_invalidos}")
        return pd.DataFrame()

    for year in years:
        for group in groups:
            for month in months:
                try:
                    logging.info(f"Baixando dados do grupo {group}, mês {month}, ano {year} para o estado {estado}")
                    df = pysus.download(year=year, month=month, group=group, estado=estado)
                    if not df.empty:
                        df_list.append(df)
                        logging.info(f"Dados do grupo {group}, mês {month}, ano {year} baixados com sucesso.")
                    else:
                        logging.warning(f"Dados do grupo {group}, mês {month}, ano {year} estão vazios.")
                except Exception as e:
                    logging.error(f"Erro ao baixar dados do grupo {group}, mês {month}, ano {year}: {e}")

    if df_list:
        try:
            data_consolidada = pd.concat(df_list, ignore_index=True)
            logging.info("Todos os dados foram baixados e consolidados com sucesso.")
            return data_consolidada
        except Exception as e:
            logging.error(f"Erro ao consolidar os dados: {e}")
            return pd.DataFrame()
    else:
        logging.warning("Nenhum dado foi baixado.")
        return pd.DataFrame()
