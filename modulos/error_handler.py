# modulos/error_handler.py

import logging
import time

def process_with_retries(func, retries=3, delay=5, backoff=2):
    """
    Processa uma função com retentativas em caso de falha.
    
    :param func: Função a ser executada
    :param retries: Número máximo de retentativas
    :param delay: Tempo inicial de espera entre retentativas
    :param backoff: Fator de aumento do tempo de espera
    :return: Resultado da função, ou levanta exceção após falhas
    """
    attempt = 0
    while attempt < retries:
        try:
            return func()
        except Exception as e:
            attempt += 1
            logging.error(f"Erro na tentativa {attempt} de {func.__name__}: {e}", exc_info=True)
            if attempt < retries:
                time.sleep(delay)
                delay *= backoff
            else:
                logging.error(f"Falha após {retries} tentativas na função {func.__name__}.")
                raise e
