import logging
import os
from datetime import datetime

def configurar_logging(nome_script: str, nivel=logging.DEBUG):
    """
    Configura sistema de logging com arquivo e console
    
    Args:
        nome_script (str): Nome do script para nome do arquivo de log
        nivel: Nível de logging (padrão: DEBUG)
    """
    logger = logging.getLogger(nome_script)
    logger.setLevel(nivel)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    # Handler para arquivo
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(log_dir, f"{nome_script}_{datetime.now().date()}.log")
    )
    fh.setFormatter(formatter)

    # Handler para console
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger 