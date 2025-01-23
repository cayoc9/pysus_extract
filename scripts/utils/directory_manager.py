import os
import logging

def criar_estrutura_diretorios():
    """
    Cria a estrutura de diretórios necessária para o projeto.
    """
    estrutura = {
        'parquet_files': {
            'SIA': ['AB', 'ABO', 'ACF', 'AD', 'AM', 'AMP', 'AN', 'AQ', 'AR', 'ATD', 'BI', 'PS'],
            'SIH': ['RD', 'RJ', 'ER', 'SP']
        },
        'Analises': ['amostras', 'resultados', 'logs'],
        'logs': []
    }

    base_path = "/root/pysus_sih"
    
    try:
        for dir_principal, subdirs in estrutura.items():
            caminho_principal = os.path.join(base_path, dir_principal)
            os.makedirs(caminho_principal, exist_ok=True)
            
            if isinstance(subdirs, dict):
                # Para estruturas como SIA e SIH
                for subdir, grupos in subdirs.items():
                    caminho_subdir = os.path.join(caminho_principal, subdir)
                    os.makedirs(caminho_subdir, exist_ok=True)
                    
                    # Criar diretórios para cada grupo
                    for grupo in grupos:
                        os.makedirs(os.path.join(caminho_subdir, grupo), exist_ok=True)
            else:
                # Para estruturas simples como Analises
                for subdir in subdirs:
                    os.makedirs(os.path.join(caminho_principal, subdir), exist_ok=True)
                    
        logging.info("Estrutura de diretórios criada com sucesso")
        return True
        
    except Exception as e:
        logging.error(f"Erro ao criar estrutura de diretórios: {e}")
        return False 