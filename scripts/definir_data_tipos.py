import os
import re
import json
import unicodedata
from datetime import datetime

def normalizar_nome(nome):
    """
    Normaliza o nome:
    - Converte para minúsculas.
    - Remove acentos e caracteres especiais.
    - Substitui caracteres não alfanuméricos por sublinhado '_'.
    - Remove sublinhados extras no início e no fim.
    """
    nome = nome.lower()
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('ASCII')
    nome = re.sub(r'\W+', '_', nome)
    nome = nome.strip('_')
    return nome

def tentar_converter_data_yyyymmdd(valor):
    """
    Tenta converter uma string no formato 'YYYYMMDD' para 'YYYY-MM-DD'.
    
    Args:
        valor (str): Valor a ser convertido.
    
    Returns:
        tuple: (data_convertida, valor_original) ou (None, valor_original) se falhar.
    """
    original = valor
    if valor == "00000000":
        return (None, original)
    while len(valor) >= 4:
        try:
            dt = datetime.strptime(valor, "%Y%m%d")
            return (dt.strftime('%Y-%m-%d'), None)
        except ValueError:
            valor = valor[:-1]
    return (None, original)

def analisar_tipo_coluna(coluna, info_coluna):
    """
    Analisa o tipo de uma coluna com base nas informações fornecidas.
    
    Args:
        coluna (str): Nome da coluna.
        info_coluna (dict): Informações da coluna.
    
    Returns:
        tuple: (tipo_sugerido, criar_new_col)
    """
    tipo_dado = info_coluna.get('tipo_dado', 'object')
    amostra_valores = info_coluna.get('amostra_valores', [])
    maior_caractere = info_coluna.get('maior_caractere', 0)
    menor_caractere = info_coluna.get('menor_caractere', 0)
    valores_unicos = info_coluna.get('valores_unicos', 0)
    valores_nulos = info_coluna.get('valores_nulos', 0)
    has_leading_zeros = info_coluna.get('has_leading_zeros', False)
    has_mixed_types = info_coluna.get('has_mixed_types', False)

    criar_new_col = False
    nome_col = coluna.lower()

    amostra_sem_nulos = [v for v in amostra_valores if v is not None]

    if valores_unicos == 0 and valores_nulos > 0:
        return ("TEXT", criar_new_col)

    # Data
    if "dt" in nome_col:
        if all(isinstance(x, str) for x in amostra_sem_nulos):
            if all(len(x) == 8 and x.isdigit() for x in amostra_sem_nulos):
                data_valida_para_todos = True
                for val in amostra_sem_nulos:
                    dt_valida, original = tentar_converter_data_yyyymmdd(val)
                    if dt_valida is None:
                        data_valida_para_todos = False
                        break
                if data_valida_para_todos:
                    return ("DATE", criar_new_col)
                else:
                    criar_new_col = True
                    return ("DATE", criar_new_col)
            elif all(len(x) == 6 and x.isdigit() for x in amostra_sem_nulos):
                return ("TEXT", criar_new_col)
            else:
                return ("TEXT", criar_new_col)

    # Valor monetário
    if "val" in nome_col:
        if all((isinstance(x, (int,float)) or (isinstance(x, str) and re.match(r"^-?\d+(\.\d+)?$", x))) for x in amostra_sem_nulos):
            return ("NUMERIC", criar_new_col)
        else:
            return ("TEXT", criar_new_col)

    if has_leading_zeros:
        return ("TEXT", criar_new_col)

    boolean_values = {'0','1','true','false','yes','no'}
    if amostra_sem_nulos and all(str(v).strip().lower() in boolean_values for v in amostra_sem_nulos):
        return ("BOOLEAN", criar_new_col)

    # Numérico
    is_numeric = True
    is_integer = True
    max_int = 0
    for v in amostra_sem_nulos:
        v_str = str(v).strip()
        if re.match(r"^-?\d+$", v_str):
            num = int(v_str)
            if abs(num) > max_int:
                max_int = abs(num)
        elif re.match(r"^-?\d+(\.\d+)?$", v_str):
            is_integer = False
        else:
            is_numeric = False
            break
    if is_numeric:
        if is_integer:
            if max_int <= 32767:
                return ("SMALLINT", criar_new_col)
            elif max_int <= 2147483647:
                return ("INTEGER", criar_new_col)
            else:
                return ("BIGINT", criar_new_col)
        else:
            return ("NUMERIC", criar_new_col)

    if has_mixed_types:
        return ("TEXT", criar_new_col)

    # Texto
    if maior_caractere == menor_caractere and maior_caractere > 0:
        return (f"CHAR({maior_caractere})", criar_new_col)
    else:
        if maior_caractere <= 255:
            return (f"VARCHAR({maior_caractere})", criar_new_col)
        else:
            return ("TEXT", criar_new_col)

def processar_dados(dados):
    """
    Processa os dados de um arquivo de análise JSON, determinando os tipos de colunas
    e adicionando as colunas id_log e uf.
    
    Args:
        dados (dict): Dados carregados do arquivo JSON.
    
    Returns:
        dict: Mapeamento de colunas com tipos sugeridos, incluindo id_log e uf.
    """
    tipo_coluna_map = {}
    for coluna, info_coluna in dados.items():
        if not isinstance(info_coluna, dict):
            continue
        coluna_norm = normalizar_nome(coluna)
        tipo_sugerido, criar_new_col = analisar_tipo_coluna(coluna, info_coluna)
        tipo_coluna_map[coluna_norm] = tipo_sugerido
        if criar_new_col:
            tipo_coluna_map[f"new_{coluna_norm}"] = "TEXT"

    # Adicionar colunas id_log e uf após o tratamento das colunas existentes
    tipo_coluna_map['id_log'] = "VARCHAR(255)"
    tipo_coluna_map['uf'] = "CHAR(2)"

    # Ordenar as chaves do dicionário em ordem alfabética
    tipo_coluna_map = dict(sorted(tipo_coluna_map.items()))

    return tipo_coluna_map

def extrair_base_e_grupo(nome_arquivo):
    """
    Extrai base e grupo do nome do arquivo no formato amostra_{BASE}_{GRUPO}.json
    Ex: amostra_SIA_AB.json -> base = 'sia', grupo = 'AB'
    
    Args:
        nome_arquivo (str): Nome do arquivo.
    
    Returns:
        tuple: (base, grupo) ou (None, None) se não corresponder.
    """
    padrao = r"^amostra_(?P<base>[A-Za-z0-9]+)_(?P<grupo>[A-Za-z0-9]+)\.json$"
    m = re.match(padrao, nome_arquivo)
    if m:
        return m.group('base').lower(), m.group('grupo').upper()
    return None, None

def gerar_grupos_info(tipo_coluna_map_files, grupos_dict, diretorio_tipos_dados):
    """
    Gera o dicionário GRUPOS_INFO com base nos arquivos tipo_coluna_map 
    
    Args:
        tipo_coluna_map_files (list): Lista de nomes de arquivos tipo_coluna_map que contêm 'cnes'.
        grupos_dict (dict): Dicionário mapeando códigos de grupos para descrições.
        diretorio_tipos_dados (str): Caminho para o diretório tipos_dados.
    
    Returns:
        dict: Dicionário GRUPOS_INFO conforme estrutura desejada.
    """
    GRUPOS_INFO = {}

    for arquivo in tipo_coluna_map_files:
        caminho = os.path.join(diretorio_tipos_dados, arquivo)
        # Extrair base e grupo do nome do arquivo
        base_grupo_match = re.match(r"tipo_coluna_map_(?P<base>[A-Za-z0-9]+)_(?P<grupo>[A-Za-z0-9]+)\.json$", arquivo, re.IGNORECASE)
        if not base_grupo_match:
            print(f"Aviso: O arquivo {arquivo} não segue o padrão esperado (tipo_coluna_map_BASE_GRUPO.json).")
            continue
        base = base_grupo_match.group('base').lower()
        grupo = base_grupo_match.group('grupo').upper()

        if grupo not in grupos_dict:
            print(f"Aviso: Não foi possível encontrar o grupo '{grupo}' no dicionário de grupos.")
            continue

        descricao = grupos_dict[grupo]
        tabela = f"{base}_{normalizar_nome(descricao)}"

        # Carregar o mapeamento de colunas do JSON
        try:
            with open(caminho, "r", encoding="utf-8") as f:
                dados = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON no arquivo '{arquivo}': {e}")
            continue
        except Exception as e:
            print(f"Erro ao processar o arquivo '{arquivo}': {e}")
            continue

        # 'dados' está no formato { "base_grupo": {"coluna": "TIPO", ...}}
        # Iterar sobre as chaves internas
        for chave_base_grupo, colunas_dict in dados.items():
            # colunas_dict é um dict colunas -> tipo
            # Normalizar os nomes das colunas e manter os tipos
            colunas_normalizadas = {normalizar_nome(col): tipo for col, tipo in colunas_dict.items()}
            GRUPOS_INFO[grupo] = {
                "tabela": tabela,
                "colunas": colunas_normalizadas
            }

    return GRUPOS_INFO

def main():
    # Diretório contendo os arquivos de análise JSON
    diretorio_analise = "./Analises/amostras/"
    # Diretório para salvar os mapeamentos de tipos de colunas
    diretorio_tipos_dados = "./tipos_dados/"
    # Assegurar que o diretório de tipos_dados existe
    os.makedirs(diretorio_tipos_dados, exist_ok=True)

    # Dicionário mapeando códigos de grupos para descrições
    grupos_dict = {
        'AB': 'apac_cirurgia_bariátrica',
        'ABO': 'apac_acompanhamento_pós_cirurgia_bariátrica',
        'ACF': 'apac_confecção_de_fístula',
        'AD': 'apac_laudos_diversos',
        'AM': 'apac_medicamentos',
        'AMP': 'apac_acompanhamento_multiprofissional',
        'AN': 'apac_nefrologia',
        'AQ': 'apac_quimioterapia',
        'AR': 'apac_radioterapia',
        'ATD': 'apac_tratamento_dialítico',
        'BI': 'sia_boletim_producao_ambulatorial_individualizado',
        'PA': 'producao_ambulatorial',
        'PS': 'raas_psicossocial',
        'SAD': 'raas_atencao_domiciliar',
        'DC': 'dados_complementares',
        'EE': 'estabelecimento_ensino',
        'EF': 'estabelecimento_filantrópico',
        'EP': 'equipes',
        'EQ': 'equipamentos',
        'GM': 'gestão_metas',
        'HB': 'habilitacao',
        'IN': 'incentivos',
        'LT': 'leitos',
        'PF': 'profissional',
        'RC': 'regra_contratual',
        'SR': 'servico_especializado',
        'ST': 'estabelecimentos'
    }

    # Processar os arquivos de análise JSON e gerar tipo_coluna_map JSONs
    print("Processando arquivos de análise JSON e gerando mapeamentos de tipos de colunas...")
    arquivos_encontrados = [arquivo for arquivo in os.listdir(diretorio_analise)
                            if arquivo.startswith("amostra_") and arquivo.endswith(".json")]

    for arquivo in arquivos_encontrados:
        caminho = os.path.join(diretorio_analise, arquivo)
        base_grupo = arquivo.replace("amostra_", "").replace(".json", "")
        # Carregar dados do arquivo
        try:
            with open(caminho, 'r', encoding='utf-8') as f:
                dados = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON no arquivo '{arquivo}': {e}")
            continue
        except Exception as e:
            print(f"Erro ao processar o arquivo '{arquivo}': {e}")
            continue

        # Processar dados para determinar tipos e adicionar id_log e uf
        tipo_coluna_map = processar_dados(dados)

        # Salvar resultado no arquivo tipo_coluna_map_{BASE}_{GRUPO}.json
        saida = f"./tipos_dados/tipo_coluna_map_{base_grupo}.json"
        try:
            with open(saida, 'w', encoding='utf-8') as f:
                json.dump({base_grupo: tipo_coluna_map}, f, ensure_ascii=False, indent=4)
            print(f"{arquivo} -> {saida}")
        except Exception as e:
            print(f"Erro ao salvar o arquivo '{saida}': {e}")
            continue

    # Filtrar os tipo_coluna_map que contêm 'cnes' no nome
    tipo_coluna_map_files = [arquivo for arquivo in os.listdir(diretorio_tipos_dados)
                             if re.search(r'sia', arquivo, re.IGNORECASE) and arquivo.endswith('.json')]

    if not tipo_coluna_map_files:
        print(f"Nenhum arquivo contendo 'cnes' encontrado no diretório '{diretorio_tipos_dados}'.")
        return

    print("\nGerando GRUPOS_INFO com base nos mapeamentos de tipos de colunas...")
    GRUPOS_INFO = gerar_grupos_info(tipo_coluna_map_files, grupos_dict, diretorio_tipos_dados)

    # Exibir o dicionário GRUPOS_INFO
    for grupo, info in GRUPOS_INFO.items():
        print(f"\n{grupo}:")
        print(f"  tabela: {info['tabela']}")
        print("  colunas:")
        for coluna, tipo in sorted(info['colunas'].items()):
            print(f"    {coluna}: {tipo}")

    # Salvar GRUPOS_INFO em grupos_info.json
    caminho_grupos_info = "grupos_info.json"
    try:
        with open(caminho_grupos_info, 'w', encoding='utf-8') as f:
            json.dump(GRUPOS_INFO, f, ensure_ascii=False, indent=4)
        print(f"\nArquivo '{caminho_grupos_info}' salvo com sucesso!")
    except Exception as e:
        print(f"Erro ao salvar o arquivo '{caminho_grupos_info}': {e}")

if __name__ == "__main__":
    main()
