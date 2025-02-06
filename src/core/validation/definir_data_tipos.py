import pandas as pd
import re
import json
import unicodedata
from datetime import datetime

def normalizar_nome(nome):
    """
    Normaliza o nome:
    - Converte para minúsculas.
    - Remove acentos e caracteres especiais.
    - Remove caracteres não alfanuméricos, exceto o sublinhado '_'.
    """
    # Converter para minúsculas
    nome = nome.lower()
    
    # Remover acentos e diacríticos
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('ASCII')
    
    # Substituir caracteres não alfanuméricos por sublinhado
    nome = re.sub(r'\W+', '_', nome)
    
    # Remover sublinhados iniciais ou finais
    nome = nome.strip('_')
    
    return nome

def analisar_tipo_coluna(coluna_info):
    """
    Analisa uma coluna e retorna o melhor tipo de dado com base na amostra.
    Args:
        coluna_info (dict): Informações sobre a coluna (amostra, unicidade, etc.)
    Returns:
        dict: Tipo de dado sugerido e justificativa.
    """
    tipo_dado = coluna_info['tipo_dado']
    amostra_valores = coluna_info['amostra_valores']
    maior_caractere = coluna_info['maior_caractere']
    menor_caractere = coluna_info['menor_caractere']
    valores_unicos = coluna_info['valores_unicos']
    valores_nulos = coluna_info['valores_nulos']
    has_leading_zeros = coluna_info.get('has_leading_zeros', False)
    has_special_chars = coluna_info.get('has_special_chars', False)
    has_mixed_types = coluna_info.get('has_mixed_types', False)

    # Resultado inicial
    resultado = {
        "tipo_sugerido": None,
        "justificativa": None
    }

    # Se todos os valores são nulos, definir como TEXT
    if valores_nulos > 0 and valores_nulos == len(amostra_valores):
        resultado["tipo_sugerido"] = "TEXT"
        resultado["justificativa"] = "Todos os valores são nulos."
        return resultado

    # Remover valores nulos da amostra
    amostra_sem_nulos = [valor for valor in amostra_valores if valor is not None]

    # 0. Verificar se há zeros à esquerda
    if has_leading_zeros:
        resultado["tipo_sugerido"] = "TEXT"
        resultado["justificativa"] = "Valores com zeros à esquerda detectados. Tipo definido como TEXT para preservar os zeros."
        return resultado

    # 1. Verificar se é booleano
    boolean_values = {'0', '1', 'True', 'False', 'yes', 'no'}
    if all(str(valor).strip().lower() in boolean_values for valor in amostra_sem_nulos):
        resultado["tipo_sugerido"] = "BOOLEAN"
        resultado["justificativa"] = "Valores booleanos detectados."
        return resultado

    # 2. Verificar se é data
    formatos_data = ['%Y-%m-%d']
    is_date = True
    for valor in amostra_sem_nulos:
        try:
            datetime.strptime(valor, formatos_data[0])
        except ValueError:
            is_date = False
            break
    if is_date:
        resultado["tipo_sugerido"] = "DATE"
        resultado["justificativa"] = "Valores de data detectados."
        return resultado

    # 3. Verificar se é numérico (inteiro ou decimal)
    is_integer = True
    is_numeric = True
    max_int = None
    for valor in amostra_sem_nulos:
        if isinstance(valor, int):
            num = valor
            if max_int is None or abs(num) > max_int:
                max_int = abs(num)
        elif isinstance(valor, float):
            is_integer = False
        else:
            is_numeric = False
            break

    if is_numeric:
        if is_integer:
            if max_int <= 32767:
                resultado["tipo_sugerido"] = "SMALLINT"
                resultado["justificativa"] = "Números inteiros pequenos detectados."
            elif max_int <= 2147483647:
                resultado["tipo_sugerido"] = "INTEGER"
                resultado["justificativa"] = "Números inteiros detectados."
            else:
                resultado["tipo_sugerido"] = "BIGINT"
                resultado["justificativa"] = "Números inteiros grandes detectados."
        else:
            resultado["tipo_sugerido"] = "NUMERIC"
            resultado["justificativa"] = "Valores numéricos decimais detectados."
        return resultado

    # 4. Verificar se possui tipos mistos
    if has_mixed_types:
        resultado["tipo_sugerido"] = "TEXT"
        resultado["justificativa"] = "Valores mistos detectados. Tipo definido como TEXT para preservar os dados."
        return resultado

    # 5. Verificar se é texto com tamanho fixo
    if maior_caractere == menor_caractere:
        resultado["tipo_sugerido"] = f"CHAR({maior_caractere})"
        resultado["justificativa"] = "Texto com tamanho fixo detectado."
    else:
        # 6. Verificar se é texto curto ou longo
        if maior_caractere <= 255:
            resultado["tipo_sugerido"] = f"VARCHAR({maior_caractere})"
            resultado["justificativa"] = "Texto com tamanho variável curto detectado."
        else:
            resultado["tipo_sugerido"] = "TEXT"
            resultado["justificativa"] = "Texto longo detectado."

    return resultado

def processar_dados(dados):
    """
    Processa as informações de todas as colunas e sugere tipos de dados.
    Args:
        dados (dict): Dicionário contendo informações das colunas.
    Returns:
        dict: Mapeamento de tipos de dados para cada tabela com nomes normalizados.
    """
    tipo_coluna_map = {}
    for tabela, colunas in dados.items():
        tabela_normalizada = normalizar_nome(tabela)
        tipo_coluna_map[tabela_normalizada] = {}
        for coluna, info in colunas.items():
            coluna_normalizada = normalizar_nome(coluna)
            resultado = analisar_tipo_coluna(info)
            tipo_coluna_map[tabela_normalizada][coluna_normalizada] = resultado['tipo_sugerido']
            # Opcional: Armazenar a justificativa se necessário
            # tipo_coluna_map[tabela_normalizada][coluna_normalizada]['justificativa'] = resultado['justificativa']
        
        # Adicionar as três novas colunas
        tipo_coluna_map[tabela_normalizada]['id'] = "SERIAL PRIMARY KEY"
        tipo_coluna_map[tabela_normalizada]['id_log'] = "VARCHAR(255)"
        tipo_coluna_map[tabela_normalizada]['uf'] = "CHAR(2)"
        
        # Organizar as colunas em ordem alfabética
        tipo_coluna_map[tabela_normalizada] = dict(sorted(tipo_coluna_map[tabela_normalizada].items()))
    
    return tipo_coluna_map

def inicializar_estrutura_padrao():
    """Define a estrutura padrão para cada coluna"""
    return {
        'tipo_dado': 'object',
        'valores_unicos': 0,
        'valores_nulos': 0,
        'amostra_valores': [],
        'maior_caractere': 0,
        'menor_caractere': 0,
        'has_leading_zeros': False,
        'has_special_chars': False,
        'has_mixed_types': False
    }

def calcular_metricas_basicas(valores):
    """Calcula métricas básicas para uma lista de valores"""
    if not valores:
        return 0, 0, False, False, False
    
    # Converte todos os valores para string para análise
    valores_str = [str(v) for v in valores]
    
    maior = max(len(v) for v in valores_str)
    menor = min(len(v) for v in valores_str)
    
    # Verifica zeros à esquerda
    has_leading = any(v.startswith('0') and len(v) > 1 for v in valores_str)
    
    # Verifica caracteres especiais
    has_special = any(not v.isalnum() and not v.isspace() for v in valores_str)
    
    # Verifica tipos mistos
    has_mixed = any(v.isdigit() for v in valores_str) and any(not v.isdigit() for v in valores_str)
    
    return maior, menor, has_leading, has_special, has_mixed

def preprocessar_dados(dados):
    """Pré-processa e valida os dados"""
    dados_processados = {}
    
    for tabela, colunas in dados.items():
        dados_processados[tabela] = {}
        
        for coluna, info in colunas.items():
            # Inicializa com estrutura padrão
            nova_info = inicializar_estrutura_padrao()
            
            # Atualiza com dados existentes
            for chave, valor in info.items():
                nova_info[chave] = valor
            
            # Calcula métricas se necessário
            if 'maior_caractere' not in info and 'amostra_valores' in info:
                maior, menor, has_leading, has_special, has_mixed = calcular_metricas_basicas(info['amostra_valores'])
                
                nova_info.update({
                    'maior_caractere': maior,
                    'menor_caractere': menor,
                    'has_leading_zeros': has_leading,
                    'has_special_chars': has_special,
                    'has_mixed_types': has_mixed
                })
            
            dados_processados[tabela][coluna] = nova_info
            
    return dados_processados

def validar_dados(dados_processados):
    """Valida os dados processados"""
    erros = []
    
    for tabela, colunas in dados_processados.items():
        for coluna, info in colunas.items():
            # Verifica campos obrigatórios
            campos_obrigatorios = ['tipo_dado', 'valores_unicos', 'valores_nulos', 
                                 'amostra_valores', 'maior_caractere', 'menor_caractere']
            
            for campo in campos_obrigatorios:
                if campo not in info:
                    erros.append(f"Campo {campo} faltando em {tabela}.{coluna}")
                    
            # Valida tipos de dados
            if not isinstance(info.get('valores_unicos', 0), (int, float)):
                erros.append(f"valores_unicos deve ser numérico em {tabela}.{coluna}")
                
    return erros


if __name__ == "__main__":
    # Dados de exemplo (substitua pelos seus dados reais)
    dados = {
        'sia_apac_cirurgia_bariatrica': {
    "AP_MVM": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "201610"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CONDIC": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "PG",
            "EP"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_GESTAO": {
        "tipo_dado": "object",
        "valores_unicos": 4,
        "valores_nulos": 0,
        "amostra_valores": [
            "354990",
            "353870",
            "350000",
            "354980"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CODUNI": {
        "tipo_dado": "object",
        "valores_unicos": 12,
        "valores_nulos": 0,
        "amostra_valores": [
            "0009601",
            "2087057",
            "2083086",
            "2079798",
            "2077477",
            "2748029",
            "2080532",
            "2748223",
            "2688689",
            "2097605"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_AUTORIZ": {
        "tipo_dado": "object",
        "valores_unicos": 1352,
        "valores_nulos": 0,
        "amostra_valores": [
            "3516242954653",
            "3516242954587",
            "3516242622453",
            "3516242623861",
            "3516242623730",
            "3516242622255",
            "3516242623157",
            "3516242621705",
            "3516242623014",
            "3516235832890"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CMP": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "201610",
            "201609",
            "201608"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PRIPAL": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0301120056"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_VL_AP": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "               40.00",
            "                0.00"
        ],
        "maior_caractere": 20,
        "menor_caractere": 20,
        "has_leading_zeros": False,
        "has_special_chars": True,
        "has_mixed_types": False
    },
    "AP_UFMUN": {
        "tipo_dado": "object",
        "valores_unicos": 9,
        "valores_nulos": 0,
        "amostra_valores": [
            "354990",
            "353870",
            "352530",
            "350950",
            "355030",
            "354140",
            "350750",
            "354980",
            "352900"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPUPS": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "05",
            "07"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TIPPRE": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "00"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MN_IND": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "M",
            "I"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJCPF": {
        "tipo_dado": "object",
        "valores_unicos": 12,
        "valores_nulos": 0,
        "amostra_valores": [
            "60194990000682",
            "54384631000261",
            "50753755000135",
            "46068425000133",
            "60742616000160",
            "45186053000187",
            "55344337000108",
            "12474705000120",
            "62779145000190",
            "60007648000383"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJMNT": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "60194990000178",
            "0000000000000 ",
            "46068425000133",
            "60007648000111",
            "66495110000180"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNSPCN": {
        "tipo_dado": "object",
        "valores_unicos": 1349,
        "valores_nulos": 0,
        "amostra_valores": [
            "|{~}{{{~",
            "{{|}~{}~",
            "{{{|}",
            "{{{{~}{",
            "}{|{{{{",
            "{{{}|~",
            "{{{",
            "{{{{~~",
            "}|{}~~{{{}",
            "}{~}~{{{"
        ],
        "maior_caractere": 15,
        "menor_caractere": 15,
        "has_leading_zeros": False,
        "has_special_chars": True,
        "has_mixed_types": False
    },
    "AP_COIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "4"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_NUIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 52,
        "valores_nulos": 0,
        "amostra_valores": [
            "63",
            "39",
            "21",
            "41",
            "49",
            "19",
            "42",
            "32",
            "40",
            "35"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_SEXO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "F",
            "M"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_RACACOR": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "02",
            "01",
            "99",
            "03",
            "04"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MUNPCN": {
        "tipo_dado": "object",
        "valores_unicos": 232,
        "valores_nulos": 0,
        "amostra_valores": [
            "354990",
            "353870",
            "353430",
            "352230",
            "355640",
            "350520",
            "352265",
            "352610",
            "353750",
            "354580"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFNACIO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "010",
            "100"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CEPPCN": {
        "tipo_dado": "object",
        "valores_unicos": 1024,
        "valores_nulos": 0,
        "amostra_valores": [
            "12226690",
            "12228454",
            "13414261",
            "13426164",
            "14620000",
            "18202365",
            "13880000",
            "17250000",
            "18385000",
            "11800000"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFDIF": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MNDIF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTINIC": {
        "tipo_dado": "object",
        "valores_unicos": 56,
        "valores_nulos": 0,
        "amostra_valores": [
            "20161001",
            "20160901",
            "20160801",
            "20160913",
            "20160803",
            "20160809",
            "20160906",
            "20161010",
            "20161007",
            "20160919"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTFIM": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "20161231",
            "20161130",
            "20161031",
            "20160930",
            "20161030",
            "20161017"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPATEN": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "05"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPAPAC": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "1",
            "2"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MOTSAI": {
        "tipo_dado": "object",
        "valores_unicos": 4,
        "valores_nulos": 0,
        "amostra_valores": [
            "21",
            "18",
            "15",
            "12"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_OBITO": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ENCERR": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PERMAN": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "1",
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ALTA": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TRANSF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTOCOR": {
        "tipo_dado": "object",
        "valores_unicos": 16,
        "valores_nulos": 0,
        "amostra_valores": [
            "        ",
            "20161001",
            "20161031",
            "20160930",
            "20161019",
            "20161005",
            "20161027",
            "20161003",
            "20161004",
            "20161006"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AP_CODEMI": {
        "tipo_dado": "object",
        "valores_unicos": 11,
        "valores_nulos": 0,
        "amostra_valores": [
            "M310620001",
            "M353870001",
            "E350000028",
            "S352079798",
            "E350000027",
            "E350000030",
            "E350000016",
            "S352748223",
            "M354980501",
            "E350000029"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CATEND": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "01"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_APACANT": {
        "tipo_dado": "object",
        "valores_unicos": 387,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000000000000",
            "3516242622453",
            "3516242623861",
            "3516242623730",
            "3516242622255",
            "3516242623157",
            "3516242621705",
            "3516242623014",
            "3516242622332",
            "3516242622013"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UNISOL": {
        "tipo_dado": "object",
        "valores_unicos": 11,
        "valores_nulos": 0,
        "amostra_valores": [
            "0009601",
            "2087057",
            "2083086",
            "2079798",
            "2077477",
            "2748029",
            "2080532",
            "0000000",
            "2097605",
            "2025507"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTSOLIC": {
        "tipo_dado": "object",
        "valores_unicos": 66,
        "valores_nulos": 0,
        "amostra_valores": [
            "20161001",
            "20161014",
            "20161021",
            "20160922",
            "20161017",
            "20160930",
            "20161020",
            "20161022",
            "20161015",
            "20160913"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTAUT": {
        "tipo_dado": "object",
        "valores_unicos": 58,
        "valores_nulos": 0,
        "amostra_valores": [
            "20161001",
            "20161108",
            "20161003",
            "20160905",
            "20160913",
            "20160901",
            "20160803",
            "20160809",
            "20160906",
            "20161010"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDCAS": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000",
            "E668"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AP_CIDPRI": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDSEC": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ETNIA": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "    "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_IMC": {
        "tipo_dado": "object",
        "valores_unicos": 98,
        "valores_nulos": 0,
        "amostra_valores": [
            "40 ",
            "35 ",
            "043",
            "037",
            "039",
            "052",
            "036",
            "047",
            "040",
            "060"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_PROCAIH": {
        "tipo_dado": "object",
        "valores_unicos": 647,
        "valores_nulos": 0,
        "amostra_valores": [
            "013020NNSN",
            "052033NNSN",
            "000000NNSN",
            "020010NNSN",
            "017035NNSN",
            "022021NNSN",
            "034045NNSN",
            "013015NNSN",
            "009013NNSN",
            "033034NNSN"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_DTCIRUR": {
        "tipo_dado": "object",
        "valores_unicos": 581,
        "valores_nulos": 0,
        "amostra_valores": [
            "20131121",
            "20040914",
            "20160607",
            "20131104",
            "20150923",
            "20150924",
            "20160913",
            "20150916",
            "20160525",
            "20160809"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_NUMAIH": {
        "tipo_dado": "object",
        "valores_unicos": 1348,
        "valores_nulos": 0,
        "amostra_valores": [
            "3513122271901",
            "0002935819205",
            "3516242622453",
            "3516242623861",
            "3516242623730",
            "3516242622255",
            "3516242623157",
            "3516242621705",
            "3516242623014",
            "3516235832890"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_PRCAIH2": {
        "tipo_dado": "object",
        "valores_unicos": 12,
        "valores_nulos": 0,
        "amostra_valores": [
            " SNSNNN   ",
            " N        ",
            " NNNNNN   ",
            " SSNNNN   ",
            " S        ",
            " SNNNNN   ",
            " NNNNNNE03",
            " SNNNSN   ",
            " SSSNNN   ",
            " NSNNNN   "
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_PRCAIH3": {
        "tipo_dado": "object",
        "valores_unicos": 61,
        "valores_nulos": 0,
        "amostra_valores": [
            " SNNNSN   ",
            " NNSNSN   ",
            " NNNNNN   ",
            " NSSNSN000",
            " NNNNNN000",
            " NNSNSS044",
            " NSSNSN   ",
            " NSSSSN000",
            " SSSNSN   ",
            " NNSNNN   "
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_NUMAIH2": {
        "tipo_dado": "object",
        "valores_unicos": 9,
        "valores_nulos": 0,
        "amostra_valores": [
            "N   N   N   N",
            "N000N000N000N",
            "S063N000N000N",
            "N000S068N000N",
            "S071N000N000N",
            "N000N000S046N",
            "N000S053N000N",
            "S060N000N000N",
            "N000N000S114N"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_DTCIRG2": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "        ",
            "000     "
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AB_MESACOM": {
        "tipo_dado": "object",
        "valores_unicos": 44,
        "valores_nulos": 0,
        "amostra_valores": [
            "00",
            "04",
            "36",
            "12",
            "01",
            "02",
            "03",
            "05",
            "19",
            "10"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_ANOACOM": {
        "tipo_dado": "object",
        "valores_unicos": 14,
        "valores_nulos": 0,
        "amostra_valores": [
            "0003",
            "0012",
            "    ",
            "2016",
            "0000",
            "0002",
            "0001",
            "0011",
            "0006",
            "0007"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AB_PONTBAR": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "E"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_TABBARR": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "6"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_NATJUR": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "3999",
            "3069",
            "1112"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    }
    },       
        'sia_apac_acompanhamento_pos_cirurgia_bariatrica': {
    "AP_MVM": {
        "tipo_dado": "object",
        "valores_unicos": 4,
        "valores_nulos": 0,
        "amostra_valores": [
            "202301",
            "202312",
            "202401",
            "202406"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CONDIC": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "EP"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_GESTAO": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "120000"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CODUNI": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "2001586"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_AUTORIZ": {
        "tipo_dado": "object",
        "valores_unicos": 31,
        "valores_nulos": 0,
        "amostra_valores": [
            "1223200015092",
            "1223200015070",
            "1222200214444",
            "1223200015147",
            "1222200214169",
            "1223200015060",
            "1222200214170",
            "1223200015103",
            "1222200214455",
            "1223200015081"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CMP": {
        "tipo_dado": "object",
        "valores_unicos": 4,
        "valores_nulos": 0,
        "amostra_valores": [
            "202301",
            "202312",
            "202401",
            "202406"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PRIPAL": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0301120056"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_VL_AP": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "40.00"
        ],
        "maior_caractere": 5,
        "menor_caractere": 5,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFMUN": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "120040"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPUPS": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "05"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPPRE": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "00"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MN_IND": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "I"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJCPF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "63602940000170"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJMNT": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000000000000 "
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNSPCN": {
        "tipo_dado": "object",
        "valores_unicos": 31,
        "valores_nulos": 0,
        "amostra_valores": [
            "123456789012345",
            "234567890123456",
            "345678901234567",
            "456789012345678",
            "567890123456789",
            "678901234567890",
            "789012345678901",
            "890123456789012",
            "901234567890123",
            "012345678901234"
        ],
        "maior_caractere": 15,
        "menor_caractere": 15,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_COIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "4"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_NUIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 20,
        "valores_nulos": 0,
        "amostra_valores": [
            "44",
            "52",
            "27",
            "19",
            "46",
            "43",
            "47",
            "25",
            "35",
            "51"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_SEXO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "F",
            "M"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_RACACOR": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "03",
            "99"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MUNPCN": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "120040",
            "120050"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFNACIO": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "010"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CEPPCN": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "69900970",
            "69940970"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFDIF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MNDIF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTINIC": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230101",
            "20221201",
            "20231201",
            "20231101",
            "20240501",
            "20240601"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTFIM": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230331",
            "20230228",
            "20240229",
            "20240131",
            "20240731",
            "20240831"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPATEND": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "05"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPAPAC": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "1",
            "2"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MOTSAI": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "21"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_OBITO": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ENCERR": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PERMAN": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ALTA": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TRANSF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTOOCOR": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "        "
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CODEMI": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "E120000001"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CATEND": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "01"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_APACAN": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000000000000"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UNISOL": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "2001586"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTSOLIC": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230101",
            "20220101",
            "20231201",
            "20240101",
            "20240601"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTAUT": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230131",
            "20220131",
            "20231231",
            "20240131",
            "20240630"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDCAS": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "CO_CIDPRIM": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "CO_CIDSEC": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ETNIA": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "    "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_IMC": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "   "
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_PROCAIH": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0407010173"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_DTCIRUR": {
        "tipo_dado": "object",
        "valores_unicos": 18,
        "valores_nulos": 0,
        "amostra_valores": [
            "20221206",
            "20221212",
            "20221122",
            "20221108",
            "20221018",
            "20221129",
            "20221220",
            "20231114",
            "20231031",
            "20231107"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_NUMAIH": {
        "tipo_dado": "object",
        "valores_unicos": 31,
        "valores_nulos": 0,
        "amostra_valores": [
            "1222100594077",
            "1222100623722",
            "1222100547932",
            "1222100551617",
            "1222100557425",
            "1222100618629",
            "1222100547789",
            "1222100604461",
            "1222100547965",
            "1222100593901"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_PRCAIH2": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "          "
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_T_PRC2": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "   "
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_PRCAIH3": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "          "
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_T_PRC3": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "   "
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_PRCAIH4": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "          "
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_T_PRC4": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "   "
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_PRCAIH5": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "          "
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_T_PRC5": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "   "
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_PRCAIH6": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "          "
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_T_PRC6": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "   "
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_NUMAIH2": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "             "
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_DTCIRG2": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "        "
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_MESACOM": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "01",
            "12",
            "06"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_ANOACOM": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "2"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_PONTBAR": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            " "
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AB_TABBARR": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            " "
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_COMORB": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "N",
            "S"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CID_C1": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "I10 "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CID_C2": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "O243"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CID_C3": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "    "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CID_C4": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "    "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CID_C5": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "    "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CID_CO": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "    "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MEDICAM": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_POLIVIT": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ATV_FIS": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "1",
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_REG_PES": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ADESAO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "1",
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_NATJUR": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "1147"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    }
    },
        'sia_apac_confeccao_de_fistula': {
    "AP_MVM": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "202301",
            "202306",
            "202401",
            "202312",
            "202406"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CONDIC": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "EP",
            "PG"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_GESTAO": {
        "tipo_dado": "object",
        "valores_unicos": 26,
        "valores_nulos": 0,
        "amostra_valores": [
            "120000",
            "270030",
            "270430",
            "270860",
            "270630",
            "160000",
            "130000",
            "290000",
            "290070",
            "290320"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CODUNI": {
        "tipo_dado": "object",
        "valores_unicos": 60,
        "valores_nulos": 0,
        "amostra_valores": [
            "2001586",
            "2005417",
            "2006197",
            "2006960",
            "2006952",
            "2010151",
            "2006448",
            "9215328",
            "2010615",
            "2007037"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_AUTORIZ": {
        "tipo_dado": "object",
        "valores_unicos": 1759,
        "valores_nulos": 0,
        "amostra_valores": [
            "1223200028248",
            "1223200152944",
            "1224600001020",
            "2723204324368",
            "2723204324490",
            "2723204324500",
            "2723204324555",
            "2723204324687",
            "2723204324698",
            "2723204324709"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CMP": {
        "tipo_dado": "object",
        "valores_unicos": 13,
        "valores_nulos": 0,
        "amostra_valores": [
            "202301",
            "202306",
            "202401",
            "202212",
            "202305",
            "202312",
            "202311",
            "202406",
            "202405",
            "202303"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PRIPAL": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0418010030"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_VL_AP": {
        "tipo_dado": "object",
        "valores_unicos": 4,
        "valores_nulos": 0,
        "amostra_valores": [
            "              859.20",
            "                0.00",
            "              915.76",
            "             1459.20"
        ],
        "maior_caractere": 20,
        "menor_caractere": 20,
        "has_leading_zeros": False,
        "has_special_chars": True,
        "has_mixed_types": False
    },
    "AP_UFMUN": {
        "tipo_dado": "object",
        "valores_unicos": 37,
        "valores_nulos": 0,
        "amostra_valores": [
            "120040",
            "270030",
            "270430",
            "270860",
            "270630",
            "160030",
            "130260",
            "292860",
            "293050",
            "290070"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPUPS": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "05",
            "36",
            "07"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TIPPRE": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "00"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MN_IND": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "I",
            "M"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJCPF": {
        "tipo_dado": "object",
        "valores_unicos": 60,
        "valores_nulos": 0,
        "amostra_valores": [
            "63602940000170",
            "04710210000124",
            "24464109000229",
            "02476391000140",
            "69976629000178",
            "12737680000100",
            "12291290000159",
            "11941964000150",
            "04611279000109",
            "12307187000150"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJMNT": {
        "tipo_dado": "object",
        "valores_unicos": 9,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000000000000 ",
            "24464109000229",
            "23086176000103",
            "00697295000105",
            "14105183000114",
            "15180714000104",
            "13937131000141",
            "14349740000142",
            "13650403000128"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNSPCN": {
        "tipo_dado": "object",
        "valores_unicos": 1665,
        "valores_nulos": 0,
        "amostra_valores": [
            "{{{|~~",
            "{{{||",
            "{~{{{~{{",
            "{~{|}}{}|}",
            "{}{{}",
            "{{{}~{|",
            "{{~{~",
            "{~{{~}}~{",
            "{{{}}|~",
            "{{}{}"
        ],
        "maior_caractere": 15,
        "menor_caractere": 15,
        "has_leading_zeros": False,
        "has_special_chars": True,
        "has_mixed_types": False
    },
    "AP_COIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "4"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_NUIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 79,
        "valores_nulos": 0,
        "amostra_valores": [
            "51",
            "23",
            "53",
            "67",
            "47",
            "59",
            "40",
            "52",
            "50",
            "58"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_SEXO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "M",
            "F"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_RACACOR": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "99",
            "03",
            "01",
            "04",
            "02",
            "05"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MUNPCN": {
        "tipo_dado": "object",
        "valores_unicos": 356,
        "valores_nulos": 0,
        "amostra_valores": [
            "120040",
            "270030",
            "270680",
            "270410",
            "270670",
            "270430",
            "270850",
            "270230",
            "270644",
            "270200"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFNACIO": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "010",
            "092",
            "245",
            "170",
            "089",
            "179"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CEPPCN": {
        "tipo_dado": "object",
        "valores_unicos": 1144,
        "valores_nulos": 0,
        "amostra_valores": [
            "69901755",
            "69914320",
            "69918048",
            "57303720",
            "57210000",
            "57330000",
            "57200000",
            "57303817",
            "57312252",
            "57308870"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFDIF": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MNDIF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTINIC": {
        "tipo_dado": "object",
        "valores_unicos": 151,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230101",
            "20230606",
            "20240101",
            "20230117",
            "20230118",
            "20230102",
            "20230111",
            "20230114",
            "20230128",
            "20221223"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTFIM": {
        "tipo_dado": "object",
        "valores_unicos": 160,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230131",
            "20230630",
            "20240131",
            "20230117",
            "20230118",
            "20230102",
            "20230111",
            "20230114",
            "20230128",
            "20221223"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPATEN": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "12"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPAPAC": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "3"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MOTSAI": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "15",
            "18",
            "12",
            "26",
            "51",
            "11"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_OBITO": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ENCERR": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PERMAN": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ALTA": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "1",
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TRANSF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTOCOR": {
        "tipo_dado": "object",
        "valores_unicos": 157,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230112",
            "20230607",
            "20240103",
            "20230117",
            "20230118",
            "20230102",
            "20230111",
            "20230114",
            "20230128",
            "20221223"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CODEMI": {
        "tipo_dado": "object",
        "valores_unicos": 33,
        "valores_nulos": 0,
        "amostra_valores": [
            "E120000001",
            "M270030001",
            "M270430201",
            "M270430200",
            "M270860001",
            "M270630001",
            "M270130001",
            "E160000001",
            "E130000001",
            "E310000013"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CATEND": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "01",
            "02"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_APACANT": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000000000000",
            "000000000000 ",
            "0            "
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UNISOL": {
        "tipo_dado": "object",
        "valores_unicos": 58,
        "valores_nulos": 0,
        "amostra_valores": [
            "2001586",
            "2005417",
            "2006197",
            "2006960",
            "2006952",
            "2010151",
            "2006448",
            "9215328",
            "2010615",
            "0000000"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTSOLIC": {
        "tipo_dado": "object",
        "valores_unicos": 164,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230101",
            "20230606",
            "20240101",
            "20230117",
            "20230118",
            "20230102",
            "20230111",
            "20230114",
            "20230128",
            "20221223"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTAUT": {
        "tipo_dado": "object",
        "valores_unicos": 165,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230102",
            "20230607",
            "20240102",
            "20230117",
            "20230118",
            "20230111",
            "20230114",
            "20230128",
            "20230131",
            "20230105"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDCAS": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "N188",
            "0000",
            "N180",
            "N185",
            "N189"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AP_CIDPRI": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDSEC": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ETNIA": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "0240",
            "0003"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "ACF_DUPLEX": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "N",
            "S"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "ACF_USOCAT": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "S",
            "N"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "ACF_PREFAV": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "S",
            "N"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "ACF_FLEBIT": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "N",
            "S"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "ACF_HEMATO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "S",
            "N"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "ACF_VEIAVI": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "S",
            "N"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "ACF_PULSO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "S",
            "N"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "ACF_VEIDIA": {
        "tipo_dado": "object",
        "valores_unicos": 28,
        "valores_nulos": 0,
        "amostra_valores": [
            "2   ",
            "0   ",
            "5   ",
            "45  ",
            "41  ",
            "43  ",
            "0000",
            "42  ",
            "3   ",
            "46  "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "ACF_ARTDIA": {
        "tipo_dado": "object",
        "valores_unicos": 27,
        "valores_nulos": 0,
        "amostra_valores": [
            "3   ",
            "0   ",
            "4   ",
            "33  ",
            "35  ",
            "0000",
            "34  ",
            "32  ",
            "10  ",
            "36  "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "ACF_FREMIT": {
        "tipo_dado": "object",
        "valores_unicos": 4,
        "valores_nulos": 0,
        "amostra_valores": [
            "4",
            "3",
            "2",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_NATJUR": {
        "tipo_dado": "object",
        "valores_unicos": 9,
        "valores_nulos": 0,
        "amostra_valores": [
            "1147",
            "2062",
            "1104",
            "3999",
            "3069",
            "1023",
            "2011",
            "2240",
            "1244"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    }
    },     
        'sia_apac_laudos_diversos': {
    "AP_MVM": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "202301",
            "202306",
            "202312",
            "202401",
            "202406"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CONDIC": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "EP",
            "PG"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_GESTAO": {
        "tipo_dado": "object",
        "valores_unicos": 37,
        "valores_nulos": 0,
        "amostra_valores": [
            "120000",
            "270030",
            "270430",
            "270670",
            "270000",
            "270860",
            "270630",
            "270800",
            "270230",
            "270240"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CODUNI": {
        "tipo_dado": "object",
        "valores_unicos": 195,
        "valores_nulos": 0,
        "amostra_valores": [
            "2002078",
            "2001586",
            "0128619",
            "5625645",
            "2005417",
            "5222931",
            "2006928",
            "0026042",
            "2004984",
            "2007037"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_AUTORIZ": {
        "tipo_dado": "object",
        "valores_unicos": 21673,
        "valores_nulos": 0,
        "amostra_valores": [
            "1223200010087",
            "1223200029590",
            "1223200012507",
            "1223200011737",
            "1223200045782",
            "1223200045793",
            "1223200029557",
            "1223200012397",
            "1223200013255",
            "1223200013486"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CMP": {
        "tipo_dado": "object",
        "valores_unicos": 17,
        "valores_nulos": 0,
        "amostra_valores": [
            "202301",
            "202306",
            "202312",
            "202401",
            "202404",
            "202406",
            "202212",
            "202211",
            "202305",
            "202304"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PRIPAL": {
        "tipo_dado": "object",
        "valores_unicos": 110,
        "valores_nulos": 0,
        "amostra_valores": [
            "0309030129",
            "0211070092",
            "0301070032",
            "0211020010",
            "0701030135",
            "0701030143",
            "0211070319",
            "0211070106",
            "0211070297",
            "0409040240"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_VL_AP": {
        "tipo_dado": "object",
        "valores_unicos": 230,
        "valores_nulos": 0,
        "amostra_valores": [
            "              688.00",
            "               24.75",
            "               21.68",
            "              730.04",
            "             1400.00",
            "             2200.00",
            "             1100.00",
            "                8.75",
            "               46.56",
            "               22.55"
        ],
        "maior_caractere": 20,
        "menor_caractere": 20,
        "has_leading_zeros": False,
        "has_special_chars": True,
        "has_mixed_types": False
    },
    "AP_UFMUN": {
        "tipo_dado": "object",
        "valores_unicos": 47,
        "valores_nulos": 0,
        "amostra_valores": [
            "120040",
            "270030",
            "270430",
            "270670",
            "270860",
            "270630",
            "270800",
            "270230",
            "270240",
            "160030"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPUPS": {
        "tipo_dado": "object",
        "valores_unicos": 7,
        "valores_nulos": 0,
        "amostra_valores": [
            "05",
            "36",
            "07",
            "62",
            "83",
            "04",
            "15"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TIPPRE": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "00"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MN_IND": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "M",
            "I"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJCPF": {
        "tipo_dado": "object",
        "valores_unicos": 189,
        "valores_nulos": 0,
        "amostra_valores": [
            "00529443000336",
            "63602940000170",
            "04034526003592",
            "08145392000199",
            "04710210000124",
            "18216973000128",
            "08427999000161",
            "10889442000194",
            "05648824000196",
            "12307187000150"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJMNT": {
        "tipo_dado": "object",
        "valores_unicos": 23,
        "valores_nulos": 0,
        "amostra_valores": [
            "00529443000174",
            "0000000000000 ",
            "04034526000143",
            "12517793000108",
            "24464109000229",
            "12250916000189",
            "12224895000127",
            "12200259000165",
            "00204125000133",
            "23086176000103"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNSPCN": {
        "tipo_dado": "object",
        "valores_unicos": 19105,
        "valores_nulos": 0,
        "amostra_valores": [
            "{|{~{|~}",
            "{}{|{",
            "{{|}{~~}",
            "{{|}|}~",
            "{{}{~",
            "{{{~~~",
            "{{{~{}",
            "{}{{{|{}{",
            "{}{}{{~|",
            "{{{|{}}|"
        ],
        "maior_caractere": 15,
        "menor_caractere": 15,
        "has_leading_zeros": False,
        "has_special_chars": True,
        "has_mixed_types": False
    },
    "AP_COIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "4",
            "3",
            "5",
            "2",
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_NUIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 99,
        "valores_nulos": 0,
        "amostra_valores": [
            "67",
            "45",
            "35",
            "38",
            "72",
            "52",
            "81",
            "01",
            "66",
            "49"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_SEXO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "F",
            "M"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_RACACOR": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "03",
            "01",
            "04",
            "99",
            "02",
            "05"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MUNPCN": {
        "tipo_dado": "object",
        "valores_unicos": 621,
        "valores_nulos": 0,
        "amostra_valores": [
            "120020",
            "120040",
            "120001",
            "120043",
            "120070",
            "130350",
            "120045",
            "120060",
            "120050",
            "120030"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFNACIO": {
        "tipo_dado": "object",
        "valores_unicos": 7,
        "valores_nulos": 0,
        "amostra_valores": [
            "010",
            "022",
            "270",
            "037",
            "038",
            "092",
            "023"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CEPPCN": {
        "tipo_dado": "object",
        "valores_unicos": 7447,
        "valores_nulos": 0,
        "amostra_valores": [
            "69980000",
            "69915783",
            "69919602",
            "69945000",
            "69900901",
            "69909804",
            "69918758",
            "69902708",
            "69903030",
            "69911842"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFDIF": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MNDIF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTINIC": {
        "tipo_dado": "object",
        "valores_unicos": 344,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230101",
            "20221201",
            "20230601",
            "20230501",
            "20230606",
            "20231201",
            "20231101",
            "20240101",
            "20240401",
            "20240601"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTFIM": {
        "tipo_dado": "object",
        "valores_unicos": 400,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230131",
            "20230101",
            "20230228",
            "20230630",
            "20230601",
            "20230831",
            "20231231",
            "20231201",
            "20240131",
            "20240101"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPATEN": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "00"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPAPAC": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "3",
            "1",
            "2"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MOTSAI": {
        "tipo_dado": "object",
        "valores_unicos": 13,
        "valores_nulos": 0,
        "amostra_valores": [
            "15",
            "12",
            "21",
            "11",
            "51",
            "18",
            "26",
            "14",
            "31",
            "28"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_OBITO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ENCERR": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PERMAN": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ALTA": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "1",
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TRANSF": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTOCOR": {
        "tipo_dado": "object",
        "valores_unicos": 366,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230126",
            "20230124",
            "20230118",
            "20230109",
            "20230101",
            "20230130",
            "20230112",
            "20230117",
            "20230125",
            "20230127"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CODEMI": {
        "tipo_dado": "object",
        "valores_unicos": 62,
        "valores_nulos": 0,
        "amostra_valores": [
            "E120000001",
            "E120000004",
            "E150000001",
            "M270030001",
            "M270430201",
            "M270670001",
            "M270010003",
            "M270430200",
            "M270430001",
            "S273439208"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CATEND": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "01",
            "02",
            "03",
            "06",
            "04"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_APACANT": {
        "tipo_dado": "object",
        "valores_unicos": 751,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000000000000",
            "1224200172888",
            "2723203603813",
            "0            ",
            "2723203626869",
            "2723203611381",
            "2723203604121",
            "2723203604132",
            "2723203603780",
            "2723203626836"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UNISOL": {
        "tipo_dado": "object",
        "valores_unicos": 186,
        "valores_nulos": 0,
        "amostra_valores": [
            "2001586",
            "2002078",
            "0000000",
            "0128619",
            "5625645",
            "5222931",
            "2006928",
            "0026042",
            "2007037",
            "2786346"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTSOLIC": {
        "tipo_dado": "object",
        "valores_unicos": 643,
        "valores_nulos": 0,
        "amostra_valores": [
            "20221109",
            "20230111",
            "20230109",
            "20221123",
            "20221122",
            "20230116",
            "20230112",
            "20230106",
            "20230110",
            "20230103"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTAUT": {
        "tipo_dado": "object",
        "valores_unicos": 499,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230120",
            "20230131",
            "20221126",
            "20221122",
            "20221116",
            "20221219",
            "20230103",
            "20221231",
            "20230110",
            "20231231"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDCAS": {
        "tipo_dado": "object",
        "valores_unicos": 26,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000",
            "N188",
            "G800",
            "M139",
            "I694",
            "G819",
            "F03 ",
            "G128",
            "N180",
            "L89 "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AP_CIDPRI": {
        "tipo_dado": "object",
        "valores_unicos": 215,
        "valores_nulos": 0,
        "amostra_valores": [
            "N200",
            "H918",
            "I200",
            "Z302",
            "Z944",
            "Z940",
            "H913",
            "Z048",
            "Q386",
            "R098"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDSEC": {
        "tipo_dado": "object",
        "valores_unicos": 35,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000",
            "B180",
            "K74 ",
            "B182",
            "B181",
            "N188",
            "N180",
            "N189",
            "I120",
            "E142"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AP_ETNIA": {
        "tipo_dado": "object",
        "valores_unicos": 12,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "0111",
            "0200",
            "0210",
            "0121",
            "0155",
            "0158",
            "0150",
            "0145",
            "0192"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AP_NATJUR": {
        "tipo_dado": "object",
        "valores_unicos": 12,
        "valores_nulos": 0,
        "amostra_valores": [
            "3999",
            "1147",
            "1023",
            "2062",
            "3069",
            "2240",
            "1112",
            "1104",
            "1244",
            "1031"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    }
    },
        'sia_apac_medicamentos': {
    "AP_MVM": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "202301",
            "202306",
            "202312",
            "202401",
            "202406"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CONDIC": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "EP"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_GESTAO": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "120000",
            "270000",
            "160000",
            "130000",
            "290000"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CODUNI": {
        "tipo_dado": "object",
        "valores_unicos": 49,
        "valores_nulos": 0,
        "amostra_valores": [
            "3542734",
            "7334710",
            "2719991",
            "6911927",
            "2020904",
            "9734872",
            "2498049",
            "0007609",
            "2816237",
            "2824655"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_AUTORIZ": {
        "tipo_dado": "object",
        "valores_unicos": 24286,
        "valores_nulos": 0,
        "amostra_valores": [
            "1222200198296",
            "1222200196646",
            "1222200172996",
            "1222200177286",
            "1222200196811",
            "1222200192500",
            "1222200215577",
            "1222200193357",
            "1222200198175",
            "1222200124354"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CMP": {
        "tipo_dado": "object",
        "valores_unicos": 17,
        "valores_nulos": 0,
        "amostra_valores": [
            "202210",
            "202303",
            "202306",
            "202305",
            "202304",
            "202312",
            "202401",
            "202406",
            "202301",
            "202309"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PRIPAL": {
        "tipo_dado": "object",
        "valores_unicos": 220,
        "valores_nulos": 0,
        "amostra_valores": [
            "0604530013",
            "0604320043",
            "0604340036",
            "0604590024",
            "0604610017",
            "0604230028",
            "0604110030",
            "0604230044",
            "0604690029",
            "0604690010"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_VL_AP": {
        "tipo_dado": "object",
        "valores_unicos": 163,
        "valores_nulos": 0,
        "amostra_valores": [
            "        0.00",
            "      256.58",
            "       10.20",
            "      771.60",
            "       36.00",
            "       50.40",
            "        6.00",
            "      367.55",
            "        3.30",
            "      513.16"
        ],
        "maior_caractere": 12,
        "menor_caractere": 12,
        "has_leading_zeros": False,
        "has_special_chars": True,
        "has_mixed_types": False
    },
    "AP_UFMUN": {
        "tipo_dado": "object",
        "valores_unicos": 36,
        "valores_nulos": 0,
        "amostra_valores": [
            "120040",
            "120020",
            "270430",
            "160030",
            "130260",
            "292740",
            "291170",
            "291480",
            "291360",
            "293330"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPUPS": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "43",
            "69",
            "36",
            "68",
            "05",
            "07"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TIPPRE": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "00"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MN_IND": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "M",
            "I"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJCPF": {
        "tipo_dado": "object",
        "valores_unicos": 17,
        "valores_nulos": 0,
        "amostra_valores": [
            "04034526000143",
            "12200259000165",
            "23086176000103",
            "01762561000190",
            "00697295000105",
            "13937131006344",
            "13937131000141",
            "15180714000104",
            "13937131001547",
            "13937131003833"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJMNT": {
        "tipo_dado": "object",
        "valores_unicos": 8,
        "valores_nulos": 0,
        "amostra_valores": [
            "04034526000143",
            "12200259000165",
            "23086176000103",
            "0000000000000 ",
            "00697295000105",
            "13937131000141",
            "15180714000104",
            "34306340000167"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNSPCN": {
        "tipo_dado": "object",
        "valores_unicos": 19984,
        "valores_nulos": 0,
        "amostra_valores": [
            "{}{}|}{",
            "{{}{{",
            "{}{{{~}~|",
            "{|{}}{}~}",
            "{}{|}|",
            "{{}|}~|",
            "{{{{|{~|{",
            "{{{~~~|",
            "{{{}}}",
            "{{{{|}"
        ],
        "maior_caractere": 15,
        "menor_caractere": 15,
        "has_leading_zeros": False,
        "has_special_chars": True,
        "has_mixed_types": False
    },
    "AP_COIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "4",
            "3",
            "5"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_NUIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 100,
        "valores_nulos": 0,
        "amostra_valores": [
            "20",
            "61",
            "15",
            "07",
            "22",
            "10",
            "21",
            "37",
            "17",
            "52"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_SEXO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "F",
            "M"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_RACACOR": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "03",
            "01",
            "99",
            "04",
            "05",
            "02"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MUNPCN": {
        "tipo_dado": "object",
        "valores_unicos": 675,
        "valores_nulos": 0,
        "amostra_valores": [
            "120040",
            "120020",
            "120050",
            "120010",
            "120060",
            "120005",
            "120038",
            "120013",
            "130070",
            "120034"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFNACIO": {
        "tipo_dado": "object",
        "valores_unicos": 10,
        "valores_nulos": 0,
        "amostra_valores": [
            "010",
            "045",
            "089",
            "092",
            "062",
            "264",
            "030",
            "022",
            "026",
            "10 "
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CEPPCN": {
        "tipo_dado": "object",
        "valores_unicos": 8021,
        "valores_nulos": 0,
        "amostra_valores": [
            "69906336",
            "69919799",
            "69980000",
            "69903364",
            "69911762",
            "69909358",
            "69901710",
            "69900478",
            "69911426",
            "69940000"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFDIF": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0 ",
            "1 "
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MNDIF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "1 "
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTINIC": {
        "tipo_dado": "object",
        "valores_unicos": 403,
        "valores_nulos": 0,
        "amostra_valores": [
            "20221021",
            "20221018",
            "20220902",
            "20221001",
            "20220930",
            "20221027",
            "20221004",
            "20220915",
            "20221019",
            "20221003"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTFIM": {
        "tipo_dado": "object",
        "valores_unicos": 52,
        "valores_nulos": 0,
        "amostra_valores": [
            "20221231",
            "20221130",
            "20221031",
            "20230430",
            "20230630",
            "20230331",
            "20230531",
            "20230831",
            "20230731",
            "20240131"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPATEN": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "06"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPAPAC": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "1",
            "2"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MOTSAI": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "21",
            "51",
            "42",
            "28",
            "31",
            "12"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_OBITO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ENCERR": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PERMAN": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "1",
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ALTA": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TRANSF": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTOCOR": {
        "tipo_dado": "object",
        "valores_unicos": 66,
        "valores_nulos": 0,
        "amostra_valores": [
            "        ",
            "20221027",
            "20221005",
            "20221024",
            "20230410",
            "20230419",
            "20230425",
            "20231201",
            "20231227",
            "20231222"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AP_CODEMI": {
        "tipo_dado": "object",
        "valores_unicos": 23,
        "valores_nulos": 0,
        "amostra_valores": [
            "E120000001",
            "E130000001",
            "E110000001",
            "E270000001",
            "E170000001",
            "E410000001",
            "E330000001",
            "E430000001",
            "E260000001",
            "E350000001"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CATEND": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "01"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_APACANT": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000000000000",
            "000000000000 "
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UNISOL": {
        "tipo_dado": "object",
        "valores_unicos": 598,
        "valores_nulos": 0,
        "amostra_valores": [
            "2001586",
            "5336171",
            "9708642",
            "2000857",
            "0650315",
            "9246010",
            "9937900",
            "2001527",
            "3733211",
            "2237253"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTSOLIC": {
        "tipo_dado": "object",
        "valores_unicos": 650,
        "valores_nulos": 0,
        "amostra_valores": [
            "20221021",
            "20221017",
            "20220831",
            "20220919",
            "20221005",
            "20220901",
            "20221027",
            "20220922",
            "20220621",
            "20221004"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTAUT": {
        "tipo_dado": "object",
        "valores_unicos": 540,
        "valores_nulos": 0,
        "amostra_valores": [
            "20221021",
            "20221018",
            "20220902",
            "20220919",
            "20220930",
            "20221027",
            "20221004",
            "20220713",
            "20220927",
            "20220915"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDCAS": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDPRI": {
        "tipo_dado": "object",
        "valores_unicos": 255,
        "valores_nulos": 0,
        "amostra_valores": [
            "M328",
            "M068",
            "N040",
            "L700",
            "E230",
            "F205",
            "E228",
            "F200",
            "M070",
            "N180"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDSEC": {
        "tipo_dado": "object",
        "valores_unicos": 36,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000",
            "J440",
            "G408",
            "F001",
            "L701",
            "G403",
            "K500",
            "F315",
            "L408",
            "F250"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AP_ETNIA": {
        "tipo_dado": "object",
        "valores_unicos": 12,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "0193",
            "0011",
            "0109",
            "0111",
            "X307",
            "0209",
            "X314",
            "0252",
            "0235"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AM_PESO": {
        "tipo_dado": "object",
        "valores_unicos": 573,
        "valores_nulos": 0,
        "amostra_valores": [
            "060",
            "061",
            "055",
            "057",
            "025",
            "072",
            "041",
            "065",
            "090",
            "045"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AM_ALTURA": {
        "tipo_dado": "object",
        "valores_unicos": 168,
        "valores_nulos": 0,
        "amostra_valores": [
            "157",
            "147",
            "172",
            "170",
            "114",
            "176",
            "166",
            "162",
            "155",
            "163"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AM_TRANSPL": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "N",
            "S"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AM_QTDTRAN": {
        "tipo_dado": "object",
        "valores_unicos": 4,
        "valores_nulos": 0,
        "amostra_valores": [
            "00",
            "01",
            "02",
            "1 "
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AM_GESTANT": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "N",
            "S"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_NATJUR": {
        "tipo_dado": "object",
        "valores_unicos": 4,
        "valores_nulos": 0,
        "amostra_valores": [
            "1023",
            "1112",
            "1104",
            "1147"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    }
    },
        'sia_apac_acompanhamento_multiprofissional': {
    "AP_MVM": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "202301",
            "202306",
            "202312",
            "202401",
            "202406"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CONDIC": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "EP",
            "PG"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_GESTAO": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "290000",
            "291750",
            "290570",
            "293010",
            "290320"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CODUNI": {
        "tipo_dado": "object",
        "valores_unicos": 12,
        "valores_nulos": 0,
        "amostra_valores": [
            "9358722",
            "7642407",
            "7833415",
            "6794009",
            "9786422",
            "0148792",
            "2802147",
            "6142702",
            "2517728",
            "9141820"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_AUTORIZ": {
        "tipo_dado": "object",
        "valores_unicos": 823,
        "valores_nulos": 0,
        "amostra_valores": [
            "2923203930702",
            "2923202353610",
            "2923202353654",
            "2923202353709",
            "2923202353797",
            "2923202353940",
            "2923202352642",
            "2923202354116",
            "2923202354160",
            "2923202354237"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CMP": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "202301",
            "202306",
            "202312",
            "202401",
            "202406",
            "202404"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PRIPAL": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0301130051",
            "0301130060"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_VL_AP": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "                0.00",
            "               61.00"
        ],
        "maior_caractere": 20,
        "menor_caractere": 20,
        "has_leading_zeros": False,
        "has_special_chars": True,
        "has_mixed_types": False
    },
    "AP_UFMUN": {
        "tipo_dado": "object",
        "valores_unicos": 11,
        "valores_nulos": 0,
        "amostra_valores": [
            "291070",
            "291460",
            "291470",
            "291750",
            "292860",
            "292880",
            "290570",
            "293010",
            "290320",
            "292740"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPUPS": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "36",
            "07"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TIPPRE": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "00"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MN_IND": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "I",
            "M"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJCPF": {
        "tipo_dado": "object",
        "valores_unicos": 12,
        "valores_nulos": 0,
        "amostra_valores": [
            "22647445000109",
            "14022332000181",
            "19575404000131",
            "22845495000192",
            "18319513000125",
            "35061220000100",
            "35557438000150",
            "09389146000145",
            "01954785000102",
            "23349388000136"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJMNT": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000000000000 ",
            "14105183000114"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNSPCN": {
        "tipo_dado": "object",
        "valores_unicos": 563,
        "valores_nulos": 0,
        "amostra_valores": [
            "{{{~{",
            "{{{~~~",
            "{}|{}~}",
            "{{}",
            "{{{|}{{",
            "{~{~}|",
            "{{{}",
            "{}{~|{{~",
            "{{|~{",
            "{}{}|{{"
        ],
        "maior_caractere": 15,
        "menor_caractere": 15,
        "has_leading_zeros": False,
        "has_special_chars": True,
        "has_mixed_types": False
    },
    "AP_COIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "4",
            "5"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_NUIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 79,
        "valores_nulos": 0,
        "amostra_valores": [
            "89",
            "72",
            "54",
            "74",
            "73",
            "46",
            "58",
            "75",
            "78",
            "61"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_SEXO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "M",
            "F"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_RACACOR": {
        "tipo_dado": "object",
        "valores_unicos": 4,
        "valores_nulos": 0,
        "amostra_valores": [
            "03",
            "01",
            "02",
            "04"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MUNPCN": {
        "tipo_dado": "object",
        "valores_unicos": 88,
        "valores_nulos": 0,
        "amostra_valores": [
            "291070",
            "290620",
            "292560",
            "293360",
            "290115",
            "291835",
            "292350",
            "291470",
            "292150",
            "291240"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFNACIO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "010",
            "092"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CEPPCN": {
        "tipo_dado": "object",
        "valores_unicos": 198,
        "valores_nulos": 0,
        "amostra_valores": [
            "48500000",
            "44890000",
            "44935000",
            "47400000",
            "44910000",
            "44920000",
            "46930000",
            "46880000",
            "48800000",
            "44970000"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFDIF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MNDIF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTINIC": {
        "tipo_dado": "object",
        "valores_unicos": 97,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230101",
            "20221201",
            "20221220",
            "20230109",
            "20230104",
            "20230118",
            "20230124",
            "20230103",
            "20230113",
            "20230601"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTFIM": {
        "tipo_dado": "object",
        "valores_unicos": 12,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230331",
            "20230228",
            "20230831",
            "20230630",
            "20240229",
            "20231231",
            "20240131",
            "20240331",
            "20240831",
            "20240630"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPATEN": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "11"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPAPAC": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "3",
            "2",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MOTSAI": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "15",
            "21",
            "26",
            "18",
            "11"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_OBITO": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ENCERR": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PERMAN": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ALTA": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "1",
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TRANSF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTOCOR": {
        "tipo_dado": "object",
        "valores_unicos": 61,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230101",
            "        ",
            "20230109",
            "20230104",
            "20230118",
            "20230124",
            "20230103",
            "20230113",
            "20230601",
            "20230607"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AP_CODEMI": {
        "tipo_dado": "object",
        "valores_unicos": 9,
        "valores_nulos": 0,
        "amostra_valores": [
            "M291070101",
            "E290000001",
            "M430770001",
            "E310000013",
            "M292740800",
            "M290570001",
            "M293010501",
            "M290320100",
            "M292740801"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CATEND": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "01"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_APACANT": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000000000000"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UNISOL": {
        "tipo_dado": "object",
        "valores_unicos": 12,
        "valores_nulos": 0,
        "amostra_valores": [
            "9358722",
            "7642407",
            "7833415",
            "6794009",
            "9786422",
            "0148792",
            "2802147",
            "6142702",
            "2517728",
            "9141820"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTSOLIC": {
        "tipo_dado": "object",
        "valores_unicos": 98,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230101",
            "20221201",
            "20221220",
            "20230109",
            "20230104",
            "20230118",
            "20230124",
            "20230103",
            "20230113",
            "20230601"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTAUT": {
        "tipo_dado": "object",
        "valores_unicos": 99,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230101",
            "20230119",
            "20230118",
            "20221201",
            "20221220",
            "20230109",
            "20230203",
            "20230124",
            "20230601",
            "20230607"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDCAS": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000",
            "N180",
            "N189"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AP_CIDPRI": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDSEC": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ETNIA": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "    "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_CARACT": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "2"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_DTINI": {
        "tipo_dado": "object",
        "valores_unicos": 180,
        "valores_nulos": 0,
        "amostra_valores": [
            "NN000000",
            "NN145058",
            "SN173115",
            "NN166063",
            "NN000011",
            "NN185093",
            "NN169078",
            "NN172070",
            "NN168056",
            "NN163065"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_DTCLI": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "N       ",
            "S       "
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_ACEVAS": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1",
            "2"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_MAISNE": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_SITINI": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "N",
            "R"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_SITTRA": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "R",
            "N",
            " "
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_SEAPTO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "N",
            "S"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_HB": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "N   "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_FOSFOR": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "N   ",
            "S   "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_KTVSEM": {
        "tipo_dado": "object",
        "valores_unicos": 9,
        "valores_nulos": 0,
        "amostra_valores": [
            "1   ",
            "8   ",
            "0   ",
            "9   ",
            "4   ",
            "6   ",
            "7   ",
            "5   ",
            "3   "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_TRU": {
        "tipo_dado": "object",
        "valores_unicos": 10,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000",
            "5   ",
            "4   ",
            "6   ",
            "2   ",
            "3   ",
            "8   ",
            "7   ",
            "10  ",
            "0   "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_ALBUMI": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000",
            "4   ",
            "5   ",
            "3   ",
            "0   "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_PTH": {
        "tipo_dado": "object",
        "valores_unicos": 178,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000",
            "31  ",
            "86  ",
            "322 ",
            "127 ",
            "73  ",
            "138 ",
            "54  ",
            "51  ",
            "30  "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_HIV": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "N",
            "P"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_HCV": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "N",
            "P"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_HBSAG": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "N"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_INTERC": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "N",
            "S"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AMP_SEPERI": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "S",
            "N"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_NATJUR": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "    "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    }
    },
        'sia_apac_nefrologia':     {
        "AP_MVM": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "201409"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CONDIC": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "EP"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_GESTAO": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "350000"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CODUNI": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "2078775",
                "2078015",
                "2079798"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_AUTORIZ": {
            "tipo_dado": "object",
            "valores_unicos": 12,
            "valores_nulos": 0,
            "amostra_valores": [
                "3514204521113",
                "3514202631291",
                "3514202631302",
                "3514202631280",
                "3514222053342",
                "3514222053232",
                "3514222053298",
                "3514222259834",
                "3514204523522",
                "3514202631313"
            ],
            "maior_caractere": 13,
            "menor_caractere": 13,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CMP": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "201407",
                "201406"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_PRIPAL": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "0305010166",
                "0305010026",
                "0305010107"
            ],
            "maior_caractere": 10,
            "menor_caractere": 10,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_VL_AP": {
            "tipo_dado": "object",
            "valores_unicos": 10,
            "valores_nulos": 0,
            "amostra_valores": [
                "             2192.07",
                "             1458.81",
                "             3407.53",
                "             2725.74",
                "             2700.87",
                "             2149.62",
                "              358.06",
                "             2369.68",
                "             3409.06",
                "             2327.39"
            ],
            "maior_caractere": 20,
            "menor_caractere": 20,
            "has_leading_zeros": False,
            "has_special_chars": True,
            "has_mixed_types": False
        },
        "AP_UFMUN": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "350280",
                "355030",
                "350950"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_TPUPS": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "05"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_TIPPRE": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "61",
                "40"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_MN_IND": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "I",
                "M"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CNPJCPF": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "43751502000167",
                "56577059000100",
                "46068425000133"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CNPJMNT": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "              ",
                "56577059000100",
                "46068425000133"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_CNSPCN": {
            "tipo_dado": "object",
            "valores_unicos": 12,
            "valores_nulos": 0,
            "amostra_valores": [
                "{{|{|{{}{|{",
                "{{}}{",
                "{|~~~|}",
                "{|~~}}~|",
                "|{}{{{}",
                "{{{{{~||",
                "{{~{{|",
                "{{|}}",
                "{{}|~|",
                "{{}}|~{}"
            ],
            "maior_caractere": 15,
            "menor_caractere": 15,
            "has_leading_zeros": False,
            "has_special_chars": True,
            "has_mixed_types": False
        },
        "AP_COIDADE": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "4"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_NUIDADE": {
            "tipo_dado": "object",
            "valores_unicos": 11,
            "valores_nulos": 0,
            "amostra_valores": [
                "69",
                "04",
                "16",
                "13",
                "67",
                "34",
                "53",
                "62",
                "75",
                "01"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_SEXO": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "M",
                "F"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_RACACOR": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "01",
                "03"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_MUNPCN": {
            "tipo_dado": "object",
            "valores_unicos": 7,
            "valores_nulos": 0,
            "amostra_valores": [
                "350280",
                "352310",
                "355030",
                "354390",
                "350950",
                "350760",
                "351907"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_UFNACIO": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "010"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CEPPCN": {
            "tipo_dado": "object",
            "valores_unicos": 12,
            "valores_nulos": 0,
            "amostra_valores": [
                "16052120",
                "08590710",
                "04828060",
                "02467060",
                "13506720",
                "13506121",
                "13075320",
                "12900120",
                "16025330",
                "02710080"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_UFDIF": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "0"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_MNDIF": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "1"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTINIC": {
            "tipo_dado": "object",
            "valores_unicos": 5,
            "valores_nulos": 0,
            "amostra_valores": [
                "20140501",
                "20140502",
                "20140510",
                "20140401",
                "20140526"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTFIM": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "20140731",
                "20140630"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_TPATEN": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "02"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_TPAPAC": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "2"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_MOTSAI": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "21",
                "41"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_OBITO": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "0",
                "1"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_ENCERR": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "0"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_PERMAN": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "1",
                "0"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_ALTA": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "0"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_TRANSF": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "0"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTOCOR": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "        ",
                "20140703"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_CODEMI": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "E350000006",
                "S352078015",
                "S352079798"
            ],
            "maior_caractere": 10,
            "menor_caractere": 10,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CATEND": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "01",
                "02"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_APACANT": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000000000000",
                "             "
            ],
            "maior_caractere": 13,
            "menor_caractere": 13,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_UNISOL": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "2078775",
                "2078015",
                "2079798"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTSOLIC": {
            "tipo_dado": "object",
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "20140501",
                "20140510",
                "20140401",
                "20140526"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTAUT": {
            "tipo_dado": "object",
            "valores_unicos": 5,
            "valores_nulos": 0,
            "amostra_valores": [
                "20140501",
                "20140730",
                "20140510",
                "20140710",
                "20140526"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CIDCAS": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "N180",
                "0000"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_CIDPRI": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "N180"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CIDSEC": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "N189",
                "0000",
                "N180"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_ETNIA": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "    "
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_DTPDR": {
            "tipo_dado": "object",
            "valores_unicos": 12,
            "valores_nulos": 0,
            "amostra_valores": [
                "20130618",
                "20120508",
                "20081207",
                "20121112",
                "20111005",
                "20120101",
                "20130608",
                "20140501",
                "20120824",
                "20130201"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_ALTURA": {
            "tipo_dado": "object",
            "valores_unicos": 7,
            "valores_nulos": 0,
            "amostra_valores": [
                "160",
                "000",
                "175",
                "007",
                "017",
                "165",
                "016"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_PESO": {
            "tipo_dado": "object",
            "valores_unicos": 5,
            "valores_nulos": 0,
            "amostra_valores": [
                "084",
                "000",
                "070",
                "175",
                "060"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_DIURES": {
            "tipo_dado": "object",
            "valores_unicos": 6,
            "valores_nulos": 0,
            "amostra_valores": [
                "0200",
                "0999",
                "0150",
                "0100",
                "0000",
                "0619"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_GLICOS": {
            "tipo_dado": "object",
            "valores_unicos": 7,
            "valores_nulos": 0,
            "amostra_valores": [
                "0102",
                "0000",
                "0084",
                "0071",
                "0098",
                "0083",
                "0114"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_ACEVAS": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "N",
                "S"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_ULSOAB": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "S",
                "N"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_TRU": {
            "tipo_dado": "object",
            "valores_unicos": 8,
            "valores_nulos": 0,
            "amostra_valores": [
                "0062",
                "0136",
                "0026",
                "0029",
                "0000",
                "0058",
                "0127",
                "0107"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_INTFIS": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "00",
                "01"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_CNCDO": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "N"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_ALBUMI": {
            "tipo_dado": "object",
            "valores_unicos": 8,
            "valores_nulos": 0,
            "amostra_valores": [
                "02",
                "04",
                "03",
                "27",
                "15",
                "44",
                "21",
                "28"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_HCV": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "N"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_HBSAG": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "N"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_HIV": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "N",
                "P"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AN_HB": {
            "tipo_dado": "object",
            "valores_unicos": 8,
            "valores_nulos": 0,
            "amostra_valores": [
                "10",
                "14",
                "09",
                "00",
                "07",
                "11",
                "04",
                "08"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        }
    },
        'sia_apac_quimioterapia': {
    "AP_MVM": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "202301",
            "202306",
            "202312",
            "202401",
            "202406"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CONDIC": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "EP",
            "PG"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_GESTAO": {
        "tipo_dado": "object",
        "valores_unicos": 12,
        "valores_nulos": 0,
        "amostra_valores": [
            "120000",
            "270430",
            "270030",
            "160000",
            "130000",
            "290000",
            "291480",
            "292740",
            "291080",
            "293330"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CODUNI": {
        "tipo_dado": "object",
        "valores_unicos": 23,
        "valores_nulos": 0,
        "amostra_valores": [
            "2001586",
            "2006197",
            "2007037",
            "2005417",
            "2006448",
            "2020645",
            "2012677",
            "0003921",
            "2772280",
            "0003786"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_AUTORIZ": {
        "tipo_dado": "object",
        "valores_unicos": 19354,
        "valores_nulos": 0,
        "amostra_valores": [
            "1222200247400",
            "1223200028402",
            "1222200247752",
            "1222200249138",
            "1222200229900",
            "1222200247620",
            "1222200247940",
            "1223200014883",
            "1223200028446",
            "1223200013960"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CMP": {
        "tipo_dado": "object",
        "valores_unicos": 17,
        "valores_nulos": 0,
        "amostra_valores": [
            "202301",
            "202212",
            "202306",
            "202305",
            "202304",
            "202312",
            "202310",
            "202311",
            "202401",
            "202406"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PRIPAL": {
        "tipo_dado": "object",
        "valores_unicos": 135,
        "valores_nulos": 0,
        "amostra_valores": [
            "0304040029",
            "0304020184",
            "0304020079",
            "0304050113",
            "0304020320",
            "0304020214",
            "0304020389",
            "0304050040",
            "0304020010",
            "0304020290"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_VL_AP": {
        "tipo_dado": "object",
        "valores_unicos": 82,
        "valores_nulos": 0,
        "amostra_valores": [
            "     1400.00",
            "      571.50",
            "      301.50",
            "       79.75",
            "      800.00",
            "     1100.00",
            "     2224.00",
            "     1700.00",
            "      483.60",
            "       17.00"
        ],
        "maior_caractere": 12,
        "menor_caractere": 12,
        "has_leading_zeros": False,
        "has_special_chars": True,
        "has_mixed_types": False
    },
    "AP_UFMUN": {
        "tipo_dado": "object",
        "valores_unicos": 13,
        "valores_nulos": 0,
        "amostra_valores": [
            "120040",
            "270430",
            "270030",
            "160030",
            "130260",
            "292740",
            "291480",
            "291080",
            "293330",
            "291840"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPUPS": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "05",
            "07",
            "36"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TIPPRE": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "00"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MN_IND": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "I",
            "M"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJCPF": {
        "tipo_dado": "object",
        "valores_unicos": 23,
        "valores_nulos": 0,
        "amostra_valores": [
            "63602940000170",
            "24464109000229",
            "12307187000150",
            "04710210000124",
            "12291290000159",
            "23086176000456",
            "34570820000130",
            "13937131005615",
            "14349740000223",
            "15180961000100"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJMNT": {
        "tipo_dado": "object",
        "valores_unicos": 7,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000000000000 ",
            "24464109000229",
            "23086176000103",
            "13937131000141",
            "14349740000142",
            "15180714000104",
            "13650403000128"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNSPCN": {
        "tipo_dado": "object",
        "valores_unicos": 14343,
        "valores_nulos": 0,
        "amostra_valores": [
            "{{{{}~}",
            "{}{}}",
            "{{{|}{",
            "{|~{|}|}}~{",
            "{~{~{{{",
            "{{~{||~",
            "{}{{~{}~",
            "{}{}|",
            "{~{{",
            "{{|{|~}}{"
        ],
        "maior_caractere": 15,
        "menor_caractere": 15,
        "has_leading_zeros": False,
        "has_special_chars": True,
        "has_mixed_types": False
    },
    "AP_COIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 4,
        "valores_nulos": 0,
        "amostra_valores": [
            "4",
            "5",
            "2",
            "3"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_NUIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 100,
        "valores_nulos": 0,
        "amostra_valores": [
            "36",
            "59",
            "62",
            "70",
            "49",
            "67",
            "50",
            "52",
            "64",
            "68"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_SEXO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "F",
            "M"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_RACACOR": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "03",
            "01",
            "02",
            "04",
            "05",
            "99"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MUNPCN": {
        "tipo_dado": "object",
        "valores_unicos": 613,
        "valores_nulos": 0,
        "amostra_valores": [
            "120020",
            "120040",
            "130070",
            "120001",
            "120013",
            "120060",
            "120005",
            "130350",
            "120010",
            "120080"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFNACIO": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "010",
            "104",
            "022",
            "105",
            "107",
            "030"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CEPPCN": {
        "tipo_dado": "object",
        "valores_unicos": 7090,
        "valores_nulos": 0,
        "amostra_valores": [
            "69980000",
            "69900970",
            "69918418",
            "69914320",
            "69850000",
            "69918126",
            "69917748",
            "69907824",
            "69945000",
            "69926000"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFDIF": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MNDIF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTINIC": {
        "tipo_dado": "object",
        "valores_unicos": 424,
        "valores_nulos": 0,
        "amostra_valores": [
            "20221129",
            "20230126",
            "20221215",
            "20221227",
            "20221103",
            "20221202",
            "20221216",
            "20230104",
            "20230125",
            "20230110"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTFIM": {
        "tipo_dado": "object",
        "valores_unicos": 120,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230131",
            "20230331",
            "20230228",
            "20230630",
            "20230831",
            "20230731",
            "20230531",
            "20240131",
            "20231231",
            "20240229"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPATEN": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "03"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPAPAC": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "2",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MOTSAI": {
        "tipo_dado": "object",
        "valores_unicos": 15,
        "valores_nulos": 0,
        "amostra_valores": [
            "21",
            "51",
            "41",
            "28",
            "18",
            "43",
            "15",
            "26",
            "22",
            "42"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_OBITO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ENCERR": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PERMAN": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "1",
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ALTA": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TRANSF": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTOCOR": {
        "tipo_dado": "object",
        "valores_unicos": 107,
        "valores_nulos": 0,
        "amostra_valores": [
            "        ",
            "20230131",
            "20230616",
            "20230621",
            "20230603",
            "20230630",
            "20230608",
            "20230623",
            "20230626",
            "20231205"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AP_CODEMI": {
        "tipo_dado": "object",
        "valores_unicos": 17,
        "valores_nulos": 0,
        "amostra_valores": [
            "E120000001",
            "E120000010",
            "M270430201",
            "M270030001",
            "M270430202",
            "M270420302",
            "E160000001",
            "E130000001",
            "E290000001",
            "M290000001"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CATEND": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "01",
            "03",
            "02"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_APACANT": {
        "tipo_dado": "object",
        "valores_unicos": 717,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000000000000",
            "2724206124618",
            "2724206124629",
            "2724206124630",
            "2724206033505",
            "2723204209286",
            "2724206033472",
            "1324203383074",
            "1324203342814",
            "0            "
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UNISOL": {
        "tipo_dado": "object",
        "valores_unicos": 23,
        "valores_nulos": 0,
        "amostra_valores": [
            "2001586",
            "0000000",
            "2006197",
            "2007037",
            "2006448",
            "2020645",
            "2012677",
            "2013274",
            "0003921",
            "0003786"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTSOLIC": {
        "tipo_dado": "object",
        "valores_unicos": 439,
        "valores_nulos": 0,
        "amostra_valores": [
            "20221129",
            "20230126",
            "20221215",
            "20221227",
            "20221103",
            "20221202",
            "20221216",
            "20230104",
            "20230125",
            "20230110"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTAUT": {
        "tipo_dado": "object",
        "valores_unicos": 432,
        "valores_nulos": 0,
        "amostra_valores": [
            "20221129",
            "20230126",
            "20221215",
            "20221227",
            "20221103",
            "20221202",
            "20221216",
            "20230104",
            "20230125",
            "20230110"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDCAS": {
        "tipo_dado": "object",
        "valores_unicos": 4,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000",
            "C61 ",
            "C20 ",
            "C50 "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AP_CIDPRI": {
        "tipo_dado": "object",
        "valores_unicos": 254,
        "valores_nulos": 0,
        "amostra_valores": [
            "C506",
            "C539",
            "C61 ",
            "C719",
            "C349",
            "C240",
            "C189",
            "C499",
            "C108",
            "C925"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDSEC": {
        "tipo_dado": "object",
        "valores_unicos": 22,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000",
            "C910",
            "C921",
            "C717",
            "C402",
            "C409",
            "C719",
            "C715",
            "C64 ",
            "C810"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AP_ETNIA": {
        "tipo_dado": "object",
        "valores_unicos": 26,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "0111",
            "0114",
            "0145",
            "0131",
            "X271",
            "0110",
            "0254",
            "0240",
            "0303"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_CID10": {
        "tipo_dado": "object",
        "valores_unicos": 117,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "C504",
            "C671",
            "C500",
            "C509",
            "C61 ",
            "D45 ",
            "C506",
            "C670",
            "C182"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AQ_LINFIN": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "S",
            "3",
            "N"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_ESTADI": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "3",
            "4",
            " ",
            "1",
            "2",
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_GRAHIS": {
        "tipo_dado": "object",
        "valores_unicos": 58,
        "valores_nulos": 0,
        "amostra_valores": [
            "02",
            "03",
            "99",
            "3 ",
            "01",
            "2 ",
            "60",
            "1 ",
            "4 ",
            "04"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_DTIDEN": {
        "tipo_dado": "object",
        "valores_unicos": 3581,
        "valores_nulos": 0,
        "amostra_valores": [
            "20221025",
            "20221110",
            "20180629",
            "20210514",
            "20170213",
            "20160205",
            "20200310",
            "20220416",
            "20211014",
            "20220726"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AQ_TRANTE": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "N",
            "S"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_CIDINI1": {
        "tipo_dado": "object",
        "valores_unicos": 206,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "C502",
            "C921",
            "C509",
            "C910",
            "C61 ",
            "C504",
            "C268",
            "C508",
            "D473"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AQ_DTINI1": {
        "tipo_dado": "object",
        "valores_unicos": 2746,
        "valores_nulos": 0,
        "amostra_valores": [
            "        ",
            "20220520",
            "20191016",
            "20140925",
            "20150727",
            "20210719",
            "20200623",
            "20210409",
            "20210209",
            "20170405"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_CIDINI2": {
        "tipo_dado": "object",
        "valores_unicos": 167,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "C921",
            "C509",
            "C910",
            "D473",
            "C504",
            "C500",
            "C61 ",
            "C349",
            "C795"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AQ_DTINI2": {
        "tipo_dado": "object",
        "valores_unicos": 1914,
        "valores_nulos": 0,
        "amostra_valores": [
            "        ",
            "20200403",
            "20150504",
            "20160404",
            "20160216",
            "20180126",
            "20150204",
            "20160223",
            "20221026",
            "20170403"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_CIDINI3": {
        "tipo_dado": "object",
        "valores_unicos": 141,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "C921",
            "C509",
            "C910",
            "D473",
            "C504",
            "C61 ",
            "C502",
            "C169",
            "C508"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AQ_DTINI3": {
        "tipo_dado": "object",
        "valores_unicos": 1229,
        "valores_nulos": 0,
        "amostra_valores": [
            "        ",
            "20201116",
            "20150707",
            "20170403",
            "20161014",
            "20180529",
            "20150317",
            "20220127",
            "20220103",
            "20200204"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_CONTTR": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "N",
            "S"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AQ_DTINTR": {
        "tipo_dado": "object",
        "valores_unicos": 1630,
        "valores_nulos": 0,
        "amostra_valores": [
            "20221129",
            "20230126",
            "20220912",
            "20210713",
            "20171001",
            "20181030",
            "20220214",
            "20221003",
            "20230125",
            "20221004"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AQ_ESQU_P1": {
        "tipo_dado": "object",
        "valores_unicos": 882,
        "valores_nulos": 0,
        "amostra_valores": [
            "ACT  ",
            "TAXOL",
            "ELIGA",
            "TAMOX",
            "ANAST",
            "TEMOD",
            "ERLOT",
            "GENCI",
            "XELOX",
            "DOCE "
        ],
        "maior_caractere": 5,
        "menor_caractere": 5,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AQ_TOTMPL": {
        "tipo_dado": "object",
        "valores_unicos": 101,
        "valores_nulos": 0,
        "amostra_valores": [
            "006",
            "024",
            "120",
            "060",
            "012",
            "036",
            "015",
            "003",
            "004",
            "009"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AQ_TOTMAU": {
        "tipo_dado": "object",
        "valores_unicos": 152,
        "valores_nulos": 0,
        "amostra_valores": [
            "000",
            "003",
            "015",
            "057",
            "048",
            "009",
            "045",
            "018",
            "010",
            "121"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AQ_ESQU_P2": {
        "tipo_dado": "object",
        "valores_unicos": 2261,
        "valores_nulos": 0,
        "amostra_valores": [
            "          ",
            "R         ",
            "IFENO     ",
            " ZOL      ",
            "I         ",
            " CISPL    ",
            "GENC      ",
            " CARBO    ",
            "B VESANOI ",
            "ZOM TMX   "
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AQ_MED01": {
        "tipo_dado": "object",
        "valores_unicos": 127,
        "valores_nulos": 0,
        "amostra_valores": [
            "046",
            "120",
            "004",
            "037",
            "011",
            "164",
            "048",
            "085",
            "028",
            "069"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AQ_MED02": {
        "tipo_dado": "object",
        "valores_unicos": 101,
        "valores_nulos": 0,
        "amostra_valores": [
            "034",
            "   ",
            "003",
            "035",
            "119",
            "085",
            "029",
            "045",
            "006",
            "078"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_MED03": {
        "tipo_dado": "object",
        "valores_unicos": 74,
        "valores_nulos": 0,
        "amostra_valores": [
            "120",
            "   ",
            "037",
            "004",
            "170",
            "172",
            "046",
            "078",
            "013",
            "133"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_MED04": {
        "tipo_dado": "object",
        "valores_unicos": 55,
        "valores_nulos": 0,
        "amostra_valores": [
            "   ",
            "023",
            "133",
            "139",
            "173",
            "034",
            "172",
            "060",
            "075",
            "176"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_MED05": {
        "tipo_dado": "object",
        "valores_unicos": 35,
        "valores_nulos": 0,
        "amostra_valores": [
            "   ",
            "046",
            "177",
            "161",
            "179",
            "034",
            "108",
            "060",
            "133",
            "075"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_MED06": {
        "tipo_dado": "object",
        "valores_unicos": 18,
        "valores_nulos": 0,
        "amostra_valores": [
            "   ",
            "108",
            "161",
            "135",
            "046",
            "176",
            "174",
            "158",
            "179",
            "034"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_MED07": {
        "tipo_dado": "object",
        "valores_unicos": 13,
        "valores_nulos": 0,
        "amostra_valores": [
            "   ",
            "142",
            "139",
            "177",
            "176",
            "174",
            "179",
            "158",
            "108",
            "060"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_MED08": {
        "tipo_dado": "object",
        "valores_unicos": 8,
        "valores_nulos": 0,
        "amostra_valores": [
            "   ",
            "158",
            "142",
            "045",
            "176",
            "174",
            "179",
            "177"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_MED09": {
        "tipo_dado": "object",
        "valores_unicos": 4,
        "valores_nulos": 0,
        "amostra_valores": [
            "   ",
            "108",
            "179",
            "124"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AQ_MED10": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "   ",
            "182"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AP_NATJUR": {
        "tipo_dado": "object",
        "valores_unicos": 8,
        "valores_nulos": 0,
        "amostra_valores": [
            "1147",
            "1104",
            "3999",
            "2062",
            "3069",
            "1023",
            "2054",
            "1244"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    }
    },
        'sia_apac_radioterapia': {
    "AP_MVM": {
        "tipo_dado": "object",
        "valores_unicos": 5,
        "valores_nulos": 0,
        "amostra_valores": [
            "202301",
            "202306",
            "202312",
            "202401",
            "202406"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CONDIC": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "EP",
            "PG"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_GESTAO": {
        "tipo_dado": "object",
        "valores_unicos": 9,
        "valores_nulos": 0,
        "amostra_valores": [
            "120000",
            "270430",
            "270030",
            "130000",
            "290000",
            "291480",
            "292740",
            "291080",
            "293330"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CODUNI": {
        "tipo_dado": "object",
        "valores_unicos": 12,
        "valores_nulos": 0,
        "amostra_valores": [
            "2001586",
            "2007037",
            "2005417",
            "2006197",
            "2012677",
            "0003786",
            "2525569",
            "2601680",
            "2802104",
            "2407205"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_AUTORIZ": {
        "tipo_dado": "object",
        "valores_unicos": 5023,
        "valores_nulos": 0,
        "amostra_valores": [
            "1223200028589",
            "1223200028688",
            "1223200028644",
            "1223200028666",
            "1223200028677",
            "1223200028655",
            "1223200028578",
            "1223200028633",
            "1223200028600",
            "1223200028534"
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CMP": {
        "tipo_dado": "object",
        "valores_unicos": 15,
        "valores_nulos": 0,
        "amostra_valores": [
            "202301",
            "202306",
            "202304",
            "202312",
            "202310",
            "202401",
            "202406",
            "202212",
            "202211",
            "202305"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PRIPAL": {
        "tipo_dado": "object",
        "valores_unicos": 20,
        "valores_nulos": 0,
        "amostra_valores": [
            "0304010413",
            "0304010367",
            "0304010537",
            "0304010502",
            "0304010553",
            "0304010405",
            "0304010421",
            "0304010383",
            "0304010456",
            "0304010375"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_VL_AP": {
        "tipo_dado": "object",
        "valores_unicos": 31,
        "valores_nulos": 0,
        "amostra_valores": [
            "             5904.00",
            "             4168.00",
            "             1729.00",
            "             3278.00",
            "             3159.00",
            "             2310.00",
            "             4608.00",
            "             3563.00",
            "             5838.00",
            "             4148.00"
        ],
        "maior_caractere": 20,
        "menor_caractere": 20,
        "has_leading_zeros": False,
        "has_special_chars": True,
        "has_mixed_types": False
    },
    "AP_UFMUN": {
        "tipo_dado": "object",
        "valores_unicos": 9,
        "valores_nulos": 0,
        "amostra_valores": [
            "120040",
            "270430",
            "270030",
            "130260",
            "292740",
            "291480",
            "291080",
            "293330",
            "291840"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPUPS": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "05",
            "07"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TIPPRE": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "00"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MN_IND": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "I",
            "M"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJCPF": {
        "tipo_dado": "object",
        "valores_unicos": 12,
        "valores_nulos": 0,
        "amostra_valores": [
            "63602940000170",
            "12307187000150",
            "04710210000124",
            "24464109000229",
            "34570820000130",
            "15180961000100",
            "14349740000304",
            "13227038000143",
            "15178551000117",
            "16205262000122"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNPJMNT": {
        "tipo_dado": "object",
        "valores_unicos": 4,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000000000000 ",
            "24464109000229",
            "14349740000142",
            "13937131000141"
        ],
        "maior_caractere": 14,
        "menor_caractere": 14,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CNSPCN": {
        "tipo_dado": "object",
        "valores_unicos": 4926,
        "valores_nulos": 0,
        "amostra_valores": [
            "{}{}{~|{",
            "{{{{}{|}}",
            "{{{~~",
            "{{{~{",
            "{}{||~{",
            "{{{||{{|~",
            "{{{{~}}{{",
            "{{{~|~}~",
            "{}{{~{|}",
            "{{}{{}{"
        ],
        "maior_caractere": 15,
        "menor_caractere": 15,
        "has_leading_zeros": False,
        "has_special_chars": True,
        "has_mixed_types": False
    },
    "AP_COIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "4",
            "2",
            "5"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_NUIDADE": {
        "tipo_dado": "object",
        "valores_unicos": 98,
        "valores_nulos": 0,
        "amostra_valores": [
            "53",
            "75",
            "65",
            "54",
            "76",
            "73",
            "81",
            "49",
            "69",
            "44"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_SEXO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "F",
            "M"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_RACACOR": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "03",
            "01",
            "02",
            "99",
            "04",
            "05"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MUNPCN": {
        "tipo_dado": "object",
        "valores_unicos": 529,
        "valores_nulos": 0,
        "amostra_valores": [
            "120010",
            "120040",
            "120070",
            "130070",
            "120060",
            "120045",
            "120034",
            "120020",
            "120050",
            "120033"
        ],
        "maior_caractere": 6,
        "menor_caractere": 6,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFNACIO": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "010"
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CEPPCN": {
        "tipo_dado": "object",
        "valores_unicos": 2955,
        "valores_nulos": 0,
        "amostra_valores": [
            "69932000",
            "69905676",
            "69920030",
            "69900970",
            "69930000",
            "69850000",
            "69970000",
            "69925000",
            "69905190",
            "69950000"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UFDIF": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MNDIF": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTINIC": {
        "tipo_dado": "object",
        "valores_unicos": 199,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230105",
            "20230130",
            "20230102",
            "20230106",
            "20230127",
            "20230118",
            "20230124",
            "20230117",
            "20230111",
            "20230113"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTFIM": {
        "tipo_dado": "object",
        "valores_unicos": 82,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230131",
            "20230630",
            "20230430",
            "20231231",
            "20231031",
            "20240131",
            "20240630",
            "20230331",
            "20230228",
            "20230123"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPATEN": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "04"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TPAPAC": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "3"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_MOTSAI": {
        "tipo_dado": "object",
        "valores_unicos": 7,
        "valores_nulos": 0,
        "amostra_valores": [
            "15",
            "51",
            "18",
            "12",
            "41",
            "43",
            "31"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_OBITO": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ENCERR": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_PERMAN": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ALTA": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "1",
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_TRANSF": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "0",
            "1"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTOCOR": {
        "tipo_dado": "object",
        "valores_unicos": 184,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230131",
            "20230630",
            "20230430",
            "20231231",
            "20231031",
            "20240131",
            "20240630",
            "20230112",
            "20230127",
            "20230123"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CODEMI": {
        "tipo_dado": "object",
        "valores_unicos": 10,
        "valores_nulos": 0,
        "amostra_valores": [
            "E120000001",
            "M270430201",
            "M270030001",
            "E130000001",
            "E290000001",
            "M290000001",
            "M291080001",
            "M293330701",
            "M292740801",
            "M291840702"
        ],
        "maior_caractere": 10,
        "menor_caractere": 10,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CATEND": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "01",
            "02"
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_APACANT": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000000000000",
            "0            ",
            "000000000000 "
        ],
        "maior_caractere": 13,
        "menor_caractere": 13,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_UNISOL": {
        "tipo_dado": "object",
        "valores_unicos": 10,
        "valores_nulos": 0,
        "amostra_valores": [
            "2001586",
            "2007037",
            "0000000",
            "2006197",
            "2012677",
            "0003786",
            "2601680",
            "2802104",
            "2407205",
            "4028155"
        ],
        "maior_caractere": 7,
        "menor_caractere": 7,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTSOLIC": {
        "tipo_dado": "object",
        "valores_unicos": 252,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230105",
            "20230130",
            "20230102",
            "20230106",
            "20230127",
            "20230118",
            "20230124",
            "20230117",
            "20230111",
            "20230113"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_DTAUT": {
        "tipo_dado": "object",
        "valores_unicos": 259,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230105",
            "20230130",
            "20230102",
            "20230106",
            "20230127",
            "20230118",
            "20230124",
            "20230117",
            "20230111",
            "20230113"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDCAS": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDPRI": {
        "tipo_dado": "object",
        "valores_unicos": 186,
        "valores_nulos": 0,
        "amostra_valores": [
            "C509",
            "C109",
            "C900",
            "C729",
            "C839",
            "C449",
            "C539",
            "C349",
            "C61 ",
            "C069"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_CIDSEC": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "0000"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_ETNIA": {
        "tipo_dado": "object",
        "valores_unicos": 11,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "0236",
            "0102",
            "0158",
            "0035",
            "0132",
            "0078",
            "0032",
            "0204",
            "0218"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AR_SMRD": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "   "
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_CID10": {
        "tipo_dado": "object",
        "valores_unicos": 153,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "C20 ",
            "C508",
            "C509",
            "C519",
            "C61 ",
            "C900",
            "C795",
            "C773",
            "C328"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_LINFIN": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            "S",
            "3",
            "N"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AR_ESTADI": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "1",
            "3",
            " ",
            "4",
            "2",
            "0"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AR_GRAHIS": {
        "tipo_dado": "object",
        "valores_unicos": 38,
        "valores_nulos": 0,
        "amostra_valores": [
            "02",
            "03",
            "99",
            "01",
            "2 ",
            "4 ",
            "10",
            "3 ",
            "5 ",
            "1 "
        ],
        "maior_caractere": 2,
        "menor_caractere": 2,
        "has_leading_zeros": True,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_DTIDEN": {
        "tipo_dado": "object",
        "valores_unicos": 1227,
        "valores_nulos": 0,
        "amostra_valores": [
            "20210929",
            "20220802",
            "20210210",
            "20210628",
            "20190412",
            "20220318",
            "20190718",
            "20220922",
            "20221026",
            "20220304"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_TRANTE": {
        "tipo_dado": "object",
        "valores_unicos": 3,
        "valores_nulos": 0,
        "amostra_valores": [
            " ",
            "N",
            "S"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_CIDINI1": {
        "tipo_dado": "object",
        "valores_unicos": 71,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "C508",
            "C793",
            "C773",
            "C509",
            "C61 ",
            "C795",
            "C501",
            "C538",
            "C058"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_DTINI1": {
        "tipo_dado": "object",
        "valores_unicos": 438,
        "valores_nulos": 0,
        "amostra_valores": [
            "        ",
            "20220801",
            "20210906",
            "20210920",
            "20170110",
            "20220418",
            "20220317",
            "20220321",
            "20220310",
            "20220930"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AR_CIDINI2": {
        "tipo_dado": "object",
        "valores_unicos": 33,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "C508",
            "C793",
            "C773",
            "C795",
            "C501",
            "C538",
            "C07 ",
            "C168",
            "C509"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_DTINI2": {
        "tipo_dado": "object",
        "valores_unicos": 210,
        "valores_nulos": 0,
        "amostra_valores": [
            "        ",
            "20221110",
            "20220307",
            "20220728",
            "20171026",
            "20220525",
            "20220912",
            "20221223",
            "20220928",
            "20221215"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AR_CIDINI3": {
        "tipo_dado": "object",
        "valores_unicos": 15,
        "valores_nulos": 0,
        "amostra_valores": [
            "    ",
            "C793",
            "C773",
            "C508",
            "C795",
            "C168",
            "C548",
            "Z510",
            "C509",
            "C678"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_DTINI3": {
        "tipo_dado": "object",
        "valores_unicos": 58,
        "valores_nulos": 0,
        "amostra_valores": [
            "        ",
            "20220921",
            "20171121",
            "20221219",
            "20221202",
            "20220722",
            "20220906",
            "20221216",
            "20210503",
            "20200527"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": True
    },
    "AR_CONTTR": {
        "tipo_dado": "object",
        "valores_unicos": 2,
        "valores_nulos": 0,
        "amostra_valores": [
            "N",
            "S"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_DTINTR": {
        "tipo_dado": "object",
        "valores_unicos": 267,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230105",
            "20230130",
            "20230102",
            "20230106",
            "20230127",
            "20230118",
            "20230124",
            "20230117",
            "20230111",
            "20230113"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_FINALI": {
        "tipo_dado": "object",
        "valores_unicos": 7,
        "valores_nulos": 0,
        "amostra_valores": [
            "1",
            "4",
            "2",
            "6",
            "5",
            "3",
            "7"
        ],
        "maior_caractere": 1,
        "menor_caractere": 1,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_CIDTR1": {
        "tipo_dado": "object",
        "valores_unicos": 185,
        "valores_nulos": 0,
        "amostra_valores": [
            "C509",
            "C109",
            "C900",
            "C729",
            "C839",
            "C449",
            "C539",
            "C349",
            "C61 ",
            "C069"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_CIDTR2": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "    "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_CIDTR3": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "    "
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_NUMC1": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "   "
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_INIAR1": {
        "tipo_dado": "object",
        "valores_unicos": 267,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230105",
            "20230130",
            "20230102",
            "20230106",
            "20230127",
            "20230118",
            "20230124",
            "20230117",
            "20230111",
            "20230113"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_INIAR2": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "        "
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_INIAR3": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "        "
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_FIMAR1": {
        "tipo_dado": "object",
        "valores_unicos": 337,
        "valores_nulos": 0,
        "amostra_valores": [
            "20230131",
            "20230630",
            "20230430",
            "20231231",
            "20231031",
            "20240131",
            "20270131",
            "20240630",
            "20230331",
            "20230228"
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_FIMAR2": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "        "
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_FIMAR3": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "        "
        ],
        "maior_caractere": 8,
        "menor_caractere": 8,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_NUMC2": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "   "
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AR_NUMC3": {
        "tipo_dado": "object",
        "valores_unicos": 1,
        "valores_nulos": 0,
        "amostra_valores": [
            "   "
        ],
        "maior_caractere": 3,
        "menor_caractere": 3,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    },
    "AP_NATJUR": {
        "tipo_dado": "object",
        "valores_unicos": 6,
        "valores_nulos": 0,
        "amostra_valores": [
            "1147",
            "3999",
            "2062",
            "1104",
            "2054",
            "1023"
        ],
        "maior_caractere": 4,
        "menor_caractere": 4,
        "has_leading_zeros": False,
        "has_special_chars": False,
        "has_mixed_types": False
    }
},
        'sia_apac_tratamento_dialitico': {
        "AP_MVM": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "202310"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CONDIC": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "EP",
                "PG"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_GESTAO": {
            "tipo_dado": "object",
            "valores_unicos": 57,
            "valores_nulos": 0,
            "amostra_valores": [
                "350000",
                "350320",
                "351050",
                "354890",
                "355030",
                "350400",
                "350950",
                "351880",
                "352410",
                "352440"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CODUNI": {
            "tipo_dado": "object",
            "valores_unicos": 144,
            "valores_nulos": 0,
            "amostra_valores": [
                "2077485",
                "2078015",
                "2748223",
                "2792168",
                "2042487",
                "9037179",
                "2080931",
                "2089785",
                "6213529",
                "2034824"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_AUTORIZ": {
            "tipo_dado": "object",
            "valores_unicos": 27515,
            "valores_nulos": 0,
            "amostra_valores": [
                "3523274364830",
                "3523274338540",
                "3523282172751",
                "3523274375621",
                "3523272736930",
                "3523231178863",
                "3523258742971",
                "3523268261865",
                "3523268262030",
                "3523268681735"
            ],
            "maior_caractere": 13,
            "menor_caractere": 13,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CMP": {
            "tipo_dado": "object",
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "202310",
                "202308",
                "202309",
                "202307"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_PRIPAL": {
            "tipo_dado": "object",
            "valores_unicos": 6,
            "valores_nulos": 0,
            "amostra_valores": [
                "0305010182",
                "0305010166",
                "0305010026",
                "0305010204",
                "0305010115",
                "0305010107"
            ],
            "maior_caractere": 10,
            "menor_caractere": 10,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_VL_AP": {
            "tipo_dado": "object",
            "valores_unicos": 4146,
            "valores_nulos": 0,
            "amostra_valores": [
                "               55.13",
                "              664.52",
                "             3392.53",
                "             3385.15",
                "             3386.25",
                "             3577.32",
                "             3426.09",
                "             3427.94",
                "             3342.62",
                "             3355.98"
            ],
            "maior_caractere": 20,
            "menor_caractere": 20,
            "has_leading_zeros": False,
            "has_special_chars": True,
            "has_mixed_types": False
        },
        "AP_UFMUN": {
            "tipo_dado": "object",
            "valores_unicos": 85,
            "valores_nulos": 0,
            "amostra_valores": [
                "355030",
                "350750",
                "351060",
                "350320",
                "351050",
                "354890",
                "355220",
                "355410",
                "354980",
                "350950"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_TPUPS": {
            "tipo_dado": "object",
            "valores_unicos": 5,
            "valores_nulos": 0,
            "amostra_valores": [
                "05",
                "36",
                "07",
                "39",
                "04"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_TIPPRE": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "00"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_MN_IND": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "I",
                "M"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CNPJCPF": {
            "tipo_dado": "object",
            "valores_unicos": 144,
            "valores_nulos": 0,
            "amostra_valores": [
                "61699567000192",
                "56577059000100",
                "46230439000101",
                "46374500013920",
                "56893852000100",
                "04666985000220",
                "59610394000142",
                "52803319000159",
                "07475651000187",
                "54329859000178"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CNPJMNT": {
            "tipo_dado": "object",
            "valores_unicos": 8,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000000000000 ",
                "46374500000194",
                "46068425000133",
                "46523031000128",
                "46177531000155",
                "45781176000166",
                "46523015000135",
                "60499365000134"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CNSPCN": {
            "tipo_dado": "object",
            "valores_unicos": 27316,
            "valores_nulos": 0,
            "amostra_valores": [
                "{{{{{{{",
                "{}{~}",
                "{|{}{~|",
                "{{{|{{~",
                "{{{{|||~{",
                "{}{{||{",
                "{{||{{}|{",
                "{{|{|",
                "{{||~",
                "{{||~"
            ],
            "maior_caractere": 15,
            "menor_caractere": 15,
            "has_leading_zeros": False,
            "has_special_chars": True,
            "has_mixed_types": False
        },
        "AP_COIDADE": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "4",
                "3"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_NUIDADE": {
            "tipo_dado": "object",
            "valores_unicos": 98,
            "valores_nulos": 0,
            "amostra_valores": [
                "07",
                "27",
                "59",
                "54",
                "37",
                "75",
                "61",
                "56",
                "64",
                "41"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_SEXO": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "M",
                "F"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_RACACOR": {
            "tipo_dado": "object",
            "valores_unicos": 5,
            "valores_nulos": 0,
            "amostra_valores": [
                "02",
                "01",
                "03",
                "04",
                "05"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_MUNPCN": {
            "tipo_dado": "object",
            "valores_unicos": 643,
            "valores_nulos": 0,
            "amostra_valores": [
                "355500",
                "355030",
                "351925",
                "353440",
                "350320",
                "351050",
                "354890",
                "351300",
                "354680",
                "355220"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_UFNACIO": {
            "tipo_dado": "object",
            "valores_unicos": 40,
            "valores_nulos": 0,
            "amostra_valores": [
                "010",
                "10 ",
                "041",
                "179",
                "045",
                "245",
                "067",
                "039",
                "022",
                "170"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CEPPCN": {
            "tipo_dado": "object",
            "valores_unicos": 20406,
            "valores_nulos": 0,
            "amostra_valores": [
                "17603210",
                "05658070",
                "18775970",
                "06147110",
                "14804204",
                "11666590",
                "13566609",
                "06717200",
                "04679150",
                "07500000"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_UFDIF": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "0",
                "1"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_MNDIF": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "1"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTINIC": {
            "tipo_dado": "object",
            "valores_unicos": 102,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231005",
                "20231016",
                "20231001",
                "20231002",
                "20231024",
                "20231013",
                "20231020",
                "20231007",
                "20230801",
                "20230901"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTFIM": {
            "tipo_dado": "object",
            "valores_unicos": 46,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231231",
                "20231002",
                "20231031",
                "20231130",
                "20230930",
                "20231029",
                "20230801",
                "20230822",
                "20230722",
                "20230730"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_TPATEN": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "10"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_TPAPAC": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "3",
                "2",
                "1"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_MOTSAI": {
            "tipo_dado": "object",
            "valores_unicos": 18,
            "valores_nulos": 0,
            "amostra_valores": [
                "15",
                "26",
                "18",
                "21",
                "28",
                "41",
                "25",
                "31",
                "43",
                "51"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_OBITO": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "0",
                "1"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_ENCERR": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "0",
                "1"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_PERMAN": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "0",
                "1"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_ALTA": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "1",
                "0"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_TRANSF": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "0",
                "1"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTOCOR": {
            "tipo_dado": "object",
            "valores_unicos": 60,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231031",
                "20231026",
                "20231002",
                "20231024",
                "20231003",
                "20231010",
                "20231028",
                "        ",
                "20231001",
                "20231025"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_CODEMI": {
            "tipo_dado": "object",
            "valores_unicos": 83,
            "valores_nulos": 0,
            "amostra_valores": [
                "E350000027",
                "S352078015",
                "S352748223",
                "M350320001",
                "M351050001",
                "M354890607",
                "M355030001",
                "E350000023",
                "E350000030",
                "E350000022"
            ],
            "maior_caractere": 10,
            "menor_caractere": 10,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CATEND": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "01",
                "05",
                "02"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_APACANT": {
            "tipo_dado": "object",
            "valores_unicos": 1551,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000000000000",
                "000000000000 ",
                "3523270030490",
                "3523270030555",
                "3523270030566",
                "3523270030599",
                "3523270030621",
                "3523256403546",
                "3523262759027",
                "3523262759159"
            ],
            "maior_caractere": 13,
            "menor_caractere": 13,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_UNISOL": {
            "tipo_dado": "object",
            "valores_unicos": 140,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000000",
                "2078015",
                "2792168",
                "2042487",
                "9037179",
                "2089785",
                "6213529",
                "2034824",
                "2071258",
                "2077396"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTSOLIC": {
            "tipo_dado": "object",
            "valores_unicos": 122,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231005",
                "20231016",
                "20231023",
                "20231002",
                "20231024",
                "20231013",
                "20231003",
                "        ",
                "20231007",
                "20230801"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_DTAUT": {
            "tipo_dado": "object",
            "valores_unicos": 130,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231005",
                "20231026",
                "20231106",
                "20231002",
                "20231024",
                "20231022",
                "20231101",
                "        ",
                "20231007",
                "20230801"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_CIDCAS": {
            "tipo_dado": "object",
            "valores_unicos": 50,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000",
                "N180",
                "N189",
                "B182",
                "B181",
                "B188",
                "B24 ",
                "Z21 ",
                "B178",
                "Z225"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_CIDPRI": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CIDSEC": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_ETNIA": {
            "tipo_dado": "object",
            "valores_unicos": 6,
            "valores_nulos": 0,
            "amostra_valores": [
                "    ",
                "0207",
                "0233",
                "0200",
                "0114",
                "0181"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "ATD_CARACT": {
            "tipo_dado": "object",
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "1",
                "2",
                "4",
                "3"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "ATD_DTPDR": {
            "tipo_dado": "object",
            "valores_unicos": 4766,
            "valores_nulos": 0,
            "amostra_valores": [
                "20190324",
                "20231016",
                "20231018",
                "20230301",
                "20231024",
                "20231013",
                "20231003",
                "20231004",
                "20230622",
                "20180329"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "ATD_DTCLI": {
            "tipo_dado": "object",
            "valores_unicos": 4247,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230917",
                "20231016",
                "20231018",
                "20230916",
                "20231024",
                "20231013",
                "20231003",
                "20231004",
                "20230622",
                "20180410"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "ATD_ACEVAS": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "3",
                "1",
                "2"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "ATD_MAISNE": {
            "tipo_dado": "object",
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "S",
                "N",
                " ",
                "I"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "ATD_SITINI": {
            "tipo_dado": "object",
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "H",
                "A",
                " ",
                "I"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "ATD_SITTRA": {
            "tipo_dado": "object",
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "1",
                "4",
                "2",
                "3"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "ATD_SEAPTO": {
            "tipo_dado": "object",
            "valores_unicos": 6,
            "valores_nulos": 0,
            "amostra_valores": [
                "1",
                "4",
                " ",
                "2",
                "3",
                "0"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "ATD_HB": {
            "tipo_dado": "object",
            "valores_unicos": 691,
            "valores_nulos": 0,
            "amostra_valores": [
                "9   ",
                "11.2",
                "0000",
                "10.0",
                "0013",
                "0   ",
                "0011",
                "12,5",
                "08.6",
                "14.4"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": True,
            "has_mixed_types": True
        },
        "ATD_FOSFOR": {
            "tipo_dado": "object",
            "valores_unicos": 1127,
            "valores_nulos": 0,
            "amostra_valores": [
                "5   ",
                "03.6",
                "0000",
                "06.4",
                "0006",
                "0   ",
                "07,2",
                "04,8",
                "06.3",
                "03.4"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": True,
            "has_mixed_types": True
        },
        "ATD_KTVSEM": {
            "tipo_dado": "object",
            "valores_unicos": 771,
            "valores_nulos": 0,
            "amostra_valores": [
                "1,6 ",
                "   0",
                "0000",
                "    ",
                "3   ",
                "1,1 ",
                "01,7",
                "0   ",
                "000,",
                "1.70"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": True,
            "has_mixed_types": True
        },
        "ATD_TRU": {
            "tipo_dado": "object",
            "valores_unicos": 1324,
            "valores_nulos": 0,
            "amostra_valores": [
                "78  ",
                "   0",
                "0000",
                "    ",
                "3   ",
                "81  ",
                "0100",
                "74,0",
                "0   ",
                "000,"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": True,
            "has_mixed_types": True
        },
        "ATD_ALBUMI": {
            "tipo_dado": "object",
            "valores_unicos": 568,
            "valores_nulos": 0,
            "amostra_valores": [
                "2,8 ",
                "0003",
                "0000",
                "04.3",
                "    ",
                "03.8",
                "04.2",
                "03.6",
                "03.9",
                "03.4"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": True,
            "has_mixed_types": True
        },
        "ATD_PTH": {
            "tipo_dado": "object",
            "valores_unicos": 4034,
            "valores_nulos": 0,
            "amostra_valores": [
                "641 ",
                "   0",
                "0000",
                "0024",
                "    ",
                "0924",
                "0222",
                "0201",
                "0788",
                "3   "
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "ATD_HIV": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "N",
                " ",
                "P"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "ATD_HCV": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "N",
                " ",
                "P"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "ATD_HBSAG": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "N",
                " ",
                "P"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "ATD_INTERC": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "S",
                "N",
                "I"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "ATD_SEPERI": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "I",
                "N",
                "S"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_NATJUR": {
            "tipo_dado": "object",
            "valores_unicos": 8,
            "valores_nulos": 0,
            "amostra_valores": [
                "3999",
                "3069",
                "1023",
                "2062",
                "1112",
                "2240",
                "1244",
                "2054"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        }
    },
        'sia_boletim_producao_ambulatorial_individualizado': {
        "CODUNI": {
            "tipo_dado": "object",
            "valores_unicos": 2109,
            "valores_nulos": 0,
            "amostra_valores": [
                "2082187",
                "2077485",
                "2091771",
                "2787903",
                "2093650",
                "2784580",
                "9755160",
                "2035162",
                "7210833",
                "6131670"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "GESTAO": {
            "tipo_dado": "object",
            "valores_unicos": 34,
            "valores_nulos": 0,
            "amostra_valores": [
                "350000",
                "355030",
                "350550",
                "352590",
                "352900",
                "352240",
                "350900",
                "354850",
                "354780",
                "354340"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "CONDIC": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "EP",
                "PG"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "UFMUN": {
            "tipo_dado": "object",
            "valores_unicos": 33,
            "valores_nulos": 0,
            "amostra_valores": [
                "354340",
                "355030",
                "350550",
                "352590",
                "352900",
                "352240",
                "350900",
                "351880",
                "354850",
                "354780"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "TPUPS": {
            "tipo_dado": "object",
            "valores_unicos": 26,
            "valores_nulos": 0,
            "amostra_valores": [
                "05",
                "80",
                "02",
                "73",
                "04",
                "81",
                "15",
                "62",
                "36",
                "39"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "TIPPRE": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "00"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "MN_IND": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "I",
                "M"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "CNPJCPF": {
            "tipo_dado": "object",
            "valores_unicos": 448,
            "valores_nulos": 0,
            "amostra_valores": [
                "57722118000140",
                "61699567000192",
                "46392130000380",
                "44780609000104",
                "45780103000150",
                "44477909000100",
                "46634358000177",
                "46523064000178",
                "46374500008926",
                "58198524000119"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "CNPJMNT": {
            "tipo_dado": "object",
            "valores_unicos": 46,
            "valores_nulos": 0,
            "amostra_valores": [
                "00000000000000",
                "46392130000380",
                "44780609000104",
                "45780103000150",
                "44477909000100",
                "46634358000177",
                "46523064000178",
                "46374500000194",
                "46522942000130",
                "58200015000183"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "CNPJ_CC": {
            "tipo_dado": "object",
            "valores_unicos": 46,
            "valores_nulos": 0,
            "amostra_valores": [
                "00000000000000",
                "46392130000380",
                "24516372000133",
                "50956440000195",
                "57722118000140",
                "47673793010217",
                "60979457000111",
                "44780609000104",
                "46395000000139",
                "47673793012937"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "DT_PROCESS": {
            "tipo_dado": "object",
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "202311"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "DT_ATEND": {
            "tipo_dado": "object",
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "202311",
                "202310",
                "202308",
                "202309"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "PROC_ID": {
            "tipo_dado": "object",
            "valores_unicos": 1158,
            "valores_nulos": 0,
            "amostra_valores": [
                "0302060022",
                "0302050027",
                "0202031071",
                "0205020097",
                "0211060011",
                "0206020031",
                "0202060217",
                "0202010180",
                "0202010201",
                "0202010317"
            ],
            "maior_caractere": 10,
            "menor_caractere": 10,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "TPFIN": {
            "tipo_dado": "object",
            "valores_unicos": 5,
            "valores_nulos": 0,
            "amostra_valores": [
                "06",
                "01",
                "07",
                "04",
                "05"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "SUBFIN": {
            "tipo_dado": "object",
            "valores_unicos": 12,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000",
                "0071",
                "0068",
                "0032",
                "0062",
                "0075",
                "0055",
                "0073",
                "0072",
                "0044"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "COMPLEX": {
            "tipo_dado": "object",
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "2",
                "3",
                "1",
                "0"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AUTORIZ": {
            "tipo_dado": "object",
            "valores_unicos": 221000,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000000000000",
                "0000000000001",
                "0000005252484",
                "0000005256488",
                "0000005259680",
                "0000005243900",
                "0000005246745",
                "0000005250784",
                "0000005241107",
                "0000005240304"
            ],
            "maior_caractere": 13,
            "menor_caractere": 13,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "CNSPROF": {
            "tipo_dado": "object",
            "valores_unicos": 41625,
            "valores_nulos": 0,
            "amostra_valores": [
                "703205602373990",
                "701202041659816",
                "700507133940051",
                "701409608287331",
                "700605485116360",
                "702005344148489",
                "705207428018971",
                "704607660077521",
                "700508105938456",
                "702405076732527"
            ],
            "maior_caractere": 15,
            "menor_caractere": 15,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "CBOPROF": {
            "tipo_dado": "object",
            "valores_unicos": 131,
            "valores_nulos": 0,
            "amostra_valores": [
                "223605",
                "221205",
                "225320",
                "225265",
                "221105",
                "223293",
                "223565",
                "322205",
                "223810",
                "225125"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "CIDPRI": {
            "tipo_dado": "object",
            "valores_unicos": 7638,
            "valores_nulos": 0,
            "amostra_valores": [
                "G998",
                "G248",
                "B24 ",
                "C509",
                "N62 ",
                "Z014",
                "0000",
                "Z00 ",
                "C240",
                "Z017"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "CATEND": {
            "tipo_dado": "object",
            "valores_unicos": 6,
            "valores_nulos": 0,
            "amostra_valores": [
                "01",
                "02",
                "03",
                "06",
                "04",
                "05"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "CNS_PAC": {
            "tipo_dado": "object",
            "valores_unicos": 2185607,
            "valores_nulos": 0,
            "amostra_valores": [
                "{{|}}{",
                "{{{||}|",
                "}{{~|~||{{{",
                "{|~~}~|}",
                "{{}}||",
                "{{{||~{~~",
                "{{|}{}",
                "{{}~}",
                "{|~~}}}",
                "{~{{||{"
            ],
            "maior_caractere": 15,
            "menor_caractere": 15,
            "has_leading_zeros": False,
            "has_special_chars": True,
            "has_mixed_types": False
        },
        "DTNASC": {
            "tipo_dado": "object",
            "valores_unicos": 35979,
            "valores_nulos": 0,
            "amostra_valores": [
                "20100209",
                "20090323",
                "19950102",
                "19630304",
                "19801008",
                "19851203",
                "19791229",
                "19770515",
                "19651108",
                "19771027"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "TPIDADEPAC": {
            "tipo_dado": "object",
            "valores_unicos": 6,
            "valores_nulos": 0,
            "amostra_valores": [
                "4",
                "3",
                "2",
                "5",
                "9",
                "0"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "IDADEPAC": {
            "tipo_dado": "object",
            "valores_unicos": 100,
            "valores_nulos": 0,
            "amostra_valores": [
                "13",
                "14",
                "28",
                "60",
                "43",
                "37",
                "46",
                "57",
                "56",
                "58"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "SEXOPAC": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "F",
                "M"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "RACACOR": {
            "tipo_dado": "object",
            "valores_unicos": 5,
            "valores_nulos": 0,
            "amostra_valores": [
                "01",
                "03",
                "02",
                "04",
                "05"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "MUNPAC": {
            "tipo_dado": "object",
            "valores_unicos": 3002,
            "valores_nulos": 0,
            "amostra_valores": [
                "354340",
                "355150",
                "352430",
                "355170",
                "353930",
                "351740",
                "350590",
                "354070",
                "354290",
                "351880"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "QT_APRES": {
            "tipo_dado": "object",
            "valores_unicos": 102,
            "valores_nulos": 0,
            "amostra_valores": [
                "          1",
                "          2",
                "          3",
                "         14",
                "          5",
                "          4",
                "          7",
                "         13",
                "         12",
                "         10"
            ],
            "maior_caractere": 11,
            "menor_caractere": 11,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "QT_APROV": {
            "tipo_dado": "object",
            "valores_unicos": 102,
            "valores_nulos": 0,
            "amostra_valores": [
                "          1",
                "          2",
                "          3",
                "         14",
                "          5",
                "          4",
                "          7",
                "         13",
                "         12",
                "         10"
            ],
            "maior_caractere": 11,
            "menor_caractere": 11,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "VL_APRES": {
            "tipo_dado": "object",
            "valores_unicos": 1213,
            "valores_nulos": 0,
            "amostra_valores": [
                "                6.35",
                "                4.67",
                "               18.00",
                "               24.20",
                "               24.24",
                "              136.41",
                "                7.85",
                "                2.25",
                "                2.01",
                "                1.85"
            ],
            "maior_caractere": 20,
            "menor_caractere": 20,
            "has_leading_zeros": False,
            "has_special_chars": True,
            "has_mixed_types": False
        },
        "VL_APROV": {
            "tipo_dado": "object",
            "valores_unicos": 1213,
            "valores_nulos": 0,
            "amostra_valores": [
                "                6.35",
                "                4.67",
                "               18.00",
                "               24.20",
                "               24.24",
                "              136.41",
                "                7.85",
                "                2.25",
                "                2.01",
                "                1.85"
            ],
            "maior_caractere": 20,
            "menor_caractere": 20,
            "has_leading_zeros": False,
            "has_special_chars": True,
            "has_mixed_types": False
        },
        "UFDIF": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "0",
                "1"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "MNDIF": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "0",
                "1"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "ETNIA": {
            "tipo_dado": "object",
            "valores_unicos": 86,
            "valores_nulos": 0,
            "amostra_valores": [
                "    ",
                "0001",
                "0222",
                "0059",
                "0181",
                "X363",
                "X290",
                "0038",
                "0251",
                "0172"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "NAT_JUR": {
            "tipo_dado": "object",
            "valores_unicos": 16,
            "valores_nulos": 0,
            "amostra_valores": [
                "3069",
                "3999",
                "1031",
                "1244",
                "1023",
                "2062",
                "2135",
                "2232",
                "2240",
                "1112"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        }
    }
    }

    # Processar dados
    tipo_coluna_map = processar_dados(dados)

    # Exibir o mapeamento final
    for tabela, colunas in tipo_coluna_map.items():
        print(f"Tabela: {tabela}")
        for coluna, tipo in colunas.items():
            print(f"  {coluna}: {tipo}")
        print()

        # Salvar o dicionário tipo_coluna_map em um arquivo JSON
    with open('tipo_coluna_map.json', 'w', encoding='utf-8') as f:
        json.dump(tipo_coluna_map, f, ensure_ascii=False, indent=4)


