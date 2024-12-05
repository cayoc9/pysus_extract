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
                "valores_unicos": 1,
                "valores_nulos": 0,
                "amostra_valores": [
                    "202303"
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
                "valores_unicos": 2,
                "valores_nulos": 0,
                "amostra_valores": [
                    "350000",
                    "353870"
                ],
                "maior_caractere": 6,
                "menor_caractere": 6,
                "has_leading_zeros": False,
                "has_special_chars": False,
                "has_mixed_types": False
            },
            "AP_CODUNI": {
                "tipo_dado": "object",
                "valores_unicos": 8,
                "valores_nulos": 0,
                "amostra_valores": [
                    "2077396",
                    "2077477",
                    "2080532",
                    "2748029",
                    "2087057",
                    "2025507",
                    "2748223",
                    "2688689"
                ],
                "maior_caractere": 7,
                "menor_caractere": 7,
                "has_leading_zeros": False,
                "has_special_chars": False,
                "has_mixed_types": False
            },
            "AP_AUTORIZ": {
                "tipo_dado": "object",
                "valores_unicos": 817,
                "valores_nulos": 0,
                "amostra_valores": [
                    "3523222905521",
                    "3523234810546",
                    "3523222876987",
                    "3523221415241",
                    "3523224716154",
                    "3523224771286",
                    "3523230938172",
                    "3523225642530",
                    "3523230938249",
                    "3523230938282"
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
                    "202303",
                    "202302",
                    "202301"
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
                "valores_unicos": 7,
                "valores_nulos": 0,
                "amostra_valores": [
                    "354980",
                    "355030",
                    "354140",
                    "354990",
                    "353870",
                    "352900",
                    "350750"
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
                "valores_unicos": 8,
                "valores_nulos": 0,
                "amostra_valores": [
                    "60003761000129",
                    "60742616000160",
                    "55344337000108",
                    "45186053000187",
                    "54384631000261",
                    "09161265000146",
                    "46230439000101",
                    "62779145000190"
                ],
                "maior_caractere": 14,
                "menor_caractere": 14,
                "has_leading_zeros": True,
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
                "valores_unicos": 816,
                "valores_nulos": 0,
                "amostra_valores": [
                    "{{{{{{{",
                    "{|{{|~{}",
                    "{|{{}~{~}",
                    "{{}{}||~",
                    "{{~{{{~",
                    "{}{||{",
                    "{{{{~{{{",
                    "{}{~",
                    "{}~{|}{|~",
                    "{{{~{}"
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
                    "43",
                    "38",
                    "51",
                    "37",
                    "46",
                    "45",
                    "29",
                    "35",
                    "41",
                    "40"
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
                "valores_unicos": 4,
                "valores_nulos": 0,
                "amostra_valores": [
                    "01",
                    "03",
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
                "valores_unicos": 153,
                "valores_nulos": 0,
                "amostra_valores": [
                    "353030",
                    "350900",
                    "355710",
                    "355500",
                    "352630",
                    "352440",
                    "354515",
                    "353870",
                    "354390",
                    "352740"
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
                "valores_unicos": 621,
                "valores_nulos": 0,
                "amostra_valores": [
                    "15135226",
                    "07745165",
                    "15500004",
                    "17600100",
                    "12130000",
                    "12307590",
                    "13440005",
                    "13409085",
                    "13402298",
                    "13400550"
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
                "valores_unicos": 62,
                "valores_nulos": 0,
                "amostra_valores": [
                    "20230307",
                    "20230310",
                    "20230208",
                    "20230127",
                    "20230103",
                    "20230301",
                    "20230201",
                    "20230330",
                    "20230116",
                    "20230111"
                ],
                "maior_caractere": 8,
                "menor_caractere": 8,
                "has_leading_zeros": False,
                "has_special_chars": False,
                "has_mixed_types": False
            },
            "AP_DTFIM": {
                "tipo_dado": "object",
                "valores_unicos": 3,
                "valores_nulos": 0,
                "amostra_valores": [
                    "20230531",
                    "20230430",
                    "20230331"
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
                "valores_unicos": 3,
                "valores_nulos": 0,
                "amostra_valores": [
                    "21",
                    "15",
                    "18"
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
            "AP_DTOOCOR": {
                "tipo_dado": "object",
                "valores_unicos": 3,
                "valores_nulos": 0,
                "amostra_valores": [
                    "        ",
                    "20230331",
                    "20230301"
                ],
                "maior_caractere": 8,
                "menor_caractere": 8,
                "has_leading_zeros": False,
                "has_special_chars": False,
                "has_mixed_types": True
            },
            "AP_CODEMI": {
                "tipo_dado": "object",
                "valores_unicos": 7,
                "valores_nulos": 0,
                "amostra_valores": [
                    "E350000022",
                    "E350000027",
                    "E350000016",
                    "E350000030",
                    "M353870001",
                    "E350000029",
                    "S352748223"
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
                "valores_unicos": 6,
                "valores_nulos": 0,
                "amostra_valores": [
                    "2077396",
                    "2077477",
                    "2080532",
                    "0000000",
                    "2087057",
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
                "valores_unicos": 64,
                "valores_nulos": 0,
                "amostra_valores": [
                    "20230307",
                    "20230310",
                    "20230208",
                    "20230127",
                    "20230103",
                    "20230329",
                    "20230326",
                    "20230328",
                    "20230330",
                    "20230116"
                ],
                "maior_caractere": 8,
                "menor_caractere": 8,
                "has_leading_zeros": False,
                "has_special_chars": False,
                "has_mixed_types": False
            },
            "AP_DTAUT": {
                "tipo_dado": "object",
                "valores_unicos": 61,
                "valores_nulos": 0,
                "amostra_valores": [
                    "20230307",
                    "20230310",
                    "20230208",
                    "20230130",
                    "20230125",
                    "20230316",
                    "20230331",
                    "20230330",
                    "20230116",
                    "20230111"
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
                "valores_unicos": 397,
                "valores_nulos": 0,
                "amostra_valores": [
                    "20221202",
                    "20230215",
                    "20220819",
                    "20230113",
                    "20210812",
                    "20221007",
                    "20220628",
                    "20230123",
                    "20221122",
                    "20220615"
                ],
                "maior_caractere": 8,
                "menor_caractere": 8,
                "has_leading_zeros": False,
                "has_special_chars": False,
                "has_mixed_types": False
            },
            "AB_NUMAIH": {
                "tipo_dado": "object",
                "valores_unicos": 813,
                "valores_nulos": 0,
                "amostra_valores": [
                    "3522121235920",
                    "3523100738355",
                    "3522116345792",
                    "3523100353828",
                    "3521117689166",
                    "3522123988581",
                    "3523230938172",
                    "3523225642530",
                    "3523230938249",
                    "3523230938282"
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
                "valores_unicos": 28,
                "valores_nulos": 0,
                "amostra_valores": [
                    "03",
                    "02",
                    "01",
                    "09",
                    "04",
                    "36",
                    "17",
                    "60",
                    "18",
                    "12"
                ],
                "maior_caractere": 2,
                "menor_caractere": 2,
                "has_leading_zeros": True,
                "has_special_chars": False,
                "has_mixed_types": False
            },
            "AB_ANOACOM": {
                "tipo_dado": "object",
                "valores_unicos": 3,
                "valores_nulos": 0,
                "amostra_valores": [
                    "2",
                    " ",
                    "0"
                ],
                "maior_caractere": 1,
                "menor_caractere": 1,
                "has_leading_zeros": False,
                "has_special_chars": False,
                "has_mixed_types": True
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
                    "0",
                    "1"
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
                    "0",
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
                "valores_unicos": 2,
                "valores_nulos": 0,
                "amostra_valores": [
                    "3069",
                    "3999"
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
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "202303"
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
            "valores_unicos": 52,
            "valores_nulos": 0,
            "amostra_valores": [
                "350000",
                "350320",
                "350400",
                "350450",
                "350550",
                "350570",
                "350960",
                "351440",
                "351630",
                "351640"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CODUNI": {
            "tipo_dado": "object",
            "valores_unicos": 116,
            "valores_nulos": 0,
            "amostra_valores": [
                "2026449",
                "2064502",
                "2069296",
                "2077396",
                "2077477",
                "2081377",
                "2089327",
                "2092328",
                "2705982",
                "2705788"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_AUTORIZ": {
            "tipo_dado": "object",
            "valores_unicos": 585,
            "valores_nulos": 0,
            "amostra_valores": [
                "3523228143457",
                "3523227917935",
                "3523223367917",
                "3523223367928",
                "3523222481031",
                "3523222481042",
                "3523222923671",
                "3523234824362",
                "3523223366993",
                "3523222831458"
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
                "202303",
                "202302",
                "202301",
                "202212"
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
                "              915.76",
                "             1515.76",
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
            "valores_unicos": 75,
            "valores_nulos": 0,
            "amostra_valores": [
                "350950",
                "354980",
                "354910",
                "355030",
                "355710",
                "351110",
                "355220",
                "351620",
                "352590",
                "354850"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_TPUPS": {
            "tipo_dado": "object",
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "36",
                "05",
                "04",
                "39"
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
            "valores_unicos": 116,
            "valores_nulos": 0,
            "amostra_valores": [
                "03777561000190",
                "46905121000183",
                "00856003000202",
                "60003761000129",
                "60742616000160",
                "72957814000120",
                "47074851000819",
                "67363028000164",
                "47969134000189",
                "70947213000372"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CNPJMNT": {
            "tipo_dado": "object",
            "valores_unicos": 6,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000000000000 ",
                "46374500000194",
                "46523015000135",
                "46177531000155",
                "60499365000134",
                "46068425000133"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CNSPCN": {
            "tipo_dado": "object",
            "valores_unicos": 582,
            "valores_nulos": 0,
            "amostra_valores": [
                "{{}{}~{|}",
                "{{{~|}~",
                "{{|}{~",
                "{|~~}}",
                "{{{|{~{{{",
                "|}~~{~}~{{|",
                "{{{{|}",
                "{|{|||",
                "{{~}|",
                "{{{{|"
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
            "valores_unicos": 70,
            "valores_nulos": 0,
            "amostra_valores": [
                "49",
                "59",
                "68",
                "46",
                "60",
                "39",
                "44",
                "72",
                "52",
                "70"
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
            "valores_unicos": 5,
            "valores_nulos": 0,
            "amostra_valores": [
                "03",
                "01",
                "02",
                "99",
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
            "valores_unicos": 219,
            "valores_nulos": 0,
            "amostra_valores": [
                "350950",
                "354980",
                "350030",
                "354910",
                "353030",
                "355030",
                "355710",
                "353510",
                "353780",
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
            "valores_unicos": 5,
            "valores_nulos": 0,
            "amostra_valores": [
                "010",
                "10 ",
                "213",
                "022",
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
            "valores_unicos": 554,
            "valores_nulos": 0,
            "amostra_valores": [
                "13053234",
                "13067290",
                "15084130",
                "15042238",
                "13860640",
                "13876429",
                "15130236",
                "08410270",
                "15501015",
                "15828000"
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
            "valores_unicos": 42,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230329",
                "20230309",
                "20230321",
                "20230314",
                "20230301",
                "20230317",
                "20230306",
                "20230324",
                "20230310",
                "20230130"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTFIM": {
            "tipo_dado": "object",
            "valores_unicos": 40,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230331",
                "20230531",
                "20230317",
                "20230306",
                "20230314",
                "20230429",
                "20230324",
                "20230301",
                "20230302",
                "20230304"
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
            "valores_unicos": 7,
            "valores_nulos": 0,
            "amostra_valores": [
                "18",
                "11",
                "15",
                "12",
                "51",
                "26",
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
            "valores_unicos": 37,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230329",
                "20230309",
                "20230331",
                "20230317",
                "20230306",
                "20230314",
                "20230324",
                "20230301",
                "20230310",
                "20230321"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CODEMI": {
            "tipo_dado": "object",
            "valores_unicos": 70,
            "valores_nulos": 0,
            "amostra_valores": [
                "E350000012",
                "E350000022",
                "E350000020",
                "E350000027",
                "E350000023",
                "E350000013",
                "E350000019",
                "E350000030",
                "S352748223",
                "E350000016"
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
            "valores_unicos": 9,
            "valores_nulos": 0,
            "amostra_valores": [
                "000000000000 ",
                "0000000000000",
                "3523232557471",
                "3523231856584",
                "3522280236244",
                "3523214513380",
                "3523231542039",
                "3523232557482",
                "3523214513918"
            ],
            "maior_caractere": 13,
            "menor_caractere": 13,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_UNISOL": {
            "tipo_dado": "object",
            "valores_unicos": 111,
            "valores_nulos": 0,
            "amostra_valores": [
                "2026449",
                "2064502",
                "2069296",
                "2077396",
                "2077477",
                "2081377",
                "2089327",
                "2092328",
                "2705982",
                "2705788"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTSOLIC": {
            "tipo_dado": "object",
            "valores_unicos": 49,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230329",
                "20230309",
                "20230321",
                "20230314",
                "20230301",
                "20230302",
                "20230317",
                "20230306",
                "20230324",
                "20230130"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_DTAUT": {
            "tipo_dado": "object",
            "valores_unicos": 44,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230329",
                "20230309",
                "20230321",
                "20230314",
                "20230404",
                "20230317",
                "20230306",
                "20230328",
                "20230324",
                "20230301"
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
                "N180",
                "I10 ",
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
        "ACF_DUPLEX": {
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
                "N",
                "S"
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
            "valores_unicos": 38,
            "valores_nulos": 0,
            "amostra_valores": [
                "0002",
                "03,5",
                "3   ",
                "0003",
                "0   ",
                "0,04",
                "0004",
                "4   ",
                "0001",
                "0000"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": True,
            "has_mixed_types": True
        },
        "ACF_ARTDIA": {
            "tipo_dado": "object",
            "valores_unicos": 39,
            "valores_nulos": 0,
            "amostra_valores": [
                "0003",
                "2   ",
                "0002",
                "0   ",
                "0,05",
                "0006",
                "3   ",
                "0001",
                "0000",
                "0052"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": True,
            "has_mixed_types": True
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
            "valores_unicos": 8,
            "valores_nulos": 0,
            "amostra_valores": [
                "2062",
                "3069",
                "3999",
                "1023",
                "2240",
                "1244",
                "1112",
                "2054"
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
            "valores_unicos": 104,
            "valores_nulos": 0,
            "amostra_valores": [
                "350000",
                "350320",
                "354340",
                "354850",
                "355030",
                "350340",
                "350570",
                "350760",
                "350810",
                "352590"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CODUNI": {
            "tipo_dado": "object",
            "valores_unicos": 408,
            "valores_nulos": 0,
            "amostra_valores": [
                "7033702",
                "9425802",
                "2082527",
                "2084414",
                "9567674",
                "2091550",
                "7096712",
                "2688638",
                "0404853",
                "2068974"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_AUTORIZ": {
            "tipo_dado": "object",
            "valores_unicos": 85544,
            "valores_nulos": 0,
            "amostra_valores": [
                "3523264031045",
                "3523248070265",
                "3523248071080",
                "3523247898720",
                "3523229141993",
                "3523229154324",
                "3523270778269",
                "3523265216416",
                "3523268414413",
                "3523258645225"
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
                "202309",
                "202308",
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
            "valores_unicos": 142,
            "valores_nulos": 0,
            "amostra_valores": [
                "0303050233",
                "0405030045",
                "0405030134",
                "0211060283",
                "0301110026",
                "0414010370",
                "0405050143",
                "0405050151",
                "0701010320",
                "0409050083"
            ],
            "maior_caractere": 10,
            "menor_caractere": 10,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_VL_AP": {
            "tipo_dado": "object",
            "valores_unicos": 588,
            "valores_nulos": 0,
            "amostra_valores": [
                "                0.00",
                "              627.28",
                "              215.22",
                "              107.61",
                "              381.08",
                "               48.00",
                "              150.00",
                "               10.50",
                "              252.00",
                "              902.95"
            ],
            "maior_caractere": 20,
            "menor_caractere": 20,
            "has_leading_zeros": False,
            "has_special_chars": True,
            "has_mixed_types": False
        },
        "AP_UFMUN": {
            "tipo_dado": "object",
            "valores_unicos": 131,
            "valores_nulos": 0,
            "amostra_valores": [
                "352410",
                "353870",
                "350320",
                "354340",
                "354850",
                "355030",
                "350950",
                "354980",
                "355220",
                "351390"
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
                "36",
                "05",
                "07",
                "62",
                "04",
                "39"
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
            "valores_unicos": 370,
            "valores_nulos": 0,
            "amostra_valores": [
                "46374500019456",
                "46374500027203",
                "43964931000112",
                "55989784000114",
                "25333751000150",
                "03456304000156",
                "67187070000252",
                "05095474000188",
                "46374500028285",
                "46374500022244"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CNPJMNT": {
            "tipo_dado": "object",
            "valores_unicos": 50,
            "valores_nulos": 0,
            "amostra_valores": [
                "46374500000194",
                "0000000000000 ",
                "46068425000133",
                "46523015000135",
                "57571275000100",
                "46523239000147",
                "60499365000134",
                "45132495000140",
                "51816247000111",
                "46177531000155"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CNSPCN": {
            "tipo_dado": "object",
            "valores_unicos": 75732,
            "valores_nulos": 0,
            "amostra_valores": [
                "{{{~|{{~",
                "{~{|{~",
                "{{{{}}{",
                "}{{{{{{{{|",
                "{{|}|",
                "{{~~|",
                "{{}{~|",
                "{{}~{{",
                "{{{{{~~",
                "{|{{~}"
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
                "3",
                "5",
                "2"
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
                "74",
                "70",
                "62",
                "75",
                "82",
                "72",
                "77",
                "67",
                "78",
                "64"
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
            "valores_unicos": 5,
            "valores_nulos": 0,
            "amostra_valores": [
                "03",
                "01",
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
        "AP_MUNPCN": {
            "tipo_dado": "object",
            "valores_unicos": 1407,
            "valores_nulos": 0,
            "amostra_valores": [
                "351620",
                "353870",
                "354400",
                "354620",
                "350170",
                "354890",
                "354340",
                "354850",
                "355030",
                "350760"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_UFNACIO": {
            "tipo_dado": "object",
            "valores_unicos": 32,
            "valores_nulos": 0,
            "amostra_valores": [
                "010",
                " 10",
                "10 ",
                "039",
                "050",
                "022",
                "035",
                "041",
                "245",
                "090"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CEPPCN": {
            "tipo_dado": "object",
            "valores_unicos": 44952,
            "valores_nulos": 0,
            "amostra_valores": [
                "14405415",
                "13417550",
                "13392140",
                "13626042",
                "14820082",
                "13569000",
                "14080040",
                "11080570",
                "03182050",
                "02810000"
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
            "valores_unicos": 155,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230828",
                "20231025",
                "20231031",
                "20231007",
                "20230801",
                "20230901",
                "20231023",
                "20231011",
                "20231016",
                "20230803"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTFIM": {
            "tipo_dado": "object",
            "valores_unicos": 115,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231031",
                "20231231",
                "20231130",
                "20231205",
                "20231013",
                "20231024",
                "20230930",
                "20231030",
                "20230921",
                "20231005"
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
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "2",
                "1",
                "3",
                "4"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_MOTSAI": {
            "tipo_dado": "object",
            "valores_unicos": 14,
            "valores_nulos": 0,
            "amostra_valores": [
                "28",
                "21",
                "18",
                "12",
                "11",
                "15",
                "14",
                "51",
                "26",
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
            "valores_unicos": 105,
            "valores_nulos": 0,
            "amostra_valores": [
                "        ",
                "20231023",
                "20231002",
                "20231003",
                "20231025",
                "20231005",
                "20231013",
                "20231031",
                "20231024",
                "20231006"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_CODEMI": {
            "tipo_dado": "object",
            "valores_unicos": 178,
            "valores_nulos": 0,
            "amostra_valores": [
                "E350000013",
                "E350000015",
                "M350320008",
                "M354340001",
                "M354850001",
                "M355030001",
                "E350000027",
                "E350000022",
                "S352078015",
                "E350000023"
            ],
            "maior_caractere": 10,
            "menor_caractere": 10,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CATEND": {
            "tipo_dado": "object",
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "01",
                "02",
                "03",
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
            "valores_unicos": 1170,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000000000000",
                "000000000000 ",
                "3523215435025",
                "3523215429536",
                "3523215427831",
                "3523215423090",
                "0            ",
                "3523274777980",
                "3523249078184",
                "3523249078404"
            ],
            "maior_caractere": 13,
            "menor_caractere": 13,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_UNISOL": {
            "tipo_dado": "object",
            "valores_unicos": 428,
            "valores_nulos": 0,
            "amostra_valores": [
                "7033702",
                "0000000",
                "2082527",
                "9567674",
                "2091550",
                "7096712",
                "2688638",
                "404853 ",
                "000000 ",
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
            "valores_unicos": 525,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230831",
                "20231025",
                "20231031",
                "20231007",
                "20230808",
                "20230905",
                "20230720",
                "20231023",
                "20231010",
                "20230901"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTAUT": {
            "tipo_dado": "object",
            "valores_unicos": 257,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230831",
                "20231030",
                "20231101",
                "20231011",
                "20230930",
                "20230809",
                "20231024",
                "20230901",
                "20231016",
                "20231225"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CIDCAS": {
            "tipo_dado": "object",
            "valores_unicos": 144,
            "valores_nulos": 0,
            "amostra_valores": [
                "H353",
                "0000",
                "H360",
                "H330",
                "H402",
                "H112",
                "H903",
                "G710",
                "I209",
                "I200"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_CIDPRI": {
            "tipo_dado": "object",
            "valores_unicos": 432,
            "valores_nulos": 0,
            "amostra_valores": [
                "H353",
                "H360",
                "H368",
                "H330",
                "H430",
                "T231",
                "Q386",
                "H189",
                "H264",
                "G809"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CIDSEC": {
            "tipo_dado": "object",
            "valores_unicos": 180,
            "valores_nulos": 0,
            "amostra_valores": [
                "H353",
                "0000",
                "H360",
                "H330",
                "H402",
                "H119",
                "H112",
                "H903",
                "G710",
                "I200"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_ETNIA": {
            "tipo_dado": "object",
            "valores_unicos": 10,
            "valores_nulos": 0,
            "amostra_valores": [
                "    ",
                "0006",
                "0207",
                "0058",
                "0002",
                "0181",
                "0015",
                "0001",
                "0222",
                "0188"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_NATJUR": {
            "tipo_dado": "object",
            "valores_unicos": 14,
            "valores_nulos": 0,
            "amostra_valores": [
                "1023",
                "3999",
                "3069",
                "1112",
                "1244",
                "2240",
                "1279",
                "2062",
                "1031",
                "1120"
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
            "valores_unicos": 39,
            "valores_nulos": 0,
            "amostra_valores": [
                "3890538",
                "3933822",
                "5450616",
                "5968607",
                "6009492",
                "6352782",
                "6055117",
                "6544290",
                "6578640",
                "7661568"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_AUTORIZ": {
            "tipo_dado": "object",
            "valores_unicos": 1106581,
            "valores_nulos": 0,
            "amostra_valores": [
                "3523271717890",
                "3523271739230",
                "3523256280830",
                "3523270560007",
                "3523256217932",
                "3523256261976",
                "3523256262207",
                "3523244704683",
                "3523251375380",
                "3523251374478"
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
                "202309",
                "202308",
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
            "valores_unicos": 284,
            "valores_nulos": 0,
            "amostra_valores": [
                "0604280076",
                "0604270038",
                "0604270054",
                "0604270062",
                "0604270070",
                "0604270089",
                "0604710011",
                "0604390092",
                "0604040024",
                "0604040032"
            ],
            "maior_caractere": 10,
            "menor_caractere": 10,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_VL_AP": {
            "tipo_dado": "object",
            "valores_unicos": 816,
            "valores_nulos": 0,
            "amostra_valores": [
                "      210.60",
                "      242.10",
                "        0.00",
                "     4756.28",
                "    11011.20",
                "     7340.80",
                "     9775.80",
                "      771.60",
                "      529.80",
                "     3342.60"
            ],
            "maior_caractere": 12,
            "menor_caractere": 12,
            "has_leading_zeros": False,
            "has_special_chars": True,
            "has_mixed_types": False
        },
        "AP_UFMUN": {
            "tipo_dado": "object",
            "valores_unicos": 28,
            "valores_nulos": 0,
            "amostra_valores": [
                "353440",
                "354260",
                "351620",
                "355220",
                "352900",
                "351640",
                "354980",
                "354140",
                "355030",
                "351880"
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
                "43",
                "36"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": False,
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
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
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
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "46374500000194"
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
                "46374500000194"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CNSPCN": {
            "tipo_dado": "object",
            "valores_unicos": 970672,
            "valores_nulos": 0,
            "amostra_valores": [
                "{{{",
                "{{~~{|~",
                "{~}{{",
                "{{{~{{|",
                "{{|{|~}|",
                "{{{~~}|",
                "}{~|}{~{{{{",
                "{{~{}~|",
                "{{{{|}~",
                "{{{{{~~{}"
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
                "3",
                "5",
                "2"
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
                "73",
                "47",
                "54",
                "62",
                "66",
                "84",
                "63",
                "58",
                "76",
                "65"
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
        "AP_MUNPCN": {
            "tipo_dado": "object",
            "valores_unicos": 1381,
            "valores_nulos": 0,
            "amostra_valores": [
                "351060",
                "353440",
                "352620",
                "352250",
                "352220",
                "354730",
                "350570",
                "354260",
                "352610",
                "352030"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_UFNACIO": {
            "tipo_dado": "object",
            "valores_unicos": 131,
            "valores_nulos": 0,
            "amostra_valores": [
                "010",
                "045",
                "021",
                "035",
                "042",
                "165",
                "104",
                "217",
                "264",
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
            "valores_unicos": 183111,
            "valores_nulos": 0,
            "amostra_valores": [
                "06322010",
                "06160180",
                "06110270",
                "06332425",
                "06950000",
                "06693590",
                "06685130",
                "06855695",
                "06501001",
                "06537120"
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
            "valores_unicos": 119,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231011",
                "20231004",
                "20230911",
                "20230901",
                "20230811",
                "20230815",
                "20231015",
                "20230808",
                "20230920",
                "20230802"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTFIM": {
            "tipo_dado": "object",
            "valores_unicos": 5,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231231",
                "20231130",
                "20231031",
                "20230930",
                "20230831"
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
            "valores_unicos": 7,
            "valores_nulos": 0,
            "amostra_valores": [
                "28",
                "18",
                "21",
                "16",
                "11",
                "31",
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
            "valores_unicos": 31,
            "valores_nulos": 0,
            "amostra_valores": [
                "        ",
                "20231001",
                "20231005",
                "20231006",
                "20231009",
                "20231004",
                "20231002",
                "20231010",
                "20231025",
                "20231016"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_CODEMI": {
            "tipo_dado": "object",
            "valores_unicos": 22,
            "valores_nulos": 0,
            "amostra_valores": [
                "E350000027",
                "E350000030",
                "S355450616",
                "E350000023",
                "E350000029",
                "E350000022",
                "E350000016",
                "S357661568",
                "S359784438",
                "S359898778"
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
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000000",
                "6503829",
                "2078287"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTSOLIC": {
            "tipo_dado": "object",
            "valores_unicos": 317,
            "valores_nulos": 0,
            "amostra_valores": [
                "        ",
                "20230830",
                "20230801",
                "20230904",
                "20231011",
                "20230616",
                "20230502",
                "20230704",
                "20230809",
                "20230523"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_DTAUT": {
            "tipo_dado": "object",
            "valores_unicos": 92,
            "valores_nulos": 0,
            "amostra_valores": [
                "        ",
                "20230830",
                "20230921",
                "20230906",
                "20231011",
                "20230811",
                "20230814",
                "20231017",
                "20230801",
                "20230810"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
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
            "valores_unicos": 362,
            "valores_nulos": 0,
            "amostra_valores": [
                "J448",
                "J440",
                "J450",
                "J441",
                "E780",
                "E781",
                "E782",
                "E788",
                "E785",
                "E784"
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
            "valores_unicos": 13,
            "valores_nulos": 0,
            "amostra_valores": [
                "    ",
                "0204",
                "0076",
                "0001",
                "0006",
                "0002",
                "0005",
                "0264",
                "0188",
                "0070"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AM_PESO": {
            "tipo_dado": "object",
            "valores_unicos": 340,
            "valores_nulos": 0,
            "amostra_valores": [
                "070",
                "078",
                "068",
                "089",
                "091",
                "052",
                "084",
                "062",
                "043",
                "065"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AM_ALTURA": {
            "tipo_dado": "object",
            "valores_unicos": 213,
            "valores_nulos": 0,
            "amostra_valores": [
                "160",
                "163",
                "154",
                "153",
                "164",
                "150",
                "165",
                "141",
                "170",
                "158"
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
                "03"
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
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "1023"
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
            "valores_unicos": 7,
            "valores_nulos": 0,
            "amostra_valores": [
                "350000",
                "350960",
                "351050",
                "351630",
                "353870",
                "354100",
                "351500"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CODUNI": {
            "tipo_dado": "object",
            "valores_unicos": 8,
            "valores_nulos": 0,
            "amostra_valores": [
                "2082187",
                "2090244",
                "9189564",
                "9037179",
                "9716351",
                "2772310",
                "7919697",
                "9904050"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_AUTORIZ": {
            "tipo_dado": "object",
            "valores_unicos": 679,
            "valores_nulos": 0,
            "amostra_valores": [
                "3523233695872",
                "3523233695993",
                "3523233696147",
                "3523233690790",
                "3523233690988",
                "3523217074927",
                "3523217086532",
                "3523217074993",
                "3523217075169",
                "3523217075257"
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
                "202310",
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
                "               61.00",
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
            "valores_unicos": 8,
            "valores_nulos": 0,
            "amostra_valores": [
                "354340",
                "350610",
                "350960",
                "351050",
                "351630",
                "353870",
                "354100",
                "351500"
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
            "valores_unicos": 8,
            "valores_nulos": 0,
            "amostra_valores": [
                "57722118000140",
                "64923618000106",
                "08896723000203",
                "04666985000220",
                "28937926000127",
                "54370630000187",
                "46177531000155",
                "29551460000190"
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
                "46177531000155"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CNSPCN": {
            "tipo_dado": "object",
            "valores_unicos": 675,
            "valores_nulos": 0,
            "amostra_valores": [
                "{}{{}}~",
                "{}{{{|}}",
                "{}{|}|||~}",
                "{{{~{}{~",
                "{|~~{|}",
                "{{|}~{|{",
                "}{|}|}{{{",
                "{{~~~}",
                "|}~{||}{{{~",
                "{~{~|~{{"
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
                "89",
                "49",
                "69",
                "50",
                "87",
                "65",
                "63",
                "84",
                "74",
                "85"
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
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "01",
                "03",
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
            "valores_unicos": 74,
            "valores_nulos": 0,
            "amostra_valores": [
                "354340",
                "354760",
                "354750",
                "351310",
                "350610",
                "353150",
                "355310",
                "355365",
                "355440",
                "355680"
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
                "045"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CEPPCN": {
            "tipo_dado": "object",
            "valores_unicos": 506,
            "valores_nulos": 0,
            "amostra_valores": [
                "14055560",
                "14270000",
                "14070110",
                "13670000",
                "14140000",
                "14702168",
                "14708082",
                "14730000",
                "14725000",
                "14700590"
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
            "valores_unicos": 38,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231001",
                "20231011",
                "20231005",
                "20231025",
                "20231031",
                "20231020",
                "20231030",
                "20231018",
                "20230929",
                "20231002"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTFIM": {
            "tipo_dado": "object",
            "valores_unicos": 5,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231231",
                "20231031",
                "20230930",
                "20231130",
                "20231006"
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
                "18",
                "15",
                "21",
                "51",
                "26"
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
            "valores_unicos": 31,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231031",
                "20231001",
                "20231011",
                "20231030",
                "20231018",
                "20230929",
                "20231002",
                "20231016",
                "        ",
                "20231017"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_CODEMI": {
            "tipo_dado": "object",
            "valores_unicos": 8,
            "valores_nulos": 0,
            "amostra_valores": [
                "S352082187",
                "E350000009",
                "M350960001",
                "M351050001",
                "M351630001",
                "M353870901",
                "M354100001",
                "M351500001"
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
            "valores_unicos": 8,
            "valores_nulos": 0,
            "amostra_valores": [
                "2082187",
                "2090244",
                "9189564",
                "9037179",
                "9716351",
                "2772310",
                "7919697",
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
            "valores_unicos": 39,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231001",
                "20231011",
                "20231005",
                "20231025",
                "20231031",
                "20231020",
                "20231030",
                "20231026",
                "20231016",
                "20231018"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTAUT": {
            "tipo_dado": "object",
            "valores_unicos": 37,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231001",
                "20231011",
                "20231005",
                "20231025",
                "20231031",
                "20231020",
                "20231030",
                "20231026",
                "20231016",
                "20230930"
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
                "N180"
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
            "valores_unicos": 338,
            "valores_nulos": 0,
            "amostra_valores": [
                "NN0  0  ",
                "SN0  085",
                "NN0  106",
                "NN0  058",
                "NN000072",
                "NN162070",
                "NN155098",
                "NN179128",
                "NN160054",
                "NN156000"
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
            "valores_unicos": 5,
            "valores_nulos": 0,
            "amostra_valores": [
                "0",
                "1",
                "7",
                "2",
                "4"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AMP_MAISNE": {
            "tipo_dado": "object",
            "valores_unicos": 10,
            "valores_nulos": 0,
            "amostra_valores": [
                "0",
                "1",
                "9",
                "6",
                "5",
                "4",
                "8",
                "3",
                "7",
                "2"
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
                " ",
                "N",
                "R"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AMP_SEAPTO": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "I",
                "S",
                "N"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AMP_HB": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "I   ",
                "N   ",
                "S   "
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AMP_FOSFOR": {
            "tipo_dado": "object",
            "valores_unicos": 3,
            "valores_nulos": 0,
            "amostra_valores": [
                "I   ",
                "S   ",
                "N   "
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AMP_KTVSEM": {
            "tipo_dado": "object",
            "valores_unicos": 12,
            "valores_nulos": 0,
            "amostra_valores": [
                "1   ",
                "6   ",
                "0   ",
                "4   ",
                "8   ",
                "2   ",
                "9   ",
                "5   ",
                "3   ",
                ",   "
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": True,
            "has_mixed_types": True
        },
        "AMP_TRU": {
            "tipo_dado": "object",
            "valores_unicos": 91,
            "valores_nulos": 0,
            "amostra_valores": [
                "5,13",
                "6,81",
                "3,39",
                "6,07",
                "3,80",
                "0000",
                "3   ",
                "4   ",
                "5   ",
                "3000"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": True,
            "has_mixed_types": True
        },
        "AMP_ALBUMI": {
            "tipo_dado": "object",
            "valores_unicos": 46,
            "valores_nulos": 0,
            "amostra_valores": [
                "    ",
                "4,51",
                "4,27",
                "4,31",
                "4,30",
                "0000",
                "4   ",
                "5   ",
                "000,",
                "3   "
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": True,
            "has_mixed_types": True
        },
        "AMP_PTH": {
            "tipo_dado": "object",
            "valores_unicos": 118,
            "valores_nulos": 0,
            "amostra_valores": [
                "137,",
                "286,",
                "136,",
                "0   ",
                "228,",
                "0000",
                "106 ",
                "113 ",
                "36  ",
                "120 "
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": True,
            "has_mixed_types": True
        },
        "AMP_HIV": {
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
        "AMP_HCV": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                " ",
                "N"
            ],
            "maior_caractere": 1,
            "menor_caractere": 1,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AMP_HBSAG": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                " ",
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
        "AMP_SEPERI": {
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
        'sia_apac_nefrologia': {
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
            "valores_unicos": 27,
            "valores_nulos": 0,
            "amostra_valores": [
                "350000",
                "350320",
                "354990",
                "355030",
                "355220",
                "351840",
                "354890",
                "354910",
                "354980",
                "352690"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CODUNI": {
            "tipo_dado": "object",
            "valores_unicos": 71,
            "valores_nulos": 0,
            "amostra_valores": [
                "2090236",
                "2705982",
                "2790602",
                "6123740",
                "2082527",
                "0009601",
                "2077590",
                "2708779",
                "2077396",
                "2081695"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_AUTORIZ": {
            "tipo_dado": "object",
            "valores_unicos": 79717,
            "valores_nulos": 0,
            "amostra_valores": [
                "3523217084783",
                "3523264043200",
                "3523264034026",
                "3523240413605",
                "3523274409446",
                "3523272727623",
                "3523272656332",
                "3523263264840",
                "3523268174195",
                "3523274780410"
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
            "valores_unicos": 138,
            "valores_nulos": 0,
            "amostra_valores": [
                "0304040207",
                "0304050342",
                "0304020095",
                "0304020109",
                "0304020397",
                "0304040185",
                "0304050270",
                "0304050288",
                "0304050296",
                "0304050300"
            ],
            "maior_caractere": 10,
            "menor_caractere": 10,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_VL_AP": {
            "tipo_dado": "object",
            "valores_unicos": 96,
            "valores_nulos": 0,
            "amostra_valores": [
                "      301.50",
                "        0.00",
                "     2224.00",
                "     1400.00",
                "      800.00",
                "       34.10",
                "       17.00",
                "     1300.00",
                "     1715.60",
                "     2129.64"
            ],
            "maior_caractere": 12,
            "menor_caractere": 12,
            "has_leading_zeros": False,
            "has_special_chars": True,
            "has_mixed_types": False
        },
        "AP_UFMUN": {
            "tipo_dado": "object",
            "valores_unicos": 42,
            "valores_nulos": 0,
            "amostra_valores": [
                "350550",
                "351620",
                "350600",
                "355030",
                "350320",
                "354990",
                "355220",
                "354980",
                "352530",
                "350750"
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
                "07",
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
            "valores_unicos": 71,
            "valores_nulos": 0,
            "amostra_valores": [
                "49150352000112",
                "47969134000189",
                "46374500014810",
                "46374500016430",
                "43964931000112",
                "60194990000682",
                "62932942000165",
                "71485056000121",
                "60003761000129",
                "46374500001409"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CNPJMNT": {
            "tipo_dado": "object",
            "valores_unicos": 9,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000000000000 ",
                "46374500000194",
                "46068425000133",
                "46523239000147",
                "45781176000166",
                "57740490000180",
                "47018676000176",
                "45301264000113",
                "59307595000175"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CNSPCN": {
            "tipo_dado": "object",
            "valores_unicos": 78612,
            "valores_nulos": 0,
            "amostra_valores": [
                "{~{|{}",
                "{{{{~",
                "{~{}}}|",
                "{{{{}|",
                "{{{~|{}{",
                "{{}~~~~}|",
                "|{~{~{{|",
                "|{|{{{",
                "{|{{~",
                "{{}}}~"
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
                "66",
                "65",
                "76",
                "73",
                "82",
                "79",
                "53",
                "72",
                "58",
                "74"
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
            "valores_unicos": 1285,
            "valores_nulos": 0,
            "amostra_valores": [
                "313420",
                "353700",
                "351620",
                "350600",
                "355030",
                "355370",
                "354990",
                "355400",
                "354520",
                "350460"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_UFNACIO": {
            "tipo_dado": "object",
            "valores_unicos": 26,
            "valores_nulos": 0,
            "amostra_valores": [
                "010",
                "10 ",
                "045",
                "041",
                "264",
                "035",
                "022",
                "051",
                "039",
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
            "valores_unicos": 48225,
            "valores_nulos": 0,
            "amostra_valores": [
                "38305068",
                "14470000",
                "14401186",
                "17033500",
                "03250080",
                "15901308",
                "12241421",
                "12229072",
                "04831050",
                "18278717"
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
                "1",
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
            "valores_unicos": 139,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231010",
                "20230919",
                "20230901",
                "20230801",
                "20231004",
                "20231001",
                "20230929",
                "20230928",
                "20230803",
                "20230814"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTFIM": {
            "tipo_dado": "object",
            "valores_unicos": 20,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231231",
                "20231130",
                "20231031",
                "20231030",
                "20230930",
                "20230831",
                "20231229",
                "20230731",
                "20231230",
                "20231123"
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
                "21",
                "28",
                "18",
                "15",
                "26",
                "41",
                "43",
                "51",
                "42",
                "23"
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
            "valores_unicos": 61,
            "valores_nulos": 0,
            "amostra_valores": [
                "        ",
                "20231001",
                "20231020",
                "20231004",
                "20231015",
                "20231008",
                "20231002",
                "20231016",
                "20231031",
                "20231019"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_CODEMI": {
            "tipo_dado": "object",
            "valores_unicos": 50,
            "valores_nulos": 0,
            "amostra_valores": [
                "E350000009",
                "E350000013",
                "E350000028",
                "E350000027",
                "M350320008",
                "M350620001",
                "M355030001",
                "M355220001",
                "E350000022",
                "E350000023"
            ],
            "maior_caractere": 10,
            "menor_caractere": 10,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CATEND": {
            "tipo_dado": "object",
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "01",
                "02",
                "03",
                "06"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_APACANT": {
            "tipo_dado": "object",
            "valores_unicos": 2493,
            "valores_nulos": 0,
            "amostra_valores": [
                "000000000000 ",
                "0000000000000",
                "3523221542412",
                "3523257242549",
                "3523221567503",
                "3523221554083",
                "3523230770279",
                "3523258101980",
                "3523258353923",
                "3523221563125"
            ],
            "maior_caractere": 13,
            "menor_caractere": 13,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_UNISOL": {
            "tipo_dado": "object",
            "valores_unicos": 64,
            "valores_nulos": 0,
            "amostra_valores": [
                "2090236",
                "2705982",
                "2790602",
                "6123740",
                "2082527",
                "0009601",
                "2077590",
                "2708779",
                "2077396",
                "2081695"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTSOLIC": {
            "tipo_dado": "object",
            "valores_unicos": 193,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231010",
                "20230919",
                "20230901",
                "20230801",
                "20231004",
                "20231006",
                "20230929",
                "20231001",
                "20230928",
                "20230803"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTAUT": {
            "tipo_dado": "object",
            "valores_unicos": 175,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231109",
                "20230919",
                "20230901",
                "20230822",
                "20231023",
                "20231011",
                "20230801",
                "20230929",
                "20231001",
                "20230928"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CIDCAS": {
            "tipo_dado": "object",
            "valores_unicos": 82,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000",
                "C20 ",
                "C504",
                "C505",
                "C502",
                "C182",
                "C183",
                "C679",
                "C61 ",
                "D471"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_CIDPRI": {
            "tipo_dado": "object",
            "valores_unicos": 313,
            "valores_nulos": 0,
            "amostra_valores": [
                "C61 ",
                "C19 ",
                "C20 ",
                "C37 ",
                "C508",
                "C504",
                "C501",
                "C500",
                "C502",
                "C505"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CIDSEC": {
            "tipo_dado": "object",
            "valores_unicos": 38,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000",
                "C795",
                "Z511",
                "C501",
                "C61 ",
                "C56 ",
                "C504",
                "C773",
                "C349",
                "C780"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_ETNIA": {
            "tipo_dado": "object",
            "valores_unicos": 9,
            "valores_nulos": 0,
            "amostra_valores": [
                "    ",
                "0003",
                "0235",
                "0010",
                "0060",
                "0246",
                "0114",
                "0173",
                "0002"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AQ_CID10": {
            "tipo_dado": "object",
            "valores_unicos": 177,
            "valores_nulos": 0,
            "amostra_valores": [
                "    ",
                "C61 ",
                "C20 ",
                "C500",
                "C670",
                "C829",
                "C504",
                "C505",
                "C502",
                "C506"
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
                "2",
                "4",
                "3",
                "1",
                " ",
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
            "valores_unicos": 93,
            "valores_nulos": 0,
            "amostra_valores": [
                "10",
                "G3",
                " 8",
                "GX",
                "00",
                " 1",
                "4 ",
                "0 ",
                "8 ",
                " 0"
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AQ_DTIDEN": {
            "tipo_dado": "object",
            "valores_unicos": 5999,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230731",
                "20221214",
                "20220719",
                "20211026",
                "20230511",
                "20230615",
                "20230810",
                "20230219",
                "20230302",
                "20220101"
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
            "valores_unicos": 261,
            "valores_nulos": 0,
            "amostra_valores": [
                "    ",
                "C61 ",
                "C19 ",
                "C20 ",
                "C37 ",
                "C501",
                "C500",
                "C508",
                "C504",
                "C505"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AQ_DTINI1": {
            "tipo_dado": "object",
            "valores_unicos": 4602,
            "valores_nulos": 0,
            "amostra_valores": [
                "        ",
                "20220919",
                "20230123",
                "20140507",
                "20180208",
                "20220519",
                "20230711",
                "20170711",
                "20220324",
                "20190829"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AQ_CIDINI2": {
            "tipo_dado": "object",
            "valores_unicos": 208,
            "valores_nulos": 0,
            "amostra_valores": [
                "    ",
                "C61 ",
                "C19 ",
                "C20 ",
                "C508",
                "C504",
                "C505",
                "C500",
                "C900",
                "C539"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AQ_DTINI2": {
            "tipo_dado": "object",
            "valores_unicos": 3501,
            "valores_nulos": 0,
            "amostra_valores": [
                "        ",
                "20230215",
                "20230524",
                "20170313",
                "20221118",
                "20220419",
                "20220418",
                "20200108",
                "20220525",
                "20170320"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AQ_CIDINI3": {
            "tipo_dado": "object",
            "valores_unicos": 129,
            "valores_nulos": 0,
            "amostra_valores": [
                "    ",
                "C20 ",
                "C504",
                "C505",
                "C900",
                "C61 ",
                "C502",
                "C501",
                "C921",
                "C180"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AQ_DTINI3": {
            "tipo_dado": "object",
            "valores_unicos": 2394,
            "valores_nulos": 0,
            "amostra_valores": [
                "        ",
                "20200831",
                "20171115",
                "20230428",
                "20221214",
                "20220601",
                "20130708",
                "20211106",
                "20220713",
                "20220701"
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
            "valores_unicos": 3487,
            "valores_nulos": 0,
            "amostra_valores": [
                "20231010",
                "20230919",
                "20230901",
                "20230801",
                "20231004",
                "20231001",
                "20230501",
                "20230928",
                "20210909",
                "20201130"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AQ_ESQU_P1": {
            "tipo_dado": "object",
            "valores_unicos": 2111,
            "valores_nulos": 0,
            "amostra_valores": [
                "GOSSE",
                "Leupr",
                "AC DE",
                "LEUPR",
                "ELIGA",
                "LUPRO",
                "ZOLAD",
                "GOSER",
                "003 -",
                "BH.HO"
            ],
            "maior_caractere": 5,
            "menor_caractere": 5,
            "has_leading_zeros": False,
            "has_special_chars": True,
            "has_mixed_types": False
        },
        "AQ_TOTMPL": {
            "tipo_dado": "object",
            "valores_unicos": 207,
            "valores_nulos": 0,
            "amostra_valores": [
                "006",
                "100",
                "012",
                "003",
                "000",
                "030",
                "021",
                "036",
                "024",
                "065"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AQ_TOTMAU": {
            "tipo_dado": "object",
            "valores_unicos": 236,
            "valores_nulos": 0,
            "amostra_valores": [
                "000",
                "003",
                "006",
                "001",
                "027",
                "018",
                "029",
                "005",
                "015",
                "058"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AQ_ESQU_P2": {
            "tipo_dado": "object",
            "valores_unicos": 4890,
            "valores_nulos": 0,
            "amostra_valores": [
                "RRELINA   ",
                "orrelina22",
                " LEUPRO   ",
                "ORRELINA  ",
                "ORRELINA 7",
                "RD        ",
                "N         ",
                "EX        ",
                "ELINA 10,8",
                "O225MG    "
            ],
            "maior_caractere": 10,
            "menor_caractere": 10,
            "has_leading_zeros": False,
            "has_special_chars": True,
            "has_mixed_types": False
        },
        "AQ_MED01": {
            "tipo_dado": "object",
            "valores_unicos": 149,
            "valores_nulos": 0,
            "amostra_valores": [
                "003",
                "004",
                "165",
                "051",
                "078",
                "028",
                "037",
                "035",
                "120",
                "145"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AQ_MED02": {
            "tipo_dado": "object",
            "valores_unicos": 124,
            "valores_nulos": 0,
            "amostra_valores": [
                "   ",
                "021",
                "078",
                "020",
                "119",
                "051",
                "173",
                "069",
                "034",
                "145"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AQ_MED03": {
            "tipo_dado": "object",
            "valores_unicos": 101,
            "valores_nulos": 0,
            "amostra_valores": [
                "   ",
                "123",
                "051",
                "175",
                "173",
                "078",
                "029",
                "145",
                "046",
                "146"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AQ_MED04": {
            "tipo_dado": "object",
            "valores_unicos": 77,
            "valores_nulos": 0,
            "amostra_valores": [
                "   ",
                "175",
                "120",
                "138",
                "060",
                "179",
                "145",
                "139",
                "046",
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
            "valores_unicos": 52,
            "valores_nulos": 0,
            "amostra_valores": [
                "   ",
                "177",
                "139",
                "158",
                "020",
                "176",
                "006",
                "075",
                "092",
                "108"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AQ_MED06": {
            "tipo_dado": "object",
            "valores_unicos": 32,
            "valores_nulos": 0,
            "amostra_valores": [
                "   ",
                "008",
                "161",
                "179",
                "176",
                "158",
                "139",
                "112",
                "011",
                "092"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AQ_MED07": {
            "tipo_dado": "object",
            "valores_unicos": 27,
            "valores_nulos": 0,
            "amostra_valores": [
                "   ",
                "011",
                "176",
                "062",
                "013",
                "045",
                "108",
                "112",
                "158",
                "155"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AQ_MED08": {
            "tipo_dado": "object",
            "valores_unicos": 21,
            "valores_nulos": 0,
            "amostra_valores": [
                "   ",
                "013",
                "177",
                "004",
                "020",
                "062",
                "161",
                "112",
                "135",
                "179"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AQ_MED09": {
            "tipo_dado": "object",
            "valores_unicos": 20,
            "valores_nulos": 0,
            "amostra_valores": [
                "   ",
                "020",
                "003",
                "021",
                "105",
                "004",
                "174",
                "062",
                "138",
                "008"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AQ_MED10": {
            "tipo_dado": "object",
            "valores_unicos": 14,
            "valores_nulos": 0,
            "amostra_valores": [
                "   ",
                "021",
                "158",
                "023",
                "003",
                "177",
                "004",
                "174",
                "011",
                "020"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_NATJUR": {
            "tipo_dado": "object",
            "valores_unicos": 7,
            "valores_nulos": 0,
            "amostra_valores": [
                "3069",
                "1023",
                "3999",
                "1112",
                "1244",
                "1210",
                "1120"
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
            "valores_unicos": 1,
            "valores_nulos": 0,
            "amostra_valores": [
                "202309"
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
            "valores_unicos": 17,
            "valores_nulos": 0,
            "amostra_valores": [
                "350950",
                "354890",
                "354980",
                "350000",
                "353870",
                "354870",
                "355030",
                "352590",
                "354780",
                "352690"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CODUNI": {
            "tipo_dado": "object",
            "valores_unicos": 42,
            "valores_nulos": 0,
            "amostra_valores": [
                "2081490",
                "2080931",
                "2798298",
                "7066376",
                "6123740",
                "2090236",
                "2078015",
                "2077477",
                "2080125",
                "2748223"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_AUTORIZ": {
            "tipo_dado": "object",
            "valores_unicos": 2960,
            "valores_nulos": 0,
            "amostra_valores": [
                "3523238396887",
                "3523258728902",
                "3523263419115",
                "3523259109403",
                "3523266606002",
                "3523260341546",
                "3523217057910",
                "3523270016442",
                "3523244411742",
                "3523272249739"
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
                "202306",
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
            "valores_unicos": 22,
            "valores_nulos": 0,
            "amostra_valores": [
                "0304010375",
                "0304010367",
                "0304010553",
                "0304010561",
                "0304010383",
                "0304010413",
                "0304010421",
                "0304010456",
                "0304010529",
                "0304010510"
            ],
            "maior_caractere": 10,
            "menor_caractere": 10,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_VL_AP": {
            "tipo_dado": "object",
            "valores_unicos": 36,
            "valores_nulos": 0,
            "amostra_valores": [
                "             4148.00",
                "             4168.00",
                "             3159.00",
                "             1729.00",
                "             3563.00",
                "             5904.00",
                "             4608.00",
                "             5838.00",
                "             2439.00",
                "             5035.00"
            ],
            "maior_caractere": 20,
            "menor_caractere": 20,
            "has_leading_zeros": False,
            "has_special_chars": True,
            "has_mixed_types": False
        },
        "AP_UFMUN": {
            "tipo_dado": "object",
            "valores_unicos": 27,
            "valores_nulos": 0,
            "amostra_valores": [
                "350950",
                "354890",
                "354980",
                "352480",
                "355030",
                "350550",
                "350750",
                "355410",
                "354140",
                "353870"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_TPUPS": {
            "tipo_dado": "object",
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "05",
                "07",
                "36",
                "39"
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
            "valores_unicos": 42,
            "valores_nulos": 0,
            "amostra_valores": [
                "47018676000176",
                "59610394000142",
                "59981712000181",
                "49150352000899",
                "46374500016430",
                "49150352000112",
                "56577059000100",
                "60742616000160",
                "60945854000172",
                "46230439000101"
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
                "47018676000176",
                "0000000000000 ",
                "46374500000194",
                "46523239000147",
                "46068425000133"
            ],
            "maior_caractere": 14,
            "menor_caractere": 14,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CNSPCN": {
            "tipo_dado": "object",
            "valores_unicos": 2901,
            "valores_nulos": 0,
            "amostra_valores": [
                "{}{|~}",
                "|~|}{{{~",
                "{~{|{}}~{",
                "{{{|}{|",
                "{{||",
                "{}{}~",
                "{{{~~}{~}{",
                "}{||~}~{{{~",
                "{}{~{~",
                "}{}|~{{{|"
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
                "42",
                "57",
                "60",
                "58",
                "68",
                "63",
                "34",
                "32",
                "64",
                "69"
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
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "01",
                "03",
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
            "valores_unicos": 517,
            "valores_nulos": 0,
            "amostra_valores": [
                "355240",
                "354070",
                "354980",
                "353730",
                "355030",
                "521880",
                "351570",
                "353440",
                "317020",
                "350450"
            ],
            "maior_caractere": 6,
            "menor_caractere": 6,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_UFNACIO": {
            "tipo_dado": "object",
            "valores_unicos": 4,
            "valores_nulos": 0,
            "amostra_valores": [
                "010",
                "022",
                "045",
                "037"
            ],
            "maior_caractere": 3,
            "menor_caractere": 3,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CEPPCN": {
            "tipo_dado": "object",
            "valores_unicos": 2603,
            "valores_nulos": 0,
            "amostra_valores": [
                "13171360",
                "13665130",
                "15046793",
                "16305078",
                "05754070",
                "03520020",
                "75907010",
                "02755130",
                "08532410",
                "08471000"
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
            "valores_unicos": 95,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230605",
                "20230801",
                "20230828",
                "20230808",
                "20230814",
                "20230717",
                "20230920",
                "20230817",
                "20230914",
                "20230918"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTFIM": {
            "tipo_dado": "object",
            "valores_unicos": 51,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230717",
                "20230831",
                "20231031",
                "20230930",
                "20231130",
                "20230818",
                "20230921",
                "20230928",
                "20230803",
                "20230926"
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
                "12",
                "15",
                "18",
                "11",
                "26",
                "41",
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
            "valores_unicos": 72,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230630",
                "20230814",
                "20230930",
                "20230927",
                "20230831",
                "20230731",
                "20230920",
                "20230829",
                "20230914",
                "20230918"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CODEMI": {
            "tipo_dado": "object",
            "valores_unicos": 32,
            "valores_nulos": 0,
            "amostra_valores": [
                "M350950001",
                "M354890001",
                "M354980501",
                "E350000022",
                "E350000027",
                "E350000009",
                "S352078015",
                "S352748223",
                "E350000030",
                "E350000016"
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
            "valores_unicos": 140,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000000000000",
                "3523258728902",
                "000000000000 ",
                "3523221568713",
                "3523221560848",
                "3523221577788",
                "3523258738043",
                "3523258728979",
                "3523221569384",
                "3523221569406"
            ],
            "maior_caractere": 13,
            "menor_caractere": 13,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_UNISOL": {
            "tipo_dado": "object",
            "valores_unicos": 36,
            "valores_nulos": 0,
            "amostra_valores": [
                "2081490",
                "2080931",
                "2798298",
                "7066376",
                "6123740",
                "2090236",
                "2078015",
                "2077477",
                "0000000",
                "3126838"
            ],
            "maior_caractere": 7,
            "menor_caractere": 7,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTSOLIC": {
            "tipo_dado": "object",
            "valores_unicos": 100,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230605",
                "20230831",
                "20230828",
                "20230808",
                "20230814",
                "20230717",
                "20230920",
                "20230817",
                "20230914",
                "20230918"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_DTAUT": {
            "tipo_dado": "object",
            "valores_unicos": 91,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230927",
                "20230831",
                "20230828",
                "20230816",
                "20230901",
                "20231010",
                "20230915",
                "20230914",
                "20230918",
                "20230822"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CIDCAS": {
            "tipo_dado": "object",
            "valores_unicos": 20,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000",
                "C61 ",
                "C601",
                "C538",
                "C795",
                "C501",
                "C717",
                "C504",
                "C449",
                "C506"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AP_CIDPRI": {
            "tipo_dado": "object",
            "valores_unicos": 207,
            "valores_nulos": 0,
            "amostra_valores": [
                "C20 ",
                "C158",
                "C062",
                "C131",
                "C329",
                "C833",
                "C920",
                "C349",
                "C504",
                "C509"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AP_CIDSEC": {
            "tipo_dado": "object",
            "valores_unicos": 2,
            "valores_nulos": 0,
            "amostra_valores": [
                "0000",
                "C509"
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
            "valores_unicos": 190,
            "valores_nulos": 0,
            "amostra_valores": [
                "C20 ",
                "    ",
                "C062",
                "C131",
                "C329",
                "C833",
                "C920",
                "C349",
                "C504",
                "C509"
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
                "N",
                "3",
                "S"
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
                "3",
                "2",
                "1",
                "4",
                " ",
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
            "valores_unicos": 49,
            "valores_nulos": 0,
            "amostra_valores": [
                "02",
                "X ",
                "1 ",
                " 9",
                " 8",
                " 4",
                "06",
                "G2",
                " 3",
                "3 "
            ],
            "maior_caractere": 2,
            "menor_caractere": 2,
            "has_leading_zeros": True,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AR_DTIDEN": {
            "tipo_dado": "object",
            "valores_unicos": 811,
            "valores_nulos": 0,
            "amostra_valores": [
                "20221027",
                "20230701",
                "20230114",
                "20230228",
                "20230413",
                "20230216",
                "20230822",
                "20221221",
                "20230703",
                "20220830"
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
                "N",
                " ",
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
                "C349",
                "C504",
                "C509",
                "C541",
                "C795",
                "C73 ",
                "C402",
                "C793",
                "C443"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AR_DTINI1": {
            "tipo_dado": "object",
            "valores_unicos": 300,
            "valores_nulos": 0,
            "amostra_valores": [
                "        ",
                "20230126",
                "20221014",
                "20230703",
                "20230503",
                "20230310",
                "20221109",
                "20230414",
                "20230424",
                "20200518"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AR_CIDINI2": {
            "tipo_dado": "object",
            "valores_unicos": 24,
            "valores_nulos": 0,
            "amostra_valores": [
                "    ",
                "C504",
                "C509",
                "C793",
                "C502",
                "C780",
                "C448",
                "C819",
                "C61 ",
                "C539"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AR_DTINI2": {
            "tipo_dado": "object",
            "valores_unicos": 143,
            "valores_nulos": 0,
            "amostra_valores": [
                "        ",
                "20230804",
                "20200528",
                "20141215",
                "20230721",
                "20221208",
                "20230503",
                "20230308",
                "20171114",
                "20170307"
            ],
            "maior_caractere": 8,
            "menor_caractere": 8,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": True
        },
        "AR_CIDINI3": {
            "tipo_dado": "object",
            "valores_unicos": 12,
            "valores_nulos": 0,
            "amostra_valores": [
                "    ",
                "C502",
                "C504",
                "C448",
                "C509",
                "C189",
                "C501",
                "C508",
                "C793",
                "C498"
            ],
            "maior_caractere": 4,
            "menor_caractere": 4,
            "has_leading_zeros": False,
            "has_special_chars": False,
            "has_mixed_types": False
        },
        "AR_DTINI3": {
            "tipo_dado": "object",
            "valores_unicos": 55,
            "valores_nulos": 0,
            "amostra_valores": [
                "        ",
                "20220216",
                "20230102",
                "20230817",
                "20180124",
                "20170801",
                "20230607",
                "20230517",
                "20230814",
                "20230704"
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
            "valores_unicos": 126,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230605",
                "20230814",
                "20230901",
                "20230808",
                "20230717",
                "20230920",
                "20230817",
                "20230828",
                "20230914",
                "20230918"
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
                "3",
                "1",
                "2",
                "4",
                "5",
                "6",
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
            "valores_unicos": 207,
            "valores_nulos": 0,
            "amostra_valores": [
                "C20 ",
                "C158",
                "C062",
                "C131",
                "C329",
                "C833",
                "C920",
                "C349",
                "C504",
                "C509"
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
            "valores_unicos": 98,
            "valores_nulos": 0,
            "amostra_valores": [
                "20220605",
                "20230801",
                "20230901",
                "20230808",
                "20230814",
                "20230717",
                "20230920",
                "20230817",
                "20230828",
                "20230914"
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
            "valores_unicos": 129,
            "valores_nulos": 0,
            "amostra_valores": [
                "20230717",
                "20230831",
                "20230930",
                "20231031",
                "20230922",
                "20230901",
                "20231130",
                "20230914",
                "20231022",
                "20230904"
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
            "valores_unicos": 7,
            "valores_nulos": 0,
            "amostra_valores": [
                "1120",
                "3999",
                "3069",
                "1023",
                "1244",
                "2062",
                "1112"
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



