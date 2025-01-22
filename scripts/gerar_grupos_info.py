import json
import re
import unicodedata

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

def gerar_grupos_info(dados_tabelas, grupos_dict):
    """
    Gera um dicionário com informações dos grupos, tabelas e colunas.
    
    Args:
        dados_tabelas (dict): Dicionário contendo as tabelas, colunas e tipos.
        grupos_dict (dict): Dicionário mapeando códigos de grupos para descrições.
    
    Returns:
        dict: Dicionário com informações dos grupos no formato desejado.
    """
    grupos_info = {}
    
    # Criar um mapeamento inverso do grupos_dict para obter códigos a partir das descrições
    descricoes_para_codigos = {v.lower(): k for k, v in grupos_dict.items()}
    
    for tabela_nome, colunas in dados_tabelas.items():
        # Normalizar o nome da tabela
        tabela_nome_normalizado = normalizar_nome(tabela_nome)
        
        # Remover a coluna 'id' se existir
        if 'id' in colunas:
            del colunas['id']
            print(f"A coluna 'id' foi removida da tabela '{tabela_nome}'.")
        
        # Tentar encontrar o código do grupo correspondente
        codigo_grupo = None
        for descricao, codigo in descricoes_para_codigos.items():
            # Remover acentos e normalizar a descrição
            descricao_normalizada = normalizar_nome(descricao)
            if descricao_normalizada in tabela_nome_normalizado:
                codigo_grupo = codigo
                break
        
        if codigo_grupo is None:
            print(f"Aviso: Não foi possível encontrar o código do grupo para a tabela '{tabela_nome}'")
            continue  # Ignora tabelas sem código de grupo correspondente
        
        # Se o grupo já existe em grupos_info, verifica se a tabela já está adicionada
        if codigo_grupo not in grupos_info:
            grupos_info[codigo_grupo] = {
                "tabela": tabela_nome_normalizado,
                "colunas": list(colunas.keys())
            }
        else:
            # Se a tabela já existe, mescla as colunas
            grupos_info[codigo_grupo]["colunas"].extend(colunas.keys())
            # Remover duplicatas
            grupos_info[codigo_grupo]["colunas"] = list(set(grupos_info[codigo_grupo]["colunas"]))
    
    return grupos_info

# Exemplo de uso
if __name__ == "__main__":
    # Dicionário fornecido com mapeamento de códigos de grupos para descrições
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
        'PS': 'RAAS Psicossocial',
        'SAD': 'raas_atencao_domiciliar'
    }

    # Carregar o JSON com as tabelas e colunas (substitua pelo seu arquivo real)
    with open('tipo_coluna_map.json', 'r', encoding='utf-8') as f:
        dados_tabelas = json.load(f)
    
    # Gerar o dicionário grupos_info
    grupos_info = gerar_grupos_info(dados_tabelas, grupos_dict)
    
    # Exibir o resultado
    for grupo, info in grupos_info.items():
        print(f"Grupo: {grupo}")
        print(f"  Tabela: {info['tabela']}")
        print(f"  Colunas: {info['colunas']}\n")
    
    # Salvar o resultado em um arquivo JSON (opcional)
    with open('grupos_info.json', 'w', encoding='utf-8') as f:
        json.dump(grupos_info, f, ensure_ascii=False, indent=4)
