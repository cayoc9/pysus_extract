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
        'AB': 'apac_de_cirurgia_bariatrica',
        'ABO': 'apac_de_acompanhamento_pos_cirurgia_bariatrica',
        'ACF': 'apac_de_confeccao_de_fistula',
        'AD': 'apac_de_laudos_diversos',
        'AM': 'apac_de_medicamentos',
        'AMP': 'apac_de_acompanhamento_multiprofissional',
        'AN': 'apac_de_nefrologia',
        'AQ': 'apac_de_quimioterapia',
        'AR': 'apac_de_radioterapia',
        'ATD': 'apac_de_tratamento_dialitico',
        'BI': 'boletim_de_producao_ambulatorial_individualizado',
        'IMPBO': 'impbo',
        'PA': 'producao_ambulatorial',
        'PAM': 'pam',
        'PAR': 'par',
        'PAS': 'pas',
        'PS': 'raas_psicossocial',
        'SAD': 'raas_de_atencao_domiciliar',
        'RD': 'aih_reduzida',
        'RJ': 'aih_rejeitada',
        'ER': 'aih_rejeitada_com_erro',
        'SP': 'servicos_profissionais',
        'CH': 'cadastro_hospitalar',
        'CM': 'cm',
        'DC': 'dados_complementares',
        'EE': 'estabelecimento_de_ensino',
        'EF': 'estabelecimento_filantropico',
        'EP': 'equipes',
        'EQ': 'equipamentos',
        'GM': 'gestao_metas',
        'HB': 'habilitacao',
        'IN': 'incentivos',
        'LT': 'leitos',
        'PF': 'profissional',
        'RC': 'regra_contratual',
        'SR': 'servico_especializado',
        'ST': 'estabelecimentos'
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
