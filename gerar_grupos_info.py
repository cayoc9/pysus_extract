import os
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
    nome = nome.lower()
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('ASCII')
    nome = re.sub(r'\W+', '_', nome)
    nome = nome.strip('_')
    return nome

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

def extrair_base_e_grupo(nome_arquivo):
    """
    Extrai base e grupo do nome do arquivo no formato tipo_coluna_map_{BASE}_{GRUPO}.json
    Ex: tipo_coluna_map_SIA_AB.json -> base = 'sia', grupo = 'AB'
    """
    padrao = r"^tipo_coluna_map_(?P<base>[A-Za-z0-9]+)_(?P<grupo>[A-Za-z0-9]+)\.json$"
    m = re.match(padrao, nome_arquivo)
    if m:
        return m.group('base').lower(), m.group('grupo').upper()
    return None, None

def main():
    diretorio = "./tipos_dados"
    arquivos = os.listdir(diretorio)

    GRUPOS_INFO = {}

    for arquivo in arquivos:
        if arquivo.startswith("tipo_coluna_map_") and arquivo.endswith(".json"):
            caminho = os.path.join(diretorio, arquivo)
            base, grupo = extrair_base_e_grupo(arquivo)
            if not base or not grupo:
                print(f"Aviso: O arquivo {arquivo} não segue o padrão esperado (tipo_coluna_map_BASE_GRUPO.json).")
                continue

            if grupo not in grupos_dict:
                print(f"Aviso: Não foi possível encontrar o grupo '{grupo}' no dicionário de grupos.")
                continue

            descricao = grupos_dict[grupo]
            tabela = f"{base}_{normalizar_nome(descricao)}"

            # Carregar o mapeamento de colunas do JSON
            with open(caminho, "r", encoding="utf-8") as f:
                dados = json.load(f)

            # 'dados' está no formato { "SIA_AB": {"coluna": "TIPO", ...}}
            # Precisamos extrair as chaves do primeiro dicionário interno
            # pois a estrutura é algo como: { "SIA_AB": {"ap_mvm": "INTEGER", ...}}
            # Então vamos iterar sobre a primeira chave do dicionário
            for chave_base_grupo, colunas_dict in dados.items():
                # colunas_dict é um dict colunas -> tipo
                colunas_lista = list(colunas_dict.keys())
                GRUPOS_INFO[grupo] = {
                    "tabela": tabela,
                    "colunas": sorted(colunas_lista)
                }

    # Exibir resultado
    for g, info in GRUPOS_INFO.items():
        print(f"{g}:")
        print(f"  tabela: {info['tabela']}")
        print("  colunas:")
        for c in info['colunas']:
            print(f"    {c}")
        print()

    # Salvar em grupos_info.json
    with open("grupos_info.json", "w", encoding="utf-8") as f:
        json.dump(GRUPOS_INFO, f, ensure_ascii=False, indent=4)
    print("Arquivo grupos_info.json salvo com sucesso!")

if __name__ == "__main__":
    main()
