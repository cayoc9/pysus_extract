import re
import os
import glob

def parse_procedimentos(procedimentos_dir, layouts_dir):
    """
    Extrai códigos e nomes de procedimentos usando os layouts correspondentes
    com correspondência exata de períodos, para arquivos em formato TXT
    """
    procedimentos_dict = {}
    
    # Mapeamento de layouts por período
    layouts = {}
    layout_files = glob.glob(os.path.join(layouts_dir, "tb_procedimento_*.txt"))
    
    if not layout_files:
        print(f"Aviso: Nenhum arquivo de layout encontrado em {layouts_dir}")
        return procedimentos_dict
    
    for layout_file in layout_files:
        periodo = extrair_periodo(layout_file)
        if periodo:
            layouts[periodo] = carregar_layout(layout_file)
        else:
            print(f"Aviso: Não foi possível extrair período do layout: {layout_file}")
    
    print(f"Layouts carregados para os períodos: {', '.join(layouts.keys())}")

    # Processa cada arquivo de procedimento com seu layout correspondente
    procedimento_files = glob.glob(os.path.join(procedimentos_dir, "tb_procedimento_*.txt"))
    
    if not procedimento_files:
        print(f"Aviso: Nenhum arquivo de procedimentos encontrado em {procedimentos_dir}")
        return procedimentos_dict
    
    for procedimento_file in procedimento_files:
        periodo = extrair_periodo(procedimento_file)
        
        if not periodo:
            print(f"Aviso: Não foi possível extrair período do arquivo: {procedimento_file}")
            continue
            
        if periodo not in layouts:
            print(f"Aviso: Layout não encontrado para o período {periodo} ({procedimento_file})")
            
            # Tenta usar o layout mais próximo
            periodos_disponiveis = sorted(layouts.keys())
            periodo_mais_proximo = min(periodos_disponiveis, key=lambda p: abs(int(p) - int(periodo)))
            
            print(f"Usando layout do período {periodo_mais_proximo} como alternativa")
            layout = layouts[periodo_mais_proximo]
        else:
            layout = layouts[periodo]
        
        count_antes = len(procedimentos_dict)
        processar_arquivo(procedimento_file, layout, procedimentos_dict, periodo)
        count_depois = len(procedimentos_dict)
        
        print(f"Processado {procedimento_file}: extraídos {count_depois - count_antes} procedimentos novos")
    
    return procedimentos_dict

def extrair_periodo(arquivo_path):
    """Extrai o período no formato AAAAMM do nome do arquivo"""
    nome_arquivo = os.path.basename(arquivo_path)
    match = re.search(r'tb_procedimento_(\d{6})\.txt', nome_arquivo)
    return match.group(1) if match else None

def carregar_layout(layout_file):
    """Carrega as definições de colunas de um arquivo de layout com validação"""
    layout = {
        'CO_PROCEDIMENTO': {'start': 0, 'end': 0},
        'NO_PROCEDIMENTO': {'start': 0, 'end': 0}
    }
    
    with open(layout_file, 'r', encoding='utf-8') as f:
        next(f)  # Pula cabeçalho
        for line in f:
            if not line.strip() or line.startswith('Coluna'):
                continue
                
            # Divide a linha por vírgulas e remove espaços extras
            col_info = [x.strip() for x in line.split(',')]
            if len(col_info) >= 5:
                coluna = col_info[0]
                inicio = int(col_info[2]) - 1  # Convertendo para base 0
                fim = int(col_info[3])
                
                if coluna == 'CO_PROCEDIMENTO':
                    layout[coluna]['start'] = inicio
                    layout[coluna]['end'] = fim
                elif coluna == 'NO_PROCEDIMENTO':
                    layout[coluna]['start'] = inicio
                    layout[coluna]['end'] = fim
    
    # Validação do layout
    if layout['CO_PROCEDIMENTO']['start'] == 0 and layout['CO_PROCEDIMENTO']['end'] == 0:
        raise ValueError(f"Layout não contém definição para CO_PROCEDIMENTO em {layout_file}")
    
    if layout['NO_PROCEDIMENTO']['start'] == 0 and layout['NO_PROCEDIMENTO']['end'] == 0:
        raise ValueError(f"Layout não contém definição para NO_PROCEDIMENTO em {layout_file}")
        
    # Validação crítica do layout
    tamanho_codigo = layout['CO_PROCEDIMENTO']['end'] - layout['CO_PROCEDIMENTO']['start']
    if tamanho_codigo != 10:
        print(f"Aviso: Layout para código de procedimento em {layout_file} tem tamanho {tamanho_codigo}, esperado 10")
        
    return layout

def processar_arquivo(arquivo, layout, procedimentos_dict, periodo):
    """Processa um arquivo de procedimentos usando o layout especificado"""
    cod_start = layout['CO_PROCEDIMENTO']['start']
    cod_end = layout['CO_PROCEDIMENTO']['end']
    nome_start = layout['NO_PROCEDIMENTO']['start']
    nome_end = layout['NO_PROCEDIMENTO']['end']
    
    total_procedimentos = 0
    procedimentos_validos = 0
    
    # Tentamos diferentes codificações para lidar com caracteres especiais
    codificacoes = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
    
    for codificacao in codificacoes:
        try:
            with open(arquivo, 'r', encoding=codificacao) as f:
                for line_num, line in enumerate(f, 1):
                    # Ignora linhas muito curtas
                    if len(line) <= cod_start:
                        continue
                        
                    # Extrai código conforme layout
                    if len(line) > cod_end:
                        codigo = line[cod_start:cod_end].strip()
                    else:
                        continue
                    
                    # Extrai nome conforme layout
                    if len(line) > nome_start:
                        nome_fim = min(nome_end, len(line))
                        nome = line[nome_start:nome_fim].strip()
                    else:
                        nome = ""
                    
                    # Verifica se é um código válido (10 dígitos numéricos)
                    if len(codigo) == 10 and codigo.isdigit():
                        total_procedimentos += 1
                        if nome:
                            salvar_procedimento(codigo, nome, procedimentos_dict, periodo)
                            procedimentos_validos += 1
                        else:
                            print(f"Aviso: Procedimento sem nome encontrado - Código: {codigo} (linha {line_num} em {arquivo})")
            
            # Se conseguimos ler o arquivo com esta codificação, não tentamos outras
            print(f"Arquivo {arquivo} processado com codificação {codificacao}")
            break
        except UnicodeDecodeError:
            # Se falhar, tentamos a próxima codificação
            if codificacao == codificacoes[-1]:
                print(f"Erro: Não foi possível decodificar o arquivo {arquivo} com nenhuma codificação suportada")
                return
    
    print(f"Total de procedimentos no arquivo {arquivo}: {total_procedimentos}")
    print(f"Procedimentos válidos extraídos: {procedimentos_validos}")

def salvar_procedimento(codigo, nome, procedimentos_dict, periodo):
    """Adiciona procedimento ao dicionário com validação temporal"""
    nome_limpo = limpar_nome(nome)
    
    if not codigo or not nome_limpo:
        return
        
    # Verifica se o código já existe
    if codigo in procedimentos_dict:
        # Mantém o nome mais recente baseado no período
        existing_periodo = procedimentos_dict[codigo]['periodo']
        if periodo > existing_periodo:
            procedimentos_dict[codigo] = {
                'nome': nome_limpo,
                'periodo': periodo
            }
    else:
        procedimentos_dict[codigo] = {
            'nome': nome_limpo,
            'periodo': periodo
        }

def limpar_nome(nome):
    """
    Limpa e normaliza o nome do procedimento
    """
    # Se vier None, retorna string vazia
    if nome is None:
        return ""
        
    # Remove espaços extras
    nome = re.sub(r'\s+', ' ', nome).strip()
    
    # Corrige caso específico da COVID-19
    nome = nome.replace("COVID-", "COVID-19")
    
    # Corrige junções incorretas de palavras
    nome = nome.replace("DAMALÁRIA", "DA MALÁRIA")
    nome = nome.replace("NAATENÇÃO", "NA ATENÇÃO")
    nome = nome.replace("EMGRUPO", "EM GRUPO")
    
    # Corrige casos de caracteres especiais mal codificados
    nome = nome.replace("��", "Ç")
    nome = nome.replace("�", "Ã")
    nome = nome.replace("�", "Ç")
    nome = nome.replace("�", "Á")
    nome = nome.replace("�", "É")
    nome = nome.replace("�", "Í")
    nome = nome.replace("�", "Ó")
    nome = nome.replace("�", "Ú")
    
    # Corrige outros problemas comuns
    nome = nome.replace("(CADA", "(CADA FRASCO)")
    
    # Corrige prefixo de medicina
    nome = nome.replace("MEDICINA,", "MEDICINA")
    
    # Remove caracteres não imprimíveis
    nome = re.sub(r'[\x00-\x1F\x7F]', '', nome)
    
    return nome

def update_definitions_file(procedimentos_dict, definitions_path):
    """
    Atualiza o arquivo definitions.py com o novo dicionário de procedimentos
    """
    # Primeiro, lê o conteúdo atual do arquivo
    with open(definitions_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Prepara o novo conteúdo do dicionário procedimentos_dict
    new_dict_text = "# Mapeamento de códigos de procedimento para nomes\nprocedimentos_dict = {\n"
    
    # Adiciona cada procedimento ao dicionário
    for codigo, info in sorted(procedimentos_dict.items()):
        # Limpa qualquer caractere problemático do nome
        nome_limpo = info['nome'].replace('"', '\\"').strip()
        new_dict_text += f'    "{codigo}": "{nome_limpo}",\n'
    
    # Fecha o dicionário
    new_dict_text += "}\n\n"
    
    # Padrão para encontrar o dicionário existente
    dict_pattern = re.compile(r'# Mapeamento de códigos de procedimento para nomes\nprocedimentos_dict = \{.*?\}\n\n', re.DOTALL)
    
    # Substitui o dicionário existente pelo novo
    if dict_pattern.search(content):
        updated_content = dict_pattern.sub(new_dict_text, content)
    else:
        # Se não encontrar o dicionário, insere após a linha de comentário de dicionários
        dict_section_pattern = re.compile(r'# -----------------------------------------------------------------------------\n# Dicionários de configuração.*?\n# -----------------------------------------------------------------------------\n', re.DOTALL)
        match = dict_section_pattern.search(content)
        if match:
            updated_content = content[:match.end()] + "\n" + new_dict_text + content[match.end():]
        else:
            # Último recurso: adiciona no início do arquivo
            updated_content = new_dict_text + content
    
    # Escreve o conteúdo atualizado no arquivo
    with open(definitions_path, 'w', encoding='utf-8') as f:
        f.write(updated_content)
    
    print(f"Arquivo {definitions_path} atualizado com {len(procedimentos_dict)} procedimentos.")

def verificar_procedimentos(procedimentos_dict):
    """Verifica a qualidade dos procedimentos extraídos"""
    nomes_vazios = 0
    nomes_curtos = 0
    nomes_longos = 0
    
    for codigo, info in procedimentos_dict.items():
        nome = info['nome']
        if not nome:
            nomes_vazios += 1
            print(f"Aviso: Procedimento sem nome - Código: {codigo}")
        elif len(nome) < 5:
            nomes_curtos += 1
            print(f"Aviso: Nome muito curto: '{nome}' - Código: {codigo}")
        elif len(nome) > 200:
            nomes_longos += 1
            print(f"Aviso: Nome muito longo ({len(nome)} caracteres) - Código: {codigo}")
    
    if nomes_vazios > 0 or nomes_curtos > 0 or nomes_longos > 0:
        print(f"Análise de qualidade: {nomes_vazios} nomes vazios, {nomes_curtos} nomes curtos, {nomes_longos} nomes longos")

if __name__ == "__main__":
    procedimentos_dir = "docs/txt/resultados_2024/"
    layouts_dir = "docs/txt/resultados_layout_2024/"
    definitions_path = "api/definitions.py"
    
    if not os.path.exists(procedimentos_dir):
        print(f"Diretório {procedimentos_dir} não encontrado.")
        exit(1)
    
    if not os.path.exists(layouts_dir):
        print(f"Diretório {layouts_dir} não encontrado.")
        exit(1)
    
    if not os.path.exists(definitions_path):
        print(f"Arquivo {definitions_path} não encontrado.")
        exit(1)
    
    print(f"Extraindo procedimentos de {procedimentos_dir}...")
    procedimentos_dict = parse_procedimentos(procedimentos_dir, layouts_dir)
    print(f"Encontrados {len(procedimentos_dict)} procedimentos únicos.")
    
    # Verifica a qualidade dos procedimentos extraídos
    verificar_procedimentos(procedimentos_dict)
    
    print(f"Atualizando {definitions_path}...")
    update_definitions_file(procedimentos_dict, definitions_path)
    
    print("Concluído!") 