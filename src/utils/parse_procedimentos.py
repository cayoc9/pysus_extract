import re
import os

def parse_procedimentos(tb_procedimento_path):
    """
    Extrai códigos e nomes de procedimentos do arquivo tb_procedimento.md
    """
    procedimentos_dict = {}
    
    # Expressão regular para identificar código de procedimento seguido por nome
    # Captura 10 dígitos seguidos por texto em maiúsculas
    codigo_nome_pattern = re.compile(r'^(\d{10})([A-ZÇÃÁÀÂÄÉÈÊËÍÌÎÏÓÒÔÖÚÙÛÜÑ /\-\(\)]+)')
    
    # Identifica linhas numéricas de controle/estatística que não fazem parte do nome
    linha_controle_pattern = re.compile(r'^\d[A-Z]\d+')
    
    # Padrão para linhas que continuam nomes de procedimentos
    continuacao_pattern = re.compile(r'^([A-ZÇÃÁÀÂÄÉÈÊËÍÌÎÏÓÒÔÖÚÙÛÜÑ /\-\(\)]+)')
    
    with open(tb_procedimento_path, 'r', encoding='utf-8') as f:
        # Variáveis para manter estado entre linhas
        current_codigo = None
        current_nome = None
        
        for line in f:
            # Remove espaços em branco no início e fim da linha
            line = line.strip()
            
            if not line:
                continue
                
            # Verifica se a linha contém um novo código e nome
            match = codigo_nome_pattern.match(line)
            if match:
                # Se temos um código e nome em andamento, salve-os antes de começar novos
                if current_codigo and current_nome:
                    procedimentos_dict[current_codigo] = limpar_nome(current_nome)
                
                # Início de um novo procedimento
                current_codigo = match.group(1)
                current_nome = match.group(2)
            else:
                # Ignora linhas de controle/estatística
                if linha_controle_pattern.match(line):
                    continue
                    
                # Verifica se é uma linha de continuação do nome
                cont_match = continuacao_pattern.match(line)
                if cont_match and current_codigo and current_nome:
                    # Adiciona essa parte ao nome atual
                    current_nome += " " + cont_match.group(1)
        
        # Não esqueça o último procedimento
        if current_codigo and current_nome:
            procedimentos_dict[current_codigo] = limpar_nome(current_nome)
    
    return procedimentos_dict

def limpar_nome(nome):
    """
    Limpa e normaliza o nome do procedimento
    """
    # Remove espaços extras
    nome = re.sub(r'\s+', ' ', nome).strip()
    
    # Corrige caso específico da COVID-19
    nome = nome.replace("COVID-", "COVID-19")
    
    # Corrige junções incorretas de palavras
    nome = nome.replace("DAMALÁRIA", "DA MALÁRIA")
    
    # Corrige outros problemas comuns
    nome = nome.replace("(CADA", "(CADA FRASCO)")
    
    # Corrige prefixo de medicina
    nome = nome.replace("MEDICINA,", "MEDICINA")
    
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
    for codigo, nome in sorted(procedimentos_dict.items()):
        # Limpa qualquer caractere problemático do nome
        nome_limpo = nome.replace('"', '\\"').strip()
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

if __name__ == "__main__":
    tb_procedimento_path = "docs/tb_procedimento.md"
    definitions_path = "api/definitions.py"
    
    if not os.path.exists(tb_procedimento_path):
        print(f"Arquivo {tb_procedimento_path} não encontrado.")
        exit(1)
    
    if not os.path.exists(definitions_path):
        print(f"Arquivo {definitions_path} não encontrado.")
        exit(1)
    
    print(f"Extraindo procedimentos de {tb_procedimento_path}...")
    procedimentos_dict = parse_procedimentos(tb_procedimento_path)
    print(f"Encontrados {len(procedimentos_dict)} procedimentos.")
    
    print(f"Atualizando {definitions_path}...")
    update_definitions_file(procedimentos_dict, definitions_path)
    
    print("Concluído!") 