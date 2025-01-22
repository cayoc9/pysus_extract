import json
import unicodedata
import re

def gerar_queries_criacao_tabelas(tipo_coluna_map, nome_arquivo_sql='criar_tabelas.sql'):
    """
    Gera queries SQL para criar tabelas com base no mapeamento de tipos de dados.

    Args:
        tipo_coluna_map (dict): Dicionário contendo o mapeamento de tipos de dados para cada tabela.
        nome_arquivo_sql (str): Nome do arquivo .sql onde as queries serão salvas (opcional).
    
    Returns:
        str: String contendo todas as queries SQL de criação de tabelas.
    """
    queries = ""
    for tabela, colunas in tipo_coluna_map.items():
        # Começa a montar a query de criação da tabela
        query = f"CREATE TABLE IF NOT EXISTS {tabela} (\n"
        colunas_definicoes = []
        for coluna, tipo in colunas.items():
            # Mapear tipos de dados para tipos válidos no PostgreSQL
            tipo_postgres = mapear_tipo_postgres(tipo)
            colunas_definicoes.append(f"    {coluna} {tipo_postgres}")
        query += ",\n".join(colunas_definicoes)
        query += "\n);\n\n"
        queries += query
    
    # Salvar as queries em um arquivo .sql
    with open(nome_arquivo_sql, 'w', encoding='utf-8') as f:
        f.write(queries)
    
    return queries

def mapear_tipo_postgres(tipo):
    """
    Mapeia o tipo de dado para o tipo correspondente no PostgreSQL.
    
    Args:
        tipo (str): Tipo de dado original.
    
    Returns:
        str: Tipo de dado mapeado para PostgreSQL.
    """
    # Remover espaços extras
    tipo = tipo.strip().upper()
    
    # Dicionário de mapeamento
    mapeamento = {
        'SMALLINT': 'SMALLINT',
        'INTEGER': 'INTEGER',
        'BIGINT': 'BIGINT',
        'NUMERIC': 'NUMERIC',
        'BOOLEAN': 'BOOLEAN',
        'DATE': 'DATE',
        'TIMESTAMP': 'TIMESTAMP',
        'TEXT': 'TEXT',
        'SERIAL': 'SERIAL',
        'VARCHAR': 'VARCHAR',
        'CHAR': 'CHAR'
    }
    
    # Verificar se o tipo é NUMERIC com precisão
    if tipo.startswith('NUMERIC'):
        return tipo  # Mantém como está
    
    # Verificar se é CHAR(n) ou VARCHAR(n)
    match = re.match(r'(CHAR|VARCHAR)\((\d+)\)', tipo)
    if match:
        return f"{match.group(1)}({match.group(2)})"
    
    # Se o tipo estiver no mapeamento, retorna o mapeamento
    if tipo in mapeamento:
        return mapeamento[tipo]
    else:
        # Caso contrário, retorna o tipo original (pode ser arriscado)
        return tipo

# Exemplo de uso
if __name__ == "__main__":
    # Carregar o dicionário tipo_coluna_map a partir do arquivo JSON
    with open('tipo_coluna_map.json', 'r', encoding='utf-8') as f:
        tipo_coluna_map = json.load(f)
    
    # Gerar as queries SQL de criação de tabelas
    queries_sql = gerar_queries_criacao_tabelas(tipo_coluna_map)
    
    # Exibir as queries geradas (opcional)
    print(queries_sql)
