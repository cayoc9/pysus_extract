import duckdb

# Abre (ou cria) uma conexão com o DuckDB
con = duckdb.connect(database=':memory:')  # ou especifique um arquivo, por exemplo, 'meubanco.duckdb'

# Instala a extensão do Postgres (apenas na primeira vez)
con.execute("INSTALL postgres;")

# Carrega a extensão do Postgres para a sessão atual
con.execute("LOAD postgres;")

# (Opcional) Verifica se a extensão foi carregada executando um comando da extensão
result = con.execute("SELECT * FROM duckdb_extensions();").fetchall()
print("Extensões disponíveis:", result)

con.close()
