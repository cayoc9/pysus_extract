# Lista de tabelas e suas sequências associadas
particoes = [
    "sih_servicos_profissionais_temp_part_AC",
    "sih_servicos_profissionais_temp_part_AL",
    "sih_servicos_profissionais_temp_part_AM",
    "sih_servicos_profissionais_temp_part_AP",
    "sih_servicos_profissionais_temp_part_BA",
    "sih_servicos_profissionais_temp_part_CE",
    "sih_servicos_profissionais_temp_part_DF",
    "sih_servicos_profissionais_temp_part_ES",
    "sih_servicos_profissionais_temp_part_GO",
    "sih_servicos_profissionais_temp_part_MA",
    "sih_servicos_profissionais_temp_part_MG",
    "sih_servicos_profissionais_temp_part_MS",
    "sih_servicos_profissionais_temp_part_MT",
    "sih_servicos_profissionais_temp_part_PA",
    "sih_servicos_profissionais_temp_part_PB",
    "sih_servicos_profissionais_temp_part_PE",
    "sih_servicos_profissionais_temp_part_PI",
    "sih_servicos_profissionais_temp_part_PR",
    "sih_servicos_profissionais_temp_part_RJ",
    "sih_servicos_profissionais_temp_part_RN",
    "sih_servicos_profissionais_temp_part_RO",
    "sih_servicos_profissionais_temp_part_RR",
    "sih_servicos_profissionais_temp_part_RS",
    "sih_servicos_profissionais_temp_part_SC",
    "sih_servicos_profissionais_temp_part_SE",
    "sih_servicos_profissionais_temp_part_SP",
    "sih_servicos_profissionais_temp_part_TO"
]

# Função para gerar SQL dinamicamente
def gerar_sql(particoes):
    sql_comandos = []
    for particao in particoes:
        # Gerar o nome da sequência baseado na partição
        sequencia = f"{particao.lower()}_id_seq"
        
        # Gerar SQL para associar a sequência à coluna `id`
        sql = f"""
        -- Configurar sequência para a partição {particao}
        ALTER SEQUENCE {sequencia}
        OWNED BY "{particao}".id;
        """
        sql_comandos.append(sql.strip())
    return sql_comandos

# Gerar os comandos SQL
comandos_sql = gerar_sql(particoes)

# Salvar os comandos em um arquivo ou exibir no console
with open("configurar_sequencias.sql", "w") as file:
    for comando in comandos_sql:
        file.write(comando + "\n\n")

# Exibir os comandos gerados
for comando in comandos_sql:
    print(comando)
    print("-" * 80)
