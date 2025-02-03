# Análise Técnica do main.py

## 1. Visão Geral do Sistema

O arquivo `main.py` implementa uma API REST usando FastAPI para consultar e processar dados do DataSUS. O sistema permite:

- Consultar dados de diferentes bases (SIH, SIA)
- Filtrar por grupos específicos de dados
- Agrupar resultados por campos selecionados
- Salvar resultados em PostgreSQL e CSV

```mermaid
graph TD
    A[Cliente] -->|POST /query| B[FastAPI Endpoint]
    B --> C{Processamento}
    C -->|1| D[Busca arquivos Parquet]
    C -->|2| E[Processa dados DuckDB]
    C -->|3| F[Salva PostgreSQL]
    C -->|4| G[Salva CSV]
    F --> H[Retorna Resultado]
    G --> H
```

## 2. Principais Componentes

### 2.1 Validação de Parâmetros (QueryParams)
- Valida base de dados (SIH, SIA, CNES)
- Valida grupos de dados
- Valida formato de competência (MM/YYYY)

### 2.2 Processamento de Arquivos
1. **get_parquet_files()**
   - Busca arquivos por UF/competência
   - Retorna lista de caminhos

2. **process_parquet_files()**
   - Usa DuckDB para query
   - Filtra por CNES
   - Agrupa por campos especificados

3. **save_results()**
   - Salva em PostgreSQL
   - Salva em CSV se < 10M registros

## 3. Fluxo de Dados

```mermaid
sequenceDiagram
    Cliente->>+API: POST /query
    API->>+Sistema: Valida parâmetros
    Sistema->>+Storage: Busca Parquet
    Storage->>-Sistema: Arquivos encontrados
    Sistema->>+DuckDB: Processa dados
    DuckDB->>-Sistema: DataFrame resultante
    Sistema->>+PostgreSQL: Salva resultado
    Sistema->>+FileSystem: Salva CSV
    Sistema->>-Cliente: Retorna resposta
```

## 4. Análise de Estruturas

### 4.1 Mapeamentos
- `grupos_dict`: Mapeamento descritivo dos grupos
- `CAMPOS_CNES`: Mapeamento colunas CNES
- `GRUPOS_INFO_SIA/SIH`: Esquemas das tabelas

### 4.2 Configurações
- Logging: Console + Arquivo
- Banco de dados: PostgreSQL
- CORS: Habilitado para todas origens

## 5. Fluxo Detalhado de Processamento

```mermaid
sequenceDiagram
    participant Cliente
    participant API
    participant Processador
    participant BancoDados
    participant Armazenamento

    Cliente->>API: POST /query {params}
    API->>Processador: Validar parâmetros
    Processador->>Armazenamento: Buscar arquivos Parquet
    Armazenamento->>Processador: Lista de arquivos
    Processador->>DuckDB: Carregar e processar dados
    DuckDB->>Processador: DataFrame resultante
    Processador->>BancoDados: Salvar resultados
    BancoDados->>API: Confirmação
    API->>Cliente: Resposta com dados
```

## 6. Estratégias de Otimização

1. **Cache de Consultas:**
   - Memória: Redis para resultados parciais
   - Disco: Armazenamento de datasets frequentes

2. **Processamento em Streaming:**
   ```python
   def process_stream():
       while has_data:
           chunk = get_next_chunk()
           yield process(chunk)
           gc.collect()
   ```

3. **Particionamento de Dados:**
   - Por UF/Ano/Mês
   - Usar estrutura de diretórios:
   ```
   parquet_files/
   ├── SIH/
   │   ├── RD/
   │   │   ├── RDAC202301.parquet
   │   │   └── ... 
   ```

4. **Compressão de Dados:**
   - Usar formato Parquet com Snappy
   - Taxa de compressão típica: 70-80%

## 7. Monitoramento e Métricas

| Métrica            | Coleta                  | Alerta                |
|---------------------|-------------------------|-----------------------|
| Uso Memória         | psutil.virtual_memory() | >70% por >5min        |
| Tempo Processamento | time.monotonic()        | >30s para 1GB de dados|
| Erros por Minuto    | logging.error counter   | >10/min               |

## 8. Próximas Etapas

1. Implementar autenticação JWT
2. Adicionar suporte a consultas agendadas
3. Desenvolver dashboard de monitoramento
4. Criar sistema de versionamento de schemas

## 9. Pontos de Atenção

1. **Performance**
   - Processamento arquivo por arquivo
   - Concatenação de todos resultados em memória

2. **Resiliência**
   - Tratamento de erros por arquivo
   - Validação de conexão DB

3. **Limitações**
   - Sem paginação de resultados
   - Sem cache de consultas
   - Autenticação removida temporariamente