# Documentação do Projeto PySUS SIH/SIA

## Visão Geral
O projeto consiste em uma API REST desenvolvida com FastAPI para processar e consultar dados do DataSUS (SIH - Sistema de Informações Hospitalares e SIA - Sistema de Informações Ambulatoriais).

## Arquitetura e Componentes

### 1. Configurações Básicas
- Utiliza variáveis de ambiente (.env)
- Sistema de logging rotativo (logs diários)
- Configurações de banco de dados PostgreSQL
- Middleware CORS habilitado

### 2. Estruturas de Dados
- **grupos_dict**: Mapeamento de códigos para nomes descritivos dos grupos
- **CAMPOS_CNES**: Mapeamento das colunas CNES por grupo
- **GRUPOS_INFO**: Schema detalhado de cada grupo (RD, RJ, ER, PA, SP)

### 3. Fluxo Principal de Processamento

#### 3.1. Entrada de Dados (QueryParams)
- `base`: SIH ou SIA
- `grupo`: Código do grupo (RD, RJ, ER, etc.)
- `cnes_list`: Lista de códigos CNES
- `campos_agrupamento`: Campos para agrupamento
- `competencia_inicio` e `competencia_fim`: Período de análise
- `table_name`: Nome da tabela destino (opcional)

#### 3.2. Pipeline de Processamento

1. **Localização de Arquivos** (`get_parquet_files`)
   - Recebe: base, grupo, período
   - Busca arquivos .parquet correspondentes
   - Retorna: lista de caminhos dos arquivos

2. **Processamento dos Arquivos** (`process_parquet_files`)
   - Recebe: lista de arquivos e parâmetros
   - Utiliza DuckDB para consultas
   - Agrupa dados conforme especificado
   - Converte tipos de dados
   - Retorna: DataFrame processado

3. **Conversão de Tipos** (`convert_datatypes`)
   - Recebe: DataFrame e informações do schema
   - Converte cada coluna para o tipo apropriado
   - Registra problemas de conversão
   - Retorna: DataFrame com tipos convertidos

4. **Salvamento dos Resultados** (`save_results`)
   - Recebe: DataFrame e nome da tabela
   - Salva no PostgreSQL (replace)
   - Registra operações no log

### 4. Funções Auxiliares

#### 4.1. Logging (`log_execution`)
- Padroniza formato dos logs
- Marca início/fim das operações
- Facilita rastreamento de execução

#### 4.2. Validações
- Validação de base de dados
- Validação de grupos
- Validação de formato de competência
- Validação de conexão com banco

### 5. Endpoint Principal (/query)

```json
POST /query
{
    "base": "SIH",
    "grupo": "RD",
    "cnes_list": ["2077485"],
    "campos_agrupamento": ["CNES", "ANO_CMPT"],
    "competencia_inicio": "01/2022",
    "competencia_fim": "12/2022",
    "table_name": "sih_rd_2022"
}