# Plano de Testes - API DataSUS

## 1. Introdução

Este documento descreve o plano de testes para a API DataSUS implementada em `main.py`. O objetivo é garantir a qualidade, confiabilidade e robustez da aplicação através de testes sistemáticos de todas as funcionalidades.

## 2. Escopo dos Testes

### 2.1 Componentes a serem testados
- Validação de parâmetros de entrada
- Processamento de arquivos Parquet
- Conexão com banco de dados
- Endpoints da API
- Manipulação de dados
- Sistema de logging
- Tratamento de erros

### 2.2 Fora do escopo
- Testes de performance em larga escala
- Testes de segurança aprofundados
- Testes de interface do usuário

## 3. Tipos de Testes

### 3.1 Testes Unitários

#### 3.1.1 Validação de Parâmetros (QueryParams)
- Teste de validação da base (SIH/SIA)
- Teste de validação do grupo
- Teste de validação do formato de competência
- Teste de validação da lista de CNES
- Teste de validação dos campos de agrupamento

#### 3.1.2 Funções de Processamento
- `get_parquet_files`
  - Teste com intervalo de datas válido
  - Teste com intervalo de datas inválido
  - Teste com arquivos inexistentes
  - Teste com diferentes UFs

- `get_cnes_column`
  - Teste com grupos mapeados
  - Teste com grupos não mapeados
  - Teste com casos especiais

- `process_parquet_files`
  - Teste com arquivos válidos
  - Teste com arquivos vazios
  - Teste com diferentes campos de agrupamento
  - Teste de filtro por CNES

#### 3.1.3 Funções de Persistência
- `save_results`
  - Teste de salvamento no PostgreSQL
  - Teste de salvamento em CSV
  - Teste com diferentes tamanhos de DataFrame
  - Teste de tratamento de erros de conexão

#### 3.1.4 Funções de Gerenciamento de Recursos
- `ManagedThreadPool`
  - Teste de alocação dinâmica de workers
  - Teste de throttling sob alta carga
  - Teste de shutdown seguro
  - Teste de liberação de recursos remanescentes

- `DuckDBConnection`
  - Teste de pool de conexões
  - Teste de reutilização de conexões
  - Teste de reset de conexão
  - Teste de limite máximo do pool

- `MemoryGuardian`
  - Teste de detecção de alta memória
  - Teste de limpeza de emergência
  - Teste de encerramento seguro

### 3.2 Testes de Integração

#### 3.2.1 Banco de Dados
- Teste de conexão com PostgreSQL
- Teste de persistência de dados
- Teste de recuperação após falha de conexão
- Teste de concorrência

#### 3.2.2 Sistema de Arquivos
- Teste de leitura de arquivos Parquet
- Teste de criação de logs
- Teste de criação de CSVs
- Teste de permissões de acesso

#### 3.2.3 Integração com Resource Manager
- Teste de processamento com múltiplos pools
- Teste de concorrência controlada
- Teste de recuperação após OOM
- Teste de alocação de memória em batches

### 3.3 Testes de API

#### 3.3.1 Endpoint /query
- Teste de requisição bem-sucedida
- Teste com parâmetros inválidos
- Teste com dados não encontrados
- Teste de timeout
- Teste de concorrência
- Teste de diferentes formatos de resposta

### 3.4 Testes de Carga
- Teste com múltiplas requisições simultâneas
- Teste com grandes volumes de dados
- Teste de tempo de resposta
- Teste de consumo de recursos

## 4. Casos de Teste Detalhados

### 4.1 Testes do Endpoint /query

#### TC001 - Consulta Básica SIH
```python
{
    "base": "SIH",
    "grupo": "RD",
    "cnes_list": ["2077485"],
    "campos_agrupamento": ["CNES", "ANO_CMPT"],
    "competencia_inicio": "01/2022",
    "competencia_fim": "12/2022"
}
```
**Resultado Esperado**: Status 200, dados agrupados por CNES e ano

#### TC002 - Consulta Básica SIA
```python
{
    "base": "SIA",
    "grupo": "PA",
    "cnes_list": ["2077485"],
    "campos_agrupamento": ["CNES", "PROC_ID"],
    "competencia_inicio": "06/2022",
    "competencia_fim": "06/2022"
}
```
**Resultado Esperado**: Status 200, dados agrupados por CNES e procedimento

### 4.2 Testes de Validação

#### TC003 - Base Inválida
```python
{
    "base": "INVALID",
    "grupo": "RD",
    "cnes_list": ["2077485"],
    "campos_agrupamento": ["CNES"],
    "competencia_inicio": "01/2022",
    "competencia_fim": "12/2022"
}
```
**Resultado Esperado**: Status 422, mensagem de erro de validação

#### TC004 - Formato de Competência Inválido
```python
{
    "base": "SIH",
    "grupo": "RD",
    "cnes_list": ["2077485"],
    "campos_agrupamento": ["CNES"],
    "competencia_inicio": "2022-01",
    "competencia_fim": "2022-12"
}
```
**Resultado Esperado**: Status 422, mensagem de erro de validação

## 5. Ambiente de Testes

### 5.1 Requisitos
- PostgreSQL configurado
- Arquivos Parquet de exemplo
- Python 3.8+
- Dependências instaladas
- Variáveis de ambiente configuradas

### 5.2 Configuração
```bash
# Variáveis de ambiente necessárias
DB_USER=test_user
DB_PASSWORD=test_pass
DB_HOST=localhost
DB_PORT=5432
DB_NAME=test_db
```

## 6. Ferramentas de Teste

### 6.1 Framework de Testes
- pytest para testes unitários e de integração
- pytest-asyncio para testes assíncronos
- pytest-cov para cobertura de código

### 6.2 Ferramentas Auxiliares
- Docker para ambiente isolado
- locust para testes de carga
- pytest-mock para mocking
- faker para geração de dados

## 7. Critérios de Aceitação

### 7.1 Cobertura de Código
- Mínimo de 80% de cobertura total
- 100% de cobertura em funções críticas
- Todos os caminhos de erro testados

### 7.2 Performance
- Tempo de resposta < 5s para consultas simples
- Tempo de resposta < 30s para consultas complexas
- Suporte a 10 requisições simultâneas

### 7.3 Qualidade
- Sem erros críticos
- Tratamento adequado de todos os casos de erro
- Logs claros e informativos

## 8. Cronograma Atualizado
1. Testes de Unidade do Resource Manager (2 dias)
2. Testes de Integração com Processamento (3 dias)
3. Testes de Carga com Monitoramento (2 dias)
4. Análise de Resultados e Ajustes (1 dia)

## 9. Responsabilidades

- Desenvolvedor: Implementação dos testes
- QA: Revisão e execução dos testes
- DevOps: Configuração do ambiente
- Tech Lead: Aprovação final

## 10. Riscos e Mitigações

### Riscos
1. Indisponibilidade do banco de dados
2. Arquivos Parquet corrompidos
3. Sobrecarga do sistema

### Mitigações
1. Banco de dados local para testes
2. Conjunto de dados de teste validado
3. Limites de carga configuráveis
