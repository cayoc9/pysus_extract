# Requisitos API FastAPI DataSUS

## Endpoint Principal
- Método: GET 
- Path: `/query`

## Parâmetros de Entrada
- base: string (ex: "SIH") - Base de dados do DataSUS
- grupo: string (ex: "SP") - Grupo de dados
- cnes_list: array[string] - Lista de CNES desejados
- campos_agrupamento: array[string] - Lista de colunas desejadas
- competencia_inicio: string (formato: "MM/YYYY")
- competencia_fim: string (formato: "MM/YYYY")
- table_name: string - Nome da tabela no banco de dados PostgreSQL

## Regras de Negócio
- buscar as colunas no parquet em maiusculo
- subir os dados para o banco em minusculo
- Sempre adicionar o CNES nas colunas selecionadas automaticamente


## Autenticação
- JWT token obrigatório
- Token deve ser enviado no header Authorization

## Processamento
1. Validar parâmetros de entrada
2. Localizar arquivos parquet correspondentes em /parquet_files/{base}/{grupo}/'{grupo}{UF}{YY}{MM}.parquet'/*
3. Carregar dados usando PyArrow e duckdb
4. Filtrar cnes selecionado e incluir colunas solicitadas
5. Retornar dados serializados
6. inserir dados no banco de dados PostgreSQL na tabela enviada

## Respostas
- 200: DataFrame serializado com dados encontrados
- 400: Erro de validação dos parâmetros
- 401: Erro de autenticação
- 404: Dados não encontrados
- 500: Erro interno

## Restrições
- Sem limite de registros por consulta
- Validação obrigatória do formato das competências
- Verificação de existência das colunas solicitadas

## Tratamento de Erros
- Validar existência da base/grupo
- Verificar arquivos parquet corrompidos
- Tratar competências fora do período disponível
- Validar colunas inexistentes

## Performance
- Cache de metadados dos arquivos parquet
- Leitura otimizada usando PyArrow
- Logging de tempo de execução das queries

Edit



Documento de requisitos criado para a API FastAPI DataSUS. Define endpoint GET /query para consulta em arquivos parquet, autenticação JWT, parâmetros de entrada (base, grupo, colunas, período), formatos de resposta e tratamento de erros. Inclui requisitos de performance e validações necessárias.

