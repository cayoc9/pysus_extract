# Requisitos API FastAPI DataSUS

## Endpoint Principal
- Método: GET 
- Path: `/query`

## Parâmetros de Entrada
- base: string (ex: "SIH") - Base de dados do DataSUS
- grupo: string (ex: "RD") - Grupo de dados
- estados: array[string] - Lista de estados desejados
- colunas: array[string] - Lista de colunas desejadas
- competencia_inicio: string (formato: "MM/YYYY")
- competencia_fim: string (formato: "MM/YYYY")

## Autenticação
- JWT token obrigatório
- Token deve ser enviado no header Authorization

## Processamento
1. Validar parâmetros de entrada
2. Localizar arquivos parquet correspondentes em /parquet_files/{base}/{grupo}/'{grupo}{UF}{MM}{YY}.parquet'/*
3. Carregar dados usando PyArrow
4. Filtrar por competência, estado e colunas solicitadas
5. Retornar DataFrame serializado
6. inserir dados no banco de dados PostgreSQL na tabela correspondente ao grupo

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

