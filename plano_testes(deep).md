# Plano de Testes Detalhado - API DataSUS

## 1. Estratégia de Testes
- **Abordagem**: Testes em pirâmide (mais unitários, menos E2E)
- **Ferramentas**: pytest, pytest-mock, DuckDB em memória, Testcontainers
- **Cobertura mínima**: 85%

## 2. Testes Unitários

### 2.1 Validação de Parâmetros (QueryParams)
```python
def test_validacao_base_invalida():
    with pytest.raises(ValueError) as exc:
        QueryParams(
            base="INVALIDO",
            grupo="RD",
            cnes_list=["123"],
            campos_agrupamento=["CNES"],
            competencia_inicio="01/2022",
            competencia_fim="12/2022"
        )
    assert "Base deve ser SIH ou SIA" in str(exc.value)

def test_validacao_grupo_invalido():
    with pytest.raises(ValueError) as exc:
        QueryParams(
            base="SIH",
            grupo="INVALIDO",
            cnes_list=["123"],
            campos_agrupamento=["CNES"],
            competencia_inicio="01/2022",
            competencia_fim="12/2022"
        )
    assert "Grupo inválido" in str(exc.value)
```

### 2.2 Funções de Arquivos Parquet
```python
def test_get_parquet_files_sem_arquivos(mocker):
    mocker.patch("glob.glob", return_value=[])
    files = get_parquet_files("SIH", "RD", "01/2020", "12/2020")
    assert len(files) == 0
```

## 3. Testes de Integração

### 3.1 Processamento de Dados
```python
async def test_processamento_arquivo_vazio():
    df = process_parquet_files([], QueryParams(...))
    assert df.empty
```

### 3.2 Conexão com Banco de Dados
```python
def test_conexao_banco_dados():
    try:
        verify_db_connection()
        assert True
    except Exception:
        pytest.fail("Conexão com banco falhou")
```

## 4. Testes de API

### 4.1 Endpoint /query
```python
async def test_query_sucesso(client):
    payload = {
        "base": "SIH",
        "grupo": "RD",
        "cnes_list": ["2077485"],
        "campos_agrupamento": ["CNES"],
        "competencia_inicio": "01/2022",
        "competencia_fim": "12/2022"
    }
    response = client.post("/query", json=payload)
    assert response.status_code == 200
    assert "dados" in response.json()
```

## 5. Testes de Performance

### 5.1 Tempo de Resposta
```python
def test_tempo_resposta():
    start_time = time.time()
    # Executar consulta
    assert time.time() - start_time < 30  # 30 segundos
```

## 6. Cenários de Erro

### 6.1 Arquivo Corrompido
```python
async def test_arquivo_corrompido(mocker):
    mocker.patch("duckdb.connect").execute.side_effect = Exception("Erro leitura")
    with pytest.raises(HTTPException) as exc:
        process_parquet_files(["corrupt.parquet"], QueryParams(...))
    assert exc.value.status_code == 500
```

## 7. Ambiente de Testes
```bash
# Configurar ambiente
docker-compose -f docker-compose.test.yml up -d
pytest tests/ --cov=app --cov-report=html
```

## 8. Monitoramento
- Cobertura de código
- Logs de execução
- Métricas de performance

## 9. Riscos
1. Dados de teste inconsistentes
2. Configurações de ambiente incorretas
3. Variação no tempo de resposta
