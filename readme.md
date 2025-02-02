# PySUS Extract

API para processamento de dados do SIH/SUS com FastAPI, DuckDB e PostgreSQL

## 📋 Funcionalidades Principais
- **Consulta flexível** de dados hospitalares e ambulatoriais
- **Processamento paralelo** de arquivos Parquet
- **Armazenamento automático** em PostgreSQL
- **Paginação** de resultados
- **Consultas personalizadas** via DuckDB
- **Logs detalhados** com rotação diária

## 🚀 Instalação Rápida

1. **Pré-requisitos**
   - Python 3.8+
   - PostgreSQL 12+
   - DuckDB 0.5+

2. **Configuração Inicial**
```bash
git clone https://github.com/seu-usuario/pysus_extract.git
cd pysus_extract
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate    # Windows
pip install -r requirements.txt
```

3. **Banco de Dados**
```sql
-- Crie um banco dedicado
CREATE DATABASE datasus;
```

4. **Variáveis de Ambiente** (`.env`)
```env
DB_USER=postgres
DB_PASSWORD=senha_segura
DB_HOST=localhost
DB_PORT=5432
DB_NAME=datasus
```

## 🛠️ Uso da API

**Iniciar Servidor:**
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

**Endpoint Principal:**
```bash
POST /query
```

**Exemplo de Requisição:**
```json
{
  "base": "SIH",
  "grupo": "RD",
  "cnes_list": ["*"], 
  "campos_agrupamento": ["cnes", "ano_cmpt"],
  "competencia_inicio": "01/2023",
  "competencia_fim": "12/2023",
  "consulta_personalizada": "SELECT cnes, COUNT(*) FROM temp_df GROUP BY cnes"
}
```

**Parâmetros Especiais:**
- `cnes_list`: Use `["*"]` para todos os CNES
- `consulta_personalizada`: SQL DuckDB para processamento adicional

**Resposta de Exemplo:**
```json
{
  "data": [...],
  "pagination": {
    "total": 15000,
    "page": 1,
    "page_size": 1000,
    "total_pages": 15
  },
  "metadata": {
    "table_name": "sih_rd_2023",
    "columns": ["cnes", "ano_cmpt", ...],
    "schema": {"cnes": "TEXT", "ano_cmpt": "INTEGER", ...}
  }
}
```

## 🔍 Documentação de Endpoints

| Método | Endpoint            | Descrição                          |
|--------|---------------------|------------------------------------|
| POST   | /query              | Consulta principal                 |
| POST   | /query/async        | Consulta assíncrona                |
| GET    | /query/jobs/{job_id}| Status de jobs assíncronos         |
| GET    | /docs               | Documentação interativa (Swagger)  |

## ⚙️ Processamento de Dados

1. **Fluxo Principal:**
   ```mermaid
   graph TD
     A[Parâmetros] --> B(Validação)
     B --> C{CNES = '*'?}
     C -->|Sim| D[Processar todos]
     C -->|Não| E[Filtrar CNES]
     D/E --> F[Agrupar dados]
     F --> G{Consulta personalizada?}
     G -->|Sim| H[Executar SQL]
     G -->|Não| I[Converter tipos]
     H --> I --> J[Salvar no PostgreSQL]
   ```

2. **Performance:**
   - Processa ~1M registros/segundo
   - Suporta até 10 requisições simultâneas

## 🐛 Depuração

**Verificar Logs:**
```bash
tail -f logs/app_$(date +%F).log
```

**Exemplo de Log:**
```
2023-10-15 14:30:45 INFO: Processando 25 arquivos [workers=4]
2023-10-15 14:32:10 INFO: Consulta concluída em 85s [total=1.2M]
```

**Testes Automatizados:**
```bash
pytest tests/ -v --cov=main --cov-report=term-missing
```

## 📚 Recursos Adicionais

- [Documentação DuckDB](https://duckdb.org/docs/)
- [Esquema de Dados SIH/SUS](https://datasus.saude.gov.br/)
- [FastAPI Best Practices](https://fastapi.tiangolo.com/pt/advanced/)

## 📄 Licença
MIT License - Consulte o arquivo [LICENSE](LICENSE) para detalhes
