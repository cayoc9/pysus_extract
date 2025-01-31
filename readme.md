# PySUS Extract

API para extração, processamento e armazenamento de dados do Sistema de Informações Hospitalares (SIH) e Sistema de Informações Ambulatoriais (SIA) utilizando FastAPI, DuckDB e PostgreSQL.

## **Instalação**

1. **Clone o repositório:**
    ```bash
    git clone https://github.com/cayo/pysus_extract.git
    cd pysus_extract
    ```

2. **Crie e ative um ambiente virtual:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # No Windows: venv\Scripts\activate
    ```

3. **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

4. **Configure as variáveis de ambiente:**
    Crie um arquivo `.env` na raiz do projeto:
    ```env
    DB_USER=seu_usuario
    DB_PASSWORD=sua_senha
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=seu_banco
    ```

## **Estrutura de Diretórios**
```
.
├── parquet_files/     # Arquivos Parquet (SIH/SIA)
├── consultas/         # Arquivos CSV gerados
├── logs/             # Logs da aplicação
└── modulos/          # Módulos do sistema
```

## **Uso da API**

1. **Inicie o servidor:**
    ```bash
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload
    ```

2. **Exemplo de consulta:**
    ```bash
    curl -X POST "http://0.0.0.0:8000/query" \
      -H "Content-Type: application/json" \
      -d '{
        "base": "SIH",
        "grupo": "RD",
        "cnes_list": ["2077485", "2077493"],
        "campos_agrupamento": ["CNES", "ANO_CMPT", "MES_CMPT"],
        "competencia_inicio": "01/2022",
        "competencia_fim": "12/2022",
        "table_name": "minha_tabela_personalizada"  # Parâmetro opcional
      }'
    ```

    > **Nota:** O parâmetro `table_name` é opcional. Se não for fornecido, o sistema gerará automaticamente um nome baseado na base e grupo selecionados.

3. **Verifique os logs em:**
    - API: `logs/app.log`
    - Consultas: `consultas/[nome_tabela]_[timestamp].csv`

## **Grupos Suportados**

- **SIH**: RD (AIH Reduzida), RJ (Rejeitadas), SP (Serviços Profissionais)
- **SIA**: PA (Produção Ambulatorial), BI (Individualizado)

## **Documentação API**

Acesse a documentação interativa em: `http://0.0.0.0:8000/docs`

## **Contribuição**

Sinta-se à vontade para abrir issues e enviar pull requests.

## **Licença**

[MIT](LICENSE)
