# PySUS Extract

Projeto para baixar e armazenar dados do Sistema de Informações Hospitalares (SIH) utilizando a biblioteca PySUS e PostgreSQL.

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

    Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

    ```env
    DB_USER=seu_usuario
    DB_PASSWORD=sua_senha
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=seu_banco
    ```

## **Uso**

1. **Executar o script principal para baixar os dados e inserir no PostgreSQL:**

    ```bash
    python main.py
    ```

2. **Verificar os logs em `logs/app.log` para monitorar o processo.**

## **Contribuição**

Sinta-se à vontade para abrir issues e enviar pull requests para melhorar este projeto.

## **Licença**

[MIT](LICENSE)
