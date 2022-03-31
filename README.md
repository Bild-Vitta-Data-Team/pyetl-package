# pyetl-package
[![codecov](https://codecov.io/gh/Bild-Vitta-Data-Team/pyetl-package/branch/main/graph/badge.svg?token=IFMEMUDG19)](https://codecov.io/gh/Bild-Vitta-Data-Team/pyetl-package)  

Ferramenta para facilitação do processo ETL feita em python

# Uso básico de algumas funcionalidades criadas
## DB_Connector  
Classe para montar conexões com varios tipos de bancos de dados utilizando SQLAlchemy.    
Exemplo de utilização:  
```python
data_source_engine = "SQL" # para MS SQLServer  
credentials = {  
    "host" : "your_sqlserver_host",  
    "db": "database_name",  
    "username": "db_username",  
    "pwd": "db_password"  
}  
connector = DB_Connector(data_source_engine, credentials)  
connector_engine = connector.create_data_source_connection()  
# or you can make:  connector_engine = DB_Connector(data_source_engine, credentials).create_data_source_connection()  
```

## DbHelper
Helper criado para auxiliar na extração de dados de um banco transacional possibilitando salva-los em uma nova tabela.  
Exemplo:  
```python
table_name = "users"
columns = "name, age, city"
data_source_engine = some_sqlalchemy_engine # popde ser criado com o DB_Connector
cutoff_columns = ["created_at", "modified_on"] # colunas de data para serem usadas como corte
query = "" #pode ser construída antes ou depois da inicialização

helper = DbHelper(table_name, columns, ds_engine, cutoff_columns, query)

helper.query = helper.select_query() #definição de uma query de select para a tabela no data_source

data_storage_engine = some_sqlalchemy_engine # popde ser criado com o DB_Connector
table_prefix = "STG_" 
data_storage_schema = "Stage_Exemplo"
# para salvar no seu db 
helper.insert_dw(data_storage_engine, table_prefix, data_storage_schema)

```

## FileHelper
Helper criado para auxiliar na extração de dados vindos de planilhas ou arquivos Json ou XML.  
Exemplo: 

```python
filepath = "./test.xslx"
filetype = "xlsx"

helper = FileHelper(filepath, filetype)

data_frame = helper.read_xls_to_dataframe()


# depois de fazer os ajustes necessarios (ou não), é só usar:
data_storage_engine = some_sqlalchemy_engine # popde ser criado com o DB_Connector
table_prefix = "STG_" 
data_storage_schema = "Stage_Exemplo"
table_name = "TestePlanilha"
helper.dataframe_to_db(data_frame, table_prefix, table_name, data_storage_engine, data_storage_schema)

```

## APIHelper
Helper criado para auxiliar na extração de dados vindos de api's.  
Exemplo:
```python
origin_url = "https:www.exemplo.com"
# não obrigatório
request_headers = {
    "Authorization": "Token <secutiry_token>"
    }
# não obrigatório
request_data = {"data" : "some request data"} 

helper = APIHelper(origin_url, request_headers, request_data)

#se quiser a resposta ja como um dataframe do pandas
data_frame = helper.get_data(dataframe=True)

#se quiser a resposta como json para depois transpormar em um dataframe na sua aplicação
response = helper.get_data()

# uma vez com um dataframe definido e desejar salvar no seu banco de dados
data_storage_engine = some_sqlalchemy_engine # popde ser criado com o DB_Connector
table_prefix = "STG_" 
data_storage_schema = "Stage_Exemplo"
table_name = "TesteAPI"

helper.dataframe_to_dw(data_frame, table_prefix, table_name, data_storage_engine, data_storage_schema)

```

## Uso básico da ferramenta de empacotamento/gerência de dependências: Poetry
### Instalação em : osx / linux / bashonwindows 
```bash
curl -sSL https://install.python-poetry.org | python3 -
```
## Uso básico do Poetry:
Uma vez instalado, para instalar as dependências do projeto é só usar o comando `poetry install`.   
Para adicionar uma nova dependência utilize `poetry add <nome do pacote>`.  
Para remover alguma dependência utilize `poetry remove <nome do pacote>`.   
Para ver a árvore do projeto utiliz `poetry show --tree`.  

Um breve exemlpo de utilização:
![Poetry Install](https://raw.githubusercontent.com/python-poetry/poetry/master/assets/install.gif)

