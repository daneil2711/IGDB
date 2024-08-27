import os
import requests
from requests.auth import HTTPBasicAuth
import dotenv

dotenv.load_dotenv(dotenv.find_dotenv('.env'))

airflow_url = "http://localhost:8099/api/v1/dags/silver_igdb/dagRuns"
user = os.getenv("user")
password = os.getenv("password")
etl_silver = "./etl"

# Listar diretórios em raw/data
table_list = [os.path.splitext(f)[0] for f in os.listdir(etl_silver) if os.path.isfile(os.path.join(etl_silver, f))]

# Loop para chamar a API do Airflow para cada diretório
for table in table_list:
    # Monta o payload
    payload = {
        "conf": {
            "table": table
        }
    }
    # Faz a chamada para a API do Airflow
    response = requests.post(
        airflow_url,
        json=payload,
        auth=HTTPBasicAuth(user, password)
    )

    # Verifica o status da requisição
    if response.status_code == 200:
        print(f"DAG para a tabela {table} iniciada com sucesso.")
    else:
        print(f"Falha ao iniciar DAG para a tabela {table}. Status: {response.status_code}, Detalhes: {response.text}")
