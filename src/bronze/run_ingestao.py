import os
import requests
from requests.auth import HTTPBasicAuth
import dotenv

dotenv.load_dotenv(dotenv.find_dotenv('.env'))

airflow_url = "http://localhost:8099/api/v1/dags/bronze_igdb/dagRuns"
user = os.getenv("user")
password = os.getenv("password")
raw_data_path = "../raw/data"

# Listar diretórios em raw/data
directories = [d for d in os.listdir(raw_data_path) if os.path.isdir(os.path.join(raw_data_path, d))]

# Loop para chamar a API do Airflow para cada diretório
for table in directories:
    # Monta o payload
    payload = {
        "conf": {
            "table": table,
            "id_fields": "id",
            "timestamp_field": "updated_at"
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
