import os
import requests
import json
from requests.auth import HTTPBasicAuth
import dotenv

dotenv.load_dotenv(dotenv.find_dotenv('../.env'))

airflow_url = "http://localhost:8099/api/v1/dags/features_igdb/dagRuns"
user = os.getenv("user")
password = os.getenv("password")
jobs_dir = "./jobs"

job_files = [f for f in os.listdir(jobs_dir) if os.path.isfile(os.path.join(jobs_dir, f))]

# Loop para cada arquivo de configuração
for job_file in job_files:
    with open(os.path.join(jobs_dir, job_file), 'r') as f:
        config = json.load(f)
        config['id_fields'] = [str(field) for field in config['id_fields']]  # Garante que todos os itens são strings
        payload = {"conf": config}

    # Faz a chamada para a API do Airflow
    response = requests.post(
        airflow_url,
        json=payload,
        auth=HTTPBasicAuth(user, password)
    )

    # Verifica o status da requisição
    if response.status_code == 200:
        print(f"DAG para a tabela {payload['conf']['table']} iniciada com sucesso.")
    else:
        print(f"Falha ao iniciar DAG para a tabela {payload['conf']['table']}. Status: {response.status_code}, Detalhes: {response.text}")
