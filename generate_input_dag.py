from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    dag_id="generate_input",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Downloads text and creates input.json for Pinecone"
)

def download_and_generate_input():
    url = "https://www.gutenberg.org/files/1342/1342-0.txt"
    response = requests.get(url)
    text = response.text

    # Process: get the first 5 non-empty lines
    lines = text.splitlines()
    sentences = [line.strip() for line in lines if len(line.split()) > 5][:5]

    with open("/opt/airflow/dags/input.json", "w") as f:
        json.dump(sentences, f)

    print("✅ input.json generated:", sentences)

generate_task = PythonOperator(
    task_id="download_and_generate_input",
    python_callable=download_and_generate_input,
    dag=dag
)
