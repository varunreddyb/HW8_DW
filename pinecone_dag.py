from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
import pinecone
import json
from sentence_transformers import SentenceTransformer

INDEX_NAME = "quickstart"
MODEL_NAME = "all-MiniLM-L6-v2"
INPUT_PATH = "/opt/airflow/dags/input.json"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    'pinecone_workflow',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

from pinecone import Pinecone, ServerlessSpec

def create_index():
    pc = Pinecone(api_key=Variable.get("PINECONE_API_KEY"))
    if INDEX_NAME not in pc.list_indexes().names():
        pc.create_index(
            name=INDEX_NAME,
            dimension=384,
            metric="cosine",
            spec=ServerlessSpec(
                cloud="aws",
                region="us-east-1"
            )
        )
        print("✅ Index created")
    else:
        print("ℹ️ Index already exists")

def embed_and_ingest():
    pc = Pinecone(api_key=Variable.get("PINECONE_API_KEY"))
    index = pc.Index(INDEX_NAME)

    with open(INPUT_PATH, "r") as f:
        texts = json.load(f)

    model = SentenceTransformer(MODEL_NAME)
    embeddings = model.encode(texts).tolist()
    vectors = [(str(i), vec) for i, vec in enumerate(embeddings)]
    index.upsert(vectors)
    print("✅ Vectors ingested into Pinecone")

def query_index():
    pc = Pinecone(api_key=Variable.get("PINECONE_API_KEY"))
    index = pc.Index(INDEX_NAME)

    model = SentenceTransformer(MODEL_NAME)
    query = "workflow orchestration"
    vector = model.encode([query])[0].tolist()

    result = index.query(vector=vector, top_k=3)
    print("🔍 Search result:", result)

create_task = PythonOperator(task_id="create_index", python_callable=create_index, dag=dag)
embed_task = PythonOperator(task_id="embed_and_ingest", python_callable=embed_and_ingest, dag=dag)
query_task = PythonOperator(task_id="query_index", python_callable=query_index, dag=dag)

create_task >> embed_task >> query_task
