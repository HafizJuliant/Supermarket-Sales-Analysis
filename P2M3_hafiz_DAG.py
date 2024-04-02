# Import Libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2 as db
import re
import pandas as pd
from elasticsearch import Elasticsearch

import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': ' hafiz ',
    # change the datetime for proceeding the automation accordingly
     'start_date': datetime(2024, 3, 23, 14, 13, 0) - timedelta(hours = 7),
    # 'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
    # 'depends_on_past': False,
    # 'max_active_runs': 1,
    'catchup': False,
    # 'execution_timeout': timedelta(minutes=30),
}
def fetch_data_from_postgres():
    conn_string = "dbname='airflow' user='airflow' password='airflow' host='postgres' port=5432"
    conn = db.connect(conn_string)
    # Take data from the table
    df = pd.read_sql("SELECT * FROM table_m3", conn)
    df.to_csv('/opt/airflow/dags/P2M3_hafiz_raw.csv')
    conn.close()

def data_cleaning_task():
    csv_file_path = '/opt/airflow/dags/P2M3_hafiz_raw.csv'
    dataset = pd.read_csv(csv_file_path)
    # Preprocess Column Name
    preprocess_col = []
    for col in dataset.columns:
        # Case Folding
        col = col.lower()

        # Mengganti '-' dan ' ' menjadi '_'
        col = re.sub(r'[-\s]', '_', col)

        # replace '(usd)' to ''
        col = col.replace('(usd)', '')
        
        # Remove white space
        col = col.rstrip('_')
        preprocess_col.append(col)

    dataset.columns = preprocess_col

    # Handle Duplicate
    if dataset.duplicated().sum() > 0:
        dataset.drop_duplicates(keep='last')

    # Handle missing value
    for col in dataset.columns:
        if dataset[col].isnull().sum() > 0:
            if col in dataset.select_dtypes(include='object').columns:
                dataset[col].fillna(dataset[col].mode())
            elif col in dataset.select_dtypes(include='number').columns:
                dataset[col].fillna(dataset[col].mean())

    # Save the cleaned data to CSV
    data_csv= dataset.to_csv('/opt/airflow/dags/P2M3_hafiz_clean.csv', index=False)
    
    return data_csv

def transport_to_elasticsearch(): 
    es = Elasticsearch(hosts='elasticsearch') #define the elasticsearch url
    df=pd.read_csv('/opt/airflow/dags/P2M3_hafiz_clean.csv') 
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="milestone_3", doc_type = "doc", body=doc)

# Define the DAG, the data is updated every 2 minutes
with DAG(
    "project_m3",
    description='Project Milestone 3',
    schedule_interval='30 6 * * *',
    default_args=default_args,
    catchup=False
) as dag:
    
    # Task 1: Fetch data from PostgreSQL
    fetch_data = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_from_postgres,
    )

    # Task for data cleaning
    data_cleaning = PythonOperator(
        task_id='data_cleaning_process',
        python_callable=data_cleaning_task,
    )

    # Task 3: Transport clean CSV into Elasticsearch
    post_to_elasticsearch = PythonOperator(
        task_id='send_to_elasticsearch_task',
        python_callable=transport_to_elasticsearch,
    )
# Define task dependencies
fetch_data >> data_cleaning >> post_to_elasticsearch