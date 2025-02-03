from airflow import DAG
import os
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from helpers.postgres_helper import execute_sql_file







SQL_FOLDER = "opt/airflow/dags/sql"

sql_files = sorted([f for f in os.listdir(SQL_FOLDER) if f.endswith(".sql")])

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024,1,1),
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    "DAG1_test",
    default_args=default_args,
    schedule_internal="@daily",
    catchup=False
) as dag:
    
    for sql_file in sql_files:
        table_name = sql_file.replace("create_", "").replace(".sql","")
        sql_file_path=os.path.join(SQL_FOLDER,sql_file)