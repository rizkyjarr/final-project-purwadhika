from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from helpers.postgres_helper import execute_sql_file 

# Folder where SQL files are stored
SQL_FOLDER = "/opt/airflow/dags/include/sql"  # Adjust based on your Airflow environment

# Get all SQL files in the folder
sql_files = sorted([f for f in os.listdir(SQL_FOLDER) if f.endswith(".sql")])

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 1),
    "retries": 1
}

# Define the DAG
with DAG(
    "create_tables_dag",
    default_args=default_args,
    schedule_interval="@daily",  # Runs daily; adjust as needed
    catchup=False
) as dag:

    # Create a task for each SQL file
    for sql_file in sql_files:
        table_name = sql_file.replace("create_", "").replace(".sql", "")  # Extract table name
        sql_file_path = os.path.join(SQL_FOLDER, sql_file)

        create_table_task = PythonOperator(
            task_id=f"create_{table_name}_table",
            python_callable=execute_sql_file,
            op_args=[sql_file_path, table_name]
        )

        create_table_task  # Each task runs independently