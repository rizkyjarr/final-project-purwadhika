from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yaml
from airflow.utils.task_group import TaskGroup
from helpers.bigquery_helper import extract_incremental_data_postgre,ensure_table_exist, upsert_to_bigquery
import pandas as pd


with open("/opt/airflow/dags/helpers/postgre_tables.yaml", "r") as file:
    config = yaml.safe_load(file)

def extract_and_push_to_xcom(**kwargs):
    """Extracts data and pushes DataFrame as JSON to XCom"""
    table_name = kwargs["table_name"]
    incremental_column = kwargs["incremental_column"]
    task_instance = kwargs['ti']  # ✅ Explicitly reference TaskInstance

    print(f"🔄 Extracting data for {table_name} using incremental column {incremental_column}...")

    df = extract_incremental_data_postgre(table_name, incremental_column)

    if df is None:
        print(f"❌ ERROR: extract_incremental_data_postgre() returned None for {table_name}.")
        return

    if df.empty:
        print(f"⚠️ No data extracted for {table_name}. Skipping XCom push.")
        return

    df_json = df.to_json()
    xcom_key = f"{table_name}_data"

    # ✅ Push to XCom with task context
    task_instance.xcom_push(key=xcom_key, value=df_json)

    print(f"✅ Extracted {len(df)} rows for {table_name} and stored in XCom with key '{xcom_key}'.")

def pull_from_xcom_and_upsert(**kwargs):
    """Retrieves JSON from XCom, converts back to DataFrame, and upserts"""
    table_name = kwargs["table_name"]
    primary_key = kwargs["primary_key"]
    task_instance = kwargs['ti']  # ✅ Explicit TaskInstance

    print(f"🔄 Pulling data from XCom for table: {table_name}...")

    # ✅ Correct task_id formatting (especially inside TaskGroups)
    task_id = f"{table_name}_data.extract_incremental_{table_name}"  
    xcom_key = f"{table_name}_data"

    print(f"🔍 Looking for XCom key: {xcom_key} from task: {task_id}")

    # ✅ Ensure correct task_id when pulling from XCom
    df_json = task_instance.xcom_pull(task_ids=task_id, key=xcom_key)

    if not df_json:
        print(f"❌ ERROR: No XCom data found for key '{xcom_key}'. Check task {task_id}.")
        return

    # ✅ Debug: Print JSON Data
    print(f"✅ Retrieved JSON from XCom: {df_json[:500]}...")  # Print only first 500 chars

    df = pd.read_json(df_json)

    print(f"✅ Successfully converted JSON to DataFrame with {len(df)} rows.")

    upsert_to_bigquery(df, table_name, primary_key)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 1),
    "retries": 5,
    "retry_delay":timedelta(seconds=10)
}

with DAG(
    "DAG2_load_to_BigQuery",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False

) as dag:
    
    for table in config["postgre_tables"]:
        table_name = table["name"]
        partition_field = table["partition_field"]
        primary_key = table["primary_key"]
        incremental_column = table["incremental_column"]

        with TaskGroup(group_id=f"{table_name}_data") as table_group:

            ensure_table_task = PythonOperator(
                task_id=f"ensure_table_exists_{table_name}",
                python_callable=ensure_table_exist,
                op_kwargs={
                    "table_name": table_name,
                    "partition_field": partition_field
                }
            )

            fetch_table_task = PythonOperator(
                task_id=f"extract_incremental_{table_name}",
                python_callable=extract_and_push_to_xcom,
                provide_context=True,
                op_kwargs={
                    "table_name": table_name,
                    "incremental_column": incremental_column
                }
            )

            upsert_task = PythonOperator(
                task_id=f"upsert_{table_name}",
                python_callable=pull_from_xcom_and_upsert,
                provide_context=True,
                op_kwargs={
                    "table_name": table_name,
                    "primary_key": primary_key
                }
            )

            # Task Dependencies
            ensure_table_task >> fetch_table_task >> upsert_task 