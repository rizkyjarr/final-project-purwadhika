from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers.scraping_helper import run_pipeline

# === AIRFLOW DAG CONFIGURATION === #
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 11),
    "depends_on_past": False,
    "retries": 1,
}

# Define DAG
with DAG(
    "p2p_scraping_rupiahcepat",  # Updated DAG name for clarity
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

# Define task
    run_task = PythonOperator(
        task_id="scrape_and_upsert_rupiahcepat",
        python_callable=run_pipeline,
        provide_context=True,  # Allows Airflow to pass execution context
        dag=dag
    )

    run_task
