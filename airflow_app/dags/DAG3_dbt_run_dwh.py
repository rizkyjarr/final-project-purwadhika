from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 10),
    "retries": 1,
}

with DAG("dbt_run_staging", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging_models",
        bash_command="docker exec -i dbt-runner dbt run --project-dir /dbt --profiles-dir /dbt --select staging"
    )

    dbt_run_staging
