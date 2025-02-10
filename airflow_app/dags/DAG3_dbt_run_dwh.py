from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 10),
    "retries": 1,
}

with DAG(
    "DAG3_dbt_run", 
    default_args=default_args, 
    schedule_interval="10 10 * * *", 
    catchup=False,
    max_active_runs=1
    
    ) as dag:

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging_models",
        bash_command="docker exec -i dbt-runner dbt run --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt --select staging"
    )

    dbt_run_facts_dim = BashOperator(
        task_id="dbt_run_facts_models",
        bash_command="docker exec -i dbt-runner dbt run --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt --select facts"
    )

    dbt_run_marts= BashOperator(
        task_id="dbt_run_marts_models",
        bash_command="docker exec -i dbt-runner dbt run --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt --select marts"
    )

    dbt_run_staging >> dbt_run_facts_dim >> dbt_run_marts