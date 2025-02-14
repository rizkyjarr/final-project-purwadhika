from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from helpers.send_discord_alert import send_discord_alert

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 10),
    "retries": 1,
    "on_failure_callback": lambda context: send_discord_alert(context, "failure"),
    "on_retry_callback": lambda context: send_discord_alert(context, "retry"),
}

with DAG(
    "DAG3_dbt_run", 
    default_args=default_args, 
    schedule_interval="10 10 * * *", 
    catchup=False,
    max_active_runs=1
    
    ) as dag:
    
    # Perform dbt run to transform and move source data to preparation layer
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging_models",
        bash_command="docker exec -i dbt-runner dbt run --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt --select staging"
    )
    
    # Perform dbt run to transform and move preparation layer to facts and dim layer
    dbt_run_facts_dim = BashOperator(
        task_id="dbt_run_facts_models",
        bash_command="docker exec -i dbt-runner dbt run --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt --select facts"
    )

    # Perform dbt run to transform and move preparation facts and dim layer to data marts layer
    dbt_run_marts= BashOperator(
        task_id="dbt_run_marts_models",
        bash_command="docker exec -i dbt-runner dbt run --project-dir /usr/app/dbt --profiles-dir /usr/app/dbt --select marts"
    )

    dbt_run_staging >> dbt_run_facts_dim >> dbt_run_marts