from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2025, 2, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_sequential_dag',
    default_args=default_args,
    description='A DAG to run dbt models in sequence',
    schedule_interval=None,
    catchup=False,
) as dag:

    run_staging = BashOperator(
        task_id='run_staging',
        bash_command='dbt run --models path.to.models.staging --profiles-dir /path/to/profiles --project-dir /path/to/dbt_project',
    )

    # run_facts = BashOperator(
    #     task_id='run_facts',
    #     bash_command='dbt run --models path.to.models.facts --profiles-dir /path/to/profiles --project-dir /path/to/dbt_project',
    # )

    # run_marts = BashOperator(
    #     task_id='run_marts',
    #     bash_command='dbt run --models path.to.models.marts --profiles-dir /path/to/profiles --project-dir /path/to/dbt_project',
    # )

    run_staging 
