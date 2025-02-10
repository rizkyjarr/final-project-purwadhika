from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('dbt_run_dag', default_args=default_args, schedule_interval=None) as dag:

    run_dbt_staging = BashOperator(
        task_id='run_dbt_staging',
        bash_command='dbt run --models staging --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt_project',
    )

    run_dbt_facts = BashOperator(
        task_id='run_dbt_facts',
        bash_command='dbt run --models facts --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt_project',
    )

    run_dbt_marts = BashOperator(
        task_id='run_dbt_marts',
        bash_command='dbt run --models marts --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt_project',
    )

    run_dbt_staging >> run_dbt_facts >> run_dbt_marts
