from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from discord_alert import send_discord_alert  # ✅ Import Discord notification function

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 10),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_discord_alert,  # ✅ Send failure alerts to Discord
}

with DAG("master_pipeline", default_args=default_args, schedule_interval="*/5 * * * *", catchup=False) as dag:

    # 🔹 Monitor DAG 1 (Runs Every 5 Min)
    monitor_dag1 = ExternalTaskSensor(
        task_id="monitor_dag1",
        external_dag_id="generate_random_data",  # ✅ Replace with actual DAG 1 ID
        external_task_id=None,  # Wait for the whole DAG to complete
        timeout=6000,
        mode="poke",
        poke_interval=30
    )

    # 🔹 Monitor DAG 2 (Runs Daily)
    monitor_dag2 = ExternalTaskSensor(
        task_id="monitor_dag2",
        external_dag_id="load_postgres_to_bigquery",  # ✅ Replace with actual DAG 2 ID
        external_task_id=None,
        timeout=6000,
        mode="poke",
        poke_interval=30
    )

    # 🔹 Monitor DAG 3 (Runs Daily After DAG 2)
    monitor_dag3 = ExternalTaskSensor(
        task_id="monitor_dag3",
        external_dag_id="dbt_run_staging",  # ✅ Replace with actual DAG 3 ID
        external_task_id=None,
        timeout=6000,
        mode="poke",
        poke_interval=30
    )

    # 🔗 DAG Monitoring Flow: DAG 1 → DAG 2 → DAG 3
    monitor_dag1 >> monitor_dag2 >> monitor_dag3
