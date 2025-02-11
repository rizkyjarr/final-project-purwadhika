
import re
import time
import pytz
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers.scraping_helper import fetch_and_map_rupiahcepat , upsert_to_bigquery, run_pipeline


# === AIRFLOW DAG CONFIGURATION === #
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 11),
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG(
    "DAG4_p2p_scrape",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

run_task = PythonOperator(
    task_id="rupiahcepat_stats",
    python_callable=run_pipeline,
    dag=dag
)

run_task
