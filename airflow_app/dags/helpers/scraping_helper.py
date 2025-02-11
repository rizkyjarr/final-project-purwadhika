import re
import time
import pytz
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
client = bigquery.Client.from_service_account_json(credentials_path)

# === FUNCTION 1: CHECK AND CREATE TABLE IF NOT EXISTS === #
def ensure_table_exists():
    """Ensures the BigQuery table exists. If not, creates it with the correct schema."""
    PROJECT_ID = "purwadika"
    DATASET_ID = "rizky_dwh_hailing_source"
    TABLE_ID = "rupiahcepat_stats"

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    schema = [
        bigquery.SchemaField("organization_name", "STRING"),
        bigquery.SchemaField("fund_recipients_since_est_no", "INTEGER"),
        bigquery.SchemaField("fund_recipients_current_year", "INTEGER"),
        bigquery.SchemaField("fund_recipients_endofperiod", "INTEGER"),
        bigquery.SchemaField("fund_providers_since_est", "INTEGER"),
        bigquery.SchemaField("fund_providers_current_year", "INTEGER"),
        bigquery.SchemaField("fund_providers_endofperiod", "INTEGER"),
        bigquery.SchemaField("funding_disbursed_since_est_rp", "INTEGER"),
        bigquery.SchemaField("funding_disbursed_current_year_rp", "INTEGER"),
        bigquery.SchemaField("funding_disbursed_endofperiod_rp", "INTEGER"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("is_active", "BOOLEAN")
    ]

    try:
        # Check if table exists
        client.get_table(table_ref)
        logger.info(f"✅ Table {table_ref} already exists.")
    except Exception as e:
        # If table does not exist, create it
        logger.warning(f"⚠️ Table {table_ref} not found. Creating table...")
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logger.info(f"✅ Table {table_ref} created successfully.")

# === FUNCTION 2: FETCH & MAP DATA === #
def fetch_and_map_rupiahcepat():
    """Scrapes statistics from Rupiah Cepat and maps them to a Pandas DataFrame."""
    url = "https://www.rupiahcepat.co.id/about/index.html"

    # Selenium options (headless mode for performance)
    chrome_options = Options()
    chrome_options.add_argument("--headless")  
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = None
    try:
        # Connect to Selenium container
        driver = webdriver.Remote(
            command_executor="http://selenium:4444/wd/hub",  
            options=chrome_options
        )

        driver.set_page_load_timeout(30)
        logger.info(f"Loading URL: {url}")
        driver.get(url)
        time.sleep(5)
        logger.info(f"Page title: {driver.title}")

        # Extract data
        div = driver.find_element(By.XPATH, "(//div[@data-v-35084c97])[1]")
        h3_elements = div.find_elements(By.TAG_NAME, "h3")
        extracted_numbers = []

        for h3 in h3_elements:
            text = h3.text.strip()
            numbers = re.findall(r"(\d+[\.,]?\d*)\s*(jt|ribu|M|T)?", text)

            converted_numbers = []
            for num, unit in numbers:
                num = float(num.replace(",", "."))
                if unit == "jt":
                    num *= 1_000_000
                elif unit == "ribu":
                    num *= 1_000
                elif unit == "T":
                    num *= 1_000_000_000_000
                elif unit == "M":
                    num *= 1_000_000_000
                converted_numbers.append(int(num))

            if converted_numbers:
                extracted_numbers.extend(converted_numbers)

        column_headers = [
            "fund_recipients_since_est_no",
            "fund_recipients_current_year",
            "fund_recipients_endofperiod",
            "fund_providers_since_est",
            "fund_providers_current_year",
            "fund_providers_endofperiod",
            "funding_disbursed_since_est_rp",
            "funding_disbursed_current_year_rp",
            "funding_disbursed_endofperiod_rp"
        ]

        if len(extracted_numbers) == len(column_headers):
            df = pd.DataFrame([extracted_numbers], columns=column_headers)
        else:
            logger.warning("⚠️ Extracted values do not match expected number of columns!")
            df = pd.DataFrame([extracted_numbers])

        jakarta_tz = pytz.timezone("Asia/Jakarta")
        current_time = datetime.now(pytz.utc).astimezone(jakarta_tz).strftime('%Y-%m-%d %H:%M:%S')

        df.insert(0, "organization_name", "RupiahCepat")
        df["created_at"] = current_time
        df["updated_at"] = current_time
        df["is_active"] = True

        return df

    except Exception as e:
        logger.error(f"⚠️ Error in fetch_and_map_rupiahcepat: {e}")
        raise
    finally:
        if driver:
            driver.quit()

# === FUNCTION 3: UPSERT TO BIGQUERY (SCD2) === #
def upsert_to_bigquery():
    """Performs SCD Type 2 upsert in BigQuery: Ensures table exists, marks old records inactive & inserts new ones, then drops the staging table."""
    PROJECT_ID = "purwadika"
    DATASET_ID = "rizky_dwh_hailing_source"
    TABLE_ID = "rupiahcepat_stats"
    STAGING_TABLE_ID = "rupiahcepat_staging"  # Staging table to be dropped later
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    staging_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}"

    try:
        # Ensure table exists before upserting
        ensure_table_exists()

        # Get new data
        df = fetch_and_map_rupiahcepat()
        logger.info("DataFrame to be inserted:")
        logger.info(df)

        # Load new data into a temporary staging table
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")  # Overwrite staging table
        load_job = client.load_table_from_dataframe(df, staging_table_ref, job_config=job_config)
        load_job.result()  # Wait for job to complete

        logger.info(f"✅ Data successfully batch-loaded into staging table {staging_table_ref}")

        # Define columns requiring explicit CAST
        
        datetime_to_timestamp_columns = {"created_at", "updated_at"}  # TIMESTAMP fields
        

        # **Perform the MERGE operation for upsert**
        merge_query = f"""
        MERGE `{table_ref}` AS target
        USING `{staging_table_ref}` AS source
        ON target.organization_name = source.organization_name
        AND target.is_active = TRUE

        WHEN MATCHED THEN
            UPDATE SET 
            {", ".join([
                f"target.{col} = CAST(source.{col} AS TIMESTAMP)" if col in datetime_to_timestamp_columns else
                f"target.{col} = source.{col}" 
                for col in df.columns if col != "organization_name"
            ])}

        WHEN NOT MATCHED THEN
            INSERT ({", ".join(df.columns)})
            VALUES ({", ".join([
                f"CAST(source.{col} AS TIMESTAMP)" if col in datetime_to_timestamp_columns else
                f"source.{col}" 
                for col in df.columns
            ])})
        """

        query_job = client.query(merge_query)
        query_job.result()  # Wait for query to complete

        logger.info(f"✅ Upsert completed successfully in {table_ref}")

        # **Drop the staging table after upsert**
        drop_query = f"DROP TABLE `{staging_table_ref}`"
        drop_job = client.query(drop_query)
        drop_job.result()  # Wait for drop query to complete
        logger.info(f"🗑️ Staging table {staging_table_ref} successfully dropped.")

    except Exception as e:
        logger.error(f"⚠️ Failed to upsert data to BigQuery: {e}")
        raise


# === FUNCTION 4: DAG EXECUTION FUNCTION === #
def run_pipeline():
    """Executes the entire pipeline: Ensuring table, scraping data, and upserting to BigQuery."""
    upsert_to_bigquery()
    logger.info("🚀 Data scraping and upsert complete!")
