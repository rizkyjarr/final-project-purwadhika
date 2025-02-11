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
from dotenv import load_dotenv
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
client = bigquery.Client.from_service_account_json(credentials_path)

# === FUNCTION 1: FETCH & MAP DATA === #
def fetch_and_map_rupiahcepat():
    """Scrapes statistics from Rupiah Cepat and maps them to a Pandas DataFrame."""
    url = "https://www.rupiahcepat.co.id/about/index.html"

    # Selenium options (headless mode for performance)
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = None  # Initialize driver outside the try block
    try:
        # Initialize WebDriver
        service = Service(ChromeDriverManager().install())
        logger.info(f"Using ChromeDriver: {service.path}")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)  # 30 seconds timeout

        # Open webpage
        logger.info(f"Loading URL: {url}")
        driver.get(url)
        time.sleep(5)  # Allow JavaScript to load
        logger.info(f"Page title: {driver.title}")

        # Find the first <div> with attribute data-v-35084c97
        div = driver.find_element(By.XPATH, "(//div[@data-v-35084c97])[1]")

        # Extract <h3> elements inside the div
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

        # Define column headers (based on your schema)
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

        # Ensure extracted values match expected columns
        if len(extracted_numbers) == len(column_headers):
            df = pd.DataFrame([extracted_numbers], columns=column_headers)
        else:
            logger.warning("⚠️ Warning: Extracted values do not match expected number of columns!")
            df = pd.DataFrame([extracted_numbers])

        # Add organization name and timestamps
        jakarta_tz = pytz.timezone("Asia/Jakarta")
        current_time = datetime.now(pytz.utc).astimezone(jakarta_tz).strftime('%Y-%m-%d %H:%M:%S')

        df.insert(0, "organization_name", "RupiahCepat")
        df["created_at"] = current_time
        df["updated_at"] = current_time
        df["is_active"] = True  # New records are active

        return df

    except Exception as e:
        logger.error(f"⚠️ Error in fetch_and_map_rupiahcepat: {e}")
        raise
    finally:
        if driver:  # Only quit if driver is initialized
            driver.quit()

# === FUNCTION 2: UPSERT TO BIGQUERY (SCD2) === #
def upsert_to_bigquery():
    """Performs SCD Type 2 upsert in BigQuery: Marks old records inactive & inserts new ones."""
    PROJECT_ID = "purwadika"
    DATASET_ID = "rizky_dwh_hailing_source"
    TABLE_ID = "rupiahcepat_stats"

    client = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        # Get new data
        df = fetch_and_map_rupiahcepat()
        logger.info("DataFrame to be inserted:")
        logger.info(df)

        rows_to_insert = df.to_dict(orient='records')

        # Query to mark previous records as inactive
        update_query = f"""
        UPDATE `{table_ref}`
        SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP()
        WHERE organization_name = 'RupiahCepat' AND is_active = TRUE
        """

        # Run update query (if table contains data)
        query_job = client.query(update_query)
        query_job.result()

        # Insert new records as active
        job = client.insert_rows_json(table_ref, rows_to_insert)
        if job:
            logger.error(f"⚠️ Error inserting rows into {table_ref}: {job}")
        else:
            logger.info(f"✅ Data successfully upserted into {table_ref}")

    except Exception as e:
        logger.error(f"⚠️ Failed to upsert data to BigQuery: {e}")
        raise

# === FUNCTION 3: DAG EXECUTION FUNCTION === #
def run_pipeline():
    upsert_to_bigquery()
    logger.info("🚀 Data scraping and upsert complete!")