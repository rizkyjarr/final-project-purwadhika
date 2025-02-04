import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime,timedelta
import pandas as pd
import yaml


# Load environment for DB configuration
load_dotenv()


# SECTION 1 --  LOAD DATA TO POSTGRE

# DB connection configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# Open .yaml file for tables configuration
with open("postgre_tables.yaml", "r") as file:
    config = yaml.safe_load(file)


# extract incremental data from tables in postgre that has been configurated in tables.yaml
def extract_incremental_data(table_name, incremental_column=None):
    conn = psycopg2.connect(**DB_CONFIG)
    
    #fetch table that has created_at column/incremental_volume
    if incremental_column:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        query = f"""

            SELECT * FROM {table_name}
            WHERE DATE({incremental_column}) = '{target_date}'

            """
    else:
        query = f"SELECT * FROM {table_name}" #fetch table that does not have incremental_volume

    df = pd.read_sql(query,conn)
    conn.close()
    return df

# SECTION X - THIS IS FOR MANUAL TESTING!
def main():
    tables = []
    for table in config["postgre_tables"]:
        table_name = table["name"]
        incremental_column = table["incremental_column"]
        df = extract_incremental_data(table_name, incremental_column)

        if df is not None and not df.empty:
            print(f"✅ Extracted {len(df)} rows from {table_name}.")
            print(df)  # Prints the whole DataFrame
            break  # Exit the loop after the first table
        else:
            print(f"⚠️ No data extracted for {table_name}.")

if __name__ == "__main__":
    main()    
