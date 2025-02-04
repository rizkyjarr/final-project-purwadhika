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

# Check whether specific table exist in the DB
def table_exists(table_name):
    query = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = '{table_name}'
    );
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(query)
        exists=cur.fetchone()[0]
        cur.close()
        conn.close()
        return exists
    except Exception as e:
        print("Error checking table existence: {e}")
        return False

# Execute SQL file
def execute_sql_file(sql_file_path, table_name):
    """Executes an SQL file if the table does not exist."""
    if table_exists(table_name):
        print(f"Table: {table_name} already exists, skipping execution")
        return

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        with open(sql_file_path, 'r') as file:
            sql_commands = file.read()

        cur.execute(sql_commands)
        conn.commit()

        print(f"SQL file executed and {table_name} table has been created")
    except Exception as e:
        print(f"Error executing SQL File: {e}")
        raise Exception(f"SQL execution failed: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# SECTION 2 -- MOVE DATA FROM POSTGRE 

# open .yaml file for tables configuration
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


 
# def main():
#     tables = []
#     for table in config["postgre_tables"]:
#         table_name = table["name"]
#         incremental_column = table["incremental_column"]
#         # tables.append(table_name)
#         df = extract_incremental_data(table_name, incremental_column)
#         # print(f"✅ Extracted {len(df)} rows from {table_name}.")
#         print(df[0])
#     # print(tables)

# if __name__ == "__main__":
#     main()

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

     

# THESE BLOCK OF CODES FOR MANUAL TESTING


# SQL_FOLDER = r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\final_project\airflow_app\dags\sql"

# # Ensure the directory exists before listing files
# sql_files = sorted([f for f in os.listdir(SQL_FOLDER) if f.endswith(".sql")])


# def main():

#     for sql_file in sql_files:
#             table_name = sql_file.replace("create_", "").replace(".sql", "")  # Extract table name
#             sql_file_path = os.path.join(SQL_FOLDER, sql_file)
#             execute_sql_file(sql_file_path,table_name)

# customer_path = r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\final_project\airflow_app\dags\sql\create_customer.sql"
# driver_path = r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\final_project\airflow_app\dags\sql\create_driver.sql"
# vehicle_path = r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\final_project\airflow_app\dags\sql\create_vehicle.sql"
# ride_path = r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\final_project\airflow_app\dags\sql\create_ride.sql"
    
# def main():
#     table_exists("customer")
#     execute_sql_file(customer_path, "customer")

#     table_exists("driver")
#     execute_sql_file(driver_path,"driver")

#     table_exists("vehicle")
#     execute_sql_file(vehicle_path,"vehicle")

#     table_exists("ride")
#     execute_sql_file(ride_path,"ride")

# if __name__ == "__main__":
#     main()