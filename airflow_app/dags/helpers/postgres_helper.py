import psycopg2
from dotenv import load_dotenv
import os

# Load environment for DB configuration
load_dotenv()


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

# try function

# sql_folder = r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\final_project\airflow_app\dags\include\sql"
# sql_files = sorted([f for f in os.listdir(sql_folder) if f.endswith(".sql")])

# for sql_file in sql_files:
#     sql_file_path = os.path.join(sql_folder,sql_file)
#     table_name = sql_file.replace("create_", "").replace(".sql", "")
#     execute_sql_file(sql_file_path, table_name)