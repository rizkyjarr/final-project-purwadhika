import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime,timedelta
import pandas as pd
import yaml
from google.cloud import bigquery


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
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Setting BigQuery connection using .json credentials
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
client = bigquery.Client.from_service_account_json(credentials_path)

# Configure BigQuery dataset:
BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET")

# Open .yaml file for tables configuration

# with open("postgre_tables.yaml", "r") as file:
#     config = yaml.safe_load(file)

with open("/opt/airflow/dags/helpers/postgre_tables.yaml", "r") as file:
    config = yaml.safe_load(file)

# extract incremental data from tables in postgre that has been configurated in tables.yaml
def extract_incremental_data_postgre(table_name, incremental_column=None):
    conn = psycopg2.connect(**DB_CONFIG)
    
    #fetch table that has created_at column/incremental_volume
    if incremental_column:
        target_date = (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d")
        query = f"""

            SELECT * FROM {table_name}
            WHERE DATE({incremental_column}) = '{target_date}'

            """
    else:
        query = f"SELECT * FROM {table_name}" #fetch table that does not have incremental_volume

    df = pd.read_sql(query,conn)
    conn.close()
    return df

def check_if_table_exists(table_name):

    table_ref =  f"{BQ_PROJECT}.{BQ_DATASET}.production_hailing_source_{table_name}"
    try:
        client.get_table(table_ref)
        print(f"✅ Table {table_name} has already existed in {BQ_PROJECT}.{BQ_DATASET}. Skipping table creation..")
        return True
    except Exception as e:
        print(f"⚠️ Table {table_name} does not exist in {BQ_PROJECT}.{BQ_DATASET}. Attempt to create table..")
        return False
    
def mapping_postgres_schema_to_bq(pg_type):
    """MAP POSTGRESQL DATA TYPES TO ALIGN WITH BQ SCHEMA"""
    type_mapping = {
        "integer": "INTEGER",
        "bigint": "INTEGER",
        "smallint": "INTEGER",
        "serial": "INTEGER",
        "bigserial": "INTEGER",

        "numeric": "NUMERIC",
        "decimal": "NUMERIC",
        "real": "FLOAT",
        "double precision": "FLOAT",

        "varchar": "STRING",
        "char": "STRING",
        "text": "STRING",
        "uuid": "STRING",

        "boolean": "BOOLEAN",

        "timestamp": "TIMESTAMP",
        "timestamp without time zone": "TIMESTAMP",
        "timestamp with time zone": "TIMESTAMP",
        "date": "DATE",
        "time": "TIME",

        "json": "STRING",
        "jsonb": "STRING",
        "bytea": "BYTES"
    }
    
    return type_mapping.get(pg_type.lower(), "STRING")

def get_postgres_schema(table_name):
    
    query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
    """
    cursor.execute(query)
    columns = cursor.fetchall()

    schema = []
    for col in columns:
        col_name, col_type = col
        bq_type = mapping_postgres_schema_to_bq(col_type)
        schema.append(bigquery.SchemaField(col_name, bq_type))

    return schema

def create_bigquery_table(table_name, schema, partition_field):

    table_ref =  f"{BQ_PROJECT}.{BQ_DATASET}.production_hailing_source_{table_name}"
    table = bigquery.Table(table_ref,schema=schema)

    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field = partition_field
        )
    print(f"✅ Table {table_name} will be partitioned on '{partition_field}'.")

    table = client.create_table(table)
    print(f"✅ Table {table_ref} successfully created in BigQuery")

def ensure_table_exist(table_name, partition_field):
    """Logical flow: 
    1. if table exists --> skip table creation
    2. if it doesnt exist --> fetch schema from PostgreSQL, and attempt to create table
    3. if partition_field is set in .yaml file, the table will be partitioned on that field
    """
   
    if check_if_table_exists(table_name):
        return # if table exist do nothing
    
    schema = get_postgres_schema(table_name)
    create_bigquery_table(table_name, schema,partition_field)
    
def upsert_to_bigquery(df, table_name, primary_key):
    """Upsert DataFrame into BigQuery using a staging table for efficiency."""
    dataset_ref = f"{BQ_PROJECT}.{BQ_DATASET}"
    target_table = f"{dataset_ref}.production_hailing_source_{table_name}"
    staging_table = f"{dataset_ref}.staging_{table_name}"

    if df.empty:
        print(f"⚠️ No new data for {table_name}. Skipping upsert.")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"  # Overwrite staging_table each time
    )

    job = client.load_table_from_dataframe(df, staging_table, job_config=job_config)
    job.result()  # ✅ Wait for the job to complete
    print(f"✅ Loaded {len(df)} rows into staging table: {staging_table}")

    # ✅ Define columns that need explicit CAST
    float_to_numeric_columns = {"distance_km", "fare"}  # ✅ Add other affected NUMERIC columns
    datetime_to_timestamp_columns = {"start_time", "end_time", "created_at", "updated_at"}  # ✅ Add TIMESTAMP columns
    string_columns = {"year"}  # ✅ Columns that must be STRING

    merge_query = f"""
    MERGE `{target_table}` AS target
    USING `{staging_table}` AS source
    ON target.{primary_key} = source.{primary_key}
    WHEN MATCHED THEN
        UPDATE SET 
        {", ".join([
            f"target.{col} = CAST(source.{col} AS NUMERIC)" if col in float_to_numeric_columns else
            f"target.{col} = CAST(source.{col} AS TIMESTAMP)" if col in datetime_to_timestamp_columns else
            f"target.{col} = CAST(source.{col} AS STRING)" if col in string_columns else
            f"target.{col} = source.{col}" 
            for col in df.columns if col != primary_key
        ])}
    WHEN NOT MATCHED THEN
        INSERT ({", ".join(df.columns)})
        VALUES ({", ".join([
            f"CAST(source.{col} AS NUMERIC)" if col in float_to_numeric_columns else
            f"CAST(source.{col} AS TIMESTAMP)" if col in datetime_to_timestamp_columns else
            f"CAST(source.{col} AS STRING)" if col in string_columns else
            f"source.{col}" 
            for col in df.columns
        ])})
    """

    query_job = client.query(merge_query)
    query_job.result()  # ✅ Wait for the MERGE to complete
    print(f"✅ Merged {len(df)} rows into {target_table}")

    # Step 3️⃣: Drop Staging Table to Clean Up
    drop_query = f"DROP TABLE `{staging_table}`"
    client.query(drop_query).result()
    print(f"🗑️ Dropped staging table: {staging_table}")




# SECTION X - THIS IS FOR MANUAL TESTING!
# def main():
#     tables = []
#     for table in config["postgre_tables"]:
#         table_name = table["name"]
#         incremental_column = table["incremental_column"]
#         df = extract_incremental_data_postgre(table_name, incremental_column)

#         if df is not None and not df.empty:
#             print(f"✅ Extracted {len(df)} rows from {table_name}.")
#             # print(df)  # Prints the whole DataFrame
#             # break  # Exit the loop after the first table
#         else:
#             print(f"⚠️ No data extracted for {table_name}.")

#     # check connection
#     print("✅ Connected to BigQuery - Project:", client.project)


# if __name__ == "__main__":
#     main()    


# SECTION 2 - CHECK FOR ENSURING TABLE TASK
# def main():
#     for table in config["postgre_tables"]:
#         table_name = table["name"]
#         partition_field = table["partition_field"]
#         primary_key = table["primary_key"]
#         incremental_column = table['incremental_column']
        
        
#         ensure_table_exist(table_name, partition_field)
#         df = extract_incremental_data_postgre(table_name,incremental_column)
#         upsert_to_bigquery(df, table_name,primary_key)
# if __name__ == "__main__":
#     main()

