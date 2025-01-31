import random
from datetime import datetime
import pytz
import psycopg2
import os
from dotenv import load_dotenv
from faker import Faker

# Declare function for datetime
local_tz = pytz.timezone('Asia/Jakarta')
created_at = datetime.now().astimezone(local_tz)
created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S")

# Loading environment from .env file
load_dotenv()

# Initiliazing Faker function
fake = Faker()

# Create PostgreSQL connection through .env file
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cursor = conn.cursor()

def create_tables():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Customers (
            customer_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            phone_number VARCHAR(20),
            address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

def generate_customers(num_customers=10):
    customers = []
    for _ in range(num_customers):
        customer = {
            "name": fake.name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "address": fake.address(),
            "created_at": created_at_str,
            "updated_at": created_at_str
        }
        customers.append(customer)
    return customers

def insert_data(table_name, data):
    columns = ", ".join(data[0].keys())
    placeholders = ", ".join(["%s"] * len(data[0]))
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    for row in data:
        cursor.execute(query, list(row.values()))
    conn.commit()

def main():

    create_tables()
    customers = generate_customers()
    insert_data("customers", customers)

    print("customer data has been successfully created and ingested to postgreSQL")

if __name__ == "__main__":
    main()

