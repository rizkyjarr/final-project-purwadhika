import random
from datetime import datetime,timedelta
import pytz
import psycopg2
from faker import Faker
import string
from dotenv import load_dotenv
import os


# Load environment for DB configuration
load_dotenv()

# Declare function for datetime
local_tz = pytz.timezone('Asia/Jakarta')
created_at = datetime.now().astimezone(local_tz)
created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S")

# Declare function to make connection with DB
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()


# Declare function for generating dummy data
fake = Faker()


# Create function for generating customer
def generate_customer(num_customer:int):
    customers = []
    for _ in range(num_customer):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}"

        customer = {
            "name": f"{first_name} {last_name}",
            "phone_number": fake.basic_phone_number(),
            "email": email,
            "created_at": created_at_str
        }
        customers.append(customer)
    return customers


# Create function for generating customer
def generate_driver(num_driver:int):
    drivers = []
    for i in range(1, num_driver + 1):  # Ensuring unique driver_id
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}"

        driver = {
            "driver_id": i,  # Assign a unique driver_id
            "name": f"{first_name} {last_name}",
            "phone_number": fake.basic_phone_number(),
            "email": email,
            "created_at": created_at_str
        }
        drivers.append(driver)
    return drivers  # Return list of drivers

def generate_license_plate():
    first_letter = random.choice(string.ascii_uppercase)
    numbers = ''.join(random.choices(string.digits, k=3))  # Three digits
    last_letters = ''.join(random.choices(string.ascii_uppercase, k=3))  # Three letters
    return f"{first_letter}{numbers}{last_letters}"

def generate_vehicle(drivers):
    vehicles = []
    used_driver_ids = set()  # Track assigned driver IDs

    for driver in drivers:
        driver_id = driver["driver_id"]
        if driver_id not in used_driver_ids:  # Ensure uniqueness
            vehicle = {
                "driver_id": driver_id,  # Use an existing driver_id
                "vehicle_type": random.choice(["Car", "Motorcycle"]),
                "license_plate": generate_license_plate(),
                "year": random.choice(["2019", "2020", "2021", "2022", "2023", "2024", "2025"]),
                "brand": random.choice(["Honda", "Toyota", "Suzuki", "Mitsubishi", "Hyundai", "Kawasaki"]),
                "created_at": created_at_str
            }
            vehicles.append(vehicle)
            used_driver_ids.add(driver_id)  # Mark driver as assigned
    
    return vehicles


def fetch_ids():
    cursor.execute("SELECT cust_id FROM customer")
    customer_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT driver_id FROM driver")
    driver_ids = [row[0] for row in cursor.fetchall()]

    return customer_ids, driver_ids

def generate_ride(num_rides:int, customer_ids, driver_ids):

    distance_km = round(random.uniform(1.0, 10.0), 2),  # Random distance in km
    fare = 1 * distance_km


    fetch_ids()

    rides = []
    for _ in range(num_rides):
        cust_id = random.choice(customer_ids)
        driver_id = random.choice(driver_ids)

        start_time = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
        start_time = start_time.replace(minute=random.randint(0, 59), second=random.randint(0, 59))

        # Ensure end_time is 10-30 minutes later than start_time
        delta_minutes = random.randint(10, 30)
        end_time = start_time + timedelta(minutes=delta_minutes)

        ride = {
            "cust_id": cust_id,
            "driver_id": driver_id,
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "distance_km": distance_km,
            "fare": fare,
            "ride_status": random.choice(["completed", "cancelled"]),
            "created_at": created_at_str
        }
        rides.append(ride)
    return rides

def insert_data(table_name, data):
    """
    Generic function to insert data into any table.
    
    :param table_name: Name of the table to insert into.
    :param data: List of dictionaries, where each dictionary represents a row.
    """
    if not data:
        print(f"No data provided for table '{table_name}'.")
        return

    # Extract column names from the first dictionary in the list
    columns = data[0].keys()
    columns_str = ", ".join(columns)
    placeholders = ", ".join([f"%({col})s" for col in columns])

    # Construct the INSERT query dynamically
    insert_query = f"""
    INSERT INTO {table_name} ({columns_str})
    VALUES ({placeholders})
    """

    try:
        # Execute the query for all rows in the data
        cursor.executemany(insert_query, data)
        conn.commit()
        print(f"Successfully inserted {len(data)} rows into table '{table_name}'.")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data into table '{table_name}': {e}")

def main():
    # Generate dummy data
    num_customers = 1
    num_drivers = 1
    num_rides = 1

    customers = generate_customer(num_customers)
    drivers = generate_driver(num_drivers)
    vehicles = generate_vehicle(drivers)

    # Insert customer and driver data first to fetch their IDs
    insert_data("customer", customers)
    insert_data("driver", drivers)

    # Fetch customer and driver IDs for ride generation
    customer_ids, driver_ids = fetch_ids()
    rides = generate_ride(num_rides, customer_ids, driver_ids)

    # Insert vehicle and ride data
    insert_data("vehicle", vehicles)
    insert_data("ride", rides)

    # Close the database connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()



