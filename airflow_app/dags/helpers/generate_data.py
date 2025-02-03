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
def generate_customer():
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}"

    customer = {
            "name": f"{first_name} {last_name}",
            "phone_number": fake.basic_phone_number(),
            "email": email,
            "created_at": created_at_str
    }
    return customer


# Create function for generating customer
def generate_driver():
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}"

        driver = {
            "name": f"{first_name} {last_name}",
            "phone_number": fake.basic_phone_number(),
            "email": email,
            "created_at": created_at_str
        }
        return driver  # Return list of drivers

def generate_license_plate():
    first_letter = random.choice(string.ascii_uppercase)
    numbers = ''.join(random.choices(string.digits, k=3))  # Three digits
    last_letters = ''.join(random.choices(string.ascii_uppercase, k=3))  # Three letters
    return f"{first_letter}{numbers}{last_letters}"

def fetch_latest_driver_id():
     cursor.execute("SELECT driver_id FROM driver ORDER BY driver_id DESC LIMIT 1;")
     result = cursor.fetchone()
     return result[0] if result else None

def generate_vehicle():        
        latest_driver_id = fetch_latest_driver_id()

        if latest_driver_id is None:  # ✅ Prevent inserting when no driver exists
            print("🚨 No driver found! Skipping vehicle generation.")
            return None

        else:   
            vehicle = {
                "driver_id": latest_driver_id,
                "vehicle_type": random.choice(["Car", "Motorcycle"]),
                "license_plate": generate_license_plate(),
                "year": random.choice(["2019", "2020", "2021", "2022", "2023", "2024", "2025"]),
                "brand": random.choice(["Honda", "Toyota", "Suzuki", "Mitsubishi", "Hyundai", "Kawasaki"]),
                "created_at": created_at_str
                }
            return vehicle

def fetch_cust_id():
    cursor.execute("SELECT cust_id FROM customer")
    customer_id = [row[0] for row in cursor.fetchall()]
    return customer_id

def fetch_driver_id():
    cursor.execute("SELECT driver_id FROM driver")
    driver_id = [row[0] for row in cursor.fetchall()]
    return driver_id

def generate_ride():
    distance_km = round(random.uniform(1.0, 10.0), 2)  # Random distance in km
    fare = 1 * distance_km

    fetched_cust_id = fetch_cust_id()
    fetched_driver_id = fetch_driver_id()
    cust_id = random.choice(fetched_cust_id)
    driver_id = random.choice(fetched_driver_id)

    random_minutes= random.randint(5,30)
    start_time = created_at - timedelta(minutes=random_minutes)
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    

    ride_status = random.choice(["Completed", "Cancelled"])
    if ride_status == "Cancelled":
        fare = 0
        start_time_str = created_at_str

    ride = {
            "cust_id": cust_id,
            "driver_id": driver_id,
            "start_time": start_time_str,
            "end_time": created_at_str,
            "distance_km": distance_km,
            "fare": fare,
            "ride_status": ride_status,
            "created_at": created_at_str
        }
    return ride

def insert_data(table_name,data):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        
        columns = data.keys()
        values = data.values()

        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(values))})"

        # Execute query
        cursor.execute(sql, list(values))
        conn.commit()
        print(f"✅ Data inserted into {table_name} successfully!")

    except Exception as e:
        print(f"❌ Error inserting into {table_name}: {e}")

    finally:
        cursor.close()
        conn.close()

# Try function
# def main():
#     customer = generate_customer()
#     insert_data("customer", customer
#                 )
#     driver = generate_driver()
#     insert_data("driver", driver)
    
#     vehicle = generate_vehicle()
#     insert_data("vehicle", vehicle)

#     ride = generate_ride()
#     insert_data("ride", ride)
#     # latest_id = fetch_latest_driver_id()
#     # print(latest_id)

# if __name__ == "__main__":
#     main()
