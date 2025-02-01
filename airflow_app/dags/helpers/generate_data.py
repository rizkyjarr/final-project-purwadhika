import random
from datetime import datetime
import pytz
import psycopg2
from faker import Faker

# Declare function for datetime
local_tz = pytz.timezone('Asia/Jakarta')
created_at = datetime.now().astimezone(local_tz)
created_at_str = created_at.strftime("%Y-%m-%d %H:%M:%S")

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
    for _ in range(num_driver):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}"

        driver = {
            "name": f"{first_name} {last_name}",
            "phone_number": fake.basic_phone_number(),
            "email": email,
            "created_at": created_at_str
        }
        
    return drivers

print(generate_driver(2))


