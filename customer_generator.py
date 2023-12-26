from faker import Faker
import json
from datetime import date


def convert_to_serializable(obj):
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def generate_fake_customers(number_of_customer):
    # Create a Faker instance
    faker = Faker()

    for i in range(number_of_customer) :
        fake_name = faker.name()
        fake_email = faker.email()
        fake_phone_number = faker.phone_number()
        fake_address = faker.address()
        fake_country_of_birth = faker.country()
        fake_date_of_birth = faker.date_of_birth()
        fake_paragraph = faker.paragraph()
        fake_job = faker.job()

        customer_object = {
            "Name": fake_name,
            "Email": fake_email,
            "Phone Number": fake_phone_number,
            "Address": fake_address,
            "Date of Birth": fake_date_of_birth,
            "Country": fake_country_of_birth,
            "Toast": fake_paragraph,
            "Job": fake_job
        }
        json_file_name = "customer_"+str(i+1)+".json"

        with open(json_file_name, 'w') as json_file:
            json.dump(customer_object, json_file, default=convert_to_serializable, indent=4)

#generate_fake_customers(3)