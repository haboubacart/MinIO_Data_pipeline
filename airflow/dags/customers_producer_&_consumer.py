from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
import json
from faker import Faker
from kafka import KafkaProducer, KafkaConsumer
from io import BytesIO
import os
import time
from dotenv import load_dotenv
load_dotenv()



class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):  # Ajout de cette condition pour gérer les objets de type date
            return obj.isoformat()
        return super().default(obj)

def upload_dict_as_json_to_minio(endpoint, access_key, secret_key, bucket_name, object_name, data_dict):
    try:
        # Convert the Python dictionary to a JSON string
        json_data = json.dumps(data_dict)

        # Initialize MinIO client
        minio_client = Minio(endpoint,
                             access_key=access_key,
                             secret_key=secret_key,
                             secure=False)  # Set secure to True if using HTTPS

        # Check if the bucket exists, create it if not
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        # Upload the JSON data to MinIO
        json_data_bytes = json_data.encode('utf-8')
        minio_client.put_object(bucket_name, object_name, BytesIO(json_data_bytes), len(json_data_bytes), "application/json")
        print(f"JSON data uploaded to MinIO bucket {bucket_name} as {object_name}")
    except :
        print("Error uploading JSON data to MinIO:")


def generate_fake_customers():
    faker = Faker()
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
    return customer_object

def produce_data():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v, cls=DateEncoder).encode('utf-8')
    )
    topic_name = 'test'
    for _ in range (50):
        message = generate_fake_customers()
        producer.send(topic_name, value=message)
        time.sleep(1)
    producer.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'customers_producer__consumer',
    default_args=default_args,
    description='A simple DAG with a Python task',
    schedule_interval=None
)

producer_task = PythonOperator(
    task_id='produce_to_kafka',
    python_callable=produce_data,
    dag=dag,
)


def consume_from_kafka(**kwargs):
    # Kafka configuration
    kafka_config = {
        'bootstrap_servers': 'localhost:9092',
        'group_id': 'group_1',
        'auto_offset_reset': 'earliest',
    }

    # Topic to consume from
    kafka_topic = 'test'
    # Create Kafka Consumer instance
    consumer = KafkaConsumer(kafka_topic, **kafka_config)
    try:
        for message in consumer:
            offset_info = {
                'partition': message.partition,
                'offset': message.offset,
                'timestamp': message.timestamp,
            }
            dict_msg = json.loads(message.value.decode('utf-8'))
            dict_msg["msg_info"] = offset_info
            print(dict_msg)
            print(message.offset)
            upload_dict_as_json_to_minio(os.getenv("MINIO_ENDPOINT"),os.getenv("MINIO_ACCESS_KEY"), os.getenv("MINIO_SECRET_KEY"),os.getenv("MINIO_BUCKET_NAME"),"/customers_profiles/customer_"+str(message.offset)+".json", dict_msg)
    except :
        print("error from consumer")      
    consumer.close()

consumer_task = PythonOperator(
    task_id='consume_from_kafka',
    python_callable=consume_from_kafka,
    dag=dag,
)

producer_task >> consumer_task

if __name__ == "__main__":
    dag.cli()
