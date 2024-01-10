from datetime import datetime, timedelta
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import json
import os
load_dotenv()

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
    except S3Error as e:
        print(f"Error uploading JSON data to MinIO: {e}")

# Function to process Kafka messages
def save_kafka_message(message_value):
    # Implement your logic to process the Kafka message
    print(f"saving Kafka message: {message_value}")

    # Example: Save the Kafka message to a file or database



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
        i = 0
        for message in consumer:
            i+=1
            print("hello")
            print(type(message.value))
            print(message.value.decode('utf-8'))
            print(type(message.value.decode('utf-8')))
            #save_kafka_message(message.value)
            #decoded_value = json.loads(message.value.decode('utf-8'))
            #upload_dict_as_json_to_minio(os.getenv("MINIO_ENDPOINT"),os.getenv("MINIO_SECRET_KEY"),os.getenv("MINIO_BUCKET_NAME"),"customer_profiles/customer_"+str(i)+"/test.json", message.value.decode('utf-8'))
            
    finally:
        # Close the consumer
        consumer.close()

consume_from_kafka()
