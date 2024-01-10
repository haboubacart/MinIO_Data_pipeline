from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from faker import Faker
from io import BytesIO
import os
import time
import psycopg2
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
load_dotenv()


def get_processed_files(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT filename FROM processed_files;")
    return {row[0] for row in cursor.fetchall()}

def mark_file_as_processed(conn, filename):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO processed_files (filename) VALUES (%s);", (filename,))
    conn.commit()

def ingest_data_to_postgres():
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    minio_bucket = os.getenv("MINIO_BUCKET_NAME")
    user=os.getenv("PSQL_USERNAME")
    password=os.getenv("PSQL_PASSEWORD")
    port=os.getenv("PSQL_PORT")
    minio_client = Minio(minio_endpoint, access_key=minio_access_key, secret_key=minio_secret_key, secure=False)

    postgres_conn_string = "dbname=customer_db2 user="+user+" password="+password+" host=localhost port="+port
    try:
        conn = psycopg2.connect(postgres_conn_string)
        cursor = conn.cursor()

        create_table_query = """
            CREATE TABLE IF NOT EXISTS customers (
                id SERIAL PRIMARY KEY,
                job VARCHAR(255),
                name VARCHAR(255),
                email VARCHAR(255) UNIQUE,
                toast TEXT,
                address TEXT,
                country VARCHAR(255),
                phone_number VARCHAR(20),
                date_of_birth DATE,
                msg_info JSONB
            );
        """
        
        cursor.execute(create_table_query)
        conn.commit()

        processed_files = get_processed_files(conn)
        minio_object_prefix = "customers_profiles/"

        objects = minio_client.list_objects(minio_bucket, prefix=minio_object_prefix, recursive=True)
        for obj in objects:
            minio_object_path = obj.object_name
            if minio_object_path not in processed_files:
                data = minio_client.get_object(minio_bucket, minio_object_path).read()

                customer_record = json.loads(data)
                customer_record["Phone Number"] = customer_record["Phone Number"][:15]
                insert_query = """
                    INSERT INTO customers (job, name, email, toast, address, country, phone_number, date_of_birth, msg_info)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, (
                    customer_record["Job"],
                    customer_record["Name"],
                    customer_record["Email"],
                    customer_record["Toast"],
                    customer_record["Address"],
                    customer_record["Country"],
                    customer_record["Phone Number"],
                    customer_record["Date of Birth"],
                    json.dumps(customer_record["msg_info"])
                ))
                conn.commit()
                mark_file_as_processed(conn, minio_object_path)
            else:
                print("Already ingested")

        print("Data ingested successfully.")
    except S3Error as e:
        if e.code == 'NoSuchKey':
            print(f"Object not found: {obj.object_name}")
        else:
            print(f"Error ingesting data: {e}")
    finally:
        if conn is not None:
            conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'load_data_from_s3_to_psql',
    default_args=default_args,
    description='A simple DAG with a Python task',
    schedule_interval='*/2 * * * *',
)

load_data_from_s3_to_psql = PythonOperator(
    task_id='load_data_from_s3_to_psql',
    python_callable=ingest_data_to_postgres,
    dag=dag,
)


if __name__ == "__main__":
    dag.cli()
