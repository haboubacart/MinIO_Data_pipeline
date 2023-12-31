from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.customer_generator import generate_fake_customers
import time
import json
from kafka import KafkaProducer

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        return super().default(obj)
    
def produce_data():
    producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v, cls=DateEncoder).encode('utf-8')
    )
    topic_name = 'test'
    list_messages = generate_fake_customers(2)
    for message in list_messages :
        print(message)
        producer.send(topic_name, value=message)
        producer.send(topic_name, value="  ")
        producer.flush()
        print()
        time.sleep(1)
    producer.close()

def my_python_function():
    produce_data()
    print("Hello from my Python task yeahhhhhhhh!")

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
    'my_python_dag',
    default_args=default_args,
    description='A simple DAG with a Python task',
    schedule_interval=timedelta(minutes=2),
)

python_task = PythonOperator(
    task_id='my_python_task',
    python_callable=my_python_function,
    dag=dag,
)

python_task

if __name__ == "__main__":
    dag.cli()
