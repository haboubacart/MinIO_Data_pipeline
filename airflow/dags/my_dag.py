# Import necessary modules
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.customer_generator import generate_fake_customers


# Default arguments for the DAG
            
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# Create a DAG instance
dag = DAG(
    'customer_generator_task_dag',
    default_args=default_args,
    description='A simple DAG with a Python task',
    schedule_interval=timedelta(minutes=2),
)

# Define the Python function to be executed by the PythonOperator
def my_python_function(**kwargs):
    # Your Python function logic goes here
    generate_fake_customers(20)
    print("Hello, Airflow!")


# Create a PythonOperator task
python_task = PythonOperator(
    task_id='execute_customer_generator_function',
    python_callable=my_python_function,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies (if any)
# Here, the python_task will be executed after the DAG is started
python_task

# Optionally, you can add more tasks and set up dependencies between them

if __name__ == "__main__":
    dag.cli()
