#!/bin/bash

# Install PostgreSQL
sudo apt update
sudo apt install postgresql postgresql-contrib

# Create a PostgreSQL user and database for Airflow
sudo -u postgres createuser --username=postgres --no-createdb --no-superuser --no-createrole --pwprompt airflow
sudo -u postgres createdb --username=postgres --owner=airflow airflow

# Install Apache Airflow with PostgreSQL support
pip install apache-airflow[postgres]

# Initialize the Airflow Database
airflow db init

# Start the Airflow Web Server
airflow webserver --port 8080 -D

# Start the Airflow Scheduler
airflow scheduler -D

echo "Apache Airflow installation and initialization completed."
echo "Airflow Web Server is running at http://localhost:8080"
