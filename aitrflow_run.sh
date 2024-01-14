#!/bin/bash

airflow webserver --port 8080 -D
airflow scheduler -D
echo "Apache Airflow installation and initialization completed."
echo "Airflow Web Server is running at http://localhost:8080"