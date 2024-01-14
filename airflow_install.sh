#!/bin/bash

sudo apt update
sudo apt install postgresql postgresql-contrib
sudo -u postgres createuser --username=postgres --no-createdb --no-superuser --no-createrole --pwprompt airflow
sudo -u postgres createdb --username=postgres --owner=airflow airflow
pip install apache-airflow[postgres]
pip install -r requirements.txt
airflow db init

