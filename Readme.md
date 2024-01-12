# Contexte :
Ce projet constitue une pipeline de données produites avec un générateur basé sur un script python, transitant via une queue kafka (Producuer + Consumer) et qui se déversent dans un datalake MinIO (hdfs compatible avec AWS S3). Ensuite les données sont récupérées à des fréquences régulière et inscrites dans un data warehouse (base de données postgres)
Tout le fonctionnement étant orchestré avec Airflow.

# Les éléments du projet : 
les 3 services lancés via le docker-compose sont
 - Kafka : servic de queue messaging permettant le streaming de la données
 - Zookeeper : utilisé pour gèrer la distribution des du système kafka
 - MinIO : est un sytème de fichiers distribué, open source, compatible avec AWS S3
Airflow a été installé en standalone (voir airflow.sh)

# Détails de jobs airflow : 

# Architecture de la pipeline : 

![Data pipeline](images/schema.png)