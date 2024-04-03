from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

with DAG(
    dag_id="green_taxi_etl",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    def extract():
        print("Extracting data from Green Taxi API")

    def transform():
        print("Transforming data")

    def load():
        print("Loading data to data warehouse")

    extract = PythonOperator(task_id="extract", python_callable=extract)
    transform = PythonOperator(task_id="transform", python_callable=transform)
    load = PythonOperator(task_id="load", python_callable=load)

    extract >> transform >> load
