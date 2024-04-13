from airflow.decorators import dag, task

from datetime import datetime, timedelta

import logging

REGION = "europe-west1"
PROJECT_ID = "dataengineeringbootcamp-419022"
CLUSTER_NAME = "weather-data-de-cluster"
BUCKET = "weather_data_de_bucket"
DATASET = "weather_data_de"


@dag(
    # Every two hours
    schedule_interval="0 */2 * * *",
    max_active_runs=1,
    start_date=datetime(2024, 4, 11),
    catchup=False,
    tags=["weather"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
)
def bucket_to_view():
    @task()
    def bucket_to_bigquery_spark():
        from airflow.providers.google.cloud.operators.dataproc import (
            DataprocSubmitJobOperator,
        )
        from airflow.operators.python import get_current_context

        context = get_current_context()

        year = context["execution_date"].year
        month = context["execution_date"].month
        day = context["execution_date"].day
        hour = context["execution_date"].hour

        op = DataprocSubmitJobOperator(
            task_id="bucket_to_bigquery_spark",
            job={
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": CLUSTER_NAME},
                "pyspark_job": {
                    "main_python_file_uri": "spark-submit.py",
                    "jar_file_uris": [
                        "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar"
                    ],
                    "args": [
                        "--year",
                        year,
                        "--month",
                        month,
                        "--day",
                        day,
                        "--hour",
                        hour,
                        "--minute",
                        "*",
                        "--project",
                        PROJECT_ID,
                        "--bucket",
                        BUCKET,
                        "--dataset",
                        DATASET,
                    ],
                },
            },
            project_id=PROJECT_ID,
            region=REGION,
        )

        op.execute()

    @task()
    def execute_dbt():
        from airflow.operators.bash import BashOperator

        op = BashOperator(
            task_id="execute_dbt",
            bash_command="dbt run",
        )

        op.execute()

    bucket_to_bigquery_spark()


bucket_to_view()
