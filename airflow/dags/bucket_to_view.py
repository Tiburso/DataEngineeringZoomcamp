from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


from datetime import datetime, timedelta

import logging

import os

REGION = "europe-west1"
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
CLUSTER_NAME = os.getenv("GCP_DATAPROC_CLUSTER_NAME", "weather-data-de-cluster")
BUCKET = os.getenv("GCP_GCS_BUCKET")
DATASET = os.getenv("GCP_BIGQUERY_DATASET")


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
    def upload_script_to_gcs():
        from airflow.providers.google.cloud.transfers.local_to_gcs import (
            LocalFilesystemToGCSOperator,
        )

        context = get_current_context()

        LocalFilesystemToGCSOperator(
            task_id="upload_script_to_gcs",
            src="/opt/airflow/dags/spark-submit.py",
            dst="spark-submit.py",
            bucket=BUCKET,
        ).execute(context=context)

    @task()
    def bucket_to_bigquery_spark():
        # Import regular spark submit job operator
        from airflow.providers.google.cloud.operators.dataproc import (
            DataprocSubmitJobOperator,
        )

        context = get_current_context()

        year = str(context["execution_date"].year)
        month = str(context["execution_date"].month)

        month = f"{month:0>2}"

        day = str(context["execution_date"].day)
        hour = str(context["execution_date"].hour)

        logging.info(f"Running spark job for {year}-{month}-{day} {hour}:00")

        DataprocSubmitJobOperator(
            task_id="bucket_to_bigquery_spark",
            job={
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": CLUSTER_NAME},
                "pyspark_job": {
                    "main_python_file_uri": f"gs://{BUCKET}/spark-submit.py",
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
        ).execute(context=context)

    @task()
    def execute_dbt():
        from airflow.operators.bash import BashOperator

        context = get_current_context()

        BashOperator(
            task_id="execute_dbt",
            bash_command="dbt run --project-dir /opt/dbt",
        ).execute(context=context)

    upload_script_to_gcs() >> bucket_to_bigquery_spark() >> execute_dbt()


bucket_to_view()
