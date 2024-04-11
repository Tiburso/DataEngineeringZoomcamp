from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

from datetime import datetime, timedelta

import logging

DATASET_NAME = "Actuele10mindataKNMIstations"
DATASET_VERSION = "2"


@dag(
    # Every hour
    schedule_interval="0 * * * *",
    max_active_runs=1,
    start_date=datetime(2024, 4, 11),
    catchup=False,
    tags=["weather"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def fetch_weather_data():
    @task()
    def fetch_data():
        import sys
        import os
        from opendata_api import OpenDataAPI, download_file_from_temporary_download_url

        api_key = os.getenv("KNMI_API_KEY")
        logging.info(
            f"Fetching latest file of {DATASET_NAME} version {DATASET_VERSION}"
        )

        api = OpenDataAPI(api_token=api_key)

        # sort the files in descending order and only retrieve the first file
        params = {"maxKeys": 1, "orderBy": "created", "sorting": "desc"}
        response = api.list_files(DATASET_NAME, DATASET_VERSION, params)
        if "error" in response:
            logging.error(f"Unable to retrieve list of files: {response['error']}")
            sys.exit(1)

        latest_file = response["files"][0].get("filename")
        logging.info(f"Latest file is: {latest_file}")

        # fetch the download url and download the file
        response = api.get_file_url(DATASET_NAME, DATASET_VERSION, latest_file)

        file_location = "/tmp/" + latest_file

        download_file_from_temporary_download_url(
            response["temporaryDownloadUrl"], file_location
        )

        return latest_file

    @task()
    def convert_nc_to_parquet(file_name: str):
        from xarray import open_dataset
        from pandas import DataFrame

        data = open_dataset(file_name)
        df: DataFrame = data.to_dataframe()

        parquet_name = file_name.replace(".nc", ".parquet")

        df.to_parquet(parquet_name)

        return parquet_name

    @task()
    def upload_to_gcs(parquet_file_name: str):
        logging.info("Uploading data to GCS")

        # Parse the parquet file name to get the bucket name
        file_date = parquet_file_name.split("_")[-1].split(".")[0]

        # Destination bucket structure
        # is gonna be year/month/day/hour/minute
        destination_name = f"{DATASET_NAME}/{DATASET_VERSION}/{file_date[:4]}/{file_date[4:6]}/{file_date[6:8]}/{file_date[8:10]}/{file_date[10:12]}"

        # Upload the file to GCS
        LocalFilesystemToGCSOperator(
            task_id="upload_parquet_to_gcs",
            src=parquet_file_name,
            dst=destination_name,
            bucket="weather_data_bucket",
            gcp_conn_id="google_cloud_default",
        ).execute(context=None)

    file_name = fetch_data()
    parquet_file_name = convert_nc_to_parquet(file_name)
    upload_to_gcs(parquet_file_name)


fetch_weather_data_dag = fetch_weather_data()
