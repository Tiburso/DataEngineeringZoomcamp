from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

from datetime import datetime, timedelta

import logging


@dag(
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
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
        dataset_name = "Actuele10mindataKNMIstations"
        dataset_version = "2"
        logging.info(
            f"Fetching latest file of {dataset_name} version {dataset_version}"
        )

        api = OpenDataAPI(api_token=api_key)

        # sort the files in descending order and only retrieve the first file
        params = {"maxKeys": 1, "orderBy": "created", "sorting": "desc"}
        response = api.list_files(dataset_name, dataset_version, params)
        if "error" in response:
            logging.error(f"Unable to retrieve list of files: {response['error']}")
            sys.exit(1)

        latest_file = response["files"][0].get("filename")
        logging.info(f"Latest file is: {latest_file}")

        # fetch the download url and download the file
        response = api.get_file_url(dataset_name, dataset_version, latest_file)

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

    @task()
    def upload_to_gcs(parquet_file_name: str):
        logging.info("Uploading data to GCS")

        # Parse the parquet file name to get the bucket name

        # Upload the file to GCS
        LocalFilesystemToGCSOperator(
            task_id="upload_parquet_to_gcs",
            src="/tmp/weather_data.csv",
            dst="weather_data.csv",
            bucket="weather-data",
            gcp_conn_id="google_cloud_default",
        ).execute(context=None)

    file_name = fetch_data()
    parquet_file_name = convert_nc_to_parquet(file_name)
    upload_to_gcs(parquet_file_name)
