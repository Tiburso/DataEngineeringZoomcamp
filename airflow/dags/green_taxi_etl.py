from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

import os
import pandas as pd
from datetime import datetime, timedelta

import logging


# Schedule your pipeline to run daily at 5AM UTC.
# with DAG(
#     dag_id="green_taxi_etl",
#     schedule_interval="0 5 * * *",
#     catchup=False,
#     default_args={"start_date": datetime(2021, 1, 1)},
# ) as dag:
@dag(
    dag_id="green_taxi_etl",
    schedule_interval=timedelta(days=1),
    catchup=False,
    default_args={"start_date": datetime(2024, 4, 4)},
)
def green_taxi_etl():
    @task
    def extract():
        # Add a data loader block and use Pandas to read data for the final quarter of 2020 (months 10, 11, 12).
        base_url = (
            "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green"
        )

        final_df = pd.DataFrame()

        # load the final three months using a for loop and pd.concat
        for month in range(10, 13):
            file_path = f"{base_url}/green_tripdata_2020-{month}.csv.gz"
            df = pd.read_csv(file_path, compression="gzip")

            logging.info(f"Loaded data from {file_path}")

            final_df = pd.concat([final_df, df])

        return final_df

    @task
    def transform(df: pd.DataFrame):
        # Remove rows where the passenger count is equal to 0 and the trip distance is equal to zero.
        # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
        # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id

        df = df[(df["passenger_count"] > 0) & (df["trip_distance"] > 0)]
        df["lpep_pickup_date"] = pd.to_datetime(df["lpep_pickup_datetime"]).dt.date
        df.columns = df.columns.str.lower().str.replace(" ", "_")

        return df

    @task
    def load_postgres(df: pd.DataFrame):
        from sqlalchemy import create_engine

        #  Using a Postgres data exporter (SQL or Python), write the dataset to a table called green_taxi in a schema airflow
        #  Replace the table if it already exists
        conn_str = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
        engine = create_engine(conn_str)

        df.to_sql(
            "green_taxi", engine, schema="airflow", if_exists="replace", index=False
        )

    @task
    def generate_parquet(df: pd.DataFrame):
        # Save the transformed data to a Parquet file
        parquet_path = "/opt/airflow/green_taxi.parquet"
        df.to_parquet(parquet_path)

        return parquet_path

    extract_data = extract()
    transformed_data = transform(extract_data)
    load_postgres(transformed_data)
    parquet = generate_parquet(transformed_data)

    bucket_name = os.getenv("GCP_GCS_BUCKET")
    local_to_gcs = LocalFilesystemToGCSOperator(
        task_id="local_to_gcs",
        src=parquet,
        dst=f"green_taxi.parquet",
        bucket=bucket_name,
    )


green_taxi_etl()
