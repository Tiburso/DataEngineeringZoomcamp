from pyspark.sql import SparkSession, DataFrame
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


def create_context() -> SparkSession:
    keyfile = "/home/manuel/Projects/DataEngineeringZoomcamp/infra/creds.json"

    # Use spark gcs connector
    conf = (
        SparkConf()
        .setAppName("GCSRead")
        .set("spark.sql.legacy.parquet.nanosAsLong", "true")
    )

    sc = SparkContext(conf=conf)

    hadoop_conf = sc._jsc.hadoopConfiguration()

    hadoop_conf.set(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    hadoop_conf.set(
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )

    return SparkSession.builder.config(conf=sc.getConf()).getOrCreate()


def read_parquet(
    spark: SparkSession, bucket, year, month, day, hour, minute
) -> DataFrame:
    file_location = f"gs://{bucket}/Actuele10mindataKNMIstations/2/{year}/{month}/{day}/{hour}/{minute}/weather_data.parquet"

    return spark.read.parquet(file_location)


def insert_big_query(df: DataFrame, bucket, project, dataset):
    (
        df.write.format("bigquery")
        .option("temporaryGcsBucket", bucket)
        .mode("append")
        .option("parentProject", project)
        .save(f"{project}:{dataset}.weather_data")
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("--year", type=str, required=True)
    parser.add_argument("--month", type=str, required=True)
    parser.add_argument("--day", type=str, required=True)
    parser.add_argument("--hour", type=str, required=True)
    parser.add_argument("--minute", type=str, required=True)

    parser.add_argument("--project", type=str, required=True)
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--bucket", type=str, required=True)

    args = parser.parse_args()

    spark = create_context()
    df = read_parquet(
        spark, args.bucket, args.year, args.month, args.day, args.hour, args.minute
    )
    insert_big_query(df, args.bucket, args.project, args.dataset)
