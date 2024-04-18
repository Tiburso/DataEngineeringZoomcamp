# Instalation

Everything in this project can be run via the `docker compose up` command. The bigquery and gcs buckets have to be setup via de `terraform apply` command in the `infra` folder

# Objective

The main goal of this project was to learn and practice data engineering basics. This is an end-to-end project where it goes from the data ingestion part to the presentation layer with Metabase.

# Tools

For this project the following tools were used:

- airflow (orchestration)
- Docker (containerization and development)
- terraform (infrastructure)
- Google Big Query/Google Cloud Storage (cloud -> data warehousing and lake)
- Apache Spark (batch processing)
- dbt (data transformation)
- Metabase (visualization)
