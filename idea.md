0. Use Terraform to manage all my infrastructure

1. Analyze the prefered dataset [KMNI Weather Data 10 mins](https://dataplatform.knmi.nl/dataset/actuele10mindataknmistations-2)

2. Use airflow to automate the process of data extraction and transformation

   1. Create a DAG that will run the process every day
   2. Upload the data to a gcs bucket

3. Use aiflow to automate the data transformation process

   1. Create a DAG that will run the process every day
   2. It will read from the previous bucket
   3. Run spark to transform the data
   4. Upload the data to big query
   5. Trigger DBT to run the models and provide views

4. Read the data from big query and create a dashboard
   1. Use metabase to create the dashboard
   2. Use the data from big query to populate the dashboard
