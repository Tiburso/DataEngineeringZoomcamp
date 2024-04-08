{{ config(
    materialized='view',
    unique_key='pickup_datetime'
) }}

SELECT *
FROM {{ source('staging', 'green_tripdata') }}
LIMIT 100