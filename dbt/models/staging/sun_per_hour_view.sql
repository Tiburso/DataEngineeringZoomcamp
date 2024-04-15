{{ config(materialized='view') }}

-- Q24H sunlight average in the last 24 hours
-- Select the average sunlight in the last 24 hours 
SELECT 
    Q24H, datetime AS date, stationname
FROM 
    {{ source('staging', 'weather_data') }}
ORDER BY 
    datetime DESC
{% if var('is_test_run', default=true) %}
    LIMIT 100
{% endif %}