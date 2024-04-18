{{ config(materialized='view') }}

-- Q24H sunlight average in the last 24 hours
-- Select the average sunlight in the last 24 hours 

-- Get the year, month, and day from the datetime column
SELECT 
    MAX(Q24H) AS max_daily_sunlight,
    date,
FROM 
    {{ source('staging', 'weather_data') }}
GROUP BY
    date
ORDER BY 
    date DESC
{% if var('is_test_run', default=false) %}
    LIMIT 100
{% endif %}