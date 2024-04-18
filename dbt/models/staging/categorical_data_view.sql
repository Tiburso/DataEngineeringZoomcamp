{{ config(materialized='view') }}

-- D1H rainfall duration in the last hour
-- Select the count of stations where it has rained for more than 1 hour in the day

SELECT 
    DISTINCT stationname, COUNT(stationname) AS count_stationname

FROM 
    {{ source('staging', 'weather_data') }}
WHERE
    D1H >= 50
GROUP BY 
    stationname
    
{% if var('is_test_run', default=false) %}
    LIMIT 100
{% endif %}