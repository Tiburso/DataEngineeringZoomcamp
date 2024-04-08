{{ config(materialized='view') }}

SELECT 
-- Identifiers
    cast("VendorID" as integer) as vendorid,
    cast("RatecodeID" as integer) as ratecodeid,
    cast("PULocationID" as integer) as pulocationid,
    cast("DOLocationID" as integer) as dolocationid,

-- Timestamps
    cast("lpep_pickup_datetime" as timestamp) as pickup_datetime,
    cast("lpep_dropoff_datetime" as timestamp) as dropoff_datetime,

-- Trip Info
    store_and_fwd_flag,
    cast("passenger_count" as integer) as passenger_count,
    cast("trip_distance" as numeric) as trip_distance,
    cast("trip_type" as integer) as trip_type,

-- Payment Info
    cast("fare_amount" as numeric) as fare_amount,
    cast("extra" as numeric) as extra,
    cast("mta_tax" as numeric) as mta_tax,
    cast("tip_amount" as numeric) as tip_amount,
    cast("tolls_amount" as numeric) as tolls_amount,
    cast("ehail_fee" as integer) as ehail_fee,
    cast("improvement_surcharge" as numeric) as improvement_surcharge,
    cast("total_amount" as numeric) as total_amount,
    cast("payment_type" as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    cast("congestion_surcharge" as numeric) as congestion_surcharge
FROM {{ source('staging', 'green_tripdata') }}
LIMIT 100