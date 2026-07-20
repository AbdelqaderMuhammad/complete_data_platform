{{ config(
    materialized='iceberg_table',
    partition_by='month(tpep_pickup_datetime)'
) }}

select *
from {{ source('raw', 'yellow_taxi_trips') }}
where
    extract(year from tpep_pickup_datetime) = extract(year from tpep_dropoff_datetime)
    and tpep_pickup_datetime <= tpep_dropoff_datetime
    and tpep_pickup_datetime >= '2009-01-01'