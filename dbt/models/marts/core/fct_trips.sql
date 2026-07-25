{{ config(
    materialized='iceberg_table',
    partition_by='month(tpep_pickup_datetime)'
) }}

select
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    date_trunc('day', tpep_pickup_datetime) as pickup_date,
    extract(hour from tpep_pickup_datetime) as pickup_hour,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    tip_pct,
    trip_duration_minutes,
    fare_per_mile,
    payment_type
from {{ ref('int_trips_enriched') }}