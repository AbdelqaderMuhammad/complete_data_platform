{{ config(materialized='ephemeral') }}

select
    *,
    date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_minutes,
    case
        when fare_amount > 0 then tip_amount / fare_amount
        else null
    end as tip_pct,
    case
        when trip_distance > 0 then fare_amount / trip_distance
        else null
    end as fare_per_mile
from {{ ref('stg_yellow_taxi_trips') }}
where
    fare_amount > 0
    and fare_amount <= 500
    and trip_distance > 0
    and trip_distance <= 100
    and tip_amount >= 0
    and tpep_pickup_datetime >= '2024-01-01'