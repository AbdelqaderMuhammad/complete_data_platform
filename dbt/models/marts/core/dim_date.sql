{{ config(materialized='iceberg_table') }}

with spine as (
    select unnest(generate_series(
        date '2024-01-01',
        date '2025-05-01',
        interval '1 day'
    )) as date_day
)

select
    date_day as date_key,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    dayname(date_day) as day_name,
    extract(dow from date_day) in (0, 6) as is_weekend
from spine