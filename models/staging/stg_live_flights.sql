{{ config(materialized='view') }}

with src as (
    select * from {{ source('eu_flights', 'flights_live_curated') }}
)

select
    safe_cast(flight_date as date) as flight_date,
    lower(status) as status,

    dep_airport,
    upper(trim(dep_icao)) as dep_icao,
    dep_iata,

    safe_cast(dep_scheduled as timestamp) as dep_scheduled_ts,
    safe_cast(dep_actual as timestamp) as dep_actual_ts,
    dep_delay_min,

    arr_airport,
    upper(trim(arr_icao)) as arr_icao,
    arr_iata,

    safe_cast(arr_scheduled as timestamp) as arr_scheduled_ts,
    safe_cast(arr_actual as timestamp) as arr_actual_ts,

    airline_name,
    airline_iata,
    flight_number,

    upper(trim(source_dep_icao)) as source_dep_icao,
    timestamp(ingestion_time_utc) as ingestion_ts,
    ingestion_date
from src;
