{{ config(materialized='view') }}

with src as (
    select
        flight_date,
        status,
        dep_airport,
        dep_icao,
        dep_iata,
        dep_scheduled,
        dep_actual,
        dep_delay_min,
        arr_airport,
        arr_icao,
        arr_iata,
        arr_scheduled,
        arr_actual,
        airline_name,
        airline_iata,
        flight_number,
        source_dep_icao,
        ingestion_time_utc,
        ingestion_date
    from {{ source('eu_flights', 'flights_live_curated') }}
)

select
    date(flight_date) as flight_date,
    status,
    dep_airport,
    dep_icao,
    dep_iata,
    timestamp(dep_scheduled) as dep_scheduled_ts,
    timestamp(dep_actual) as dep_actual_ts,
    dep_delay_min,
    arr_airport,
    arr_icao,
    arr_iata,
    timestamp(arr_scheduled) as arr_scheduled_ts,
    timestamp(arr_actual) as arr_actual_ts,
    airline_name,
    airline_iata,
    flight_number,
    source_dep_icao,
    timestamp(ingestion_time_utc) as ingestion_ts,
    ingestion_date
from src
