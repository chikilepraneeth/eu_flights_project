{{ config(materialized='view') }}

with src as (
  select
    flight_date, status,
    dep_airport, dep_icao, dep_iata, dep_scheduled, dep_actual, dep_delay_min,
    arr_airport, arr_icao, arr_iata, arr_scheduled, arr_actual,
    airline_name, airline_iata, flight_number,
    source_dep_icao, ingestion_time_utc, ingestion_date
  from {{ source('eu_flights', 'flights_live_curated') }}
)

select
  safe_cast(flight_date as date) as flight_date,
  lower(status) as status,

  dep_airport,
  upper(trim(dep_icao)) as dep_icao,
  dep_iata,

  safe_cast(dep_scheduled as timestamp) as dep_scheduled_ts,
  safe_cast(dep_actual as timestamp) as dep_actual_ts,
  safe_cast(dep_delay_min as float64) as dep_delay_min,

  arr_airport,
  upper(trim(arr_icao)) as arr_icao,
  arr_iata,

  safe_cast(arr_scheduled as timestamp) as arr_scheduled_ts,
  safe_cast(arr_actual as timestamp) as arr_actual_ts,

  airline_name,
  airline_iata,
  flight_number,

  upper(trim(source_dep_icao)) as source_dep_icao,

  safe_cast(ingestion_time_utc as timestamp) as ingestion_ts,
  ingestion_date
from src;
