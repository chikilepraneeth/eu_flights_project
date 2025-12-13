{{ config(materialized='view') }}

with src as (
  select
    YEAR, MONTH_NUM, MONTH_MON, FLT_DATE,
    APT_ICAO, APT_NAME, STATE_NAME,
    FLT_DEP_1, FLT_ARR_1, FLT_TOT_1
  from {{ source('eu_flights', 'static_flights') }}
)

select
  cast(YEAR as int64) as year,
  cast(MONTH_NUM as int64) as month_num,
  MONTH_MON as month_name,
  date(FLT_DATE) as flight_date,
  date_trunc(date(FLT_DATE), month) as month_start,

  upper(trim(APT_ICAO)) as airport_icao,
  APT_NAME as airport_name,
  STATE_NAME as country_name,

  cast(FLT_DEP_1 as int64) as dep_ifr_count,
  cast(FLT_ARR_1 as int64) as arr_ifr_count,
  cast(FLT_TOT_1 as int64) as total_ifr_movements
from src
