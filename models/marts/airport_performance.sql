{{ config(materialized='view') }}

with static_agg as (
    select
        airport_icao,
        airport_name,
        country_name,
        avg(total_ifr_movements) as avg_daily_ifr_movements
    from {{ ref('stg_static_flights') }}
    group by 1, 2, 3
),

live_agg as (
    select
        dep_icao as airport_icao,
        count(*) as live_departures,
        avg(dep_delay_min) as avg_dep_delay_min,
        sum(case when status = 'active' then 1 else 0 end) as active_flights,
        sum(case when status = 'landed' then 1 else 0 end) as landed_flights,
        sum(case when status = 'cancelled' then 1 else 0 end) as cancelled_flights
    from {{ ref('stg_live_flights') }}
    group by 1
)

select
    s.airport_icao,
    s.airport_name,
    s.country_name,
    s.avg_daily_ifr_movements,
    l.live_departures,
    l.avg_dep_delay_min,
    l.active_flights,
    l.landed_flights,
    l.cancelled_flights
from static_agg s
left join live_agg l
  on s.airport_icao = l.airport_icao
