{{ config(materialized='view') }}


with static_agg as (
    select
        airport_icao,
        airport_name,
        country_name,

        avg(total_ifr_movements) as avg_daily_ifr_movements,
        avg(dep_ifr_count) as avg_daily_dep_ifr,
        avg(arr_ifr_count) as avg_daily_arr_ifr
    from {{ ref('stg_static_flights') }}
    group by 1,2,3
),


live_agg as (
    select
        dep_icao as airport_icao,

        count(*) as live_departures,

        avg(dep_delay_min) as avg_dep_delay_min,

        
        approx_quantiles(dep_delay_min, 100)[offset(50)] as median_dep_delay_min,
        approx_quantiles(dep_delay_min, 100)[offset(90)] as p90_dep_delay_min,

        
        sum(case when status = 'active' then 1 else 0 end) as active_flights,
        sum(case when status = 'landed' then 1 else 0 end) as landed_flights,
        sum(case when status = 'cancelled' then 1 else 0 end) as cancelled_flights,

        
        count(distinct arr_icao) as distinct_destinations,

        
        max(ingestion_ts) as last_ingestion_ts

    from {{ ref('stg_live_flights') }}
    where dep_icao is not null
    group by 1
)


select
    s.airport_icao,
    s.airport_name,
    s.country_name,

    
    s.avg_daily_ifr_movements,
    s.avg_daily_dep_ifr,
    s.avg_daily_arr_ifr,

    
    l.live_departures,
    l.avg_dep_delay_min,
    l.median_dep_delay_min,
    l.p90_dep_delay_min,
    l.active_flights,
    l.landed_flights,
    l.cancelled_flights,
    l.distinct_destinations,
    l.last_ingestion_ts,

   
    safe_divide(l.live_departures, s.avg_daily_dep_ifr) as congestion_index,
    safe_divide(l.cancelled_flights, l.live_departures) as cancellation_rate

from static_agg s
left join live_agg l
  on s.airport_icao = l.airport_icao
