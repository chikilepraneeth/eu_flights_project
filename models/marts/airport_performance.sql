{{ config(materialized='view') }}

with static_agg as (
    select
        airport_icao,
        airport_name,
        country_name,

        -- baseline from static dataset (historical)
        avg(total_ifr_movements) as avg_daily_ifr_movements,
        avg(dep_ifr_count)       as avg_daily_dep_ifr,
        avg(arr_ifr_count)       as avg_daily_arr_ifr
    from {{ ref('stg_static_flights') }}
    group by 1, 2, 3
),

live_agg as (
    select
        dep_icao as airport_icao,

        count(*) as live_departures,

        -- delay metrics
        avg(dep_delay_min) as avg_dep_delay_min,
        approx_quantiles(dep_delay_min, 100)[offset(50)] as median_dep_delay_min,
        approx_quantiles(dep_delay_min, 100)[offset(90)] as p90_dep_delay_min,

        -- status counts
        sum(case when status = 'active'    then 1 else 0 end) as active_flights,
        sum(case when status = 'landed'    then 1 else 0 end) as landed_flights,
        sum(case when status = 'cancelled' then 1 else 0 end) as cancelled_flights,

        -- network richness
        count(distinct arr_icao) as distinct_destinations,

        -- latest ingestion timestamp
        max(ingestion_ts) as last_ingestion_ts

    from {{ ref('stg_live_flights') }}
    group by 1
)

select
    s.airport_icao,
    s.airport_name,
    s.country_name,

    -- baseline
    s.avg_daily_ifr_movements,
    s.avg_daily_dep_ifr,
    s.avg_daily_arr_ifr,

    -- live
    l.live_departures,
    l.active_flights,
    l.landed_flights,
    l.cancelled_flights,

    -- delays
    l.avg_dep_delay_min,
    l.median_dep_delay_min,
    l.p90_dep_delay_min,

    -- network
    l.distinct_destinations,

    -- "cool" index for visualization (simple + effective)
    safe_divide(l.live_departures, nullif(s.avg_daily_ifr_movements, 0)) as congestion_index,

    -- freshness
    l.last_ingestion_ts

from static_agg s
left join live_agg l
  on s.airport_icao = l.airport_icao
