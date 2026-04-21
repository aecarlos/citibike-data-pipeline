-- dbt/models/marts/mart_station_realtime_metrics.sql

with stations as (
    select *
    from {{ ref('stg_citibike_stations') }}
),

status as (
    select *
    from {{ ref('stg_citibike_station_status') }}
)

select
    s.station_id,
    s.station_name,
    s.lat,
    s.lon,
    s.capacity,

    coalesce(st.num_bikes_available, 0) as num_bikes_available,
    coalesce(st.num_docks_available, 0) as num_docks_available,

    -- Availability Logic
    coalesce(
        safe_divide(st.num_bikes_available, nullif(s.capacity, 0)),
        0
    ) as availability_rate,

    case
        when coalesce(st.num_bikes_available, 0) = 0 then true
        else false
    end as is_empty,

    case
        when coalesce(st.num_docks_available, 0) = 0 then true
        else false
    end as is_full,

    (st.is_renting and st.is_returning) as is_operational,

    -- Timestamps
    st.last_reported        -- Solo el tiempo de la API

from stations s
left join status st
    on cast(s.station_id as string) = cast(st.station_id as string)
