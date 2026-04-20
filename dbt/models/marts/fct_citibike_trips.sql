with staged as (
    select *
    from {{ ref('stg_citibike_trips') }}
)

select
    generate_uuid() as trip_id,

    ride_id,
    rideable_type,

    start_time,
    end_time,

    trip_duration_seconds,
    trip_duration_minutes,

    start_station_id,
    end_station_id,

    start_station_name,
    end_station_name,

    start_latitude,
    start_longitude,
    end_latitude,
    end_longitude,

    member_casual

from staged
