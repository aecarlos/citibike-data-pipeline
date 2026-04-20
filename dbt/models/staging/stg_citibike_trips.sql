with raw_data as (
    select *
    from {{ source('citibike_raw', 'raw_citibike_trips') }}
)

select
    -- identifiers
    ride_id,
    rideable_type,

    -- timestamps
    safe_cast(started_at as timestamp) as start_time,
    safe_cast(ended_at as timestamp) as end_time,

    -- duration
    timestamp_diff(
        safe_cast(ended_at as timestamp),
        safe_cast(started_at as timestamp),
        second
    ) as trip_duration_seconds,

    timestamp_diff(
        safe_cast(ended_at as timestamp),
        safe_cast(started_at as timestamp),
        second
    ) / 60.0 as trip_duration_minutes,

    -- stations
    safe_cast(start_station_id as string) as start_station_id,
    safe_cast(end_station_id as string) as end_station_id,
    start_station_name,
    end_station_name,

    -- geography
    safe_cast(start_lat as float64) as start_latitude,
    safe_cast(start_lng as float64) as start_longitude,
    safe_cast(end_lat as float64) as end_latitude,
    safe_cast(end_lng as float64) as end_longitude,

    -- USER TYPE
    lower(member_casual) as member_casual

from raw_data
