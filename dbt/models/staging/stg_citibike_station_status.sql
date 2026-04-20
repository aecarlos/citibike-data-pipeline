-- dbt/models/staging/stg_citibike_station_status.sql

with source as (
    select *
    from {{ source('citibike_raw', 'raw_citibike_station_status') }}
)

select
    cast(station_id as string) as station_id,
    cast(num_bikes_available as int64) as num_bikes_available,
    cast(num_docks_available as int64) as num_docks_available,

    cast(is_installed as int64) = 1 as is_installed,
    cast(is_renting as int64) = 1 as is_renting,
    cast(is_returning as int64) = 1 as is_returning,

    -- Clean the API timestamp
    case
        when cast(last_reported as int64) between 946684800 and 4102444800
        then timestamp_seconds(cast(last_reported as int64))
        else null
    end as last_reported,

    -- ADD THESE TWO:
    extraction_timestamp, -- The real-time stamp from your Python script

    -- Fallback logic: Use API time if valid, otherwise use ingestion time
    coalesce(
        case
            when cast(last_reported as int64) between 946684800 and 4102444800
            then timestamp_seconds(cast(last_reported as int64))
            else null
        end,
        extraction_timestamp
    ) as status_timestamp

from source
