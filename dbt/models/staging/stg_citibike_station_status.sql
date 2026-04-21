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

    -- =========================================
    -- FIXED TIMESTAMP NORMALIZATION
    -- =========================================

    case
        -- nanoseconds → micros (FORZANDO INT64)
        when cast(last_reported as int64) > 1000000000000000000
            then timestamp_micros(
                cast(cast(last_reported as int64) / 1000 as int64)
            )

        -- seconds epoch
        when cast(last_reported as int64) between 946684800 and 4102444800
            then timestamp_seconds(cast(last_reported as int64))

        -- garbage
        else null
    end as last_reported

from source
