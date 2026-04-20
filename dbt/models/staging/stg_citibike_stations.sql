with source as (

    select *
    from {{ source('citibike_raw', 'raw_citibike_stations') }}

)

select
    station_id,
    station_name,
    lat,
    lon,
    capacity

from source
