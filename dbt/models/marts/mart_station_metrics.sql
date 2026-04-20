with base as (

    select *
    from {{ ref('dim_stations') }}

)

select
    station_id,
    station_name,
    lat,
    lon,
    capacity,

    -- size segmentation
    case
        when capacity >= 50 then 'large'
        when capacity >= 25 then 'medium'
        else 'small'
    end as station_size,

    -- geospatial buckets
    round(lat, 2) as lat_bucket,
    round(lon, 2) as lon_bucket,

    -- simple zone proxy
    case
        when lat > 40.8 then 'north'
        when lat < 40.7 then 'south'
        else 'central'
    end as city_zone,

    -- inactive flag
    case
        when capacity = 0 then true
        else false
    end as is_inactive

from base
