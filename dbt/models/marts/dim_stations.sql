select
    station_id,
    station_name,
    lat,
    lon,
    capacity
from {{ ref('stg_citibike_stations') }}
