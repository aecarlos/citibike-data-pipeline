with trips as (
    select *
    from {{ ref('fct_citibike_trips') }}
),

base as (
    select
        date(start_time) as trip_date,
        trip_duration_minutes,
        member_casual,
        1 as trip_count
    from trips
)

select
    trip_date,
    member_casual as member_type,
    count(*) as total_trips,
    avg(trip_duration_minutes) as avg_duration_minutes
from base
group by trip_date, member_type
order by trip_date
