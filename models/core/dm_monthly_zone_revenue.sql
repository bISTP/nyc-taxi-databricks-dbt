{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)
select
    -- Revenue grouping
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    
    -- Use date_trunc for consistent month formatting
    date_trunc('month', trips_data.pickup_datetime) as revenue_month,

    -- trip calculations
    count(trips_data.tripid) as total_monthly_trips,
    
    -- Revenue calculation
    sum(trips_data.total_amount) as total_monthly_revenue,

    -- Cost calculation
    sum(trips_data.total_amount) / count(trips_data.tripid) as average_revenue_per_trip

from trips_data
inner join {{ ref('dim_zones') }} as pickup_zone
on trips_data.pickup_locationid = pickup_zone.locationid
group by 1, 2, 3