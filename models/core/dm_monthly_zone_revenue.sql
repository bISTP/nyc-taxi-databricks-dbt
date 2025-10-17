{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)
select
    -- Grouping dimensions
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    trips_data.service_type,
    date_trunc('month', trips_data.pickup_datetime) as revenue_month,

    -- Revenue calculation (broken down)
    sum(trips_data.fare_amount) as revenue_monthly_fare,
    sum(trips_data.extra) as revenue_monthly_extra,
    sum(trips_data.mta_tax) as revenue_monthly_mta_tax,
    sum(trips_data.tip_amount) as revenue_monthly_tip_amount,
    sum(trips_data.tolls_amount) as revenue_monthly_tolls_amount,
    sum(trips_data.ehail_fee) as revenue_monthly_ehail_fee,
    sum(trips_data.improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(trips_data.congestion_surcharge) as revenue_monthly_congestion_surcharge,
    sum(trips_data.cbd_congestion_fee) as revenue_monthly_cbd_congestion_fee,
    sum(trips_data.airport_fee) as revenue_monthly_airport_fee,
    sum(trips_data.total_amount) as revenue_monthly_total_amount,

    -- Additional operational metrics
    count(trips_data.tripid) as total_monthly_trips,
    avg(trips_data.passenger_count) as avg_monthly_passenger_count,
    avg(trips_data.trip_distance) as avg_monthly_trip_distance

from trips_data
inner join {{ ref('dim_zones') }} as pickup_zone
on trips_data.pickup_locationid = pickup_zone.locationid
group by 1, 2, 3, 4
