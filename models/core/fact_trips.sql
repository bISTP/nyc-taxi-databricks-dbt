{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["tripid"],
    )
}}

with
    green_tripdata as (
        select
            -- Select all columns EXCEPT the old tripid
            vendorid,
            ratecodeid,
            pickup_locationid,
            dropoff_locationid,
            pickup_datetime,
            dropoff_datetime,
            store_and_fwd_flag,
            passenger_count,
            trip_distance,
            trip_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            ehail_fee,
            improvement_surcharge,
            total_amount,
            payment_type,
            payment_type_description,
            congestion_surcharge,
            cbd_congestion_fee,
            null as airport_fee,  -- Green taxis don't have this fee
            'Green' as service_type
        from {{ ref("stg_green_tripdata") }}
    ),
    yellow_tripdata as (
        select
            vendorid,
            ratecodeid,
            pickup_locationid,
            dropoff_locationid,
            pickup_datetime,
            dropoff_datetime,
            store_and_fwd_flag,
            passenger_count,
            trip_distance,
            null as trip_type,  -- Yellow taxis don't have trip_type
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            null as ehail_fee,  -- Yellow taxis don't have ehail_fee
            improvement_surcharge,
            total_amount,
            payment_type,
            payment_type_description,
            congestion_surcharge,
            cbd_congestion_fee,
            airport_fee,
            'Yellow' as service_type
        from {{ ref("stg_yellow_tripdata") }}
    ),
    trips_unioned as (
        select *
        from green_tripdata
        union all
        select *
        from yellow_tripdata
    )
select
    -- CREATE THE GLOBALLY UNIQUE TRIPID HERE
    {{
        dbt_utils.generate_surrogate_key(
            ["vendorid", "pickup_datetime", "service_type"]
        )
    }} as tripid,
    vendorid,
    service_type,
    ratecodeid,
    pickup_locationid,
    dropoff_locationid,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    payment_type_description,
    congestion_surcharge,
    cbd_congestion_fee,
    airport_fee
from trips_unioned

-- The incremental logic should be outside the dev limit block
{% if is_incremental() %}
    -- This filter will only be applied on an incremental run
    -- seeks to find the max pickup_datetime from the already existing table
    where pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}

{% if var("is_test_run", default=true) %}
    -- Apply the limit directly to the fact table during development
    limit 100
{% endif %}
