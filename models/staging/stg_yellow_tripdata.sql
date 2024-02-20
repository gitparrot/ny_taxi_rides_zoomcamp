{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by vendor_id, pickup_datetime) as rn
  from {{ source('staging','yellow_tripdata') }}
  where vendor_id is not null 
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast("vendor_id", "INT64") }} as vendorid,
    rate_code,  -- Assuming rate_code does not need casting to INT64 due to its STRING type
    pickup_location_id,  -- Assuming no casting required; remove safe_cast if casting from STRING to INT64 not needed
    dropoff_location_id,  -- Same as above

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", "INT64") }} as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    '1' as trip_type,  -- Assuming the trip type for yellow cabs is always '1' for street-hail

    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(0 as numeric) as ehail_fee,  -- Assuming ehail_fee is not applicable to yellow cabs
    cast(imp_surcharge as numeric) as improvement_surcharge,  -- Correcting to match your provided column names
    cast(total_amount as numeric) as total_amount,
    coalesce({{ dbt.safe_cast("payment_type", "INT64") }}, 0) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description
from tripdata
where rn = 1
