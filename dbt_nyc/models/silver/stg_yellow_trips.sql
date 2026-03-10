{{
    config(
        materialized='table',
        description='Cleaned and standardized yellow taxi trips'
    )
}}

with source as (
    select * from {{ source('bronze', 'yellow_trips') }}
),

renamed as (
    select
        VendorID as vendor_id,
        PULocationID as pickup_location_id,
        DOLocationID as dropoff_location_id,
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        passenger_count::integer as passenger_count,
        trip_distance,
        RatecodeID::integer as rate_code_id,
        store_and_fwd_flag,
        payment_type::integer as payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        Airport_fee as airport_fee,
        total_amount,
        _ingested_at,
        _source_file,
        _year,
        _month
    from source
),

filtered as (
    select *
    from renamed
    where 
        pickup_datetime is not null
        and dropoff_datetime is not null
        and trip_distance >= 0
        and fare_amount >= 0
        and total_amount >= 0
        and trip_distance <= 500
        and fare_amount <= 1000
        and total_amount <= 2000
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by vendor_id, pickup_datetime, dropoff_datetime, pickup_location_id
            order by _ingested_at desc
        ) as _row_num
    from filtered
)

select
    md5(
        coalesce(cast(vendor_id as varchar), '') || '-' ||
        coalesce(cast(pickup_datetime as varchar), '') || '-' ||
        coalesce(cast(dropoff_datetime as varchar), '') || '-' ||
        coalesce(cast(pickup_location_id as varchar), '')
    ) as trip_id,
    vendor_id,
    pickup_location_id,
    dropoff_location_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    _ingested_at,
    _source_file,
    _year,
    _month
from deduplicated
where _row_num = 1
