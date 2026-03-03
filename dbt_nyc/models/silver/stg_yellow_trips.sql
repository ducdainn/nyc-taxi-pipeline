-- filepath: dbt_nyc/models/silver/stg_yellow_trips.sql
{{
    config(
        materialized='view',
        description='Cleaned and standardized yellow taxi trips'
    )
}}

with source as (
    select * from read_parquet(
        '{{ var("bronze_path") }}',
        hive_partitioning = true
    )
),

renamed as (
    select
        -- IDs
        VendorID as vendor_id,
        PULocationID as pickup_zone_id,
        DOLocationID as dropoff_zone_id,
        
        -- Timestamps
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        
        -- Trip details
        passenger_count::integer as passenger_count,
        trip_distance,
        RatecodeID::integer as rate_code_id,
        store_and_fwd_flag,
        
        -- Payment
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
        
        -- Metadata
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
        -- Filter out invalid records
        pickup_datetime is not null
        and dropoff_datetime is not null
        and trip_distance >= 0
        and fare_amount >= 0
        and total_amount >= 0
        -- Filter out unrealistic values
        and trip_distance <= 500
        and fare_amount <= 1000
        and total_amount <= 2000
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by vendor_id, pickup_datetime, dropoff_datetime, pickup_zone_id
            order by _ingested_at desc
        ) as _row_num
    from filtered
)

select
    -- Generate surrogate key
    md5(
        coalesce(cast(vendor_id as varchar), '') || '-' ||
        coalesce(cast(pickup_datetime as varchar), '') || '-' ||
        coalesce(cast(dropoff_datetime as varchar), '') || '-' ||
        coalesce(cast(pickup_zone_id as varchar), '')
    ) as trip_id,
    
    vendor_id,
    pickup_zone_id,
    dropoff_zone_id,
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

