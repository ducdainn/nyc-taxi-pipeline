{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='trip_id',
        description='Fact table for yellow taxi trips (incremental)'
    )
}}

with trips as (
    select * from {{ ref('stg_yellow_trips') }}
    {% if is_incremental() %}
    where pickup_datetime > (select max(pickup_datetime) from {{ this }})
    {% endif %}
),

enriched as (
    select
        t.trip_id,
        t.vendor_id,
        t.pickup_location_id,
        t.dropoff_location_id,
        t.pickup_datetime,
        t.dropoff_datetime,
        date_trunc('month', t.pickup_datetime) as pickup_month,
        date_trunc('day', t.pickup_datetime) as pickup_date,
        extract(hour from t.pickup_datetime)::integer as pickup_hour,
        extract(dow from t.pickup_datetime)::integer as pickup_day_of_week,
        t.passenger_count,
        t.trip_distance,
        datediff('minute', t.pickup_datetime, t.dropoff_datetime) as trip_duration_minutes,
        t.rate_code_id,
        t.store_and_fwd_flag,
        t.payment_type,
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.congestion_surcharge,
        t.airport_fee,
        t.total_amount,
        case 
            when t.trip_distance > 0 then t.fare_amount / t.trip_distance 
            else 0 
        end as fare_per_mile,
        case 
            when t.total_amount > 0 then t.tip_amount / t.total_amount 
            else 0 
        end as tip_percentage,
        t._ingested_at,
        t._source_file,
        current_timestamp as _dbt_updated_at
    from trips t
)

select * from enriched




