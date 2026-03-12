{{
    config(
        materialized='table',
        description='Dimension table for TLC taxi zones'
    )
}}

with zones as (
    select * from {{ ref('stg_taxi_zones') }}
),

final as (
    select
        zone_id,
        borough,
        zone_name,
        service_zone,
        case borough
            when 'Manhattan' then 'Manhattan'
            when 'Brooklyn' then 'Brooklyn'
            when 'Queens' then 'Queens'
            when 'Bronx' then 'Bronx'
            when 'Staten Island' then 'Staten Island'
            when 'EWR' then 'Airport'
            else 'Unknown'
        end as borough_group,
        case 
            when zone_name like '%Airport%' or borough = 'EWR' then true
            else false
        end as is_airport,
        current_timestamp as _dbt_updated_at
    from zones
)

select * from final




