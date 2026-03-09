-- filepath: dbt_nyc/models/silver/stg_taxi_zones.sql
{{
    config(
        materialized='view',
        description='TLC taxi zone lookup table'
    )
}}

select
    "LocationID" as zone_id,
    "Borough" as borough,
    "Zone" as zone_name,
    "service_zone" as service_zone
from {{ ref('taxi_zones') }}




