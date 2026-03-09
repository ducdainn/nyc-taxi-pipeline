-- filepath: dbt_nyc/models/gold/dim_datetime.sql
{{
    config(
        materialized='table',
        description='Date/time dimension table for analytics'
    )
}}

with date_spine as (
    -- Generate dates from 2020-01-01 to 2030-12-31
    select unnest(generate_series(
        '2020-01-01'::date,
        '2030-12-31'::date,
        interval '1 day'
    ))::date as date_day
),

hours as (
    select unnest(generate_series(0, 23)) as hour_of_day
),

datetime_base as (
    select
        d.date_day,
        h.hour_of_day,
        d.date_day + (h.hour_of_day || ' hours')::interval as datetime_hour
    from date_spine d
    cross join hours h
),

final as (
    select
        -- Keys
        datetime_hour,
        date_day,
        hour_of_day,
        
        -- Date parts
        extract(year from date_day)::integer as year,
        extract(month from date_day)::integer as month,
        extract(day from date_day)::integer as day_of_month,
        extract(dow from date_day)::integer as day_of_week,
        extract(doy from date_day)::integer as day_of_year,
        extract(week from date_day)::integer as week_of_year,
        extract(quarter from date_day)::integer as quarter,
        
        -- Date names
        strftime(date_day, '%A') as day_name,
        strftime(date_day, '%B') as month_name,
        strftime(date_day, '%Y-%m') as year_month,
        
        -- Flags
        case 
            when extract(dow from date_day) in (0, 6) then true 
            else false 
        end as is_weekend,
        
        case 
            when hour_of_day between 7 and 9 then true
            when hour_of_day between 16 and 19 then true
            else false 
        end as is_rush_hour,
        
        case 
            when hour_of_day between 6 and 11 then 'Morning'
            when hour_of_day between 12 and 16 then 'Afternoon'
            when hour_of_day between 17 and 20 then 'Evening'
            else 'Night'
        end as time_of_day,
        
        current_timestamp as _dbt_updated_at
        
    from datetime_base
)

select * from final




