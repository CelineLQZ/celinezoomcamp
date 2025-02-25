{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)

select
    pickup_year,
    pickup_quarter,
    service_type, 
    sum(total_amount) as quarterly_total_amount
from trips_data
group by pickup_year, pickup_quarter, service_type







