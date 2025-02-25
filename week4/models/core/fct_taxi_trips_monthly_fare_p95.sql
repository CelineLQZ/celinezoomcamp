{{
    config(
        materialized='table'
    )
}}

WITH filtered_trips AS (
    -- 过滤掉无效的订单
    SELECT 
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS revenue_year,
        EXTRACT(MONTH FROM pickup_datetime) AS revenue_month,
        fare_amount
    FROM {{ ref('fact_trips') }}
    WHERE 
        fare_amount > 0
        AND trip_distance > 0
        AND payment_type_description IN ('Cash', 'Credit card')
),

percentile_calculations AS (
    -- 计算不同的分位数
    SELECT
        service_type,
        revenue_year,
        revenue_month,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (
            PARTITION BY service_type, revenue_year, revenue_month
        ) AS p97_fare,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (
            PARTITION BY service_type, revenue_year, revenue_month
        ) AS p95_fare,
        PERCENTILE_CONT(fare_amount, 0.90) OVER (
            PARTITION BY service_type, revenue_year, revenue_month
        ) AS p90_fare
    FROM filtered_trips
)

SELECT DISTINCT
    service_type,
    revenue_year,
    revenue_month,
    p97_fare,
    p95_fare,
    p90_fare
FROM percentile_calculations
WHERE revenue_year = 2020
  AND revenue_month = 4