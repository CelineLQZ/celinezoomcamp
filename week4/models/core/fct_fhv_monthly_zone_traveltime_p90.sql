{{ config(materialized="table") }}

WITH trip_data AS (
    -- 获取 FHV 行程数据，并计算 trip_duration
    SELECT
        tripid,
        pickup_locationid,
        dropoff_locationid,
        pickup_datetime,
        dropoff_datetime,
        pickup_year,
        pickup_month,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref("dim_fhv_trips") }}
),

p90_trip_duration AS (
    -- 计算 trip_duration 的 P90（连续分位数）
    SELECT
        pickup_year,
        pickup_month,
        pickup_locationid,
        dropoff_locationid,
        PERCENTILE_CONT(trip_duration, 0.90) OVER (
            PARTITION BY pickup_year, pickup_month, pickup_locationid, dropoff_locationid
        ) AS p90_trip_duration
    FROM trip_data
)

SELECT DISTINCT * FROM p90_trip_duration;
