{{ config(
    materialized='view'
) }}

WITH tripdata AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY dispatching_base_num, pickup_datetime) AS rn
    FROM {{ source('staging', 'fhv_tripdata_ext') }}
)

SELECT
    -- **唯一标识**
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} AS tripid,
    
    -- **调度基站信息**
    dispatching_base_num,

    -- **时间字段**
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropOff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- **位置信息**
    SAFE_CAST(PUlocationID AS INT64) AS pickup_locationid,
    SAFE_CAST(DOlocationID AS INT64) AS dropoff_locationid,

    -- **行程信息**
    SR_Flag,

    -- **公司信息**
    Affiliated_base_number

FROM tripdata
WHERE rn = 1
  AND dispatching_base_num IS NOT NULL 


