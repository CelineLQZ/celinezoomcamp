{{ config(materialized="table") }}

WITH fhv_trips AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} AS tripid,
        dispatching_base_num,
        CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(dropOff_datetime AS TIMESTAMP) AS dropoff_datetime,
        SAFE_CAST(PUlocationID AS INT64) AS pickup_locationid,
        SAFE_CAST(DOlocationID AS INT64) AS dropoff_locationid,
        EXTRACT(YEAR FROM pickup_datetime) AS pickup_year,
        EXTRACT(QUARTER FROM pickup_datetime) AS pickup_quarter,
        SR_Flag,
        Affiliated_base_number
    FROM {{ ref('stg_fhv_tripdata') }}
    WHERE dispatching_base_num IS NOT NULL
),

dim_zones AS (
    SELECT * FROM {{ ref('dim_zones') }} WHERE borough != 'Unknown'
)

SELECT 
    -- **唯一标识**
    f.tripid,

    -- **调度基站信息**
    f.dispatching_base_num,

    -- **时间字段**
    f.pickup_datetime,
    f.pickup_year,
    f.pickup_quarter,
    f.dropoff_datetime,

    -- **接送位置信息**
    f.pickup_locationid,
    pz.borough AS pickup_borough, 
    pz.zone AS pickup_zone, 
    f.dropoff_locationid,
    dz.borough AS dropoff_borough, 
    dz.zone AS dropoff_zone,

    -- **行程信息**
    f.SR_Flag,
    f.Affiliated_base_number

FROM fhv_trips f
LEFT JOIN dim_zones pz 
    ON f.pickup_locationid = pz.locationid
LEFT JOIN dim_zones dz 
    ON f.dropoff_locationid = dz.locationid


