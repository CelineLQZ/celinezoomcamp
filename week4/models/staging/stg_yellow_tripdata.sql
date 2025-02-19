{{
    config(
        materialized='view'
    )
}}

WITH tripdata AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY VendorID, pickup_datetime) AS rn
    FROM {{ source('staging', 'yellow_tripdata_ext') }}
    WHERE VendorID IS NOT NULL
)

SELECT
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['VendorID', 'pickup_datetime']) }} AS tripid,
    {{ dbt.safe_cast("VendorID", api.Column.translate_type("integer")) }} AS VendorID,
    {{ dbt.safe_cast("RateCodeID", api.Column.translate_type("integer")) }} AS RateCodeID,
    {{ dbt.safe_cast("Pickup_locationid", api.Column.translate_type("integer")) }} AS Pickup_locationid,
    {{ dbt.safe_cast("dropoff_locationid", api.Column.translate_type("integer")) }} AS dropoff_locationid,

    -- timestamps
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- trip info
    Store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} AS passenger_count,
    CAST(Trip_distance AS NUMERIC) AS Trip_distance,

    -- payment info
    CAST(Fare_amount AS NUMERIC) AS Fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(Tip_amount AS NUMERIC) AS Tip_amount,
    CAST(Tolls_amount AS NUMERIC) AS Tolls_amount,
    CAST(Improvement_surcharge AS NUMERIC) AS Improvement_surcharge,
    CAST(Total_amount AS NUMERIC) AS Total_amount,
    COALESCE({{ dbt.safe_cast("Payment_type", api.Column.translate_type("integer")) }}, 0) AS Payment_type,
    {{ get_payment_type_description("Payment_type") }} AS payment_type_description

FROM tripdata
WHERE rn = 1

