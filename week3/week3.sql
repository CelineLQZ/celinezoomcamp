CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-449620.nyc_taxi_data.yellow_tripdata_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_celine/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE `kestra-sandbox-449620.nyc_taxi_data.yellow_tripdata` AS
SELECT * FROM `kestra-sandbox-449620.nyc_taxi_data.yellow_tripdata_ext`;

SELECT COUNT(*) AS total_records
FROM `kestra-sandbox-449620.nyc_taxi_data.yellow_tripdata_ext`;

CREATE OR REPLACE TABLE `kestra-sandbox-449620.nyc_taxi_data.yellow_tripdata_table`
AS
SELECT * FROM `kestra-sandbox-449620.nyc_taxi_data.yellow_tripdata_ext`;

SELECT 
    COUNT(DISTINCT PULocationID) AS distinct_pulocations
FROM `kestra-sandbox-449620.nyc_taxi_data.yellow_tripdata_ext`;

SELECT 
    COUNT(DISTINCT PULocationID) AS distinct_pulocations
FROM `kestra-sandbox-449620.nyc_taxi_data.yellow_tripdata_table`;

SELECT PULocationID 
FROM `kestra-sandbox-449620.nyc_taxi_data.yellow_tripdata_table`;

SELECT PULocationID, DOLocationID 
FROM `kestra-sandbox-449620.nyc_taxi_data.yellow_tripdata_table`;

SELECT COUNT(*) AS zero_fare_trips
FROM `kestra-sandbox-449620.nyc_taxi_data.yellow_tripdata_table`
WHERE fare_amount = 0;

CREATE OR REPLACE TABLE `kestra-sandbox-449620.nyc_taxi_data.optimized_yellow_tripdata`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `kestra-sandbox-449620.nyc_taxi_data.yellow_tripdata_table`;

SELECT DISTINCT VendorID
FROM `kestra-sandbox-449620.nyc_taxi_data.yellow_tripdata_table`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID
FROM `kestra-sandbox-449620.nyc_taxi_data.optimized_yellow_tripdata`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT COUNT(*) 
FROM `kestra-sandbox-449620.nyc_taxi_data.yellow_tripdata_table`;
