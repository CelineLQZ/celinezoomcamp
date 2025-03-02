-- Create the yellow_tripdata_ext
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-449620.zoomcamp_week4_hw.yellow_tripdata_ext`
OPTIONS (
  format = 'CSV',
  uris = [
    'gs://zoomcamp_week4_hw/yellow_tripdata_2019-*.csv.gz',
    'gs://zoomcamp_week4_hw/yellow_tripdata_2020-*.csv.gz'
  ]
);

-- Create the green_tripdata_ext
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-449620.zoomcamp_week4_hw.green_tripdata_ext`
OPTIONS (
  format = 'CSV',
  uris = [
    'gs://zoomcamp_week4_hw/green_tripdata_2019-*.csv.gz',
    'gs://zoomcamp_week4_hw/green_tripdata_2020-*.csv.gz'
  ]
);

-- Create the fhv_tripdata_ext
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-449620.zoomcamp_week4_hw.fhv_tripdata_ext`
OPTIONS (
  format = 'CSV',
  uris = [
    'gs://zoomcamp_week4_hw/fhv_tripdata_2019-*.csv.gz']
);

-- Total num is 43,244,696
SELECT COUNT(*) AS TOTAL_NUM
FROM `kestra-sandbox-449620.zoomcamp_week4_hw.fhv_tripdata_ext` 
