-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `bigdata-405714.trips_data_all.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_bigdata-405714/data/yellow/yellow_tripdata_2019-*.parquet', 'gs://dtc_data_lake_bigdata-405714/data/yellow/yellow_tripdata_2020-*.parquet']
);

-- Check yello trip data
SELECT * FROM bigdata-405714.trips_data_all.external_yellow_tripdata limit 10;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE bigdata-405714.trips_data_all.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM bigdata-405714.trips_data_all.external_yellow_tripdata;


SELECT DISTINCT(VendorID)
FROM bigdata-405714.trips_data_all.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `trips_data_all.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE bigdata-405714.trips_data_all.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM bigdata-405714.trips_data_all.external_yellow_tripdata;

SELECT count(*) as trips
FROM bigdata-405714.trips_data_all.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;