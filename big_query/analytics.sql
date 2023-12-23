-- 1. Overview report
CREATE OR REPLACE TABLE bigdata-405714.production.overview_report
PARTITION BY
     DATE(pickup_datetime) AS
(
     SELECT 
          passenger_count AS NumPassengers,
          dim_date.pickup_datetime,
          fare_amount AS Revenue,
          trip_distance AS Distance,
          service_type AS Service,
          payment_code AS Payment,
          zones.Zone,
          pickup_month AS Month,
          rate_name AS Rate,
     FROM bigdata-405714.production.fact_table AS fact_table
          JOIN bigdata-405714.production.dim_datetime AS dim_date
               ON fact_table.datetime_id = dim_date.datetime_id
          JOIN bigdata-405714.production.dim_passenger AS dim_passenger
               ON fact_table.passenger_id = dim_passenger.passenger_id
          JOIN bigdata-405714.production.dim_trip AS dim_trip
               ON fact_table.trip_distance_id = dim_trip.trip_distance_id
          JOIN bigdata-405714.production.dim_location AS dim_location
               ON fact_table.location_id = dim_location.location_id
          JOIN bigdata-405714.production.zones AS zones
               ON dim_location.PULocationID = zones.LocationID
          JOIN bigdata-405714.production.dim_payment_type AS dim_payment_type
               ON fact_table.payment_id = dim_payment_type.payment_id
          JOIN bigdata-405714.production.dim_Rate AS dim_rate
               ON fact_table.rate_id = dim_rate.rate_id
);



--2. Analytics Report
CREATE OR REPLACE TABLE bigdata-405714.production.anlytics_report
PARTITION BY
     DATE(pickup_datetime) AS
(
     SELECT 
          dim_date.pickup_datetime,
          zones_pu.Zone AS Zone,
          zones_pu.Borough AS Borough,
          CONCAT(zones_pu.Zone, '-', zones_do.Zone) AS Route,
          EXTRACT(HOUR FROM dim_date.pickup_datetime) AS Hour,
          FORMAT_DATE('%A', dim_date.pickup_datetime) AS Day,
          trip_distance AS Distance,
          trip_duration AS Duration,
          service_type AS Service,
          (CASE
               WHEN tip_amount = 0
                    THEN 'No Tip'
               WHEN ROUND(tip_amount * 100 / (fare_amount + tip_amount + tolls_amount), 2) <= 5 
                    THEN 'Less than 5%'
               WHEN ROUND(tip_amount * 100 / (fare_amount + tip_amount + tolls_amount), 2) <= 10 
                    THEN '5% to 10%'
               WHEN ROUND(tip_amount * 100 / (fare_amount + tip_amount + tolls_amount), 2) <= 15 
                    THEN '10% to 15%'
               WHEN ROUND(tip_amount * 100 / (fare_amount + tip_amount + tolls_amount), 2) <= 20 
                    THEN '15% to 20%'
               WHEN ROUND(tip_amount * 100 / (fare_amount + tip_amount + tolls_amount), 2) <= 25 
                    THEN '20% to 25%'
               ELSE 'More than 25%' 
          END) AS TipRate
     FROM bigdata-405714.production.fact_table AS fact_table
          JOIN bigdata-405714.production.dim_datetime AS dim_date
               ON fact_table.datetime_id = dim_date.datetime_id
          JOIN bigdata-405714.production.dim_location AS dim_location
               ON fact_table.location_id = dim_location.location_id
          JOIN bigdata-405714.production.zones AS zones_pu
               ON dim_location.PULocationID = zones_pu.LocationID
          JOIN bigdata-405714.production.zones AS zones_do
               ON dim_location.DOLocationID = zones_do.LocationID
          JOIN bigdata-405714.production.dim_trip AS dim_trip
               ON fact_table.trip_distance_id = dim_trip.trip_distance_id
);