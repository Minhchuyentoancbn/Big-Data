-- Create dim_passenger table
CREATE OR REPLACE TABLE bigdata-405714.production_bq_test.dim_passenger
AS (
    SELECT passenger_count, 
           ROW_NUMBER() OVER (ORDER BY passenger_count) AS passenger_id
    FROM (
        SELECT DISTINCT(passenger_count)
        FROM bigdata-405714.trips_data_all.trips_data_cleaned
    )
);
    
-- Create dim_trip table
CREATE OR REPLACE TABLE bigdata-405714.production_bq_test.dim_trip
AS (
    SELECT trip_distance, 
           pickup_datetime, 
           dropoff_datetime, 
           trip_distance_id, 
           trip_duration
    FROM (
        SELECT trip_distance, 
               pickup_datetime, 
               dropoff_datetime, 
               ROW_NUMBER() OVER (ORDER BY trip_distance) AS trip_distance_id, 
               ROUND((UNIX_SECONDS(dropoff_datetime) - UNIX_SECONDS(pickup_datetime)) / 60, 2) AS trip_duration
        FROM (
            SELECT DISTINCT trip_distance, 
                   pickup_datetime, 
                   dropoff_datetime
            FROM bigdata-405714.trips_data_all.trips_data_cleaned
        )
    )
);

-- Create dim_location
CREATE OR REPLACE TABLE bigdata-405714.production_bq_test.dim_location
AS (
    SELECT PULocationID, 
           DOLocationID, 
           location_id
    FROM (
        SELECT PULocationID,
                DOLocationID,
                ROW_NUMBER() OVER (ORDER BY PULocationID) AS location_id
        FROM (
            SELECT DISTINCT PULocationID,
                            DOLocationID
            FROM bigdata-405714.trips_data_all.trips_data_cleaned
        )
    )
);


-- Create dim_Rate
CREATE OR REPLACE TABLE bigdata-405714.production_bq_test.dim_Rate
AS (
    SELECT RateCodeID, 
           rate_id, 
           rate_name
    FROM (
        SELECT  RateCodeID, 
                ROW_NUMBER() OVER (ORDER BY RateCodeID) AS rate_id, 
                CASE RateCodeID
                    WHEN 1 THEN 'Standard rate'
                    WHEN 2 THEN 'JFK'
                    WHEN 3 THEN 'Newark'
                    WHEN 4 THEN 'Nassau or Westchester'
                    WHEN 5 THEN 'Negotiated fare'
                    WHEN 6 THEN 'Group ride'
                END AS rate_name
        FROM (
            SELECT DISTINCT RateCodeID
            FROM bigdata-405714.trips_data_all.trips_data_cleaned
        )
    )
);

-- Create dim_payment_type
CREATE OR REPLACE TABLE bigdata-405714.production_bq_test.dim_payment_type
AS (
    SELECT payment_type, 
           payment_id, 
           payment_code
    FROM (
        SELECT payment_type, 
                ROW_NUMBER() OVER (ORDER BY payment_type) AS payment_id, 
                CASE payment_type
                    WHEN 1 THEN 'Credit card'
                    WHEN 2 THEN 'Cash'
                    WHEN 3 THEN 'No charge'
                    WHEN 4 THEN 'Dispute'
                    WHEN 5 THEN 'Unknown'
                    WHEN 6 THEN 'Voided trip'
                END AS payment_code
        FROM (
            SELECT DISTINCT payment_type
            FROM bigdata-405714.trips_data_all.trips_data_cleaned
        )
    )
);

-- Create dim_datetime
CREATE OR REPLACE TABLE bigdata-405714.production_bq_test.dim_datetime
AS (
    SELECT 
            datetime_id,
            pickup_datetime,
            EXTRACT(YEAR FROM pickup_datetime) AS pickup_year,
            EXTRACT(MONTH FROM pickup_datetime) AS pickup_month,
            EXTRACT(DAY FROM pickup_datetime) AS pickup_day,
            EXTRACT(DAYOFWEEK FROM pickup_datetime) AS pickup_weekday,
            dropoff_datetime,
            EXTRACT(YEAR FROM dropoff_datetime) AS dropoff_year,
            EXTRACT(MONTH FROM dropoff_datetime) AS dropoff_month,
            EXTRACT(DAY FROM dropoff_datetime) AS dropoff_day,
            EXTRACT(DAYOFWEEK FROM dropoff_datetime) AS dropoff_weekday
    FROM (
        SELECT pickup_datetime,
                dropoff_datetime,
                ROW_NUMBER() OVER (ORDER BY pickup_datetime) AS datetime_id,
        FROM (
            SELECT DISTINCT pickup_datetime,
                            dropoff_datetime
            FROM bigdata-405714.trips_data_all.trips_data_cleaned
        )
    )
);