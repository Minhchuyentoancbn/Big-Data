
'''1. Total Trips Per Year:'''
SELECT *
FROM bigdata-405714.production.fact_table AS fact_table
     JOIN bigdata-405714.production.dim_datetime AS dim_date
     ON fact_table.datetime_id = dim_date.datetime_id;

'''2. Average Passenger Count:'''
SELECT *
FROM bigdata-405714.production.fact_table AS fact_table
     JOIN bigdata-405714.production.dim_passenger AS dim_passenger
     ON fact_table.passenger_id = dim_passenger.passenger_id
     JOIN bigdata-405714.production.dim_datetime AS dim_date
     ON fact_table.datetime_id = dim_date.datetime_id;


'''3. Distance'''
SELECT *
FROM bigdata-405714.production.fact_table AS fact_table
     JOIN bigdata-405714.production.dim_trip AS dim_trip
     ON fact_table.trip_distance_id = dim_trip.trip_distance_id
     JOIN bigdata-405714.production.dim_datetime AS dim_date
     ON fact_table.datetime_id = dim_date.datetime_id;
     


'''4. Popular Pickup and Dropoff Locations:'''
SELECT *
FROM bigdata-405714.production.fact_table AS fact_table
        JOIN bigdata-405714.production.dim_location AS dim_location
        ON fact_table.location_id = dim_location.location_id
        JOIN bigdata-405714.production.zones AS zones
        ON dim_location.PULocationID = zones.LocationID
        JOIN bigdata-405714.production.dim_datetime AS dim_date
         ON fact_table.datetime_id = dim_date.datetime_id;

SELECT dropoff_location, COUNT(*) AS dropoff_count
FROM `nyc_yellow_taxi.data_warehouse.fact_table`
GROUP BY dropoff_location
ORDER BY dropoff_count DESC
LIMIT 10;

'''4. Average Fare Amount by Payment Type:'''

SELECT payment_type, AVG(fare_amount) AS avg_fare_amount
FROM `nyc_yellow_taxi.data_warehouse.fact_table`
GROUP BY payment_type
ORDER BY avg_fare_amount DESC;

'''5. Busiest Days of the Week for Trips:'''

SELECT pickup_weekday, COUNT(*) AS total_trips
FROM `nyc_yellow_taxi.data_warehouse.fact_table`
GROUP BY pickup_weekday
ORDER BY pickup_weekday;

'''6. Average Tip Amount by Rate Code:'''

SELECT rate_name, AVG(tip_amount) AS avg_tip_amount
FROM `nyc_yellow_taxi.data_warehouse.fact_table`
JOIN `nyc_yellow_taxi.data_warehouse.dim_rate` ON fact_table.rate_id = dim_rate.rate_id
GROUP BY rate_name
ORDER BY avg_tip_amount DESC;