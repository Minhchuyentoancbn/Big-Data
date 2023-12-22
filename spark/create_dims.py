import pyspark
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

PROJECT_ID = 'bigdata-405714'

parser = argparse.ArgumentParser()
parser.add_argument('--dataset', default='trips_data_all', type=str)
args = parser.parse_args()

class DataProcessor:
    def __init__(self, spark):
        self.spark = spark

    def load_data(self):
        input_table = f"{PROJECT_ID}.trips_data_all.trips_data_cleaned"
        df = self.spark.read.format('bigquery').option('table', input_table).load()
        return df

    def create_dim_passenger(self, combined_df):
        # Creating passenger table
        dim_passenger = combined_df.select('passenger_count'). \
                            dropDuplicates(). \
                            withColumn("passenger_id", F.monotonically_increasing_id()). \
                            select('passenger_id','passenger_count')
        return dim_passenger
        
    def create_dim_trip(self, combined_df):
        # Creating trip table
        dim_trip = combined_df.select('trip_distance', 'pickup_datetime', 'dropoff_datetime'). \
                        dropDuplicates(). \
                        withColumn("trip_distance_id", F.monotonically_increasing_id()). \
                        withColumn("trip_duration", F.round((F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60, 2))
                        # select('trip_distance_id', 'trip_distance', 'trip_duration')
        return dim_trip

    def create_dim_location(self, combined_df):
        # Creating location table
        dim_location = combined_df.select('PULocationID', 'DOLocationID'). \
                            dropDuplicates(). \
                            withColumn("location_id", F.monotonically_increasing_id())
                            # select('location_id', 'pickup_location', 'dropoff_location')
        return dim_location


    def create_dim_rate(self, combined_df):
        #Creating rate table
        rate_code_type = {1: "Standard rate", 2: "JFK", 3: "Newark", 4: "Nassau or Westchester", 5: "Negotiated fare", 6: "Group ride"}

        def map_rate_code(rate_code):
            return rate_code_type[rate_code]

        map_udf_rate = F.udf(map_rate_code, StringType())
        
        dim_rate=combined_df.select('RateCodeID'). \
                        dropDuplicates(). \
                        withColumn('rate_id', F.monotonically_increasing_id()). \
                        withColumn('rate_name', map_udf_rate(F.col('RateCodeID')))
        return dim_rate


    def create_dim_payment_type(self, combined_df):
        # Creating payment_type table
        payment_type = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided trip"}

        def map_payment_code(payment_code):
            return payment_type[payment_code]
        
        map_udf_payment = F.udf(map_payment_code, StringType())
        
        dim_payment_type=combined_df.select('payment_type').\
                                    dropDuplicates().\
                                    withColumn('payment_id', F.monotonically_increasing_id()).\
                                    withColumn('payment_code', map_udf_payment(F.col('payment_type')))
        return dim_payment_type


    def create_dim_datetime(self, combined_df):
        # Creating datetime table
        dim_datetime=combined_df.select('pickup_datetime','dropoff_datetime').\
                            dropDuplicates().\
                            select(
                                'pickup_datetime',
                                'dropoff_datetime',
                                F.year('pickup_datetime').alias('pickup_year'),
                                F.month('pickup_datetime').alias('pickup_month'),
                                F.dayofmonth('pickup_datetime').alias('pickup_day'),
                                F.dayofweek('pickup_datetime').alias('pickup_weekday'),
                                F.year('dropoff_datetime').alias('dropoff_year'),
                                F.month('dropoff_datetime').alias('dropoff_month'),
                                F.dayofmonth('dropoff_datetime').alias('dropoff_day'),
                                F.dayofweek('dropoff_datetime').alias('dropoff_weekday')).\
                            withColumn('datetime_id', F.monotonically_increasing_id()).\
                            select('datetime_id','pickup_datetime','pickup_year','pickup_month',
                                    'pickup_day','pickup_weekday','dropoff_datetime','dropoff_year',
                                    'dropoff_month','dropoff_day','dropoff_weekday')
        return dim_datetime



    def create_fact_table(self, combined_df, dim_passenger, dim_trip, dim_location, dim_rate, dim_payment_type, dim_datetime):
        # Creating the fact table using Spark SQL joins
        fact_table = combined_df.join(dim_passenger, on='passenger_count', how='left')\
            .join(dim_trip, on=['trip_distance', 'pickup_datetime', 'dropoff_datetime'], how='left')\
            .join(dim_location, on=['PULocationID', 'DOLocationID'], how='left')\
            .join(dim_rate, on='RatecodeID', how='left')\
            .join(dim_payment_type, on='payment_type', how='left')\
            .join(dim_datetime, on=['pickup_datetime', 'dropoff_datetime'], how='left')\
            .select('VendorID', 'datetime_id', 'passenger_id', 'trip_distance_id', 'rate_id', 'store_and_fwd_flag',
                    'location_id', 'payment_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge')
        return fact_table


spark = SparkSession.builder.appName('clean_data').getOrCreate()
spark.conf.set('temporaryGcsBucket', 'dataproc-temp-asia-east2-285145462114-ku4fpzno')
data_processor = DataProcessor(spark)

combined_df = data_processor.load_data()

# Create and process dimension tables
dim_passenger = data_processor.create_dim_passenger(combined_df)
dim_trip = data_processor.create_dim_trip(combined_df)
dim_location = data_processor.create_dim_location(combined_df)
dim_rate = data_processor.create_dim_rate(combined_df)
dim_payment_type = data_processor.create_dim_payment_type(combined_df)
dim_datetime = data_processor.create_dim_datetime(combined_df)

# Upload data to GCS
dataframe_dict = {
    'dim_passenger': dim_passenger, 'dim_location': dim_location,
    'dim_trip': dim_trip, 'dim_Rate': dim_rate, 'dim_payment_type': dim_payment_type,
    'dim_datetime': dim_datetime
}

for name, df in dataframe_dict.items():
    df.write.format('bigquery').option('table', f'{PROJECT_ID}.{args.dataset}.{name}').save()
    print(f"Uploaded {name} dataframe to BigQuery.")