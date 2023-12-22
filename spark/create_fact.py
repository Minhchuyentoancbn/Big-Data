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
    
    def read_dim_table(self):
        dim_dict = {}
        dim_passenger = self.spark.read.format('bigquery').option('table', f'{PROJECT_ID}.{args.dataset}.dim_passenger').load()
        dim_trip = self.spark.read.format('bigquery').option('table', f'{PROJECT_ID}.{args.dataset}.dim_trip').load()
        dim_location = self.spark.read.format('bigquery').option('table', f'{PROJECT_ID}.{args.dataset}.dim_location').load()
        dim_rate = self.spark.read.format('bigquery').option('table', f'{PROJECT_ID}.{args.dataset}.dim_Rate').load()
        dim_payment_type = self.spark.read.format('bigquery').option('table', f'{PROJECT_ID}.{args.dataset}.dim_payment_type').load()
        dim_datetime = self.spark.read.format('bigquery').option('table', f'{PROJECT_ID}.{args.dataset}.dim_datetime').load()

        dim_dict['dim_passenger'] = dim_passenger
        dim_dict['dim_trip'] = dim_trip
        dim_dict['dim_location'] = dim_location
        dim_dict['dim_Rate'] = dim_rate
        dim_dict['dim_payment_type'] = dim_payment_type
        dim_dict['dim_datetime'] = dim_datetime

        return dim_dict

    def create_fact_table(self, combined_df, dim_passenger, dim_trip, dim_location, dim_rate, dim_payment_type, dim_datetime):
        # Creating the fact table using Spark SQL joins
        fact_table = combined_df.join(dim_passenger, on='passenger_count', how='left')\
            .join(dim_trip, on=['trip_distance', 'pickup_datetime', 'dropoff_datetime'], how='left')\
            .join(dim_location, on=['PULocationID', 'DOLocationID'], how='left')\
            .join(dim_rate, on='RatecodeID', how='left')\
            .join(dim_payment_type, on='payment_type', how='left')\
            .join(dim_datetime, on=['pickup_datetime', 'dropoff_datetime'], how='left')\
            .select('VendorID', 'datetime_id', 'passenger_id', 'trip_distance_id', 'rate_id', 'store_and_fwd_flag',
                    'location_id', 'payment_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 
                    'improvement_surcharge', 'service_type')
        # Explain the query plan
        # fact_table.explain(extended=False)
        return fact_table
    

    def create_fact_table_broadcast(self, combined_df, dim_passenger, dim_trip, dim_location, dim_rate, dim_payment_type, dim_datetime):
        # Creating the fact table using Spark SQL broadcast joins
        # self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 

        fact_table = combined_df\
            .join(F.broadcast(dim_passenger), on='passenger_count', how='left')\
            .join(F.broadcast(dim_location), on=['PULocationID', 'DOLocationID'], how='left')\
            .join(F.broadcast(dim_rate), on='RatecodeID', how='left')\
            .join(F.broadcast(dim_payment_type), on='payment_type', how='left')\
            .join(F.broadcast(dim_trip), on=['trip_distance', 'pickup_datetime', 'dropoff_datetime'], how='left')\
            .join(F.broadcast(dim_datetime), on=['pickup_datetime', 'dropoff_datetime'], how='left')\
            .select('VendorID', 'datetime_id', 'passenger_id', 'trip_distance_id', 'rate_id', 'store_and_fwd_flag',
                    'location_id', 'payment_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge')
        # Explain the query plan
        # fact_table.explain(extended=False)
        return fact_table
    

# class Utils:
#     @staticmethod
#     def cache_and_broadcast_tables(spark, dim_tables):
#         # Cache smaller dimension tables
#         for dim_name, dim_df in dim_tables.items():
#             dim_df.cache()
#             print(f"Cached {dim_name} dataframe.")

#         # Broadcast smaller tables
#         spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # Disable auto-broadcast joins
#         for dim_name, dim_df in dim_tables.items():
#             dim_df.createOrReplaceTempView(dim_name)
#             print(f"Broadcasting {dim_name} dataframe.")

#     @staticmethod
#     def uncache_tables(dim_tables):
#         # Uncache previously cached dimension tables
#         for dim_df in dim_tables.values():
#             dim_df.unpersist()


spark = SparkSession.builder.appName('clean_data').getOrCreate()
spark.conf.set('temporaryGcsBucket', 'dataproc-temp-asia-east2-285145462114-ku4fpzno')
data_processor = DataProcessor(spark)

combined_df = data_processor.load_data()

# Create and process dimension tables
dim_tables = data_processor.read_dim_table()
dim_passenger = dim_tables['dim_passenger']
dim_trip = dim_tables['dim_trip']
dim_location = dim_tables['dim_location']
dim_rate = dim_tables['dim_Rate']
dim_payment_type = dim_tables['dim_payment_type']
dim_datetime = dim_tables['dim_datetime']


# # # Cache and broadcast tables
# Utils.cache_and_broadcast_tables(spark, dim_tables)

# Create the fact table using Spark SQL joins
fact_table = data_processor.create_fact_table(combined_df, dim_passenger, dim_trip, dim_location, dim_rate, dim_payment_type, dim_datetime)
# fact_table = data_processor.create_fact_table_broadcast(combined_df, dim_passenger, dim_trip, dim_location, dim_rate, dim_payment_type, dim_datetime)

# Upload data to BigQuery
fact_table.write.format('bigquery').option('table', f'{PROJECT_ID}.{args.dataset}.fact_table').save()