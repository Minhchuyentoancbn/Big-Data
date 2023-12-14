import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

    green_schema = types.StructType([
        types.StructField("VendorID", types.IntegerType(), True),
        types.StructField("lpep_pickup_datetime", types.TimestampType(), True),
        types.StructField("lpep_dropoff_datetime", types.TimestampType(), True),
        types.StructField("store_and_fwd_flag", types.StringType(), True),
        types.StructField("RatecodeID", types.IntegerType(), True),
        types.StructField("PULocationID", types.IntegerType(), True),
        types.StructField("DOLocationID", types.IntegerType(), True),
        types.StructField("passenger_count", types.IntegerType(), True),
        types.StructField("trip_distance", types.DoubleType(), True),
        types.StructField("fare_amount", types.DoubleType(), True),
        types.StructField("extra", types.DoubleType(), True),
        types.StructField("mta_tax", types.DoubleType(), True),
        types.StructField("tip_amount", types.DoubleType(), True),
        types.StructField("tolls_amount", types.DoubleType(), True),
        types.StructField("ehail_fee", types.DoubleType(), True),
        types.StructField("improvement_surcharge", types.DoubleType(), True),
        types.StructField("total_amount", types.DoubleType(), True),
        types.StructField("payment_type", types.IntegerType(), True),
        types.StructField("trip_type", types.IntegerType(), True),
        types.StructField("congestion_surcharge", types.DoubleType(), True)
    ])

    yellow_schema = types.StructType([
        types.StructField("VendorID", types.IntegerType(), True),
        types.StructField("tpep_pickup_datetime", types.TimestampType(), True),
        types.StructField("tpep_dropoff_datetime", types.TimestampType(), True),
        types.StructField("passenger_count", types.IntegerType(), True),
        types.StructField("trip_distance", types.DoubleType(), True),
        types.StructField("RatecodeID", types.IntegerType(), True),
        types.StructField("store_and_fwd_flag", types.StringType(), True),
        types.StructField("PULocationID", types.IntegerType(), True),
        types.StructField("DOLocationID", types.IntegerType(), True),
        types.StructField("payment_type", types.IntegerType(), True),
        types.StructField("fare_amount", types.DoubleType(), True),
        types.StructField("extra", types.DoubleType(), True),
        types.StructField("mta_tax", types.DoubleType(), True),
        types.StructField("tip_amount", types.DoubleType(), True),
        types.StructField("tolls_amount", types.DoubleType(), True),
        types.StructField("improvement_surcharge", types.DoubleType(), True),
        types.StructField("total_amount", types.DoubleType(), True),
        types.StructField("congestion_surcharge", types.DoubleType(), True)
    ])


    year = 2020

    for month in range(1, 13):
        print(f'processing data for {year}/{month}')

        input_path = f'data/raw/green/{year}/{month:02d}/'
        output_path = f'data/pq/green/{year}/{month:02d}/'

        try:
            df_green = spark.read.option("header", "true").schema(green_schema).csv(input_path)
            df_green.repartition(4).write.parquet(output_path)
        except:
            print(f'no data for {year}/{month}')
            break
        

    year = 2021 

    for month in range(1, 13):
        print(f'processing data for {year}/{month}')

        input_path = f'data/raw/green/{year}/{month:02d}/'
        output_path = f'data/pq/green/{year}/{month:02d}/'

        try:
            df_green = spark.read.option("header", "true").schema(green_schema).csv(input_path)
            df_green.repartition(4).write.parquet(output_path)
        except:
            print(f'no data for {year}/{month}')
            break

    year = 2020
    for month in range(1, 13):
        print(f'processing data for {year}/{month}')

        input_path = f'data/raw/yellow/{year}/{month:02d}/'
        output_path = f'data/pq/yellow/{year}/{month:02d}/'

        try:
            df_yellow = spark.read.option("header", "true").schema(yellow_schema).csv(input_path)
            df_yellow.repartition(4).write.parquet(output_path)
        except:
            print(f'no data for {year}/{month}')
            break

    year = 2021
    for month in range(1, 13):
        print(f'processing data for {year}/{month}')

        input_path = f'data/raw/yellow/{year}/{month:02d}/'
        output_path = f'data/pq/yellow/{year}/{month:02d}/'

        try:
            df_yellow = spark.read.option("header", "true").schema(yellow_schema).csv(input_path)
            df_yellow.repartition(4).write.parquet(output_path)
        except:
            print(f'no data for {year}/{month}')
            break