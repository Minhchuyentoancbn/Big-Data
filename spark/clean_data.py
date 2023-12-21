#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ID = 'bigdata-405714'

COMMON_COLUMNS = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]


def parse_args():
    parser = argparse.ArgumentParser()
    # parser.add_argument('--process_all', default=0, type=int)
    parser.add_argument('--input_table', default='yellow_trips_data', type=str)
    args = parser.parse_args()
    return args


def process_table(input_table, output, color):
    # Read data from BigQuery
    df = spark.read.format('bigquery').option('table', input_table).load()

    # Rename columns
    if color == 'green':
        df = df.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
    elif color == 'yellow':
        df = df.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime').withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

    # Select common columns
    df = df.select(COMMON_COLUMNS).withColumn('service_type', F.lit(color))

    # Filter data
    df = df.dropna(). \
            filter(df.dropoff_datetime > df.pickup_datetime). \
            filter(df.passenger_count > 0). \
            filter(df.fare_amount >= 0). \
            filter(df.total_amount >= 0). \
            filter(df.trip_distance >= 0). \
            filter(df.payment_type.between(1, 6)). \
            filter(df.VendorID.isin([1, 2])). \
            filter(df.PULocationID.between(1, 265)). \
            filter(df.DOLocationID.between(1, 265)). \
            filter(df.RatecodeID.between(1, 6)). \
            filter(df.extra >= 0). \
            filter(df.mta_tax >= 0). \
            filter(df.tip_amount >= 0). \
            filter(df.tolls_amount >= 0). \
            filter(df.improvement_surcharge >= 0). \
            filter(df.congestion_surcharge >= 0). \
            dropDuplicates()

    df.write.format('bigquery').option('table', output).save()
    return df


def clean_data(args):
    if not args.input_table.startswith('all'):
        # Check color
        if args.input_table.startswith('green'):
            color = 'green'
        elif args.input_table.startswith('yellow'):
            color = 'yellow'
        else:
            raise NotImplementedError(f'Unknown color for table {args.input_table}')

        input_table = f"{PROJECT_ID}.trips_data_all.{args.input_table}"
        output = f"{PROJECT_ID}.trips_data_all.{args.input_table}_cleaned"
        process_table(input_table, output, color)
    else:
        # Process all tables
        input_tables = [f'{PROJECT_ID}.trips_data_all.green_trips_data',
                        f'{PROJECT_ID}.trips_data_all.yellow_trips_data']
        output_tables = [f'{PROJECT_ID}.trips_data_all.green_trips_data_cleaned',
                         f'{PROJECT_ID}.trips_data_all.yellow_trips_data_cleaned']
        colors = ['green', 'yellow']
        outputs_df = []
        for input_table, output_table, color in zip(input_tables, output_tables, colors):
            outputs_df.append(
                process_table(input_table, output_table, color)
            )
        df_result = outputs_df[0].unionAll(outputs_df[1])
        df_result.write.format('bigquery').option('table', f'{PROJECT_ID}.trips_data_all.trips_data_cleaned').save()

    
args = parse_args()

spark = SparkSession.builder.appName('clean_data').getOrCreate()
spark.conf.set('temporaryGcsBucket', 'dataproc-temp-asia-east2-285145462114-ku4fpzno')

clean_data(args)