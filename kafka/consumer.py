from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import pyspark.sql.types as T
import os

PROJECT_ID = 'bigdata-405714'
CONSUME_TOPIC_RIDES_CSV = 'rides'
KAFKA_ADDRESS= "35.220.200.137"
# KAFKA_BOOTSTRAP_SERVERS = f'{KAFKA_ADDRESS}:9092,{KAFKA_ADDRESS}:9093'
KAFKA_BOOTSTRAP_SERVERS = f'{KAFKA_ADDRESS}:9092'

GCP_GCS_BUCKET = "dtc_data_lake_bigdata-405714"
GCS_STORAGE_PATH = 'gs://' + GCP_GCS_BUCKET + '/realtime2'
CHECKPOINT_PATH = 'gs://' + GCP_GCS_BUCKET + '/realtime2/checkpoint/'
CHECKPOINT_PATH_BQ = 'gs://' + GCP_GCS_BUCKET + '/realtime2/checkpoint_bq/'

RIDE_SCHEMA = T.StructType(
    [
        T.StructField("vendor_id", T.IntegerType()),
        T.StructField('tpep_pickup_datetime', T.TimestampType()),
        T.StructField('tpep_dropoff_datetime', T.TimestampType()),
        T.StructField("passenger_count", T.IntegerType()),
        T.StructField("trip_distance", T.FloatType()),
        T.StructField("RatecodeID", T.IntegerType()),
        T.StructField("store_and_fwd_flag", T.StringType()),
        T.StructField("PULocationID", T.IntegerType()),
        T.StructField("DOLocationID", T.IntegerType()),
        T.StructField("payment_type", T.IntegerType()),
        T.StructField("fare_amount", T.FloatType()),
        T.StructField("extra", T.FloatType()),
        T.StructField("mta_tax", T.FloatType()),
        T.StructField("tip_amount", T.FloatType()),
        T.StructField("tolls_amount", T.FloatType()),
        T.StructField("improvement_surcharge", T.FloatType()),
        T.StructField("total_amount", T.FloatType()),
        T.StructField("congestion_surcharge", T.FloatType()),
    ]
)


def read_from_kafka(consume_topic: str):
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "latest") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .option("failOnDataLoss", "false") \
        .load()
    # .option("startingOffsets", "earliest") \
    return df_stream


def parse_ride_from_kafka_message(df, schema):
    """ take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema """
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # split attributes to nested array in one Column
    col = F.split(df['value'], ', ')

    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


def create_file_write_stream(stream, storage_path, checkpoint_path='/checkpoint', trigger="5 seconds", output_mode="append", file_format="parquet"):
    write_stream = (stream
                    .writeStream
                    .format(file_format)
                    .partitionBy("PULocationID")
                    .option("path", storage_path)
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=trigger)
                    .outputMode(output_mode))

    return write_stream


def create_file_write_stream_bq(stream,  checkpoint_path='/checkpoint', trigger="5 seconds", output_mode="append"):
    write_stream = (stream
                    .writeStream
                    .format("bigquery")
                    .option("table", f"{PROJECT_ID}.realtime.rides")
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=trigger)
                    .outputMode(output_mode))

    return write_stream


if __name__ == "__main__":
    os.environ['KAFKA_ADDRESS'] = KAFKA_ADDRESS
    os.environ['GCP_GCS_BUCKET'] = 'dtc_data_lake_bigdata-405714'
    spark = SparkSession.builder.appName('streaming-examples').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.streams.resetTerminated()

    # read_streaming data
    df_consume_stream = read_from_kafka(consume_topic=CONSUME_TOPIC_RIDES_CSV)
    print(df_consume_stream.printSchema())

    # parse streaming data
    df_rides = parse_ride_from_kafka_message(df_consume_stream, RIDE_SCHEMA)
    print(df_rides.printSchema())

    # Write to GCS
    write_stream = create_file_write_stream(df_rides, GCS_STORAGE_PATH, checkpoint_path=CHECKPOINT_PATH)
    write_bq = create_file_write_stream_bq(df_rides, checkpoint_path=CHECKPOINT_PATH_BQ)

    write_stream.start()
    write_bq.start()

    spark.streams.awaitAnyTermination()