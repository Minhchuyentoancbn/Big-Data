import pyspark.sql.types as T

INPUT_DATA_PATH = './resources/rides_big.csv'
BOOTSTRAP_SERVERS = ['35.220.200.137:9092', '35.220.200.137:9093',]

TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed'

PRODUCE_TOPIC_RIDES_CSV = CONSUME_TOPIC_RIDES_CSV = 'rides_csv_3'

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