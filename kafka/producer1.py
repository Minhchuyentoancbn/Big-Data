import csv
import json
import sys
import argparse
from time import sleep
from typing import Dict
from ride import Ride
from kafka import KafkaProducer

PRODUCE_TOPIC_RIDES_CSV = 'rides_csv_4'
PRODUCE_TOPIC_RIDES_JSON = 'rides_json'

INPUT_DATA_PATH = './resources/rides1.csv'
BOOTSTRAP_SERVERS = ['35.220.200.137:9092', ]

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class RideCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        # self.producer = Producer(producer_props)

    @staticmethod
    def read_records(resource_path: str):
        records, records_csv, ride_keys = [], [], []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            i = 0
            for row in reader:
                # vendor_id, passenger_count, trip_distance, payment_type, total_amount
                records_csv.append(f'{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}, {row[7]}, {row[8]}, {row[9]}, {row[10]}, {row[11]}, {row[12]}, {row[13]}, {row[14]}, {row[15]}, {row[16]}, {row[17]}')
                records.append(Ride(arr=row))
                ride_keys.append(str(row[1]) + '-' + str(row[7]))

                i += 1
                if i == 1000:
                    break
        return zip(ride_keys, records, records_csv)

    def publish(self, records: [str, str], sleep_time: float = 0.5):
        for key_value in records:
            key, value, value_csv = key_value
            try:
                record = self.producer.send(topic=PRODUCE_TOPIC_RIDES_JSON, key=key, value=value)
                record_csv = self.producer.send(topic=PRODUCE_TOPIC_RIDES_CSV, key=key, value=value_csv)
                # print('Record {} successfully produced at offset {}'.format(key, record.get().offset))
                # print(f"Producing record for <key: {key}, value:{value}>")
                print(f"Record {key} successfully produced at offset {record.get().offset} and {record_csv.get().offset}")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}") 

            sleep(sleep_time)

        self.producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--time', type=float, default=0.5, help='time interval between each message')
    args = parser.parse_args(sys.argv[1:])

    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'key_serializer': lambda x: str(x).encode('utf-8'),
        # 'value_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: json.dumps(x.__dict__, default=str).encode('utf-8'),
        # 'acks': 'all',
    }
    producer = RideCSVProducer(props=config)
    ride_records = producer.read_records(resource_path=INPUT_DATA_PATH)
    # print(ride_records)
    print(f"Producing records to topic: {PRODUCE_TOPIC_RIDES_CSV} and {PRODUCE_TOPIC_RIDES_JSON}")
    producer.publish(records=ride_records, sleep_time=args.time)