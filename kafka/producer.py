import csv
import sys
import argparse
from time import sleep
from typing import Dict
from kafka import KafkaProducer

from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH, PRODUCE_TOPIC_RIDES_CSV


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
        records, ride_keys = [], []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                # vendor_id, passenger_count, trip_distance, payment_type, total_amount
                records.append(f'{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}, {row[7]}, {row[8]}, {row[9]}, {row[10]}, {row[11]}, {row[12]}, {row[13]}, {row[14]}, {row[15]}, {row[16]}, {row[17]}')
                ride_keys.append(str(row[1]) + '-' + str(row[7]))
        return zip(ride_keys, records)

    def publish(self, topic: str, records: [str, str], sleep_time: float = 0.5):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
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
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8'),
        'acks': 'all',
    }
    producer = RideCSVProducer(props=config)
    ride_records = producer.read_records(resource_path=INPUT_DATA_PATH)
    print(ride_records)
    producer.publish(topic=PRODUCE_TOPIC_RIDES_CSV, records=ride_records, sleep_time=args.time)