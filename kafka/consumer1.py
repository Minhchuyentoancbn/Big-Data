import argparse

from json import loads
from typing import Dict, List
from kafka import KafkaConsumer
from ride import Ride

CONSUME_TOPIC = 'rides_json'

BOOTSTRAP_SERVERS = ['35.220.200.137:9092', ]

class RideJSONConsumer:
    def __init__(self, props: Dict):
        self.consumer = KafkaConsumer(**props)

    def consume_from_kafka(self, topics: List[str]):
        self.consumer.subscribe(topics=topics)
        print('Consuming from Kafka started')
        print('Available topics to consume: ', self.consumer.subscription())
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = self.consumer.poll(1.0)
                if msg is None or msg == {}:
                    continue
                for msg_key, msg_values in msg.items():
                    for msg_val in msg_values:
                        print(f'Key:{msg_val.key}-type({type(msg_val.key)}), '
                              f'Value:{msg_val.value}-type({type(msg_val.value)})')
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Kafka Consumer')
    parser.add_argument('--topic', type=str, default=CONSUME_TOPIC)
    args = parser.parse_args()

    topic = args.topic
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'key_deserializer': lambda key: key.decode('utf-8'),
        'value_deserializer': lambda x: loads(x.decode('utf-8'), object_hook=lambda d: Ride.from_dict(d)),
        'group_id': 'consumer.group.id.csv-example.1',
    }
    csv_consumer = RideJSONConsumer(props=config)
    csv_consumer.consume_from_kafka(topics=[topic])


# class RideCSVConsumer:
#     def __init__(self, props: Dict):
#         self.consumer = KafkaConsumer(**props)

#     def consume_from_kafka(self, topics: List[str]):
#         self.consumer.subscribe(topics=topics)
#         print('Consuming from Kafka started')
#         print('Available topics to consume: ', self.consumer.subscription())
#         while True:
#             try:
#                 # SIGINT can't be handled when polling, limit timeout to 1 second.
#                 msg = self.consumer.poll(1.0)
#                 if msg is None or msg == {}:
#                     continue
#                 for msg_key, msg_values in msg.items():
#                     for msg_val in msg_values:
#                         print(f'Key:{msg_val.key}-type({type(msg_val.key)}), '
#                               f'Value:{msg_val.value}-type({type(msg_val.value)})')
#             except KeyboardInterrupt:
#                 break

#         self.consumer.close()