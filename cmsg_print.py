#!/bin/usr/env python3
# encode:utf8

import json
from kafka import KafkaConsumer, TopicPartition
import sys

if len(sys.argv) != 2:
    print(f'Usage: python3 {sys.argv[0]} <topic>')
    print(f'Example: python3 {sys.argv[0]} covid-msg')
    exit(-1)

TOPIC = sys.argv[1]

def print_last_messages(topic_name, cnt=1000000):
    global current_rec_cnt

    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        enable_auto_commit=True,
        consumer_timeout_ms=100,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    tp = TopicPartition(topic_name, 0)
    consumer.assign([tp])

    current_rec_cnt = consumer.position(tp)

    if current_rec_cnt > 0:
        consumer.seek(tp, max(0, consumer.position(tp) - cnt))
        for msg in consumer:
            print(msg.value)


if __name__ == '__main__':
    print_last_messages(TOPIC)


# eof
