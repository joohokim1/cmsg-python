#!/bin/usr/env python3
# encode:utf8

import requests
import re
import json
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

SRC_TOPIC_NAME = 'cmsg-safekorea'
DEST_TOPIC_NAME = 'cmsg-transformed'

last_seq = -1
site_msg_total = -1
site_msg_left = -1
process_limit = 100
location_code = {}  # send_location -> {location_id, si_do, si_gun_gu}
record_stack = []


def get_last_seq(topic_name):
    global last_seq

    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        enable_auto_commit=True,
        consumer_timeout_ms=100,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    tp = TopicPartition(topic_name, 0)
    consumer.assign([tp])
    print('last_pos in {}: {}'.format(topic_name, consumer.position(tp)))

    pos = consumer.position(tp)

    if pos > 0:
        consumer.seek(tp, consumer.position(tp) - 1)
        for msg in consumer:
            last_seq = max(last_seq, int(msg.value['md102_sn']) - 10)

    return last_seq


def get_site_msg_total():
    global site_msg_total
    global site_msg_left

    r = requests.get('http://mepv2.safekorea.go.kr/disasterBreaking/showList2.do?rows=1&page=1&nowPage=&locationCode=')
    site_msg_left = site_msg_total = r.json()['total']
    print('site_msg_total in mepv2.safekorea.go.kr: {}'.format(site_msg_total))


def load_location_code():
    with open('location_code.json', encoding='utf-8-sig') as f:
        for line in f:
            rec = json.loads(line)
            location_code[rec['full_loc']] = rec


def fetch_and_stack():
    global site_msg_left

    fetch_cnt = 100
    page_start = 1

    while True:
        if site_msg_left == 0 or site_msg_total - site_msg_left >= process_limit:
            break

        r = requests.get(
            'http://mepv2.safekorea.go.kr/disasterBreaking/showList2.do?rows={}&page={}'.format(fetch_cnt, page_start))

        seq = -1
        for row in r.json()['rows']:
            seq = int(row['rnum'])

            if (seq <= last_seq):
                break

            r2 = requests.get('http://mepv2.safekorea.go.kr/disaster/showBreakingDetail.do?seq={}'.format(seq))
            m = re.search('<h1>.*송출지역.*</h1>\s*<h2>(.*)</h2>', r2.text)
            send_location = m.group(1)

            # delete rows where contains(location_id, ',')
            if send_location.index(',') >= 0:
                continue

            location_record = location_code[send_location]
            print(row['createDate'], location_record['location_id'], location_record['si_do'],
                  location_record['si_gun_gu'], 'dong', seq, row['msg'], 'tracing', 'confirmed')

            d = dict()
            d['create_time'] = row['createDate']
            d['location_id'] = location_record['location_id']
            d['si_do'] = location_record['si_do']
            d['si_gun_gu'] = location_record['si_gun_gu']
            d['dong'] = []
            d['md102_sn'] = seq
            d['msg'] = row['msg']
            d['tracing'] = '0'
            d['confirmed'] = 1

            record_stack.append(d)
            site_msg_left -= 1

        if (seq <= last_seq):
            break


def put_records_into_kafka():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'))

    while True:
        if len(record_stack) == 0:
            break
        producer.send(TOPIC_NAME, value=record_stack.pop())

    producer.flush()


def print_all_in_topic():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=100,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    for message in consumer:
        print(message)


if __name__ == '__main__':
    print('last_md102_sn: {}'.format(get_last_seq(TOPIC_NAME)))
    get_site_msg_total()
    load_location_code()
    fetch_and_stack()
    put_records_into_kafka()
    print_all_in_topic()

# eof
