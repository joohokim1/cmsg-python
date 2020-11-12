#!/bin/usr/env python3
# encode:utf8

import re
import json
import time
import datetime
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import sys

if len(sys.argv) != 4:
    print(f'Usage: python3 {sys.argv[0]} <src_topic> <target_topic> <prod>')
    print(f'Example: python3 {sys.argv[0]} cmsg-safekorea covid-msg True')
    exit(-1)

SRC_TOPIC_NAME, DEST_TOPIC_NAME = sys.argv[1:3]
PROD = bool(sys.argv[3])

location_code = {}  # send_location -> {location_id, si_do, si_gun_gu}


def get_last_seq(topic_name):
    last_seq = -1

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
            last_seq = max(last_seq, int(msg.value['md102_sn']))

    return last_seq


def load_location_code():
    with open('location_code.json', encoding='utf-8-sig') as f:
        for line in f:
            rec = json.loads(line)
            location_code[rec['full_loc']] = rec


def transform(src_topic_name, dest_topic_name, dest_last_seq, skip_cnt=0):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'))

    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        enable_auto_commit=True,
        consumer_timeout_ms=100,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    tp = TopicPartition(src_topic_name, 0)
    consumer.assign([tp])
    src_msg_cnt = consumer.position(tp)
    print(f'transform(): src_msg_cnt={src_msg_cnt}')

    src_msg_no = 0
    create_rec_cnt = 0
    consumer.seek_to_beginning(tp)
    for cmsg in consumer:
        row = cmsg.value
        if row['md102_sn'] <= dest_last_seq:
            continue

        print(f'src[{src_msg_no}]: {row}')
        src_msg_no += 1

        # delete rows where contains(location_id, ',')
        # delete rows where contains(location_id, 'br')
        loc = row['send_location'].strip()
        if ',' in loc or 'br' in loc or loc == '' or loc == '전국':
            continue

        msg = row['msg']

        # delete rows where contains(msg, '거리두기') || contains(msg, '다중이용시설') || contains(msg, '생계지원') ...
        if '거리두기' in msg or '다중이용시설' in msg or '생계지원' in msg or '독감' in msg or '보이스피싱' in msg or \
            '선별진료소' in msg or '중대본' in msg or '방역수칙' in msg or '집회금지' in msg or '집합금지' in msg or \
            '긴급생계' in msg or '집합제한시설' in msg or '의무화' in msg or '생계기준' in msg or '방역완료' in msg or \
            '소상공인' in msg or '저금리' in msg or '멧돼지' in msg or '종교활동' in msg or '타지역' in msg or '전국' in msg:
            continue

        # extract /확진자 \d+명.*발생/ 1 time from msg
        m = re.search('확진자\s*\d+명.*발생', msg)
        c1 = m.group(0) if m else None

        # extract /\d+명.*확진/ 1 time from msg
        m = re.search('\d+명.*확진', msg)
        c2 = m.group(0) if m else None

        # extract /\d+,\d+,\d+번.*발생/ 1 time from msg
        m = re.search('\d+,\d+,\d+번.*발생', msg)
        c3 = m.group(0) if m else None

        # extract /\d+,\d+번.*발생/ 1 time from msg
        m = re.search('\d+,\d+번.*발생', msg)
        c4 = m.group(0) if m else None

        # extract /\d+~\d+번.*발생/ 1 time from msg
        m = re.search('\d+~\d+번.*발생', msg)
        c5 = m.group(0) if m else None

        # extract /\d+~\d+번.*발생/ 1 time from msg
        m = re.search('\d+번~\d+번.*발생', msg)
        c6 = m.group(0) if m else None

        # extract /\d+번.*발생/ 1 time from msg
        m = re.search('\d+번.*발생', msg)
        c7 = m.group(0) if m else None

        # extract /\d+번.*동선/ 1 time from msg
        m = re.search('\d+번.*동선', msg)
        t1 = m.group(0) if m else None

        # extract /\d+번.*방문/ 1 time from msg
        m = re.search('\d+번.*방문', msg)
        t2 = m.group(0) if m else None

        # extract /동선안내/ 1 time from msg
        m = re.search('동선\s*안내', msg)
        t3 = m.group(0) if m else None

        # extract /방문자는/ 1 time from msg
        m = re.search('방문자는', msg)
        t4 = m.group(0) if m else None

        # extract /방문하신/ 1 time from msg
        m = re.search('방문하신', msg)
        t5 = m.group(0) if m else None

        # extract /확진자\s*동선/ 1 time from msg
        m = re.search('확진자\s*동선', msg)
        t6 = m.group(0) if m else None

        if c1 is None and c2 is None and c3 is None and c4 is None and c5 is None and c6 is None and c7 is None and \
            t1 is None and t2 is None and t3 is None and t4 is None and t5 is None and t6 is None:
            continue

        confirmed = 0
        tracing = '0'

        # replace /확진자 (\d+)명/ from extract_msg1 with '$1'
        if c1:
            confirmed = int(re.sub('확진자\s*(\d+)명.*', r'\1', c1))
            c2 = c3 = c4 = c5 = c6 = c7 = None

        # replace /(.*)(\d+)명(.*)/ from extract_msg1_1 with '$2'
        if c2:
            confirmed = int(re.sub('(.*)(\d+)명(.*)', r'\2', c2))
            c3 = c4 = c5 = c6 = c7 = None

        # set extract_msg1_2 to if(isnull(`$col`), 0, 3)
        if c3:
            confirmed = 3
            c4 = c5 = c6 = c7 = None

        # set extract_msg1_3 to if(isnull(`$col`), 0, 2)
        if c4:
            confirmed = 2
            c5 = c6 = c7 = None

        # replace /(\d+)(~.*)/ from extract_msg1_4_1 with '$1'
        # replace /(.*~)(\d+)(번.*)/ from extract_msg1_4_1_1 with '$2'
        if c5:
            start = re.sub('(\d+)~.*', r'\1', c5)
            end = re.sub('.*~(\d+)번.*', r'\1', c5)
            confirmed = int(end) - int(start) + 1

            if confirmed < 0:
                plus = int(start[:-1]) * 10
                confirmed = plus + int(end) - int(start) + 1

                if confirmed < 0:
                    plus = int(start[:-2]) * 100
                    confirmed = plus + int(end) - int(start) + 1

                    if confirmed < 0:
                        continue
            c6 = c7 = None

        if c6:
            start = re.sub('(\d+)번~.*', r'\1', c6)
            end = re.sub('.*~(\d+)번.*', r'\1', c6)
            confirmed = int(end) - int(start) + 1

            if confirmed < 0:
                plus = int(start[:-1]) * 10
                confirmed = plus + int(end) - int(start) + 1

                if confirmed < 0:
                    plus = int(start[:-2]) * 100
                    confirmed = plus + int(end) - int(start) + 1

                    if confirmed < 0:
                        continue
            c7 = None

        # set extract_msg1_5 to if(extract_msg1_4_1_1 > 0, '0', '1')
        if c7:
            confirmed = 1

        if confirmed > 41:
            continue

        if t1 or t2 or t3 or t4 or t5 or t6:
            tracing = '1'

        d = dict()
        d['md102_sn'] = row['md102_sn']
        d['createDate'] = row['createDate']

        d['msg'] = msg

        location_record = location_code[loc]
        d['location_id'] = location_record['location_id']
        d['si_do'] = location_record['si_do']
        d['si_gun_gu'] = location_record['si_gun_gu']
        d['dong'] = []

        d['tracing'] = tracing
        d['confirmed'] = confirmed

        producer.send(DEST_TOPIC_NAME, value=d)

        create_rec_cnt += 1

    producer.flush()
    print(f'{datetime.datetime.now()} transform(): flushed {create_rec_cnt} rows.')


def print_last_messages(topic_name, cnt=5):
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
    load_location_code()

    dest_last_seq = get_last_seq(DEST_TOPIC_NAME)
    print('dest_last_seq in {}: {}'.format(DEST_TOPIC_NAME, dest_last_seq))
    transform(SRC_TOPIC_NAME, DEST_TOPIC_NAME, dest_last_seq)

    while PROD:
        time.sleep(30)
        dest_last_seq = get_last_seq(DEST_TOPIC_NAME)
        print('dest_last_seq in {}: {}'.format(DEST_TOPIC_NAME, dest_last_seq))
        transform(SRC_TOPIC_NAME, DEST_TOPIC_NAME, dest_last_seq)

    print_last_messages(DEST_TOPIC_NAME, cnt=200)

# eof
