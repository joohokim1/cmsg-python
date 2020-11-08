#!/bin/usr/env python3
# encode:utf8

import requests
import re
import json
import time
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

PROD = True
TOPIC_NAME = 'cmsg-safekorea'
page_size = 1000  # 1000 is good
skip_page_cnt = 42  # refer to total_page_cnt to set
limit_seq = 99999  # for test, set less than current max md102_sn

location_code = {}  # send_location -> {location_id, si_do, si_gun_gu}
site_msg_total = -1
total_page_cnt = -1
last_seq = -1

last_rec_cnt = 0
create_rec_cnt = 0
current_rec_cnt = 0

global_lo = 999999
global_hi = -1
page_no_lo = 99999
page_no_hi = -1


def get_last_seq(topic_name):
    global last_rec_cnt
    global last_seq

    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        enable_auto_commit=True,
        consumer_timeout_ms=100,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    tp = TopicPartition(topic_name, 0)
    consumer.assign([tp])
    last_rec_cnt = consumer.position(tp)
    print('last_pos in {}: {}'.format(topic_name, last_rec_cnt))

    if last_rec_cnt > 0:
        consumer.seek(tp, consumer.position(tp) - 1)
        for msg in consumer:
            last_seq = max(last_seq, int(msg.value['md102_sn']))

    return last_seq


def get_site_msg_total():
    global site_msg_total

    r = requests.get('http://mepv2.safekorea.go.kr/disasterBreaking/showList2.do?rows=1&page=1&nowPage=&locationCode=')
    site_msg_total = r.json()['total']
    print('site_msg_total in mepv2.safekorea.go.kr: {}'.format(site_msg_total))


def load_location_code():
    with open('location_code.json', encoding='utf-8-sig') as f:
        for line in f:
            rec = json.loads(line)
            location_code[rec['full_loc']] = rec


def fetch_and_put_into_kafka():
    global create_rec_cnt
    global global_lo
    global global_hi
    global page_no_lo
    global page_no_hi
    global total_page_cnt

    full_loaded_page_cnt = int(site_msg_total / page_size)
    last_page_record_cnt = site_msg_total % page_size

    if last_page_record_cnt == 0:
        page_no = full_loaded_page_cnt
    else:
        page_no = full_loaded_page_cnt + 1

    page_no -= skip_page_cnt

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'))

    while True:
        r = requests.get(
            'http://mepv2.safekorea.go.kr/disasterBreaking/showList2.do?rows={}&page={}'.format(page_size, page_no))
        total_page_cnt = r.json()['total']

        print(f'GET page# {page_no}: record_cnt={len(r.json()["rows"])}', end=' ')

        page_no_lo = min(page_no_lo, page_no)
        page_no_hi = max(page_no_hi, page_no)

        lo = 999999
        hi = -1

        for row in reversed(r.json()['rows']):
            seq = int(row['rnum'])
            lo = min(lo, seq)
            hi = max(hi, seq)

            if seq > limit_seq:
                break

            if seq <= last_seq:
                continue

            r2 = requests.get('http://mepv2.safekorea.go.kr/disaster/showBreakingDetail.do?seq={}'.format(seq))
            m = re.search('<h1>.*송출지역.*</h1>\s*<h2>(.*)</h2>', r2.text)
            send_location = m.group(1)

            # delete rows where contains(location_id, ',')
            # if ',' in send_location:
            #     continue

            # location_record = location_code[send_location]
            print(f'seq={seq} createDate={row["createDate"]}, send_location={send_location}, msg={row["msg"]}')

            d = dict()
            # d['seq'] = seq
            d['md102_sn'] = seq
            d['createDate'] = row['createDate']
            d['send_location'] = send_location
            # d['location_id'] = location_record['location_id']
            # d['si_do'] = location_record['si_do']
            # d['si_gun_gu'] = location_record['si_gun_gu']
            # d['dong'] = []
            d['msg'] = row['msg']
            # d['tracing'] = '0'
            # d['confirmed'] = 1

            producer.send(TOPIC_NAME, value=d)
            create_rec_cnt += 1

        print(f'({lo}~{hi})')
        global_lo = min(global_lo, lo)
        global_hi = max(global_hi, hi)

        if seq > limit_seq:
            break

        if page_no == 1:
            break

        page_no -= 1
        producer.flush()

    print(f'flushed {create_rec_cnt} rows.')


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
        consumer.seek(tp, consumer.position(tp) - cnt)
        for msg in consumer:
            print(msg.value)


if __name__ == '__main__':
    print('last_md102_sn: {}'.format(get_last_seq(TOPIC_NAME)))
    get_site_msg_total()
    load_location_code()
    fetch_and_put_into_kafka()
    while PROD:
        time.sleep(30)
        fetch_and_put_into_kafka()
    print_last_messages(TOPIC_NAME, cnt=200)

    print()
    print(f'Last total records: {last_rec_cnt}')
    print(f'Created {create_rec_cnt} records in this execution.')
    print(f'Current total records: {current_rec_cnt}')

    print(f'Page size was {page_size}')
    print(f'Total page count was {total_page_cnt}')
    print(f'Skipped {skip_page_cnt} pages')

    print(f'Scanned page# {page_no_lo} ~ page# {page_no_hi} ({global_lo} ~ {global_hi})')

    print(f'Remote total message count: {site_msg_total}')

# eof
