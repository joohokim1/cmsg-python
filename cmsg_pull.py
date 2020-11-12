#!/bin/usr/env python3
# encode:utf8

import requests
import re
import json
import time
import datetime
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import sys

argc = len(sys.argv)
if argc not in range(3, 5):
    print(f'Usage: python3 {sys.argv[0]} <target_topic> <prod> [<skip_page_cnt>]')
    print(f'Example: python3 {sys.argv[0]} cmsg-safekorea True')
    exit(-1)

TOPIC_NAME, PROD = sys.argv[1], bool(sys.argv[2])
if argc == 4:
    SKIP_PAGE_CNT = int(sys.argv[3])    # refer to total_page_cnt to set. 42 is good for 2020.11.12 when PAGE_SIZE=1000
else:
    SKIP_PAGE_CNT = 0

URL_FOR_CNT = 'http://mepv2.safekorea.go.kr/disasterBreaking/showList2.do?rows=1&page=1&nowPage=&locationCode='
URL_FOR_MSG = 'http://mepv2.safekorea.go.kr/disasterBreaking/showList2.do'
URL_FOR_DTL = 'http://mepv2.safekorea.go.kr/disaster/showBreakingDetail.do'

PAGE_SIZE = 1000  # 1000 is good
LIMIT_SEQ = 99999  # for test, set less than current max md102_sn

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

    while True:
        r = requests.get(URL_FOR_CNT)
        if r.status_code == 200:
            break
        print(f'error while GET {URL_FOR_CNT}: status={r.status_code}')
        time.sleep(0.1)

    site_msg_total = r.json()['total']
    print('site_msg_total in mepv2.safekorea.go.kr: {}'.format(site_msg_total))


def fetch_and_put_into_kafka():
    global create_rec_cnt
    global global_lo
    global global_hi
    global page_no_lo
    global page_no_hi
    global total_page_cnt

    last_seq = get_last_seq(TOPIC_NAME)

    full_loaded_page_cnt = int(site_msg_total / PAGE_SIZE)
    last_page_record_cnt = site_msg_total % PAGE_SIZE

    if last_page_record_cnt == 0:
        page_no = full_loaded_page_cnt
    else:
        page_no = full_loaded_page_cnt + 1

    page_no -= SKIP_PAGE_CNT

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'))

    create_rec_cnt_local = 0

    while True:
        url = f'{URL_FOR_MSG}?rows={PAGE_SIZE}&page={page_no}'
        r = requests.get(url)
        if r.status_code != 200:
            print(f'error while GET {url}: status={r.status_code}')
            time.sleep(0.1)
            continue
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

            if seq > LIMIT_SEQ:
                break

            if seq <= last_seq:
                continue

            url2 = f'{URL_FOR_DTL}?seq={seq}'
            while True:
                r2 = requests.get(url2)
                if r2.status_code == 200:
                    break
                print(f'error while GET {url2}: status={r2.status_code}')
                time.sleep(0.1)

            m = re.search('<h1>.*송출지역.*</h1>\s*<h2>(.*)</h2>', r2.text)
            send_location = m.group(1)

#print(f'seq={seq} createDate={row["createDate"]}, send_location={send_location}, msg={row["msg"]}')

            d = dict()
            d['md102_sn'] = seq
            d['createDate'] = row['createDate']
            d['send_location'] = send_location
            d['msg'] = row['msg']

            producer.send(TOPIC_NAME, value=d)
            create_rec_cnt += 1
            create_rec_cnt_local += 1

        print(f'({lo}~{hi})')
        global_lo = min(global_lo, lo)
        global_hi = max(global_hi, hi)

        if seq > LIMIT_SEQ:
            break

        if page_no == 1:
            break

        page_no -= 1
        producer.flush()

    print(f'{datetime.datetime.now()} fetch_and_put_into_kafka(): flushed {create_rec_cnt_local} rows.')


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
    print(f'skip_page_cnt: {SKIP_PAGE_CNT}')
    get_site_msg_total()
    fetch_and_put_into_kafka()
    while PROD:
        time.sleep(30)
        fetch_and_put_into_kafka()
    print_last_messages(TOPIC_NAME, cnt=200)

    print()
    print(f'Last total records: {last_rec_cnt}')
    print(f'Created {create_rec_cnt} records in this execution.')
    print(f'Current total records: {current_rec_cnt}')

    print(f'Page size was {PAGE_SIZE}')
    print(f'Total page count was {total_page_cnt}')
    print(f'Skipped {SKIP_PAGE_CNT} pages')

    print(f'Scanned page# {page_no_lo} ~ page# {page_no_hi} ({global_lo} ~ {global_hi})')

    print(f'Remote total message count: {site_msg_total}')

# eof
