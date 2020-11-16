#!/bin/usr/env python3
# encode:utf8

import requests
import re
import sys
import pymysql
import xmltodict
import time
import datetime

if len(sys.argv) != 8:
    print(f'Usage: python3 {sys.argv[0]} <host> <port> <user> <passwd> <db> <table> <prod>')
    print(f'Example: python3 {sys.argv[0]} localhost 3306 covid covid covid covid_stat True')
    exit(-1)

HOST, PORT, USER, PASSWD, DB, TABLE = sys.argv[1:7]
PROD = bool(sys.argv[7])

AUTH = 'hCTKxSxsFFXHAn3gYSeFfp7Wq8HpNNXC19VgcbSxCbLucH8lF8ybI5LpMY9KXiBObsKiyiNmxLJB2pwJFpXBLQ%3D%3D'
URL = 'http://openapi.data.go.kr/openapi/service/rest/Covid19/getCovid19SidoInfStateJson'

START = '20200410'
END = '99991231'

while True:
    try:
        print(f'Connecting to Open API ... URL={URL} START={START} END={END}')

        r = requests.get(f'{URL}?serviceKey={AUTH}&pageNo=1&numOfRows=50&startCreateDt={START}&endCreateDt={END}')
        d = xmltodict.parse(r.text)
        rows = d['response']['body']['items']['item']

        print(f'Connecting to DB ... HOST={HOST} PORT={PORT} USER={USER} DB={DB} TABLE={TABLE} PROD={PROD}')

        db = pymysql.connect(HOST, USER, PASSWD, DB)
        cursor = db.cursor()

        for row in reversed(rows):
            base_date = re.sub('(\d+)[^\d]+(\d+)[^\d]+(\d+).*', r'\1.\2.\3', row['stdDay'])
            y = int(re.sub('(\d+)\.(\d+)\.(\d+)', r'\1', base_date))
            m = int(re.sub('(\d+)\.(\d+)\.(\d+)', r'\2', base_date))
            d = int(re.sub('(\d+)\.(\d+)\.(\d+)', r'\3', base_date))
            base_date = f'{y:04d}.{m:02d}.{d:02d}'
            if len(base_date) != len('0000.00.00'):
                print(f'wrong base_date: {base_date}', f'row: {row}')
                exit(-1)

            location = row['gubun']
            if location not in ['검역', '제주', '경남', '경북', '전남', '전북', '충남', '충북', '강원', '경기', '세종', '울산', '대전',
                                '광주', '인천', '대구', '부산', '서울', '합계', '검역', '제주', '경남', '경북', '전남', '전북', '충남',
                                '충북', '강원', '경기', '세종', '울산', '대전', '광주', '인천', '대구', '부산', '서울', '합계']:
                continue

            total_confirmed_cnt = row['defCnt']
            daily_confirmed_cnt = row['incDec']
            local_cnt = row['localOccCnt']
            inflow_cnt = row['overFlowCnt']
            isolation_cnt = row['isolIngCnt']
            release_cnt = row['isolClearCnt']
            dead_cnt = row['deathCnt']
            ratio_100k = row['qurRate']
            if ratio_100k == '-':
                ratio_100k = '0.0'
            elif ratio_100k[-1] == '.':
                ratio_100k = ratio_100k[:-1]    # data error on 2020.10.31

            sql = f"replace into covid_stat values ('{base_date}', '{location}', {total_confirmed_cnt}, " \
                  f"{daily_confirmed_cnt}, {local_cnt}, {inflow_cnt}, {isolation_cnt}, {release_cnt}, {dead_cnt}, " \
                  f"{ratio_100k})"
            print(sql)
            cursor.execute(sql)
        db.commit()
    except Exception as e:
        print(e)
        if db:
            db.rollback()
            db.close()
        exit(-1)

    if not PROD:
        break

    print(f'Sleep for 600 sec ... at {datetime.datetime.now()}')
    time.sleep(600)

db.close()

# eof
