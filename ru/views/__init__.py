import os
import io
import re
import json
import time
import uuid
import redis
import datetime
import threading
import requests
import traceback
import pandas as pd
from django.http import HttpResponse
from django.http import JsonResponse
from django.http import HttpResponseBadRequest
from django.shortcuts import render
from io import BytesIO
from elasticsearch import Elasticsearch
from ru.utils.common import *
from ru.utils.query import *
from prometheus_client import Counter, start_http_server
from apscheduler.schedulers.background import BackgroundScheduler

ip_addr = '121.41.42.251'
# ip_addr = '10.150.86.161'

# redis control
config = (ip_addr + ":6379:1").split(':')
pool = redis.ConnectionPool(host=config[0], port=config[1], decode_responses=True, db=config[2])
redis_con = redis.Redis(connection_pool=pool)

# set up prometheus scrape port
start_http_server(9000)
prometheus_add_car = Counter('stop_car_add_car', 'Add car counter', ['username', 'car_number', 'state', 'msg'])
prometheus_error = Counter('stop_car_error', 'System error collect counter', ['username', 'operate', 'state', 'msg'])

# elasticsearch control
request = requests.session()
es_ctrl = Elasticsearch([{'host': ip_addr, 'port': 9200}])
# es_ctrl = Elasticsearch([{'host': 'localhost', 'port': 9200}])
# docker run --rm --name test-prometheus -d -v D:\projects\stop_car_back\prometheus.yml:/etc/prometheus/prometheus.yml -p 127.0.0.1:9090:9090 prom/prometheus


def get_elm(data, car_number):
    for elm in data:
        if elm['card'] == car_number:
            return elm
    return {}


def send(car_number):
    try:
        with open("config.json", 'r') as f:
            config = json.load(f)
        payload = {"cards": str(car_number)}
        return request.post(config['add_url'], data=json.dumps(payload), headers=config['add_headers'], timeout=3)
    except Exception as e:
        prometheus_add_car.labels(username='send', car_number=car_number, state='error', msg=traceback.format_exc()).inc(1)


def insert_db(username, car):
    try:
        print(car)
        res = es_ctrl.search(index='stop-car-data', body=query_with([['car_number', car['车牌号']]]))['hits']['hits']
        if len(res) > 0:
            res = es_ctrl.search(index='stop-car-data',body=query_with([['username', str(username)], ['car_number', car['车牌号']]]))['hits']['hits']
            if len(res) > 0:
                res[0]['_source']['origin_info'] = car
                _ = es_ctrl.update(index='stop-car-data', body={'doc': res[0]['_source']}, id=res[0]['_id'])
            else:
                _ = es_ctrl.index(index='stop-car-data', body={'username': username, 'car_number': car['车牌号'], 'state': '待定', 'history': [], 'origin_info': car, 'query_info': {}})
            prometheus_add_car.labels(username=str(username), car_number=car['车牌号'], state='ok', msg='').inc(1)
            return
        else:
            _ = es_ctrl.index(index='stop-car-data', body={'username': username, 'car_number': car['车牌号'], 'state': '待定','history': [], 'origin_info': car, 'query_info': {}})
            _ = es_ctrl.index(index='stop-car-data', body={'username': '133923', 'car_number': car['车牌号'], 'state': '待定', 'history': [], 'origin_info': car, 'query_info': {}})
            send_res = send(car['车牌号'])

        if send_res.content.decode() == 'ok':
            prometheus_add_car.labels(username=str(username), car_number=car['车牌号'], state='ok', msg='').inc(1)
        else:
            prometheus_add_car.labels(username=str(username), car_number=car['车牌号'], state='error', msg=send_res.content.decode()).inc(1)
    except Exception as e:
        prometheus_add_car.labels(username=str(username), car_number=car['车牌号'], state='error', msg=traceback.format_exc()).inc(1)


def query_data():
    try:
        start = time.time()
        with open("config.json", 'r') as f:
            config = json.load(f)
            query_object = Query(es_ctrl)
            try:
                ret = request.get(config['query_url'], headers=config['query_headers'], timeout=5)
                data = json.loads(ret.content)
            except Exception as e:
                prometheus_error.labels(username='query', operate="query_data", state="error", msg=traceback.format_exc()).inc(1)
                return

            if len(query_object.cars) > 0:
                for i, elm in enumerate(query_object.cars):
                    car = get_elm(data['vips'], elm['_source']['car_number'])
                    if elm['_source']['state'] == '入场':
                        if len(car) > 0:
                            query_object.cars[i]['_source']['query_info'] = car
                        else:
                            query_object.cars[i]['_source']['state'] = '离场'
                            query_object.cars[i]['_source']['history'].append({'address1':'', 'address2':'', 'timed':datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'state':'离场'})
                    elif elm['_source']['state'] == '离场':
                        if len(car) > 0:
                            query_object.cars[i]['_source']['state'] = '入场'
                            query_object.cars[i]['_source']['history'].append({'address1':car['address1'], 'address2':car['address2'], 'timed':car['comeTime'], 'state':'入场'})
                            query_object.cars[i]['_source']['query_info'] = car
                        else:
                            continue
                    elif elm['_source']['state'] == '待定':
                        if len(car) > 0:
                            if "address1" in car.keys():
                                query_object.cars[i]['_source']['state'] = '入场'
                                query_object.cars[i]['_source']['history'].append({'address1': car['address1'], 'address2': car['address2'], 'timed': car['comeTime'], 'state': '入场'})
                                query_object.cars[i]['_source']['query_info'] = car
                        else:
                            continue
                    _ = es_ctrl.update(index='stop-car-data', body={'doc': query_object.cars[i]['_source']}, id=query_object.cars[i]['_id'])

            for car in data['vips']:
                tmp = es_ctrl.search(index='stop-car-data', body=query_with([['car_number', car['card']]]))['hits']['hits']
                if len(tmp) == 0:
                    insert_data = {'username': '133923', 'car_number': car['card'], 'state': '待定', 'history': [], 'origin_info': {'车牌号': car['card'], '详情': car['detail'] if "detail" in car.keys() else ''}, 'query_info': car}
                    if "address1" in car.keys():
                        insert_data['state'] = '入场'
                        insert_data['history'].append({'address1': car['address1'], 'address2': car['address2'], 'timed': car['comeTime'], 'state': '入场'})
                    _ = es_ctrl.index(index='stop-car-data', body=insert_data)
            print('date:', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'query cars:', len(data['vips']), ' | cost time:', time.time() - start)
    except Exception as e:
        prometheus_error.labels(username='query', operate="query_data", state="error", msg=traceback.format_exc()).inc(1)


def send_data():
    for _ in range(0, redis_con.scard('stop_car_data')):
        elm = json.loads(redis_con.spop('stop_car_data'))
        insert_db(elm['username'], elm['info'])
        time.sleep(0.2)


scheduler = BackgroundScheduler()
scheduler.add_job(query_data, 'interval', minutes=0.1)
scheduler.add_job(send_data, 'interval', minutes=0.02)
scheduler.start()