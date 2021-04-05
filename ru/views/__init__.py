import os
import io
import re
import json
import time
import uuid
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
from apscheduler.schedulers.background import BackgroundScheduler


request = requests.session()
# es_ctrl = Elasticsearch([{'host': '121.41.42.251', 'port': 9200}])
es_ctrl = Elasticsearch([{'host': 'localhost', 'port': 9200}])


def get_elm(data, car_number):
    for elm in data:
        if elm['card'] == car_number:
            return elm
    return {}


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
        traceback.print_exc()


scheduler = BackgroundScheduler()
scheduler.add_job(query_data, 'interval', minutes=0.1)
scheduler.start()