import os
import io
import re
import json
import time
import uuid
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
from apscheduler.schedulers.background import BackgroundScheduler


server_address = 'localhost'
es_ctrl = Elasticsearch([{'host': '121.41.42.251', 'port': 9200}])


def get_elm(data, car_number):
    for elm in data:
        if elm['card'] == car_number:
            return elm
    return {}


def query_data():
    try:
        with open("config.json", 'r') as f:
            config = json.load(f)

            ret = requests.get(config['query_url'], headers=config['query_headers'])
            data = json.loads(ret.content)
            num = int(es_ctrl.count(index='stop-car-data')['count'])
            res = []
            for i in range(0, int(num/1000)+1):
                res.extend(es_ctrl.search(index='stop-car-data', from_=i * 1000, size=1000)['hits']['hits'])

            if len(res) > 0:
                for elm in res:
                    car = get_elm(data['vips'], elm['_source']['car_number'])
                    if len(car) > 0:
                        elm['_source']['state'] = '入场'
                        elm['_source']['info'] = car
                    else:
                        elm['_source']['state'] = '待定'
                    _ = es_ctrl.update(index='stop-car-data', body={'doc': elm['_source']}, id=elm['_id'])

            for car in data['vips']:
                tmp = es_ctrl.search(index='stop-car-data', body=query_with([['car_number', car['card']]]))['hits']['hits']
                if len(tmp) == 0:
                    _ = es_ctrl.index(index='stop-car-data', body={'username': '133923', 'car_number': car['card'], 'state': '入场', 'info': car})
            print('query ok!')
    except Exception as e:
        traceback.print_exc()


scheduler = BackgroundScheduler()
scheduler.add_job(query_data, 'interval', minutes=0.1)
scheduler.start()