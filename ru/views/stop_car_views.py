from ru.views import *


def index(request):
    return HttpResponse("Hello World!")


def add_car(stop_car_table, username):
    for car in json.loads(stop_car_table.to_json(orient='records')):
        redis_con.sadd('stop_car_data', json.dumps({'username': username, 'info': car}))
    prometheus_error.labels(username=str(username), operate="run", state="ok", msg='').inc(1)


def query(request):
    try:
        if request.method == 'GET':
            car_number = request.GET.get('car_number')
            username = request.GET.get('username')
            redis_con.sadd('stop_car_data', json.dumps({'username': username, 'info': {'车牌号': car_number}}))
            prometheus_error.labels(username=str(username), operate="query", state="ok", msg='').inc(1)
            return JsonResponse({'content': 'ok'})
    except Exception as e:
        prometheus_error.labels(username=str(username), operate="query", state="error", msg=traceback.format_exc()).inc(1)


def run(request):
    try:
        if request.method == 'POST':
            username = request.POST.get('username')
            upload_file = request.FILES.get('file')
            stop_car_table = pd.read_excel(upload_file)
            t1 = threading.Thread(target=add_car, args=(stop_car_table, username,))
            t1.start()
        return JsonResponse({'content': 'ok'})
    except Exception as e:
        prometheus_error.labels(username=str(username), operate="run", state="error", msg=traceback.format_exc()).inc(1)


def get(request):
    try:
        if request.method == 'GET':
            username = request.GET.get('username')
            res = []
            for elm in es_ctrl.search(index='stop-car-data', body=query_with([['username', str(username)], ['state', '入场']]), size=1000)['hits']['hits']:
                if 'state' in json.dumps(elm['_source']['query_info']):
                    res.append(elm['_source']['query_info'])
                else:
                    res.append({'card': elm['_source']['query_info']['card'], 'pltName': '', 'img': '', 'address1':'', 'address2':'', 'comeTime':'', 'state':''})
            return JsonResponse({'content': res})
    except Exception as e:
        prometheus_error.labels(username=str(username), operate="get", state="error", msg=traceback.format_exc()).inc(1)


def history_info(request):
    try:
        if request.method == 'GET':
            car_number = request.GET.get('car_number')
            username = request.GET.get('username')
            res = es_ctrl.search(index='stop-car-data', body=query_with([['username', str(username)], ['car_number', car_number]]))['hits']['hits']
            if len(res) > 0:
                tmp = []
                for i, elm in enumerate(res[0]['_source']['history']):
                    if elm['state'] == '入场':
                        if i + 1 < len(res[0]['_source']['history']):
                            tmp.append({'address1': elm['address1'], 'address2': elm['address2'], 'comeTime': elm['timed'],'leaveTime': res[0]['_source']['history'][i + 1]['timed']})
                        else:
                            tmp.append({'address1': elm['address1'], 'address2': elm['address2'], 'comeTime': elm['timed'],'leaveTime': ''})
                return JsonResponse({'content': tmp})
            else:
                return JsonResponse({'content': [{'name': 'msg', 'value': '没有查询到历史信息.'}]})
    except Exception as e:
        prometheus_error.labels(username=str(username), operate="history_info", state="error", msg=traceback.format_exc()).inc(1)


def origin_info(request):
    try:
        if request.method == 'GET':
            car_number = request.GET.get('car_number')
            username = request.GET.get('username')
            res = es_ctrl.search(index='stop-car-data', body=query_with([['username', str(username)], ['car_number', car_number]]))['hits']['hits']
            if len(res) > 0:
                tmp = []
                for key in res[0]['_source']['origin_info'].keys():
                    tmp.append({'name': key, 'value': res[0]['_source']['origin_info'][key]})
                return JsonResponse({'content': tmp})
            else:
                return JsonResponse({'content': [{'name': 'msg', 'value': '没有查询到车辆详情.'}]})
    except Exception as e:
        prometheus_error.labels(username=str(username), operate="origin_info", state="error", msg=traceback.format_exc()).inc(1)


