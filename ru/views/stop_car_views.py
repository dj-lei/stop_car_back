from ru.views import *


def insert_db(username, car):
    try:
        res = es_ctrl.search(index='stop-car-data', body=query_with([['username', str(username)], ['car_number', car['车牌号']]]))['hits']['hits']
        if len(res) > 0:
            res[0]['_source']['origin_info'] = car
            _ = es_ctrl.update(index='stop-car-data', body={'doc': res[0]['_source']}, id=res[0]['_id'])
            send(car['车牌号'])
        else:
            _ = es_ctrl.index(index='stop-car-data', body={'username': username, 'car_number': car['车牌号'], 'state': '待定','history': [], 'origin_info': car, 'query_info': {}})
            send(car['车牌号'])
        print(username, car['车牌号'], 'add ok!')
    except Exception as e:
        traceback.print_exc()


def send(car_number):
    with open("config.json", 'r') as f:
        config = json.load(f)
    payload = {"cards": str(car_number)}
    ret = request.post(config['add_url'], data=json.dumps(payload), headers=config['add_headers'], timeout=3)


def query(request):
    try:
        if request.method == 'GET':
            car_number = request.GET.get('car_number')
            username = request.GET.get('username')
            insert_db(username, {'车牌号': car_number})
            return JsonResponse({'content': 'ok'})
    except Exception as e:
        traceback.print_exc()


def add_car(stop_car_table, username):
    for car in json.loads(stop_car_table.to_json(orient='records')):
        try:
            insert_db(username, car)
            time.sleep(0.5)
        except Exception as e:
            traceback.print_exc()
    print('reset cars ok!')


def run(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        upload_file = request.FILES.get('file')
        stop_car_table = pd.read_excel(upload_file)
        t1 = threading.Thread(target=add_car, args=(stop_car_table, username,))
        t1.start()
    return JsonResponse({'content': 'ok'})


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
        traceback.print_exc()


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
        traceback.print_exc()


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
        traceback.print_exc()


def index(request):
    return HttpResponse("Hello World!")
