from ru.views import *


def insert_db(username, car_number):
    try:
        res = es_ctrl.search(index='stop-car-data', body=query_with([['username', str(username)], ['car_number', str(car_number)]]))['hits']['hits']
        if len(res) > 0:
            send(car_number)
        else:
            _ = es_ctrl.index(index='stop-car-data', body={'username': username, 'car_number': car_number, 'state': '待定', 'info': {}})
            send(car_number)
    except Exception as e:
        traceback.print_exc()


def send(car_number):
    with open("config.json", 'r') as f:
        config = json.load(f)
    payload = {"cards": str(car_number)}
    ret = requests.post(config['add_url'], data=json.dumps(payload), headers=config['add_headers'], timeout=3)


def query(request):
    try:
        if request.method == 'GET':
            car_number = request.GET.get('car_number')
            username = request.GET.get('username')
            insert_db(username, car_number)
            return JsonResponse({'content': 'ok'})
    except Exception as e:
        traceback.print_exc()


def add_car(cars, username):
    for car_number in cars:
        try:
            insert_db(username, car_number)
            print(username, car_number, 'add ok!')
            time.sleep(0.5)
        except Exception as e:
            traceback.print_exc()


def run(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        upload_file = request.FILES.get('file')
        stop_car_table = pd.read_excel(upload_file)
        t1 = threading.Thread(target=add_car, args=(list(stop_car_table['车牌号'].values), username,))
        t1.start()
    return JsonResponse({'content': 'ok'})


def get(request):
    try:
        if request.method == 'GET':
            username = request.GET.get('username')
            res = []
            for elm in es_ctrl.search(index='stop-car-data', body=query_with([['username', str(username)], ['state', '入场']]), size=100)['hits']['hits']:
                if 'state' in json.dumps(elm['_source']['info']):
                    res.append(elm['_source']['info'])
                else:
                    res.append({'card': elm['_source']['info']['card'], 'pltName': '', 'address1':'', 'address2':'', 'comeTime':'', 'state':''})
            return JsonResponse({'content': res})
    except Exception as e:
        traceback.print_exc()


def index(request):
    return HttpResponse("Hello World!")
