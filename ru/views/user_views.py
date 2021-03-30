from ru.views import *


def login(request):
    try:
        if request.method == 'POST':
            username = request.POST.get('username')
            password = request.POST.get('password')

            user = es_ctrl.search(index='stop-car-account', body=query_with([['username', username]]))['hits']['hits'][0]
            if password == user['_source']['password']:
                user['_source']['cookie'] = uuid.uuid4()
                _ = es_ctrl.update(index='stop-car-account', body={'doc': user['_source']}, id=user['_id'])
                ret = JsonResponse({'content': 'ok'})
                ret.set_cookie('stop.car.test', user['_source']['cookie'])
                return ret
            else:
                return HttpResponseBadRequest
        return HttpResponse(404)
    except Exception as e:
        traceback.print_exc()



