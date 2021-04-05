from django.urls import path

from .views import stop_car_views
from .views import user_views


urlpatterns = [
    path('', stop_car_views.index, name='index'),
    path('stop_car/get', stop_car_views.get),
    path('stop_car/query', stop_car_views.query),
    path('stop_car/run', stop_car_views.run),
    path('stop_car/history_info', stop_car_views.history_info),
    path('stop_car/origin_info', stop_car_views.origin_info),
    path('user/login', user_views.login),
]
