from ru.utils import *


class Query(object):
    def __init__(self, es_con):
        self.es_ctrl = es_con
        self.cars = []
        self.reset_cars()

    def reset_cars(self):
        self.cars = []
        num = int(self.es_ctrl.count(index='stop-car-data')['count'])
        for i in range(0, int(num / 1000) + 1):
            self.cars.extend(self.es_ctrl.search(index='stop-car-data', from_=i * 1000, size=1000)['hits']['hits'])

    def query_data(self):
        for i in range(0,10):
            if i > 4:
                self.reset_cars()
            print(self.cars)