# -*- coding:utf-8 -*-
from tornado.concurrent import run_on_executor
from tornado import gen
from tornado.web import asynchronous
import time
import datetime
import redis
import sys
sys.path.append('/home/webapps/dianfeng')
from restServer.apiHandler.base import BaseHandler
from utils.ajax import JsonResponse,JsonError

class realDataHandler(BaseHandler):
    @asynchronous
    @gen.coroutine
    def get(self, device_sn):
        res = yield self.getRealData(device_sn)
        self.write(JsonResponse(res))
        self.finish()

    @run_on_executor
    def getRealData(self,device_sn):
        device_sn = device_sn.lower()
        key_power = device_sn+'/'+'power'
        r = redis.Redis(connection_pool=self.redisPool)
        result = {}
        if r.get(key_power) is not None:
            result['online'] = True
            result['power'] = r.get(key_power)
        else:
            result['online'] = False
        return result

class dayHisDataHandler(BaseHandler):
    @asynchronous
    @gen.coroutine
    def get(self, device_sn):
        fromDate = self.get_argument('from',None)
        toDate = self.get_argument('to',None)
        startTime = None
        endTime = None
        if fromDate is not None:
            temp = time.strptime(fromDate,"%Y-%m-%d")
            y, m, d = temp[0:3]
            startTime = datetime.datetime(y, m, d)
            if toDate is not None:
                temp = time.strptime(toDate,"%Y-%m-%d")
                y, m, d = temp[0:3]
                endTime = datetime.datetime(y, m, d)
            else:
                temp = datetime.datetime.now()
                endTime = datetime.datetime(temp.year,temp.month,temp.day)
        else:
            temp = datetime.datetime.now()
            startTime = datetime.datetime(temp.year, temp.month, temp.day)
            endTime = datetime.datetime(temp.year, temp.month, temp.day)
        print startTime
        print endTime

        result=[]
        cursor = self.db.data_hour.find({'device_sn': device_sn, 'dayTime': {'$gte': startTime,'$lte': endTime}},
                                         sort=[('dayTime', 1)])
        for document in (yield cursor.to_list(length=100)):
            print document.__str__
            day = {}
            for k,v in document.items():
                print k
                if k == '_id':
                    continue
                elif k == 'dayTime':
                    day[k] = document[k].strftime("%Y-%m-%d")
                else:
                    day[k] = v
            result.append(day)
        self.write(JsonResponse(result))
        self.finish()

class getDevicesHandler(BaseHandler):
    @asynchronous
    @gen.coroutine
    def get(self, zigbee_sn):
        zigbee_sn = zigbee_sn.upper()
        result1 = yield self.db.data_all.distinct('device_sn',{'zigbee_sn':zigbee_sn})
        result2 = yield self.db.data_hour.distinct('device_sn',{'zigbee_sn':zigbee_sn})
        set1 = set(result1)
        set2 = set(result2)
        set3 = set1 | set2
        listResult = list(set3)
        self.write(JsonResponse(listResult))
        self.finish()

class monthHisDataHandler(BaseHandler):
    @asynchronous
    @gen.coroutine
    def get(self, device_sn):
        device_sn = device_sn.lower()
        currentTime = datetime.datetime.now()
        year = self.get_argument('year', currentTime.year)
        yearTime = datetime.datetime(int(year), 1, 1)
        result = yield self.db.data_month.find_one({'device_sn': device_sn, 'year': yearTime})
        monthHis = {}
        if result is not None:
            for k, v in result.items():
                    print k
                    if k == '_id':
                        continue
                    elif k == 'year':
                        monthHis[k] = result[k].strftime("%Y")
                    else:
                        monthHis[k] = v
        self.write(JsonResponse(monthHis))
        self.finish()



