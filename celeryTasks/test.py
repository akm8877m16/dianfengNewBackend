# -*- coding:utf-8 -*-
__author__ = 'wenhao Yin <akm8877m16@126.com>'
__copyright__ = 'Copyright 2016 wenhao'
from pymongo import MongoClient
import datetime
import redis
import sys
import time
from bson.codec_options import DEFAULT_CODEC_OPTIONS
sys.path.append('/home/webapps/dianfeng')
from celeryTasks.repeatTasks import repeatMessageHandler
from utils.electricTime import ELECTRIC_TIME
hexTrans = lambda x: int(x, 16)
#data_all = db.data_all  # 数据集合
#data_day = db.data_day  # 电能集合

#redisPool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
#options = DEFAULT_CODEC_OPTIONS.with_options(tz_aware=True)


def messageHandler(message):
    results = message.split(",")
    zigbee_sn = results[0][2:]
    data = results[1]
    dataArray = map(int, data.split(" "))
    if dataArray[0] == 50 and len(dataArray) == 13:   #0x32  表示电表
        device_sn = ''
        for i in range(1, 5):
            temp = hex(dataArray[i])
            if len(temp) == 3:
                device_sn = device_sn + '0'+ temp[2:]
            else:
                device_sn = device_sn + temp[2:]
        power = dataArray[6]*256+dataArray[7]
        electricity = (dataArray[8]*16777216 + dataArray[9]*65536 + dataArray[10]*256 +dataArray[11])*0.001;
        r = redis.Redis(connection_pool=redisPool)
        r.set(device_sn+'/'+'power', power, ex=20)  # expire 20s
        r.set(device_sn + '/' + 'electricity', electricity, ex=20)  # expire 20s
        print(device_sn+'/'+'power')
        print(device_sn + '/' + 'electricity')
        client = MongoClient('localhost', 27017)
        db = client["dianfeng"]  # database name: dianfeng
        data_all_name = "data_all"
        if data_all_name in db.collection_names():
            pass
        else:
            db.create_collection("data_all")
            print ("data_real" + ":  created")
        post = {}
        post['device_sn'] = device_sn
        post['zigbee_sn'] = zigbee_sn
        post['power'] = power
        post['electricity'] = electricity
        post["postTime"] = datetime.datetime.now()
        dataAllCollection = db["data_all"]
        result = dataAllCollection.insert_one(post)
        print(result.inserted_id)

'''
1 通过data_all计算之前一个小时的耗电总量
2 然后把data_all里前一个小时的数据都删掉
3 同时，document构建基于时间桶的设计
'''

def getHourHistory():
    queryTime = datetime.datetime.now()
    starttime = datetime.datetime(queryTime.year, queryTime.month, queryTime.day, queryTime.hour)
    endtime = datetime.datetime(queryTime.year, queryTime.month, queryTime.day, queryTime.hour, 59, 59)
    dayTime = datetime.datetime(queryTime.year, queryTime.month, queryTime.day)
    hour = queryTime.hour

    queryTime_test = datetime.datetime.now() - datetime.timedelta(hours=17)
    hour_test = queryTime_test.hour

    print queryTime_test
    print hour_test
    print starttime
    print endtime
    print dayTime
    print hour
    client = MongoClient('localhost', 27017)
    db = client["dianfeng"]  # database name: dianfeng
    dataAllCollection = db["data_all"]
    dataHistoryHour = db["data_hour"]
    sns = dataAllCollection.distinct('device_sn')
    for sn in sns:
        result_latest = db.get_collection('data_all', codec_options=options).find_one(
            {'device_sn': sn, 'postTime': {"$lte": endtime, "$gte": starttime}},
            sort=[('postTime', -1)])
        print result_latest
        result_first = db.get_collection('data_all', codec_options=options).find_one(
            {'device_sn': sn, 'postTime': {"$lte": endtime, "$gte": starttime}},
            sort=[('postTime', 1)])
        print result_first
        if result_first is not None:
            consume = result_latest['electricity'] - result_first['electricity'] #本小时耗电量
            result = db.get_collection('data_hour', codec_options=options).find({'device_sn': sn, 'dayTime':dayTime}).limit(1)
            print('matched:  ' + str(result.count()))
            if result.count() == 1:
                print(sn + " " + dayTime.__str__() + " "+ 'exist')
                #document exist, update
                insert_result = dataHistoryHour.update_one({'device_sn': sn, 'dayTime': dayTime},{'$inc': {'consume': consume} })
                print("consume update:  " + str(insert_result.matched_count))
                if hour_test < ELECTRIC_TIME['Shanghai']['LOW'] or hour_test >= ELECTRIC_TIME['Shanghai']['HIGH']:
                    path = 'hour_history.valley'
                    print path
                    insert_result = dataHistoryHour.update_one({'device_sn': sn, 'dayTime': dayTime},
                                                               {'$addToSet': {path: {str(hour_test): consume}},
                                                                '$inc': {'consumeValley': consume}})
                else:
                    path = 'hour_history.peak'
                    print path
                    insert_result = dataHistoryHour.update_one({'device_sn': sn, 'dayTime': dayTime},
                                                               {'$addToSet': {path: {str(hour_test): consume}},
                                                                '$inc': {'consumePeak': consume}})
                print("hour record of the day update:  " + str(insert_result.matched_count))
            else:
                print(sn + " " + dayTime.__str__() + " " + 'not exist')
                # document not exist, create new
                dayRecord = {}
                dayRecord['device_sn'] = sn
                dayRecord['zigbee_sn'] = result_latest['zigbee_sn']
                dayRecord['location'] = 'Shanghai'
                dayRecord['dayTime'] = dayTime
                dayRecord['consume'] = consume
                dayRecord['hour_history'] = {}
                dayRecord['hour_history']['valley'] = []
                dayRecord['hour_history']['peak'] = []
                dayRecord['consumePeak'] = 0
                dayRecord['consumeValley'] = 0
                if hour_test < ELECTRIC_TIME['Shanghai']['LOW'] or hour_test >= ELECTRIC_TIME['Shanghai']['HIGH']:
                    dayRecord['hour_history']['valley'].append({str(hour_test): consume})
                    dayRecord['consumeValley'] = consume
                else:
                    dayRecord['hour_history']['peak'].append({str(hour_test): consume})
                    dayRecord['consumePeak'] = consume
                    intset_result = dataHistoryHour.insert_one(dayRecord)
                    print (intset_result.inserted_id)

def getHourHistory2():
    queryTime = datetime.datetime.now()
    queryTime_test = datetime.datetime.now() - datetime.timedelta(hours=5)
    starttime = datetime.datetime(queryTime.year, queryTime.month, queryTime.day, queryTime.hour)
    endtime = datetime.datetime(queryTime.year, queryTime.month, queryTime.day, queryTime.hour, 59, 59)
    dayTime = datetime.datetime(queryTime.year, queryTime.month, queryTime.day)
    monthTime = datetime.datetime(queryTime.year, 1, 1)
    hour = queryTime_test.hour
    month = queryTime.month
    print starttime
    print endtime
    print dayTime
    print monthTime
    print hour
    print month
    client = MongoClient('localhost', 27017)
    db = client["dianfeng"]  # database name: dianfeng
    dataAllCollection = db["data_all"]
    dataHistoryHour = db["data_hour"]
    dataHistoryMonth = db["data_month"]
    sns = dataAllCollection.distinct('device_sn')
    for sn in sns:
        result_latest = db.get_collection('data_all', codec_options=options).find_one(
            {'device_sn': sn, 'postTime': {"$lte": endtime, "$gte": starttime}},
            sort=[('postTime', -1)])
        print result_latest
        result_first = db.get_collection('data_all', codec_options=options).find_one(
            {'device_sn': sn, 'postTime': {"$lte": endtime, "$gte": starttime}},
            sort=[('postTime', 1)])
        print result_first
        if result_first is not None:
            consume = result_latest['electricity'] - result_first['electricity']  # 本小时耗电量
            result = db.get_collection('data_hour', codec_options=options).find(
                {'device_sn': sn, 'dayTime': dayTime}).limit(1)
            result2 = db.get_collection('data_month', codec_options=options).find(
                {'device_sn': sn, 'year': monthTime}).limit(1)
            print('matched:  ' + str(result.count()))
            print('matched:  ' + str(result2.count()))
            # update/create hour history
            if result.count() == 1:
                print(sn + " " + dayTime.__str__() + " " + 'exist')
                # document exist, update
                insert_result = dataHistoryHour.update_one({'device_sn': sn, 'dayTime': dayTime},
                                                           {'$inc': {'consume': consume}})
                print("consume update:  " + str(insert_result.matched_count))
                if hour < ELECTRIC_TIME['Shanghai']['LOW'] or hour >= ELECTRIC_TIME['Shanghai']['HIGH']:
                    path = 'hour_history.valley'
                    print path
                    insert_result = dataHistoryHour.update_one({'device_sn': sn, 'dayTime': dayTime},
                                                               {'$addToSet': {path: {str(hour): consume}},
                                                                '$inc': {'consumeValley': consume}})
                else:
                    path = 'hour_history.peak'
                    print path
                    insert_result = dataHistoryHour.update_one({'device_sn': sn, 'dayTime': dayTime},
                                                               {'$addToSet': {path: {str(hour): consume}},
                                                                '$inc': {'consumePeak': consume}})
                print("hour record of the day update:  " + str(insert_result.matched_count))
            else:
                print(sn + " " + dayTime.__str__() + " " + 'not exist')
                # document not exist, create new
                dayRecord = {}
                dayRecord['device_sn'] = sn
                dayRecord['zigbee_sn'] = result_latest['zigbee_sn']
                dayRecord['location'] = 'Shanghai'
                dayRecord['dayTime'] = dayTime
                dayRecord['consume'] = consume
                dayRecord['hour_history'] = {}
                dayRecord['hour_history']['valley'] = []
                dayRecord['hour_history']['peak'] = []
                dayRecord['consumePeak'] = 0
                dayRecord['consumeValley'] = 0
                if hour < ELECTRIC_TIME['Shanghai']['LOW'] or hour >= ELECTRIC_TIME['Shanghai']['HIGH']:
                    dayRecord['hour_history']['valley'].append({str(hour): consume})
                    dayRecord['consumeValley'] = consume
                else:
                    dayRecord['hour_history']['peak'].append({str(hour): consume})
                    dayRecord['consumePeak'] = consume
                intset_result = dataHistoryHour.insert_one(dayRecord)
                print (intset_result.inserted_id)

#test Singleton
class Singleton:
    _singleton = None
    def getSingleton(cls):
        if not isinstance(cls._singleton, cls):
            cls._singleton = cls()
        return cls._singleton
    getSingleton = classmethod(getSingleton)


if __name__ == '__main__':
    #message = "M/TESTTEST,50 205 241 244 14 1 0 19 0 0 0 108 237"
    #messageHandler(message)
    #getHourHistory2()
    '''
    dayRecord = {}
    dayRecord['device_sn'] = 'teasgasfd'
    dayRecord['zigbee_sn'] = 'asfsafsaf'
    dayRecord['location'] = 'Shanghai'
    dayRecord['dayTime'] = 'asfasfsad'
    dayRecord['consume'] = 'asfasfsaf'
    dayRecord['hour_history'] = {}
    dayRecord['hour_history']['valley'] = []
    dayRecord['hour_history']['peak'] = []
    dayRecord['consumePeak'] = 0
    dayRecord['consumeValley'] = 0
    dayRecord['hour_history']['valley'].append({'123': 11})
    print dayRecord
    '''

    '''
    class Test(Singleton):
        def test(self):
            print self.__class__, id(self)

    class Test1(Test):
        def test1(self):
            print self.__class__, id(self), 'Test1'


    t1 = Test.getSingleton()
    t2 = Test.getSingleton()
    t1.test()
    t2.test()
    assert (isinstance(t1, Test))
    assert (isinstance(t2, Test))
    assert (id(t1) == id(t2))
    t1 = Test1.getSingleton()
    t2 = Test1.getSingleton()
    assert (isinstance(t1, Test1))
    assert (isinstance(t2, Test1))
    assert (id(t1) == id(t2))
    t1.test()
    t1.test1()
    t2.test()
    t1.test1()
    t3 = Test.getSingleton()
    t3.test()
    assert (isinstance(t3, Test))
    '''
    taskMsg = "12 23 12 23 12 12"
    payload = taskMsg.split(' ')
    ack = bytearray()
    ack.extend(map(hexTrans, payload))

    taskInfo = {}
    taskInfo['topic'] = 'D/testtest'
    taskInfo['payload'] = ' '.join(hex(x) for x in ack)#这句能把byarray里的数据遍历一遍转换成hex格式，而且用空格相连
    taskInfo['time'] = int(time.time())
    taskInfo['delay'] = 15
    repeatMessageHandler.delay(taskInfo)
