# -*- coding:utf-8 -*-
__author__ = 'wenhao Yin <akm8877m16@126.com>'
__copyright__ = 'Copyright 2016 wenhao'


from worker import app
from pymongo import MongoClient
import datetime
import redis
import sys
import base64
import paho.mqtt.client as mqtt
from bson.codec_options import DEFAULT_CODEC_OPTIONS
sys.path.append('/home/webapps/dianfeng')
from utils.electricTime import ELECTRIC_TIME
redisPool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
options = DEFAULT_CODEC_OPTIONS.with_options(tz_aware=True)

#mqttClient = mqtt.Client()
#mqttClient.connect("118.190.202.155", 1883)

@app.task(bind=True)
def messageHandler(self,message):
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
        electricity = (dataArray[8]*16777216 + dataArray[9]*65536 + dataArray[10]*256 +dataArray[11])*0.01;
        r = redis.Redis(connection_pool=redisPool)
        if not r.get(device_sn+'/'+'power'):
            print "redis add key"
            r.set(device_sn+'/'+'power', power, ex=200)  # expire 20s
        if not r.get(device_sn + '/' + 'electricity'):
            print "redis add key"
            r.set(device_sn + '/' + 'electricity', electricity, ex=200)  # expire 20s
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
@app.task()
def getHourHistory():
    currentTime = datetime.datetime.now();
    queryTime = datetime.datetime.now() - datetime.timedelta(hours=1)
    starttime = datetime.datetime(queryTime.year, queryTime.month, queryTime.day, queryTime.hour)
    endtime = datetime.datetime(queryTime.year, queryTime.month, queryTime.day, queryTime.hour, 59, 59)
    dayTime = datetime.datetime(currentTime.year, currentTime.month, currentTime.day)
    monthTime = datetime.datetime(currentTime.year, 1, 1)
    hour = currentTime.hour
    month = currentTime.month
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
            consume = result_latest['electricity'] - result_first['electricity'] #本小时耗电量
            result = db.get_collection('data_hour', codec_options=options).find({'device_sn': sn, 'dayTime':dayTime}).limit(1)
            result2 = db.get_collection('data_month', codec_options=options).find({'device_sn': sn, 'year':monthTime}).limit(1)
            print('matched:  ' + str(result.count()))
            print('matched:  ' + str(result2.count()))
            #update/create hour history
            if result.count() == 1:
                print(sn + " " + dayTime.__str__() + " "+ 'exist')
                #document exist, update
                insert_result = dataHistoryHour.update_one({'device_sn': sn, 'dayTime': dayTime},{'$inc': {'consume': consume} })
                print("consume update:  " + str(insert_result.matched_count))
                if hour < ELECTRIC_TIME['Shanghai']['LOW'] or hour >= ELECTRIC_TIME['Shanghai']['HIGH']:
                    path = 'hour_history.valley'
                    print path
                    insert_result = dataHistoryHour.update_one({'device_sn':sn,'dayTime':dayTime},{'$addToSet': {path: {str(hour): consume}}, '$inc': {'consumeValley': consume}})
                else:
                    path = 'hour_history.peak'
                    print path
                    insert_result =dataHistoryHour.update_one({'device_sn':sn,'dayTime':dayTime},{'$addToSet': {path: {str(hour): consume}}, '$inc': {'consumePeak': consume}})
                print("hour record of the day update:  " + str(insert_result.matched_count))
            else:
                print(sn + " " + dayTime.__str__() + " " + 'not exist')
                #document not exist, create new
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

            #update/create month history
            if result2.count() == 1:
                print(sn + " " + monthTime.__str__() + " "+ 'exist')
                #document exist, update
                insert_result = dataHistoryMonth.update_one({'device_sn': sn, 'year': monthTime},{'$inc': {'consume': consume} })
                print("consume update:  " + str(insert_result.matched_count))
                if hour < ELECTRIC_TIME['Shanghai']['LOW'] or hour >= ELECTRIC_TIME['Shanghai']['HIGH']:
                    path = 'month_history.valley.month'
                    print path
                    insert_result1 = dataHistoryMonth.update_one({'device_sn':sn, 'year': monthTime},{'$inc': {'consumeValley': consume}})
                    insert_result2 = dataHistoryMonth.update_one({'device_sn':sn, 'year': monthTime, path:month },{'$inc': {"month_history.valley.$.value": consume}})
                else:
                    path = 'month_history.peak.month'
                    print path
                    insert_result1 =dataHistoryMonth.update_one({'device_sn':sn,'year': monthTime},{'$inc': {'consumePeak': consume}})
                    insert_result2 = dataHistoryMonth.update_one({'device_sn':sn, 'year': monthTime, path:month },{'$inc': {"month_history.peak.$.value": consume}})

                print("hour record of the month update:  " + str(insert_result2.matched_count))
            else:
                print(sn + " " + monthTime.__str__() + " " + 'not exist')
                #document not exist, create new
                monthRecord = {}
                monthRecord['device_sn'] = sn
                monthRecord['zigbee_sn'] = result_latest['zigbee_sn']
                monthRecord['location'] = 'Shanghai'
                monthRecord['year'] = monthTime
                monthRecord['consumePeak'] = 0
                monthRecord['consumeValley'] = 0
                monthRecord['consume'] = consume
                monthRecord['month_history'] = {}
                monthRecord['month_history']['valley'] = [{'month': 1, 'value':0}, {'month': 2, 'value':0}, {'month': 3, 'value':0},
                                                          {'month': 4, 'value':0}, {'month': 5, 'value':0}, {'month': 6, 'value':0}, {'month': 7, 'value':0},
                                                          {'month': 8, 'value':0}, {'month': 9, 'value':0}, {'month': 10, 'value':0}, {'month': 11, 'value':0},
                                                          {'month': 12, 'value':0}]
                monthRecord['month_history']['peak'] = [{'month': 1, 'value':0}, {'month': 2, 'value':0}, {'month': 3, 'value':0},
                                                          {'month': 4, 'value':0}, {'month': 5, 'value':0}, {'month': 6, 'value':0}, {'month': 7, 'value':0},
                                                          {'month': 8, 'value':0}, {'month': 9, 'value':0}, {'month': 10, 'value':0}, {'month': 11, 'value':0},
                                                          {'month': 12, 'value':0}]
                if hour < ELECTRIC_TIME['Shanghai']['LOW'] or hour >= ELECTRIC_TIME['Shanghai']['HIGH']:
                    monthRecord['month_history']['valley'][month-1]['value'] = consume
                    monthRecord['consumeValley'] = consume
                else:
                    monthRecord['month_history']['peak'][month-1]['value'] = consume
                    monthRecord['consumePeak'] = consume

                    intset_result = dataHistoryMonth.insert_one(monthRecord)
                    print (intset_result.inserted_id)
    #dataAllCollection.delete_many({'postTime': {"$lte": endtime}})







