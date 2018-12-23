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
options = DEFAULT_CODEC_OPTIONS.with_options(tz_aware=True)

'''
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
'''
1 通过data_all计算之前一个小时的耗电总量
2 然后把data_all里前一个小时的数据都删掉
3 同时，document构建基于时间桶的设计
'''
'''
deleteTime 异常数据出现的那一天
sn 设备sn 
简单一点，异常出现的那一天的数据全部清空，相应的，把月记录里峰值谷值累计减掉
'''
def  removeAbnormalValueInHistory(sn, deleteTime):
    dayTime = datetime.datetime(deleteTime.year, deleteTime.month, deleteTime.day)
    monthTime = datetime.datetime(deleteTime.year, 1, 1)

    print dayTime
    print monthTime
    month = deleteTime.month

    client = MongoClient('localhost', 27017)
    db = client["dianfeng"]  # database name: dianfeng
    dataHistoryHour = db["data_hour"]
    dataHistoryMonth = db["data_month"]

    result = db.get_collection('data_hour', codec_options=options).find_one(
                {'device_sn': sn, 'dayTime': dayTime})
    result2 = db.get_collection('data_month', codec_options=options).find_one(
                {'device_sn': sn, 'year': monthTime})
    print('matched:  ' + str(result))
    print('matched:  ' + str(result2))
    # update/create hour history
    if result is not None:
            print(sn + " " + dayTime.__str__() + " " + 'exist')
            # document exist, update
            print ("error peak value: " + str(result["consumePeak"]))
            print ("error valley value: " + str(result["consumeValley"]))
            deleteValuePeak = result["consumePeak"]
            deleteValueValley = result["consumeValley"]
            # update/create month history
            if result2 is not None:
                print(sn + " " + monthTime.__str__() + " " + 'exist')
                # document exist, update
                updateConsume = result2["consume"] - deleteValuePeak - deleteValueValley
                updateValuePeak = result2["consumePeak"] - deleteValuePeak
                updateValueValley = result2["consumeValley"] - deleteValueValley
                updateValuePeakMonth = result2["month_history"]["peak"][month-1]["value"] - deleteValuePeak
                updateValueValleyMonth =result2["month_history"]["valley"][month-1]["value"] - deleteValueValley

                print(str(result2["month_history"]["valley"][month-1]["value"]))
                print("updateConsume: " + str(updateConsume))
                print("updateValuePeak: " + str(updateValuePeak))
                print("updateValueValley: " + str(updateValueValley))
                print("updateValuePeakMonth: " + str(updateValuePeakMonth))
                print("updateValueValleyMonth: " + str(updateValueValleyMonth))

                dataHistoryMonth.update_one({'device_sn': sn, 'year': monthTime},
                                                            {'$set': {'consume': updateConsume,'consumePeak':updateValuePeak, 'consumeValley':updateValueValley},
                                                            })
                path1 = 'month_history.valley.month'
                dataHistoryMonth.update_one({'device_sn': sn, 'year': monthTime, path1: month},
                                        {'$set': {"month_history.valley.$.value": updateValueValleyMonth}})
                path2 = 'month_history.peak.month'
                dataHistoryMonth.update_one({'device_sn': sn, 'year': monthTime, path2: month},
                                        {'$set': {"month_history.peak.$.value": updateValuePeakMonth}})

                print(sn + " day and month update complete")
            dataHistoryHour.delete_one({'device_sn': sn, 'dayTime': dayTime})

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
    '''
    sn = "cdf1f40e"
    deleteTime = datetime.datetime(2018, 4, 10)

    client = MongoClient('localhost', 27017)
    db = client["dianfeng"]  # database name: dianfeng
    dataHistoryHour = db["data_hour"]
    sns = dataHistoryHour.distinct('device_sn')
    for sn in sns:
        removeAbnormalValueInHistory(sn, deleteTime)