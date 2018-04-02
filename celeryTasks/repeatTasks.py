# -*- coding:utf-8 -*-
__author__ = 'wenhao Yin <akm8877m16@126.com>'
__copyright__ = 'Copyright 2016 wenhao'


from worker import app
import datetime
import sys
import time
import paho.mqtt.client as mqtt
sys.path.append('/home/webapps/dianfeng')
#redisPool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
#options = DEFAULT_CODEC_OPTIONS.with_options(tz_aware=True)

#mqttClient = mqtt.Client()
#mqttClient.connect("118.190.202.155", 1883)
hexTrans = lambda x: int(x,16)
@app.task(bind=True)
def repeatMessageHandler(self, repeatInfo):
    print type(repeatInfo)
    print repeatInfo['topic']
    topic = repeatInfo['topic']
    print repeatInfo['payload']
    print repeatInfo['time']
    print repeatInfo['delay']
    delay = repeatInfo['delay']
    startTime = repeatInfo['time']
    currentTime = int(time.time())
    if (currentTime - startTime) < delay:
        repeatMessageHandler.delay(repeatInfo)
    else:
        payload = repeatInfo['payload']
        payload = payload.split(' ')
        ack = bytearray()
        ack.extend(map(hexTrans, payload))
        mqttClient = mqtt.Client()
        mqttClient.connect("39.104.49.84", 1883)
        mqttClient.publish(topic, ack)
        mqttClient.disconnect()




