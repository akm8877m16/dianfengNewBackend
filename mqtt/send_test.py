# -*- coding:utf-8 -*-
__author__ = 'wenhao Yin <akm8877m16@126.com>'
__copyright__ = 'Copyright 2016 wenhao'

'''
    mqtt client: receive mqtt messages and put them to rabbitmq

'''
import logging
import socket
import sys
import time
import paho.mqtt.client as mqtt
from tornado.options import options, define
sys.path.append('/home/webapps/dianfeng')

define("mqttPort", default=1883, help="mqtt port")
define("mqttServer", default="39.104.49.84")

class mqttMsg:
    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload


hexTrans = lambda x: str(ord(x))

# 主程序
if __name__ == '__main__':
    options.parse_command_line()
    msg = mqttMsg("E/TESTTEST",
                  [0x32, 0xcd, 0xf1, 0xf4, 0x0e, 0x01, 0x00, 0x13, 0x00, 0x00, 0x3e, 0x6c, 0xed])
    mqttHost = options.mqttServer
    mqttPort = options.mqttPort
    mqttc = mqtt.Client()
    logging.info("Attempting connection to MQTT broker %s:%d..." % (mqttHost, int(mqttPort)))

    mqttc.connect(mqttHost, int(mqttPort))
    ack = bytearray()
    ack.extend(msg.payload)
    mqttc.publish(msg.topic, ack)

    logging.info("Message send on %s: %s" % (msg.topic, repr(msg.payload)))



