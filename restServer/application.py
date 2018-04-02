# -*- coding:utf-8 -*-
from url import urls
import tornado.web
import motor.motor_tornado
import redis
from sqlalchemy import create_engine

class Application(tornado.web.Application):
    def __init__(self):
        handlers = urls
        settings = dict(

        )
        super(Application, self).__init__(handlers, **settings)
        # Have one global connection to the blog DB across all handlers
        conn = motor.motor_tornado.MotorClient('localhost', 27017)
        self.db = conn["dianfeng"]
        self.redisPool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
        self.engine = create_engine('mysql+pymysql://root:Ywh@68531026!@localhost:3306/dianfeng?charset=utf8',
                                    echo=False, pool_recycle=3600)  # True will turn on the logging
