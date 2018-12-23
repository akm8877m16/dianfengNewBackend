# -*- coding:utf-8 -*-
"""
This is an example showing how to make the scheduler into a remotely accessible service.
It uses RPyC to set up a service through which the scheduler can be made to add, modify and remove
jobs.

To run, first install RPyC using pip. Then change the working directory to the ``rpc`` directory
and run it with ``python -m server``.
"""

import rpyc
from rpyc.utils.server import ThreadedServer
from datetime import datetime
from pymongo import MongoClient
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
import paho.mqtt.client as mqtt
import logging

logging.basicConfig()

# 定时时间到具体需要执行的任务
def send_msg(topic, payload):
    mqttClient = mqtt.Client()
    mqttClient.connect("39.104.49.84", 1883)
    mqttClient.publish(topic, payload)
    mqttClient.disconnect()


class SchedulerService(rpyc.Service):
    def exposed_add_job(self, *args, **kwargs):
        result = scheduler.add_job(send_msg, *args, **kwargs)
        print result
        return result

    def exposed_modify_job(self, job_id, **changes):
        result = scheduler.modify_job(job_id, jobstores, **changes)
        print result
        return result

    def exposed_reschedule_job(self, job_id, trigger=None, **trigger_args):
        result = scheduler.reschedule_job(job_id, jobstores, trigger, **trigger_args)
        print result
        return result

    def exposed_pause_job(self, job_id):
        result = scheduler.pause_job(job_id)
        print result
        return result

    def exposed_resume_job(self, job_id):
        result = scheduler.resume_job(job_id)
        print result
        return result

    def exposed_remove_job(self, job_id, jobstore=None):
        scheduler.remove_job(job_id)

    def exposed_get_job(self, job_id):
        result = scheduler.get_job(job_id)
        print result
        return result

    def exposed_get_jobs(self):
        result = scheduler.get_jobs(jobstores)
        print result
        return result


if __name__ == '__main__':
    # MongoDB 参数
    '''
    host = 'localhost'
    port = 27017
    client = MongoClient(host, port)
    '''
    url = 'mysql://root:Ywh@68531026!@localhost:3306/dianfeng'
    # 存储方式
    jobstores = {
        'default': SQLAlchemyJobStore(url=url),
    }

    executors = {
        'default': ThreadPoolExecutor(10),
        'processpool': ProcessPoolExecutor(3)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 3
    }
    scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
    scheduler.start()
    protocol_config = {'allow_public_attrs': True, 'allow_pickle':True}
    server = ThreadedServer(SchedulerService, port=12345, protocol_config=protocol_config)
    try:
        server.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        scheduler.shutdown()
