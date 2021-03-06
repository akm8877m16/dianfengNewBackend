# -*- coding:utf-8 -*-
__author__ = 'wenhao Yin <akm8877m16@126.com>'
__copyright__ = 'Copyright 2016 wenhao'

'''
    mqtt Worker:  handle mqtt messages and put them to mongodb

'''

from celery import Celery
import config


app = Celery('celeryTasks',
             broker='pyamqp://guest@localhost//',
             include=['celeryTasks.tasks', 'celeryTasks.repeatTasks'])
app.config_from_object(config)
app.conf.task_routes = {'celeryTasks.repeatTasks.*': {'queue': 'repeat'}}