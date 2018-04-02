# -*- coding:utf-8 -*-

import tornado.web
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from tornado.ioloop import IOLoop

MAX_WORKERS = 5

class CeleryResultMixin(object):
    """
    Adds a callback function which could wait for the result asynchronously
    """
    def wait_for_result(self, task, callback):
        if task.ready():
            callback(task.result)
        else:
            # TODO: Is this going to be too demanding on the result backend ?
            # Probably there should be a timeout before each add_callback
            tornado.ioloop.IOLoop.instance().add_callback(
                partial(self.wait_for_result, task, callback)
            )

class BaseHandler(CeleryResultMixin, tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db

    @property
    def redisPool(self):
        return self.application.redisPool

    @property
    def engine(self):
        return self.application.engine

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

