# -*- coding:utf-8 -*-
from tornado.options import define, options
import tornado.httpserver
import tornado.ioloop
import tornado.options
from application import Application

define("port", default=8888, help="run on the given port", type=int)

def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()
    print ("http server start at "+ options.port)
if __name__ == "__main__":
    main()
