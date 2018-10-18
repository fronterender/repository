# -*- encoding:utf-8 -*-
import tornado.httpserver
import tornado.ioloop
import tornado.web
from QueryData import QueryData


class IndexHandler(tornado.web.RequestHandler):
    # def get(self):
    #     self.write("welcome to vinston english")
    print ("\033[1;34;40m")
    print ("程序启动")
    def post(self):
        self.write("welcome to vinston english")

if __name__ == "__main__":
    app = tornado.web.Application(handlers=[(r"/", IndexHandler)])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(9000)
    tornado.ioloop.IOLoop.instance().start()
