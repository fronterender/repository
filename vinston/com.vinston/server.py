import tornado.httpserver
import tornado.ioloop
import tornado.web



class IndexHandler(tornado.web.RequestHandler):
    def get(self):

        self.write("welcome to vinston english")

if __name__ == "__main__":
    app = tornado.web.Application(handlers=[(r"/", IndexHandler)])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(9000)
    tornado.ioloop.IOLoop.instance().start()
