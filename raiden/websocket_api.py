import os
import json

from geventwebsocket.server import WebSocketServer
from geventwebsocket.resource import Resource, WebSocketApplication
from geventwebsocket.protocols.wamp import WampProtocol, export_rpc

from raiden.raiden_service import (
    RaidenAPI,
    RaidenService,
    NoPathError,
    InvalidAddress,
    InvalidAmount
)



#  monkey patch: gevent-websocket to support 'extra' argument
class _Resource(Resource):
    def __init__(self, apps=None, extra=None):
        super(_Resource, self).__init__(apps)
        assert type(extra) is dict or None
        if extra is not None:
            self.extra = extra

    def __call__(self, environ, start_response):
        environ = environ
        is_websocket_call = 'wsgi.websocket' in environ
        current_app = self._app_by_path(environ['PATH_INFO'], is_websocket_call)

        if current_app is None:
            raise Exception("No apps defined")

        if is_websocket_call:
            ws = environ['wsgi.websocket']
            extra = self.extra
            # here the WebSocketApplication objects get constructed
            current_app = current_app(ws, extra)
            current_app.ws = ws  # TODO: needed?
            current_app.handle()
            # Always return something, calling WSGI middleware may rely on it
            return []
        else:
            return current_app(environ, start_response)

class _WampProtocol(WampProtocol):

    def __init__(self, *args, **kwargs):
        super(_WampProtocol,self).__init__(*args, **kwargs)

    def on_message(self, message):
        # FIX: handle when ws is already closed (message is None)
        if message is None:
            return

        data = json.loads(message)

        if not isinstance(data, list):
            raise Exception('incoming data is no list')

        if data[0] == self.MSG_PREFIX and len(data) == 3:
            prefix, uri = data[1:3]
            self.prefixes.add(prefix, uri)

        elif data[0] == self.MSG_CALL and len(data) >= 3:
            return self.rpc_call(data)

        elif data[0] in (self.MSG_SUBSCRIBE, self.MSG_UNSUBSCRIBE,
                         self.MSG_PUBLISH):
            return self.pubsub_action(data)
        else:
            raise Exception("Unknown call")


class WebsocketAPI(WebSocketApplication):
    protocol_class = _WampProtocol

    def __init__(self, ws, extra=None):
        super(WebsocketAPI, self).__init__(ws)
        # self.extra = extra
        # self.raiden_app = self.extra['raiden_app']
        # setattr(self.handler, 'ui_service', self)
        self.api = extra['raiden_app'].raiden.api
        self.port = extra['ws_port']

    def register_pubsub(self, topic):
        # if isinstance(topic, str):
        #     self.protocol.register_pubsub(
        #         "http://localhost:{}/raiden#{}".format(self.port, topic))
        # else:
        #     raise Exception('Topic subscription not supported')
        pass

    def on_open(self):
        # register UIHandler object for RPC-calls:
        self.protocol.register_object(
            "http://localhost:{}/api#".format(self.port), self.api)
        # register all PubSub topics:
        # for topic in self.registrars:
        #     self.register_pubsub(topic)
        #     print 'Publish URI created: /raiden#{}'.format(topic)
        print "WebsocketAPI running\n"

    def on_message(self, message):
        # FIXME: handle client reload/reconnect

        print "message: ", message
        if message is None:
            return
        super(UIService, self).on_message(message)

    def on_close(self, reason):
        print "closed"



# class WebUI(object):
#     """ Wrapping class to start ws/http server
#     """
#     def __init__(self, handler, registrars=None, port=8080):
#         self.handler = handler
#         self.port = self.handler.port = port
#         self.path = os.path.dirname(__file__)
#         if registrars is None:
#             registrars = ['transfer_cb']
#         self.registrars = registrars
#
#     def make_static_application(self, basepath, staticdir):
#         def content_type(path):
#             """Guess mime-type
#             """
#
#             if path.endswith(".css"):
#                 return "text/css"
#             elif path.endswith(".html"):
#                 return "text/html"
#             elif path.endswith(".jpg"):
#                 return "image/jpeg"
#             elif path.endswith(".js"):
#                 return "text/javascript"
#             else:
#                 return "application/octet-stream"
#
#         def not_found(environ, start_response):
#             start_response('404 Not Found', [('content-type', 'text/html')])
#             return ["""<html><h1>Page not Found</h1><p>
#                        That page is unknown. Return to
#                        the <a href="/">home page</a></p>
#                        </html>""", ]
#
#         def app(environ, start_response):
#             path = environ['PATH_INFO']
#             if path.startswith(basepath):
#                 path = path[len(basepath):]
#                 path = os.path.join(staticdir, path)
#                 if os.path.exists(path):
#                     h = open(path, 'r')
#                     content = h.read()
#                     h.close()
#                     headers = [('Content-Type', content_type(path))]
#                     start_response("200 OK", headers)
#                     return [content, ]
#             return not_found(environ, start_response)
#         return app
#
#     def serve_index(self, environ, start_response):
#         path = os.path.join(self.path, 'webui/index.html')
#         start_response("200 OK", [("Content-Type", "text/html")])
#         return open(path).readlines()
#
#     def run_websocket_server(self):
#         static_path = os.path.join(self.path, 'webui')
#
#         routes = [('^/static/', self.make_static_application('/static/', static_path)),
#                   ('^/$', self.serve_index),
#                   ('^/ws$', APIServer)
#                   ]
#
#         data = {  # object
#                 'raiden_app': self  # list of topics
#                 }
#         resource = _Resource(routes, extra=data)
#
#         server = WebSocketServer(("", self.port), resource, debug=True)
#         server.serve_forever()
#
#     def stop():
#         raise NotImplementedError()
