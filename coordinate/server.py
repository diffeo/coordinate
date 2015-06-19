'''Top-level coordinated RPC server.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import
import inspect
import logging
from SocketServer import ThreadingTCPServer, StreamRequestHandler
import threading
import traceback

from coordinate.exceptions import ProgrammerError
import yakonfig

from .base_client import cbor_return, make_cbor_tagmapper
from .job_server import JobQueue

logger = logging.getLogger(__name__)


class MultiBackendProxyObject(object):
    '''handler proxy object which wraps two disparate pieces of server
    business logic: the tree lock service and the job queue service.

    '''

    def __init__(self, do_rpclog=True, module_config=None, module_instances=None):
        self.modules = []
        if module_instances:
            self.modules.extend(module_instances)
        if module_config:
            import importlib
            for mc in module_config:
                pmodule = importlib.import_module(mc['module'])
                pclass = getattr(pmodule, mc['class'])
                self.modules.append(pclass())
        for mo in self.modules:
            self._compose_members(mo)
        self.do_rpclog = do_rpclog

    def _compose_members(self, ob):
        # cause this object to pass through all public methods of ob
        for member_name, ob_member in inspect.getmembers(ob, inspect.ismethod):
            if member_name.startswith('_'):
                continue
            if hasattr(self, member_name):
                raise ProgrammerError(
                    'member collision, already have {!r}, got {!r} from {!r}'
                    .format(getattr(self, member_name), ob_member, ob))
            setattr(self, member_name, ob_member)


# helper for CborRpcHandler
def _error_response(msg_id, msg):
    return {
        'id': msg_id,
        'error': {
            'message': msg,
        }
    }


# helper for CborRpcHandler
def _response(msg_id, result):
    return {
        'id': msg_id,
        'result': result,
    }


class CborRpcHandler(StreamRequestHandler):
    '''
    instantiated once per connection to the server

    has a reference to a proxy_object which it calls methods on per request.
    '''
    def __init__(self, *args, **kwargs):
        self.proxy_object = kwargs.pop('proxy_object', self)
        # LOL, standard library stuff is old style class
        #
        # super(CborRpcHandler, self).__init__(*args, **kwargs)
        #
        # So, call the super constructor the old way ...
        # Work gets done in the constructor! WTF!
        StreamRequestHandler.__init__(self, *args, **kwargs)

    def handle(self):
        if self.proxy_object.do_rpclog:
            mlog = logging.getLogger('coordinate.server.rpc')
        tagmapper = make_cbor_tagmapper()
        while True:
            try:
                message = tagmapper.load(self.rfile)
                if self.proxy_object.do_rpclog:
                    mlog.debug('request %r', message)
                if message is None:
                    return
                rval = self.handle_message(message)
                if self.proxy_object.do_rpclog:
                    mlog.debug('response %r', rval)
                returnbytes = cbor_return(tagmapper, rval)
                self.wfile.write(returnbytes)
            except EOFError:
                # ok
                return

    def handle_message(self, message):
        if isinstance(message, list):
            # handle batched requests
            return [self.handle_message(msg) for msg in message]

        message_id = message.get('id')
        method_name = message.get('method')
        params = message.get('params')

        try:
            method = getattr(self.proxy_object, method_name)
            if isinstance(params, dict):
                rval = method(**params)
            elif isinstance(params, list):
                rval = method(*params)
            else:
                return _error_response(message_id,
                                       "don't understand params: {0!r}"
                                       .format(params))
            return _response(message_id, rval)
        except Exception as ex:
            logger.error('returning with Exception: %s', ex, exc_info=True)
            return _error_response(message_id, repr(ex) + '\n' + traceback.format_exc())


def LockdHandlerFactoryFactory(proxy_object):
    # Yup, a factory factory, not just for Java anymore!
    #
    # Why? Minimal scope capture. Only the arguments to this function
    # are in the scope capture of the inner function.
    def LockdHandlerFactory(*args, **kwargs):
        # Factory is standin for the constructor CborRpcHandler(), but with
        # more context (the proxy_object).
        #
        # Called by TCPServer() framework to get a handler for a connection.
        kwargs['proxy_object'] = proxy_object
        it = CborRpcHandler(*args, **kwargs)
        return it
    return LockdHandlerFactory


class CoordinateServer(object):
    '''
    main context object of a running server.
    work hapens inside a run() which contains the event loop and runs forever.
    '''
    def __init__(self, host=None, port=None, config=None, httpd=None):
        self.config = config or yakonfig.get_global_config(
            'coordinate', 'server')
        self.host = host or self._cfget('host')
        self.port = port or self._cfget('port')
        self.do_rpclog = self._cfget('rpclog')

        # TCPServer
        self.server = None
        # There is one server state instance which is the target
        # for all request handlers.
        # Methods should synchronize as needed and be thread safe.
        self.pobj = MultiBackendProxyObject(
            self.do_rpclog,
            module_config=self._cfget('modules'),
            module_instances=[JobQueue()]
        )

        self.httpd_params = httpd or self._cfget('httpd')
        if self.httpd_params is not None and ':' not in self.httpd_params:
            raise ProgrammerError(
                'httpd config needs ip:port or :port to serve on, got {!r}'
                .format(self.httpd_params))
        self.httpd = None
        self.httpd_thread = None

    config_name = 'server'
    default_config = {
        'host': 'localhost',
        'port': 5932,
        'httpd': None,
        'rpclog': False,  # noisy cpu intense log. enable for debugging.
        'modules': [], # [{'module':str, 'class':str}, ...]
    }

    def _cfget(self, name):
        if name in self.config:
            return self.config[name]
        return self.default_config[name]

    def run(self):
        if self.httpd_params:
            self.run_httpd()
        logger.info('serving on %s:%s', self.host, self.port)
        self.server = ThreadingTCPServer(
            (self.host, self.port),
            LockdHandlerFactoryFactory(self.pobj)
        )
        self.server.serve_forever()

    def run_httpd(self):
        host, port = self.httpd_params.split(':')
        port = int(port)
        from wsgiref.simple_server import make_server
        for mo in self.pobj.modules:
            if hasattr(mo, 'wsgi'):
                app = mo.wsgi()
                if not app:
                    continue
                self.httpd = make_server(host, port, app)
                self.httpd_thread = threading.Thread(target=self.httpd.serve_forever)
                self.httpd_thread.daemon = True
                logger.info('serving http info on %s:%s', host, port)
                self.httpd_thread.start()
                return
                # TODO: do some sort of path routing and allow multiple modules to export wsgi handlers
