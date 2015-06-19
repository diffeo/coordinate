from __future__ import absolute_import
import logging

import pytest

from coordinate.server import CborRpcHandler

class TestServer(CborRpcHandler):
    def __init__(self, proxy_object):
        self.proxy_object = proxy_object
        # DON'T call super constructor.

class TestProxyObject(object):
    def fail(self):
        raise Exception('la la la')

    def assertfail(self):
        assert False, 'everything is wrong'

def test_handle_message_exception():
    server = TestServer(proxy_object=TestProxyObject())

    response = server.handle_message({'id':1, 'method':'fail', 'params': []})
    print response['error']['message']
    assert 'la la la' in response['error']['message']

    response = server.handle_message({'id':1, 'method':'assertfail', 'params': []})
    print response['error']['message']
    assert 'everything is wrong' in response['error']['message']
