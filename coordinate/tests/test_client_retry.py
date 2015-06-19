from __future__ import absolute_import
import logging
import socket

import cbor

from coordinate.base_client import CborRpcClient

logger = logging.getLogger(__name__)


class FakeSocket(object):
    def __init__(self, recvdata):
        self.recvdata = recvdata
        self.recvpos = 0
        self.outlist = []

    def recv(self, num):
        logger.info('recv %s@%s', num, self.recvpos)
        out = self.recvdata[self.recvpos:self.recvpos+num]
        self.recvpos += num
        return out

    def send(self, data):
        logger.info('send %s', len(data))
        self.outlist.append(data)


class TestClient(CborRpcClient):
    def __init__(self, connlist, config=None):
        if config is None:
            config = {}
        config['base_retry_seconds'] = 0.01
        config['address'] = ('nonesuch', -1)
        super(TestClient, self).__init__(config=config)
        self.connlist = connlist

    def _conn(self):
        if self._socket is None:
            out = self.connlist.pop(0)
            if isinstance(out, Exception):
                raise out
            self._socket = out
        return self._socket


def test_basic_retry():
    connectfail = socket.error("connect fail")
    recvdata = cbor.dumps({'id': 1, 'result': 'ok'})
    fs = FakeSocket(recvdata)
    tc = TestClient([connectfail, connectfail, fs])

    xok = tc._rpc('foo', ['ha', 'ha'])
    assert xok == 'ok'


def test_toomany_retries():
    connectfail = socket.error("connect fail")
    recvdata = cbor.dumps({'id': 1, 'result': 'ok'})
    fs = FakeSocket(recvdata)
    tc = TestClient([connectfail, connectfail, fs], config={'retries': 1})

    try:
        tc._rpc('foo', ['ha', 'ha'])
    except Exception as ex:
        assert ex is connectfail
        assert len(tc.connlist) == 1
        return
    assert False, "shouldn't get here"
