'''Base class for client code.

Task-specific client classes should derive from :class:`CborRpcClient`.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

.. autoclass:: CborRpcClient

'''
from __future__ import absolute_import
import logging
import random
import socket
import time
from uuid import UUID

from cbor.cbor import CBOR_TAG_CBOR, Tag
from cbor.tagmap import ClassTag, TagMapper


logger = logging.getLogger(__name__)

CBOR_TAG_UUID = 37
DIFFEO_CBOR_TAG_TUPLE = 128


def make_cbor_tagmapper():
    '''Make a :class:`cbor.TagMapper`.

    This knows how to manage CBOR-encoded-CBOR and UUID tags.

    :returntype: :class:`cbor.TagMapper`

    '''
    tagmapper = None
    uuid_tag = ClassTag(CBOR_TAG_UUID, UUID,
                        lambda uuid: uuid.get_bytes(),
                        lambda b: UUID(bytes=b))
    # TagMapper.decode() is non-recursive for tagged objects :-(
    tuple_tag = ClassTag(DIFFEO_CBOR_TAG_TUPLE, tuple,
                         lambda t: [tagmapper.encode(v) for v in t],
                         lambda l: tuple(tagmapper.decode(v) for v in l))
    # Same for WrappedCBOR
    cbor_tag = ClassTag(CBOR_TAG_CBOR, None,
                        lambda obj: tagmapper.dumps(obj),
                        lambda b: tagmapper.loads(b))
    tagmapper = TagMapper([cbor_tag, uuid_tag, tuple_tag],
                          raise_on_unknown_tag=True)
    return tagmapper


def cbor_return(tagmapper, obj):
    '''Return `obj` as CBOR-encoded CBOR.'''
    return tagmapper.dumps(Tag(CBOR_TAG_CBOR, tagmapper.dumps(obj)))


class SocketReader(object):
    '''
    Simple adapter from socket.recv to file-like-read
    '''
    def __init__(self, sock):
        self.socket = sock
        self.timeout_seconds = 10.0

    def read(self, num):
        start = time.time()
        data = self.socket.recv(num)
        while len(data) < num:
            now = time.time()
            if now > (start + self.timeout_seconds):
                break
            ndat = self.socket.recv(num - len(data))
            if ndat:
                data += ndat
        return data


class CborRpcClient(object):
    '''Base class for all client objects.

    This provides common `addr_family`, `address`, and `registry_addresses`
    configuration parameters, and manages the connection back to the server.

    Automatic retry and time based fallback is managed from
    configuration parameters `retries` (default 5), and
    `base_retry_seconds` (default 0.5). Retry time doubles on each
    retry. E.g. try 0; wait 0.5s; try 1; wait 1s; try 2; wait 2s; try
    3; wait 4s; try 4; wait 8s; try 5; FAIL. Total time waited just
    under base_retry_seconds * (2 ** retries).

    .. automethod:: __init__
    .. automethod:: _rpc
    .. automethod:: close

    '''

    def __init__(self, config=None):
        self._socket_family = config.get('addr_family', socket.AF_INET)
        self._socket_addr = None
        # may need to be ('host', port)
        if self._socket_family == socket.AF_INET:
            addr = config.get('address')
            registry_addresses = config.get('addresses')
            if (not addr) and registry_addresses:
                addr = random.choice(registry_addresses)
            if isinstance(addr, tuple):
                self._socket_addr = addr
            elif isinstance(addr, list):
                self._socket_addr = tuple(addr)
            elif isinstance(addr, (basestring, str, unicode)):
                host, port = addr.rsplit(':', 1)
                port = int(port)
                self._socket_addr = (host, port)
            if self._socket_addr is not None:
                # python socket standard library insists this be tuple!
                assert isinstance(self._socket_addr, tuple)
                assert len(self._socket_addr) == 2, 'address must be length-2 tuple ("hostname", port number), got {!r} tuplified to {!r}'.format(self._socket_addr, tsocket_addr)
        self._socket = None
        self._rfile = None
        self._local_addr = None
        self._message_count = 0
        self._retries = config.get('retries', 5)
        self._base_retry_seconds = float(config.get('base_retry_seconds', 0.5))
        self._tagmapper = make_cbor_tagmapper()

    def _conn(self):
        # lazy socket opener
        if self._socket is None:
            try:
                self._socket = socket.create_connection(self._socket_addr)
                self._local_addr = self._socket.getsockname()
            except Exception:
                logger.error('error connecting to %r', self._socket_addr)
                raise
        return self._socket

    def close(self):
        '''Close the connection to the server.

        The next RPC call will reopen the connection.

        '''
        if self._socket is not None:
            self._rfile = None
            try:
                self._socket.shutdown(socket.SHUT_RDWR)
                self._socket.close()
            except socket.error:
                logger.warn('error closing lockd client socket',
                            exc_info=True)
            self._socket = None

    @property
    def rfile(self):
        if self._rfile is None:
            conn = self._conn()
            self._rfile = SocketReader(conn)
        return self._rfile

    def _rpc(self, method_name, params):
        '''Call a method on the server.

        Calls ``method_name(*params)`` remotely, and returns the results
        of that function call.  Expected return types are primitives, lists,
        and dictionaries.

        :raise Exception: if the server response was a failure

        '''
        ## it's really time and file-space consuming to log all the
        ## rpc data, but there are times when a developer needs to.
        mlog = None
        #mlog = logging.getLogger('coordinate.client.rpc')
        tryn = 0
        delay = self._base_retry_seconds
        self._message_count += 1
        message = {
            'id': self._message_count,
            'method': method_name,
            'params': list(params)
        }
        if mlog is not None:
            mlog.debug('request %r', message)
        buf = cbor_return(self._tagmapper, message)

        errormessage = None
        while True:
            try:
                conn = self._conn()
                conn.send(buf)
                response = self._tagmapper.load(self.rfile)
                if mlog is not None:
                    mlog.debug('response %r', response)
                assert response['id'] == message['id']
                if 'result' in response:
                    return response['result']
                # From here on out we got a response, the server
                # didn't have some weird intermittent error or
                # non-connectivity, it gave us an error message. We
                # don't retry that, we raise it to the user.
                errormessage = response.get('error')
                if errormessage:
                    errormessage = errormessage.get('message')
                if not errormessage:
                    errormessage = repr(response)
                break
            except socket.error as ex:
                if tryn < self._retries:
                    tryn += 1
                    logger.debug('ex in %r (%s), retrying %s in %s sec...',
                                 method_name, ex, tryn, delay)
                    self.close()
                    time.sleep(delay)
                    delay *= 2
                    continue
                logger.error('failed in rpc %r %r', method_name, params,
                             exc_info=True)
                raise
        raise Exception(errormessage)
