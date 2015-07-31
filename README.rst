Coordinate
==========

Coordinate is a standalone server process and client library for
multiple-system coordination and synchronization capabilities. It
manages work specifications and work units, weighted scheduling,
priority scheduling, and dependency graph.

Configuration and execution
---------------------------

In a global yakonfig config.yaml configuration file, add ``coordinate``
at the top level:

::

    coordinate:
      server:
        host: 0.0.0.0
        port: 5932
      namespace: *namespace
      registry_addresses: ['10.0.1.x:5932']

Run ``coordinated`` with this configuration, or manually specify
``--host`` and ``--port``. The default configuration is to only listen
on the localhost interface on port 5932.

The main interface is through the ``TaskMaster`` object. This
implementation makes RPC calls to the coordinated server.

The server is pluggable and additional functionality can be through
other server objects similar to the JobQueue class.

RPC system
----------

Coordinated uses a simple RPC system based on CBOR messages. Client
processes initiate a TCP connection as indicated in the configuration
and send CBOR messages like (JSON syntax):

::

    {"id": 1,
     "method": "lock",
     "params": [...]}

where ``id`` is a sequential number per client object, starting at 1.
There is one TCP connection per client object, but this connection is
held open until explicitly closed.

The responses look like:

::

    {"id": 1,
     "result": "value"}

Or:

::

    {"id": 2,
     "error": {"message": "No such method"}}

If the server-side call raised an exception, the formatted traceback is
included as the error message.

The server supports a CBOR list of request messages, and will return a
CBOR list of responses.

Client classes subclass ``coordinated._cbor_rpc_client.CborRpcClient``
and call ``_rpc()`` to perform a call. This method blocks on the
connection and response. If the server returned the error form, this is
raised as an exception.

The server is run with a proxy object, presently
``coordinated.lockd.MultiBackendProxyObject``, and calls the requested
methods on that object with the requested parameters.
