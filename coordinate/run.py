#!/usr/bin/env python
'''Run the coordinate daemon.

.. Your use of this software is governed by your license agreement.
   Copyright 2014 Diffeo, Inc.

:program:`coordinated` provides a job-queue service

:program:`coordinated` supports the standard :option:`--config
<yakonfig --config>`, :option:`--dump-config <yakonfig
--dump-config>`, :option:`--verbose <dblogger --verbose>`,
:option:`--quiet <dblogger --quiet>`, and :option:`--debug <dblogger
--debug>` options.  It takes several additional command-line options.
All of these are optional; the defaults run a coordinated server
listening only on the localhost interface on port 5932 without
snapshotting or the optional Web interface.

:program:`coordinated` takes several additional command-line options.

.. program:: coordinated

.. option:: --host <localhost>

    Listen for connections on a specific IP address, or ``0.0.0.0``
    for all addresses

.. option:: --port <5932>

    Listen for connections on a specific TCP port

.. option:: --pid <file>

    Write the process ID of the server to `file`

.. option:: --snapshot-dir <dir>

    Write periodic snapshots of job server state to `dir`

.. option:: --httpd <ip:port>

    Run a minimal Web server providing job statistics on the specified
    `ip:port`

The server itself has equivalent YAML configuration:

.. code-block:: yaml

    coordinate:
      server:
        host: localhost
        port: 5932
        httpd: localhost:5933

Programs that use :program:`coordinated` are generally configured
via yaml files that must include this block:

.. code-block:: yaml

      coordinate:
        namespace: *namespace
        address: [localhost, 5932]

'''
from __future__ import absolute_import

import argparse
import os
import pdb
import sys
import threading
import time

try:
    import yappi
except:
    yappi = None

import dblogger
import yakonfig

import coordinate
from coordinate.server import CoordinateServer


## yappi profiling glue
def _ycfstr(x):
    "YChildFuncStat string my way"
    if x.ncall != x.nactualcall:
        callstr = '{}/{}'.format(x.nactualcall, x.ncall)
    else:
        callstr = str(x.ncall)
    return '\t{0.ttot}\t{0.tsub}\t{0.tavg}\t{1}\t{0.full_name}'.format(x, callstr)

def _yfstr(x):
    "YFuncStat to string, my way"
    if x.ncall != x.nactualcall:
        callstr = '{}/{}'.format(x.nactualcall, x.ncall)
    else:
        callstr = str(x.ncall)
    children = ''
    if x.children:
        children = '\n'.join(map(_ycfstr, sorted(x.children, key=lambda z: z.ttot, reverse=True)))
        children = '\n' + children
    return '{0.ttot}\t{0.tsub}\t{0.tavg}\t{1}\t{0.full_name}{2}\n'.format(x, callstr, children)

def yappi_logger(path):
    while True:
        time.sleep(60)
        yf = yappi.get_func_stats()
        they = sorted(yf, key=lambda x: x.ttot, reverse=True)
        #they = sorted(filter(lambda x: not x.builtin, yf), key=lambda x: x.ttot, reverse=True)
        opath = path + time.strftime('%Y%m%d_%H%M%S') + '.txt'
        with open(opath, 'wb') as fout:
            if True:
                sum_func_time = sum(map(lambda x: x.tsub, they))
                fout.write('total {} seconds function time\n'.format(sum_func_time))
                fout.write('func+subs\tfunc\ttot/call\tncalls\n')
                for x in they:
                    fout.write(_yfstr(x))
            else:
                yf.print_all(
                    out=fout,
                    columns={
                        0:("name",80),
                        1:("ncall", 9),
                        2:("tsub", 8),
                        3:("ttot", 8),
                        4:("tavg",8),
                    }
                )
        sys.stderr.write('wrote profiling\n{}\n'.format(opath))
        if os.path.exists('bobreak'):
            pdb.set_trace()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default=None,  # NOT -h, that's help
                    help='host that coordinated will listen on, '
                    '0.0.0.0 for any input interface')
    ap.add_argument('--port', '-p', type=int, default=None,
                    help='port number that coordinated will listen on')
    ap.add_argument('--pid', default=None,
                    help='file to write pid to')
    ap.add_argument('--snapshot-dir', default=None,
                    help='direcotry to write snapshots to')
    ap.add_argument('--httpd', default=None,
                    help='ip:port or :port to serve http info on')
    if yappi is not None:
        ap.add_argument('--yappi', default=None, help='file to write yappi profiling to. will be suffied by {timestamp}.txt')
    args = yakonfig.parse_args(ap, [yakonfig, dblogger, coordinate])

    if args.pid:
        with open(args.pid, 'w') as f:
            f.write(str(os.getpid()))

    if args.snapshot_dir is not None:
        cjqconfig = yakonfig.get_global_config('coordinate', 'job_queue')
        # (This modifies the global configuration in place)
        cjqconfig['snapshot_path_format'] = os.path.join(
            args.snapshot_dir, 'snapshot_{timestamp}')

    if (yappi is not None) and args.yappi:
        yappi.start()
        yt = threading.Thread(target=yappi_logger, args=(args.yappi,))
        yt.daemon = True
        yt.start()

    daemon = CoordinateServer(host=args.host, port=args.port, httpd=args.httpd)
    daemon.run()


if __name__ == '__main__':
    main()
