'''Command-line :mod:`coordinate_worker` tool for launching the
worker daemon.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

.. option:: --pidfile /path/to/file.pid

    If ``--pidfile`` is specified, the process ID of the worker is
    written to the named file, which must be an absolute path.

.. option:: --logpath /path/to/file.log

    If ``--logpath`` is specified, log messages from the worker will
    be written to the specified file, which again must be an absolute
    path; this is in addition to any logging specified in the
    configuration file.

 Start a worker as a background task.  The worker may be shut down by
 globally switching to ``mode terminate``, or by ``kill $(cat
 /path/to/file.pid)``.

'''
from __future__ import absolute_import
import argparse
import lockfile
import logging
import os
import sys
import time
import traceback

import daemon

import dblogger
import coordinate
from coordinate.exceptions import NoSuchWorkSpecError, NoSuchWorkUnitError
from coordinate.workers import run_worker, MultiWorker, SingleWorker, \
    ForkWorker
import yakonfig

logger = logging.getLogger(__name__)

def absolute_path(string):
    '''"Convert" a string to a string that is an absolute existing path.'''
    if not os.path.isabs(string):
        msg = '{0!r} is not an absolute path'.format(string)
        raise argparse.ArgumentTypeError(msg)
    if not os.path.exists(os.path.dirname(string)):
        msg = 'path {0!r} does not exist'.format(string)
        raise argparse.ArgumentTypeError(msg)
    return string

def args_run_worker(parser):
    parser.add_argument('--pidfile', metavar='FILE', type=absolute_path,
                        help='file to hold process ID of worker')
    parser.add_argument('--logpath', metavar='FILE', type=absolute_path,
                        help='file to receive local logs')
    parser.add_argument('--foreground', action='store_true',
                        help='do not run the worker as a daemon')

def start_logging(gconfig, logpath):
    '''Turn on logging and set up the global config.

    This expects the :mod:`yakonfig` global configuration to be unset,
    and establishes it.  It starts the log system via the :mod:`dblogger`
    setup.  In addition to :mod:`dblogger`'s defaults, if `logpath` is
    provided, a :class:`logging.handlers.RotatingFileHandler` is set to
    write log messages to that file.

    This should not be called if the target worker is
    :class:`coordinate.workers.ForkWorker`, since that manages logging on
    its own.

    :param dict gconfig: the :mod:`yakonfig` global configuration
    :param str logpath: optional location to write a log file

    '''
    yakonfig.set_default_config([coordinate, dblogger], config=gconfig)
    if logpath:
        formatter = dblogger.FixedWidthFormatter()
        # TODO: do we want byte-size RotatingFileHandler or TimedRotatingFileHandler?
        handler = logging.handlers.RotatingFileHandler(
            logpath, maxBytes=10000000, backupCount=3)
        handler.setFormatter(formatter)
        logging.getLogger('').addHandler(handler)

def start_worker(which_worker, config={}):
    '''Start some worker class.

    :param str which_worker: name of the worker
    :param dict config: ``coordinate`` config block

    '''
    if which_worker == 'multi_worker':
        cls = MultiWorker
    elif which_worker == 'fork_worker':
        cls = ForkWorker
    else:
        # Don't complain too hard, just fall back
        cls = ForkWorker
    return run_worker(cls, config)

def go(gconfig, args):
    '''Actually run the worker.

    This does some required housekeeping, like setting up logging for
    :class:`~coordinate.workers.MultiWorker` and establishing the global
    :mod:`yakonfig` configuration.  This expects to be called with the
    :mod:`yakonfig` configuration unset.

    :param dict gconfig: the :mod:`yakonfig` global configuration
    :param args: command-line arguments

    '''
    rconfig = gconfig['coordinate']
    which_worker = rconfig.get('worker', 'fork_worker')
    if which_worker == 'fork_worker':
        yakonfig.set_default_config([coordinate], config=gconfig)
    else:
        start_logging(gconfig, args.logpath)
    return start_worker(which_worker, rconfig)

def fork_worker(gconfig, args):
    '''Run the worker as a daemon process.

    This uses :mod:`daemon` to run the standard double-fork, so it can
    return immediately and successfully in the parent process having forked.

    :param dict gconfig: the :mod:`yakonfig` global configuration
    :param args: command-line arguments

    '''
    if args.pidfile:
        pidfile_lock = lockfile.FileLock(args.pidfile)
    else:
        pidfile_lock = None
    with daemon.DaemonContext(pidfile=pidfile_lock, detach_process=True):
        try:
            if args.pidfile:
                with open(args.pidfile, 'w') as f:
                    f.write(str(os.getpid()))
            go(gconfig, args)
        except Exception, exc:
            logp = logpath or os.path.join('/tmp', 'coordinate-failure.log')
            open(logp, 'a').write(traceback.format_exc(exc))
            raise

def main():
    parser = argparse.ArgumentParser(
        description='run a coordinate work-handling daemon')
    args_run_worker(parser)
    args = yakonfig.parse_args(parser, [yakonfig, coordinate])
    gconfig = yakonfig.get_global_config()
    yakonfig.clear_global_config()
    if args.foreground:
        go(gconfig, args)
    else:
        fork_worker(gconfig, args)


if __name__ == '__main__':
    main()
