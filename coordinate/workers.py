'''Coordinate Worker framework.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

Gets work units from the coordinate server, manages configuration of
the local environment, calls the appropriate code to process the work
unit, and manages marking work units as finished or failed.

Other implementation strategies are definitely possible.  The
:class:`SingleWorker` class here will run exactly one job when
invoked.  It is also possible for a program that intends to do some
work, possibly even in parallel, but wants to depend on coordinate daemon for
queueing, to call :meth:`coordinate.TaskMaster.get_work` itself and do
work based on whatever information is in the work spec; that would not
use this worker infrastructure at all.

.. autoclass:: Worker
   :members:

.. autoclass:: SingleWorker
    :members:
    :show-inheritance:

.. autoclass:: LoopWorker
    :members:
    :show-inheritance:

.. autoclass:: ForkWorker
    :members:
    :show-inheritance:

.. autofunction:: run_worker

'''
from __future__ import absolute_import, division, print_function
import abc
from cProfile import Profile
from datetime import datetime
import errno
import logging
import os
import random
import signal
import socket
import struct
import sys
import time
import traceback

import pkg_resources
import psutil
from setproctitle import setproctitle

import dblogger
import coordinate
import yakonfig
from . import TaskMaster
from .exceptions import LostLease, ProgrammerError

logger = logging.getLogger(__name__)


def nice_identifier():
    rbytes = os.urandom(16)
    return ''.join('{:x}'.format(ord(c)) for c in rbytes)


def run_worker(worker_class, *args, **kwargs):
    '''Bridge function to run a worker under :mod:`multiprocessing`.

    The :mod:`multiprocessing` module cannot
    :meth:`~multiprocessing.Pool.apply_async` to a class constructor,
    even if the ``__init__`` calls ``.run()``, so this simple wrapper
    calls ``worker_class(*args, **kwargs)`` and logs any exceptions
    before re-raising them.

    This is usually only used to create a :class:`HeadlessWorker`, but
    it does run through the complete
    :meth:`~coordinate.Worker.register`, :meth:`~coordinate.Worker.run`,
    :meth:`~coordinate.Worker.unregister` sequence with some logging
    on worker-level failures.

    '''
    try:
        worker = worker_class(*args, **kwargs)
    except Exception:
        logger.critical('failed to create worker %r', worker_class,
                        exc_info=True)
        raise
    # A note on style here:
    #
    # If this runs ForkWorker, ForkWorker will os.fork() LoopWorker
    # (or SingleWorker) children, and the child will have this in its
    # call stack.  Eventually the child will sys.exit(), which raises
    # SystemExit, which is an exception that will trickle back through
    # here.  If there is a try:...finally: block, the finally: block
    # will execute on every child exit.  except Exception: won't run
    # on SystemExit.
    try:
        worker.register()
        worker.run()
        worker.unregister()
    except Exception:  # pylint: disable=broad-except
        logger.error('worker %r died', worker_class, exc_info=True)
        try:
            worker.unregister()
        except Exception:  # pylint: disable=broad-except
            pass
        raise


class Worker(object):
    '''Core interface for a worker.

    ..automethod:: __init__
    '''
    __metaclass__ = abc.ABCMeta

    def __init__(self, config, task_master=None):
        '''Create a new worker.

        :param dict config: Configuration for the worker, typically
          the contents of the ``coordinate`` block in the global config

        '''
        #: Configuration for the worker
        self.config = config
        #: Task interface to talk to the data store
        if task_master is None:
            task_master = TaskMaster(config)
        self.task_master = task_master
        #: Worker ID, only valid after :meth:`register`
        self.worker_id = None
        #: Parent worker ID
        self.parent = None
        #: Required maximum time between :meth:`heartbeat`
        self.lifetime = 100 * 60

    def environment(self):
        '''Get raw data about this worker.

        This is recorded in the :meth:`heartbeat` info, and can be
        retrieved by :meth:`TaskMaster.get_heartbeat`.  The dictionary
        includes keys ``worker_id``, ``host``, ``fqdn``, ``version``,
        ``working_set``, and ``memory``.

        '''
        hostname = socket.gethostname()
        aliases = ()
        ipaddrs = ()

        # This sequence isn't 100% reliable.  We might try a socket()
        # sequence like RedisBase._ipaddress(), or just decide that
        # socket.fqdn() and/or socket.gethostname() is good enough.
        try:
            ip = socket.gethostbyname(hostname)
        except socket.herror:
            # If you're here, then $(hostname) doesn't resolve.
            ip = None

        try:
            if ip is not None:
                hostname, aliases, ipaddrs = socket.gethostbyaddr(ip)
        except socket.herror:
            # If you're here, then $(hostname) resolves, but the IP
            # address that results in doesn't reverse-resolve.  This
            # has been observed on OSX at least.
            ipaddrs = (ip,)

        env = dict(
            worker_id=self.worker_id,
            parent=self.parent,
            hostname=hostname,
            aliases=tuple(aliases),
            ipaddrs=tuple(ipaddrs),
            fqdn=socket.getfqdn(),
            version=(pkg_resources  # pylint: disable=no-member
                     .get_distribution("coordinate").version),
            working_set=[(dist.key, dist.version)
                         for dist in pkg_resources.WorkingSet()],
            # config_hash=self.config['config_hash'],
            # config_json = self.config['config_json'],
            memory=psutil.virtual_memory(),
            pid=os.getpid(),
        )
        return env

    def register(self, parent=None):
        '''Record the availability of this worker and get a unique identifer.

        This sets :attr:`worker_id` and calls :meth:`heartbeat`.  This
        cannot be called multiple times without calling
        :meth:`unregister` in between.

        '''
        if self.worker_id:
            raise ProgrammerError('Worker.register cannot be called again '
                                  'without first calling unregister; it is '
                                  'not idempotent')
        self.parent = parent
        self.worker_id = nice_identifier()
        self.task_master.worker_id = self.worker_id
        self.heartbeat()
        return self.worker_id

    def unregister(self):
        '''Remove this worker from the list of available workers.

        This requires the worker to already have been :meth:`register()`.

        '''
        self.task_master.worker_unregister(self.worker_id)
        self.task_master.worker_id = None
        self.worker_id = None

    def heartbeat(self):
        '''Record the current worker state in the registry.

        This records the worker's current mode, plus the contents of
        :meth:`environment`, in the data store for inspection by others.

        :returns mode: Current mode, as :meth:`TaskMaster.get_mode`

        '''
        try:
            mode = self.task_master.get_mode()
            self.task_master.worker_heartbeat(self.worker_id, mode,
                                              self.lifetime,
                                              self.environment(),
                                              parent=self.parent)
            return mode
        except Exception:
            # If there is a server-side failure heartbeating, don't
            # let that stop us from doing our job
            return self.task_master.RUN

    @abc.abstractmethod
    def run(self):
        pass


class SingleWorker(Worker):
    '''Worker that runs exactly one job when called.

    This is used by the :meth:`coordinate.run.Manager.do_run_one`
    command to run a single job; that just calls :meth:`run`.  This
    is also invoked as the child process by :class:`ForkWorker`,
    which calls :meth:`as_child`.

    If the configuration dictionary contains a key `profile`, then
    this will run the standard :mod:`cProfile` profiler.  Under that
    key there are two parameters.  `work_specs` is a list of work spec
    names to profile.  `destination` is a destination filename.  Both
    of these parameters must be passed for profiling to occur.  The
    destination filename is percent formatted, understanding keys:

    `work_spec_name`
      Name of the work spec
    `work_unit_key`
      Name of the specific work unit
    `ymd`
      8-digit year, month, day
    `hms`
      6-digit (24-hour, integral) hour, minute, second

    '''
    def __init__(self, config, task_master=None, work_spec_names=None,
                 max_jobs=1):
        super(SingleWorker, self).__init__(config, task_master)
        self.work_spec_names = work_spec_names
        self.max_jobs = config.get('worker_job_fetch', max_jobs)
        profile_config = config.get('profile', {})
        self.profile_work_specs = profile_config.get('work_specs', [])
        self.profile_destination = profile_config.get('destination', None)

    def run(self, set_title=False):
        '''Do some work.

        The standard implementation here calls :meth:`run_one`.

        :param set_title: if true, set the process's title with the
          work unit name
        :return: :const:`True` if there was a job (even if it failed)

        '''
        return self.run_one(set_title)

    def run_one(self, set_title=False):
        '''Get exactly one job, run it, and return.

        Does nothing (but returns :const:`False`) if there is no work
        to do.  Ignores the global mode; this will do work even
        if :func:`coordinate.TaskMaster.get_mode` returns
        :attr:`~coordinate.TaskMaster.TERMINATE`.

        :param set_title: if true, set the process's title with the
          work unit name
        :return: :const:`True` if there was a job (even if it failed)

        '''
        units = self.task_master.get_work(
            self.worker_id,
            available_gb=psutil.virtual_memory().available / 2**30,
            work_spec_names=self.work_spec_names, max_jobs=self.max_jobs)
        if not units:
            return False
        if not isinstance(units, (list, tuple)):
            # got only one work unit back, package it
            units = [units]
        # Now run all of the units; or if we get a critical failure
        # abandon the ones we haven't run yet
        ok = True
        for unit in units:
            if ok:
                ok = self._run_unit(unit, set_title)
            else:
                try:
                    unit.update(-1)
                except LostLease:
                    pass
                except Exception:  # pylint: disable=broad-except
                    # we're already quitting everything, but this is
                    # weirdly bad.
                    logger.error('failed to release lease on %r %r',
                                 unit.work_spec_name, unit.key,
                                 exc_info=True)
        return ok

    def _run_unit(self, unit, set_title=False):
        try:
            if set_title:
                setproctitle('coordinate worker {0!r} {1!r}'
                             .format(unit.work_spec_name, unit.key))
            profiler = None
            if ((self.profile_destination and
                 unit.work_spec_name in self.profile_work_specs)):
                now = datetime.now()
                unit_info = {
                    'work_spec_name': unit.work_spec_name,
                    'work_unit_key': unit.key,
                    'ymd': now.strftime('%Y%m%d'),
                    'hms': now.strftime('%H%M%S'),
                }
                destination = self.profile_destination % unit_info
                profiler = Profile()
                profiler.enable()
            unit.run()
            if profiler:
                profiler.disable()
                profiler.dump_stats(destination)
            unit.finish()
        except LostLease:
            # We don't own the unit any more so don't try to report on it
            logger.warn('Lost Lease on %r', unit.key)
        except Exception, exc:  # pylint: disable=broad-except
            unit.fail(exc)
        return True

    #: Exit code from :meth:`as_child` if it ran a work unit (maybe
    #: unsuccessfully).
    EXIT_SUCCESS = 0
    #: Exit code from :meth:`as_child` if there was a failure getting
    #: the work unit.
    EXIT_EXCEPTION = 1
    #: Exit code from :meth:`as_child` if there was no work to do.
    EXIT_BORED = 2

    @classmethod
    def as_child(cls, global_config, parent=None):
        '''Run a single job in a child process.

        This method never returns; it always calls :func:`sys.exit`
        with an error code that says what it did.

        '''
        try:
            setproctitle('coordinate worker')
            random.seed()  # otherwise everyone inherits the same seed
            yakonfig.set_default_config([yakonfig, dblogger, coordinate],
                                        config=global_config)
            worker = cls(yakonfig.get_global_config(coordinate.config_name))
            worker.register(parent=parent)
            did_work = worker.run(set_title=True)
            worker.unregister()
            if did_work:
                sys.exit(cls.EXIT_SUCCESS)
            else:
                sys.exit(cls.EXIT_BORED)
        except Exception, e:  # pylint: disable=broad-except
            # There's some off chance we have logging.
            # You will be here if redis is down, for instance,
            # and the yakonfig dblogger setup runs but then
            # the get_work call fails with an exception.
            if len(logging.root.handlers) > 0:
                logger.critical('failed to do any work', exc_info=e)
            else:
                sys.stderr.write('exception: {0}\n{1}\n'
                                 .format(e, traceback.format_exc()))
            sys.exit(cls.EXIT_EXCEPTION)


class LoopWorker(SingleWorker):
    '''Worker that runs jobs for a fixed length of time.

    This does jobs as :meth:`SingleWorker.run_one`.  However,
    the worker itself is configured with a maximum lifetime, and
    if a job finishes before the lifetime has passed, this worker
    will try to do another job.

    '''
    def run(self, set_title=False):
        '''Do some work.

        The standard implementation here calls :meth:`run_loop`.

        :param set_title: if true, set the process's title with the
          work unit name
        :return: :const:`True` if there was a job (even if it failed)

        '''
        return self.run_loop(set_title)

    def run_loop(self, set_title=False):
        # Find our deadline from the global configuration.
        # We don't really love this, but it's data we're already
        # passing across process boundaries.
        now = time.time()
        try:
            duration = yakonfig.get_global_config('coordinate', 'fork_worker',
                                                  'child_lifetime')
        except KeyError:
            duration = 10
        deadline = now + duration

        while time.time() < deadline:
            ret = self.run_one(set_title)
            if not ret:
                break

        return ret


class ForkWorker(Worker):
    '''Parent worker that runs multiple jobs concurrently.

    This manages a series of child processes, each of which runs
    a :class:`SingleWorker`.  It runs as long as the global coordinate
    state is not :class:`coordinate.TaskMaster.TERMINATE`.

    This takes some additional optional configuration options.  A
    typical configuration will look like:

    .. code-block:: yaml

        coordinate:
          # required coordinate configuration
          registry_addresses: [ '127.0.0.1:5932' ]
          app_name: foo_app
          namespace: namespace

          # indicate which worker to use
          worker: fork_worker
          fork_worker:
            # set this or num_workers; num_workers takes precedence
            num_workers_per_core: 1
            # how often to check if there is more work
            poll_interval: 1
            # how often to start more workers
            spawn_interval: 0.01
            # how often to record our existence
            heartbeat_interval: 15
            # minimum time a working worker will live
            child_lifetime: 10
            # kill off jobs this long before their deadlines
            stop_jobs_early: 15

    This spawns child processes to do work.  Each child process does at
    most one work unit.  If `num_workers` is set, at most this many
    concurrent workers will be running at a time.  If `num_workers` is
    not set but `num_workers_per_core` is, the maximum number of workers
    is a multiple of the number of processor cores available.  The
    default setting is 1 worker per core, but setting this higher can
    be beneficial if jobs are alternately network- and CPU-bound.

    The parent worker runs a fairly simple state machine.  It awakens
    on startup, whenever a child process exits, or after a timeout.
    When it awakens, it checks on the status of all of its children,
    and collects the exit status of those that have finished.  If any
    failed or reported no more work, the timeout is set to
    `poll_interval`, and no more workers are started until that
    timeout has passed.  Otherwise, if it is not running the maximum
    number of workers, it starts one exactly and sets the timeout to
    `spawn_interval`.

    This means that if the system is operating normally, and there is
    work to do, then it will start all of its workers in `num_workers`
    times `spawn_interval` time.  If `spawn_interval` is 0, then any
    time the system thinks it may have work to do, it will spawn the
    maximum number of processes immediately, each of which will
    connect to Redis.  If the system runs out of work, or if it starts
    all of its workers, it will check for work or system shutdown
    every `poll_interval`.  The parent worker will contact Redis,
    recording its state and retrieving the global mode, every
    `heartbeat_interval`.

    Every `heartbeat_interval` the parent also checks on the jobs its
    children are running.  If any of them are overdue now or being
    worked on by other workers, the parent will kill them to avoid
    having multiple workers doing the same work unit.  Furthermore, if
    any childrens' jobs will expire within `stop_jobs_early` seconds,
    those jobs will be killed too even if they aren't expired yet, and
    any jobs killed this way will be marked failed if they are still
    owned by the same child worker.  If `stop_jobs_early` is at least
    `heartbeat_interval`, this will reliably cause jobs that take
    longer than the expiry interval (default 300 seconds) to be killed
    off rather than retried.

    '''

    '''Several implementation notes:

    fork() and logging don't mix.  While we're better off here than
    using :mod:`multiprocessing` (which forks from a thread, so the
    child can start up with a log handler locked) there are still
    several cases like syslog or dblogger handlers that depend on a
    network connection that won't be there in the child.

    That means that *nothing in the parent gets to log at all*.  We
    need to maintain a separate child process to do logging.

    ``debug_worker`` is a hidden additional configuration option.  It
    can cause a lot of boring information to get written to the log.
    It is a list of keywords.  ``children`` logs all process creation
    and destruction.  ``loop`` shows what we're thinking in the main
    loop.

    '''

    config_name = 'fork_worker'
    default_config = {
        'num_workers': None,
        'num_workers_per_core': 1,
        'poll_interval': 1,
        'spawn_interval': 0.1,
        'heartbeat_interval': 15,
        'child_lifetime': 10,
        'stop_jobs_early': 15,
        'debug_worker': None,
    }

    @classmethod
    def config_get(cls, config, key):
        return config.get(key, cls.default_config[key])

    def __init__(self, config):
        '''Create a new forking worker.

        This starts up with no children and no logger, and it will
        set itself up and contact Redis as soon as :meth:`run` is called.

        :param dict config: ``coordinate`` config dictionary

        '''
        super(ForkWorker, self).__init__(config)
        c = config.get(self.config_name, {})
        self.num_workers = self.config_get(c, 'num_workers')
        if not self.num_workers:
            num_cores = 0
            # This replicates the Good Unix case of multiprocessing.cpu_count()
            try:
                num_cores = os.sysconf('SC_NPROCESSORS_ONLN')
            except (ValueError, OSError, AttributeError):
                pass
            num_cores = max(1, num_cores)
            num_workers_per_core = self.config_get(c, 'num_workers_per_core')
            self.num_workers = num_workers_per_core * num_cores
        self.poll_interval = self.config_get(c, 'poll_interval')
        self.spawn_interval = self.config_get(c, 'spawn_interval')
        self.heartbeat_interval = self.config_get(c, 'heartbeat_interval')
        self.heartbeat_deadline = time.time() - 1  # due now
        self.child_lifetime = self.config_get(c, 'child_lifetime')
        self.stop_jobs_early = self.config_get(c, 'stop_jobs_early')
        self.debug_worker = c.get('debug_worker', [])
        self.children = set()
        self.log_child = None
        self.log_fd = None
        self.shutting_down = False
        self.last_mode = None
        self.old_sigabrt = None
        self.old_sigint = None
        self.old_sigpipe = None
        self.old_sigterm = None

    @staticmethod
    def pid_is_alive(pid):
        try:
            os.kill(pid, 0)
            # This doesn't actually send a signal, but does still do the
            # "is pid alive?" check.  If we didn't get an exception, it is.
            return True
        except OSError, e:
            if e.errno == errno.ESRCH:
                # "No such process"
                return False
            raise

    def set_signal_handlers(self):
        '''Set some signal handlers.

        These react reasonably to shutdown requests, and keep the
        logging child alive.

        '''
        def handler(f):
            def wrapper(signum, backtrace):
                return f()
            return wrapper

        self.old_sigabrt = signal.signal(signal.SIGABRT,
                                         handler(self.scram))
        self.old_sigint = signal.signal(signal.SIGINT,
                                        handler(self.stop_gracefully))
        self.old_sigpipe = signal.signal(signal.SIGPIPE,
                                         handler(self.live_log_child))
        signal.siginterrupt(signal.SIGPIPE, False)
        self.old_sigterm = signal.signal(signal.SIGTERM,
                                         handler(self.stop_gracefully))

    def clear_signal_handlers(self):
        '''Undo :meth:`set_signal_handlers`.

        Not only must this be done on shutdown, but after every fork
        call too.

        '''
        signal.signal(signal.SIGABRT, self.old_sigabrt)
        signal.signal(signal.SIGINT, self.old_sigint)
        signal.signal(signal.SIGPIPE, self.old_sigpipe)
        signal.signal(signal.SIGTERM, self.old_sigterm)

    def log(self, level, message):
        '''Write a log message via the child process.

        The child process must already exist; call :meth:`live_log_child`
        to make sure.  If it has died in a way we don't expect then
        this will raise :const:`signal.SIGPIPE`.

        '''
        if self.log_fd is not None:
            prefix = struct.pack('ii', level, len(message))
            os.write(self.log_fd, prefix)
            os.write(self.log_fd, message)

    def debug(self, group, message):
        '''Maybe write a debug-level log message.

        In particular, this gets written if the hidden `debug_worker`
        option contains `group`.

        '''
        if group in self.debug_worker:
            if 'stdout' in self.debug_worker:
                print(message)
            self.log(logging.DEBUG, message)

    def log_spewer(self, gconfig, fd):
        '''Child process to manage logging.

        This reads pairs of lines from `fd`, which are alternating
        priority (Python integer) and message (unformatted string).

        '''
        setproctitle('coordinate fork_worker log task')
        yakonfig.set_default_config([yakonfig, dblogger], config=gconfig)
        try:
            while True:
                prefix = os.read(fd, struct.calcsize('ii'))
                level, msglen = struct.unpack('ii', prefix)
                msg = os.read(fd, msglen)
                logger.log(level, msg)
        except Exception, e:
            logger.critical('log writer failed', exc_info=e)
            raise

    def start_log_child(self):
        '''Start the logging child process.'''
        self.stop_log_child()
        gconfig = yakonfig.get_global_config()
        read_end, write_end = os.pipe()
        pid = os.fork()
        if pid == 0:
            # We are the child
            self.clear_signal_handlers()
            os.close(write_end)
            yakonfig.clear_global_config()
            self.log_spewer(gconfig, read_end)
            sys.exit(0)
        else:
            # We are the parent
            self.debug('children', 'new log child with pid {0}'.format(pid))
            self.log_child = pid
            os.close(read_end)
            self.log_fd = write_end

    def stop_log_child(self):
        '''Stop the logging child process.'''
        if self.log_fd:
            os.close(self.log_fd)
            self.log_fd = None
        if self.log_child:
            try:
                self.debug('children', 'stopping log child with pid {0}'
                           .format(self.log_child))
                os.kill(self.log_child, signal.SIGTERM)
                os.waitpid(self.log_child, 0)
            except OSError, exc:
                if exc.errno == errno.ESRCH or exc.errno == errno.ECHILD:
                    # already gone
                    pass
                else:
                    raise
            self.log_child = None

    def live_log_child(self):
        '''Start the logging child process if it died.'''
        if not (self.log_child and self.pid_is_alive(self.log_child)):
            self.start_log_child()

    def do_some_work(self, can_start_more):
        '''Run one cycle of the main loop.

        If the log child has died, restart it.  If any of the worker
        children have died, collect their status codes and remove them
        from the child set.  If there is a worker slot available, start
        exactly one child.

        :param bool can_start_more: Allowed to start a child?
        :return:  Time to wait before calling this function again

        '''
        # any_happy_children = False
        any_sad_children = False
        any_bored_children = False

        self.debug('loop', 'starting work loop, can_start_more={0!r}'
                   .format(can_start_more))

        # See if anyone has died
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except OSError, exc:
                if exc.errno == errno.ECHILD:
                    # No children at all
                    pid = 0
                else:
                    raise
            if pid == 0:
                break
            elif pid == self.log_child:
                self.debug('children',
                           'log child with pid {0} exited'.format(pid))
                self.start_log_child()
            elif pid in self.children:
                self.children.remove(pid)
                if os.WIFEXITED(status):
                    code = os.WEXITSTATUS(status)
                    self.debug('children',
                               'worker {0} exited with code {1}'
                               .format(pid, code))
                    if code == SingleWorker.EXIT_SUCCESS:
                        pass  # any_happy_children = True
                    elif code == SingleWorker.EXIT_EXCEPTION:
                        self.log(logging.WARNING,
                                 'child {0} reported failure'.format(pid))
                        any_sad_children = True
                    elif code == SingleWorker.EXIT_BORED:
                        any_bored_children = True
                    else:
                        self.log(logging.WARNING,
                                 'child {0} had odd exit code {1}'
                                 .format(pid, code))
                elif os.WIFSIGNALED(status):
                    self.log(logging.WARNING,
                             'child {0} exited with signal {1}'
                             .format(pid, os.WTERMSIG(status)))
                    any_sad_children = True
                else:
                    self.log(logging.WARNING,
                             'child {0} went away with unknown status {1}'
                             .format(pid, status))
                    any_sad_children = True
            else:
                self.log(logging.WARNING,
                         'child {0} exited, but we don\'t recognize it'
                         .format(pid))

        # ...what next?
        # (Don't log anything here; either we logged a WARNING message
        # above when things went badly, or we're in a very normal flow
        # and don't want to spam the log)
        if any_sad_children:
            self.debug('loop', 'exit work loop with sad child')
            return self.poll_interval

        if any_bored_children:
            self.debug('loop', 'exit work loop with no work')
            return self.poll_interval

        # This means we get to start a child, maybe.
        if can_start_more and len(self.children) < self.num_workers:
            pid = os.fork()
            if pid == 0:
                # We are the child
                self.clear_signal_handlers()
                if self.log_fd:
                    os.close(self.log_fd)
                LoopWorker.as_child(yakonfig.get_global_config(),
                                    parent=self.worker_id)
                # This should never return, but just in case
                sys.exit(SingleWorker.EXIT_EXCEPTION)
            else:
                # We are the parent
                self.debug('children', 'new worker with pid {0}'.format(pid))
                self.children.add(pid)
                self.debug('loop', 'exit work loop with a new worker')
                return self.spawn_interval

        # Absolutely nothing is happening; which means we have all
        # of our potential workers and they're doing work
        self.debug('loop', 'exit work loop with full system')
        return self.poll_interval

    def check_spinning_children(self):
        '''Stop children that are working on overdue jobs.'''
        try:
            child_jobs = self.task_master.get_child_work_units(self.worker_id)
        except Exception:
            # This is purely an administrative task, and while it's
            # very good to do it, if we can't actually, that's okay.
            # Against the Go Coordinate server this call can fail, and
            # if it does an uncaught exception here shouldn't take down
            # the entire worker.
            return
        
        # We will kill off any jobs that are due before "now".  This
        # isn't really now now, but now plus a grace period to make
        # sure spinning jobs don't get retried.
        now = time.time() + self.stop_jobs_early
        for child, wul in child_jobs.iteritems():
            if not isinstance(wul, (list, tuple)):
                # Support old style get_child_work_units which returns
                # single WorkUnit objects instead of list of them.
                wul = [wul]
            if not wul:
                # This worker is idle, but oddly, still present; it should
                # clean up after itself
                continue
            # filter on those actually assigned to the child worker
            wul = [unit for unit in wul if unit.worker_id == child]
            # check for any still active not-overdue job
            if any(unit.expires > now for unit in wul):
                continue
            # So either someone else is doing its work or it's just overdue
            # (As above, ignore server-side failures)
            try:
                environment = self.task_master.get_heartbeat(child)
            except Exception:
                environment = {}
            if not environment:
                continue  # derp
            if 'pid' not in environment:
                continue  # derp
            if environment['pid'] not in self.children:
                continue  # derp
            os.kill(environment['pid'], signal.SIGTERM)
            # This will cause the child to die, and do_some_work will
            # reap it; but we'd also like the job to fail if possible
            for unit in wul:
                if unit.data is not None:
                    unit.data['traceback'] = 'job expired'
                unit.fail()

    def stop_gracefully(self):
        '''Refuse to start more processes.

        This runs in response to SIGINT or SIGTERM; if this isn't a
        background process, control-C and a normal ``kill`` command
        cause this.

        '''
        if self.shutting_down:
            self.log(logging.INFO,
                     'second shutdown request, shutting down now')
            self.scram()
        else:
            self.log(logging.INFO, 'shutting down after current jobs finish')
            self.shutting_down = True

    def stop_all_children(self):
        '''Kill all workers.'''
        # There's an unfortunate race condition if we try to log this
        # case: we can't depend on the logging child actually receiving
        # the log message before we kill it off.  C'est la vie...
        self.stop_log_child()
        for pid in self.children:
            try:
                os.kill(pid, signal.SIGTERM)
                os.waitpid(pid, 0)
            except OSError, exc:
                if exc.errno == errno.ESRCH or exc.errno == errno.ECHILD:
                    # No such process
                    pass
                else:
                    raise

    def scram(self):
        '''Kill all workers and die ourselves.

        This runs in response to SIGABRT, from a specific invocation
        of the ``kill`` command.  It also runs if
        :meth:`stop_gracefully` is called more than once.

        '''
        self.stop_all_children()
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        sys.exit(2)

    def run(self):
        '''Run the main loop.

        This is fairly invasive: it sets a bunch of signal handlers
        and spawns off a bunch of child processes.

        '''
        setproctitle('coordinate fork_worker for namespace {0}'
                     .format(self.config.get('namespace', None)))
        self.set_signal_handlers()
        try:
            self.start_log_child()
            while True:
                can_start_more = not self.shutting_down
                if time.time() >= self.heartbeat_deadline:
                    mode = self.heartbeat()
                    if mode != self.last_mode:
                        self.log(logging.INFO,
                                 'coordinate global mode is {0!r}'
                                 .format(mode))
                        self.last_mode = mode
                    self.heartbeat_deadline = (time.time() +
                                               self.heartbeat_interval)
                    self.check_spinning_children()
                else:
                    mode = self.last_mode
                if mode != self.task_master.RUN:
                    can_start_more = False
                interval = self.do_some_work(can_start_more)
                # Normal shutdown case
                if len(self.children) == 0:
                    if mode == self.task_master.TERMINATE:
                        self.log(logging.INFO,
                                 'stopping for coordinate global shutdown')
                        break
                    if self.shutting_down:
                        self.log(logging.INFO,
                                 'stopping in response to signal')
                        break
                time.sleep(interval)
        except Exception:  # pylint: disable=broad-except
            self.log(logging.CRITICAL,
                     'uncaught exception in worker: ' + traceback.format_exc())
        finally:
            # See the note in run_workers() above.  clear_signal_handlers()
            # calls signal.signal() which explicitly affects the current
            # process, parent or child.
            self.clear_signal_handlers()
