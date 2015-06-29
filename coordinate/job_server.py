'''python server logic for coordinate daemon

.. Your use of this software is governed by your license agreement.
   Copyright 2012-2015 Diffeo, Inc.
'''
from __future__ import absolute_import
from __future__ import division
import collections
import glob
import hashlib
import logging
import math
import os
import random
import signal
import sys
import threading
import time

# try:
#     import cPickle as pickle
# except ImportError:
#     import pickle

import cbor
import yakonfig

# special improved version of standard library's heapq
from coordinate import heapq
from .constants import AVAILABLE, FINISHED, FAILED, PENDING, \
    WORK_UNIT_STATUS_NAMES_BY_NUMBER, \
    RUNNABLE, PAUSED, \
    PRI_GENERATED, PRI_STANDARD, \
    MAX_LEASE_SECONDS, MIN_LEASE_SECONDS, DEFAULT_LEASE_SECONDS
from .fifolock import FifoLock
from .job_sqlite import SqliteJobStorage
from .work_spec import WorkSpec, nmin
from .server_work_unit import WorkUnit
from .sqlite_work_spec import SqliteWorkSpec
from .postgres_work_spec import PostgresWorkSpec
from .zopen import zopen, zopenw


logger = logging.getLogger(__name__)


def timestamp():
    "return now as YYYYmmdd_HHMMSS.mmm"
    # MUST MATCH _TIMESTAMP_GLOB BELOW
    now = time.time()
    gmt = time.gmtime(now)
    ymdhms = time.strftime('%Y%m%d_%H%M%S', gmt)
    millis = math.trunc((now - math.floor(now)) * 1000)
    return ymdhms + '.{:03d}'.format(millis)


# for glob.glob() to find things matching timestamp()
_TIMESTAMP_GLOB = '????????_??????.???'


class Worker(object):
    '''
    record of some client doing work with this job server
    '''
    def __init__(self, worker_id, mode, expire_time, data, parent):
        # expire_time should be absolute time
        self.worker_id = worker_id
        self.mode = mode
        self.last_heard_from = time.time()
        self.expire_time = expire_time
        self.data = data
        self.parent = parent
        self.work_spec = None
        self.work_unit = None

    def __lt__(self, other):
        "used by heapq -- smallest item is first in queue"
        return self.expire_time < other.expire_time


class WorkerPool(object):
    '''
    Container of Worker objects to keep clutter out of JobQueue
    '''

    def __init__(self):
        # map[worker_id] = Worker
        self.workers = {}
        # map[parent_id] = set(child_id)
        self.children = collections.defaultdict(set)
        # heap on .expire_time
        self.expire_queue = []
        self.mutex = FifoLock()

    def stats(self):
        with self.mutex:
            return {
                'num_workers': len(self.workers),
                'num_children': len(self.children),
                'num_expirable': len(self.expire_queue),
            }

    def _expire_workers(self):
        # call inside a mutex lock content
        now = time.time()
        while self.expire_queue and (self.expire_queue[0].expire_time < now):
            wx = heapq.heappop(self.expire_queue)
            self.workers.pop(wx.worker_id)

    def update(self, worker_id, mode, expire_seconds, data, parent):
        # expire_seconds, coming in from client, should be relative time.
        # convert it to server-local absolute time.
        message = ''
        if expire_seconds < 0:
            message += ('negative expire time {} defaulted to 5 minutes'
                        .format(expire_seconds))
            expire_seconds = 300
        elif expire_seconds > (24*3600):
            message += ('huge expire time {} defaulted to one hour'
                        .format(expire_seconds))
            expire_seconds = 3600
        now = time.time()
        expire_time = expire_seconds + now
        with self.mutex:
            xw = self.workers.get(worker_id)
            if xw is None:
                xw = Worker(worker_id, mode, expire_time, data, parent)
                self.workers[worker_id] = xw
                if parent:
                    self.children[parent].add(worker_id)
                heapq.heappush(self.expire_queue, xw)
            else:
                pos = self.expire_queue.index(xw)
                xw.last_heard_from = now
                xw.mode = mode
                xw.data = data
                xw.expire_time = expire_time
                if parent != xw.parent:
                    self.children[xw.parent].remove(worker_id)
                    self.children[parent].add(worker_id)
                    xw.parent = parent
                heapq.heapreplace(self.expire_queue, xw, index=pos)

            self._expire_workers()

        # '' -> None
        message = message or None
        # return ok, message
        return True, message

    def unregister(self, worker_id):
        with self.mutex:
            xw = self.workers.pop(worker_id, None)
            if xw is None:
                return False, 'no worker {!r}'.format(worker_id)
            if xw.parent:
                self.children[xw.parent].remove(worker_id)
            pos = self.expire_queue.index(xw)
            heapq.heappop(self.expire_queue, pos)

            self._expire_workers()
        # return ok, message
        return True, None

    def list_worker_modes(self):
        with self.mutex:
            self._expire_workers()
            out = {}
            for worker_id, xw in self.workers.iteritems():
                out[worker_id] = xw.mode
            # return data, message
            return out, None

    def get_worker_info(self, worker_id):
        with self.mutex:
            self._expire_workers()
            xw = self.workers.get(worker_id)
            if xw is None:
                # not an error, so no message, it's just not there
                return None, None
            out = dict(xw.data)
            out['age_seconds'] = time.time() - xw.last_heard_from
            # return data, message
            return xw.data, None

    def mode_counts(self):
        with self.mutex:
            self._expire_workers()
            out = {}
            for wx in self.workers.itervalues():
                out[wx.mode] = out.get(wx.mode, 0) + 1
            # return data, message
            return out, None

    def get_child_work_units(self, worker_id):
        "returns {child worker id: [{work unit data}, ...]"
        result = {}
        with self.mutex:
            for child in self.children[worker_id]:
                worker = self.workers.get(child)
                if worker:
                    # Record this work unit; but skip it if it
                    # is owned by the current worker and it is done
                    if not worker.work_unit:
                        result[child] = None
                    else:
                        result[child] = [
                            {
                                'work_spec_name': worker.work_spec.name,
                                'work_unit_key': work_unit.key,
                                'work_unit_data': work_unit.data,
                                'worker_id': work_unit.worker_id,
                                'expires': work_unit.lease_time,
                            }
                            for work_unit in worker.work_unit
                        ]
        return result, None


class JobQueue(object):
    '''
    Server object that handles requests.
    There should be one of these.
    '''
    def __init__(self, config=None):
        self.work_specs = {}
        self.mutex = FifoLock()
        self.scheduler = WorkSpecScheduler(self.work_specs)

        self.config = config or yakonfig.get_global_config('coordinate', 'job_queue')

        self.workers = WorkerPool()

        self.log_disabled = False
        self.logfile = None
        self.log_path_format = self._cfget('log_path_format')
        self.snapshot_path_format = self._cfget('snapshot_path_format')
        self.snapshot_period_seconds = self._cfget('snapshot_period_seconds')
        self.do_recover = self._cfget('do_recover')
        # e.g. delete_snapshots_beyond=5, keep 5 snapshots and delete older
        self.delete_snapshots_beyond = self._cfget('delete_snapshots_beyond')
        self.last_snapshot_time = None
        self.do_joblog = self._cfget('joblog')

        # oldest at [0], new paths .append()
        self.old_snapshot_paths = []

        self.limit_completed_count = self._cfget('limit_completed_count')
        self.limit_completed_age = self._cfget('limit_completed_age')

        # have there been actions done which might merit a snapshot?
        self._dirty = False

        self.sqlite_path = self._cfget('sqlite_path')
        self.storage = None
        if self.sqlite_path:
            self.storage = SqliteJobStorage(self.sqlite_path)

        if self.do_recover:
            # sadly, work that kinda doesn't make sense except in the
            # constructor. ew. gross.
            self._do_recover()

        self.snapshot_thread = None
        self.snapshot_error_count = 0
        self.snapshot_error_limit = 5
        if (((self.snapshot_path_format is not None) and
             (self.snapshot_period_seconds is not None))):
            self.snapshot_thread = threading.Thread(
                target=self._snapshotter_thread)
            self.snapshot_thread.daemon = True
            self.snapshot_thread.start()

    config_name = 'job_queue'
    default_config = {
        'snapshot_period_seconds': None,
        'snapshot_path_format': None,
        'log_path_format': None,
        'do_recover': False,
        'delete_snapshots_beyond': None,
        'limit_completed_count': None,
        'limit_completed_age': None,
        'joblog': False,  # noisy cpu intense log. enable for debugging.
        'sqlite_path': None,  # sqlite storage for large queues
    }

    def _cfget(self, name):
        if name in self.config:
            return self.config[name]
        return self.default_config[name]

    def _snapshotter_thread(self):
        try:
            self._snapshotter_loop()
        except:
            logger.error('snapshotter thread erroring out. not safe to continue', exc_info=True)
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(2)
            sys.exit(1)

    def _snapshotter_loop(self):
        while (self.snapshot_path_format is not None) and (self.snapshot_period_seconds is not None):
            did_snapshot = False
            last_exception = None
            try:
                did_snapshot = self.maybe_periodic_snapshot()
            except Exception as e:
                logger.error('error in snapshot thread', exc_info=True)
                last_exception = e

            if did_snapshot and (last_exception is None):
                self.snapshot_error_count = 0

            if last_exception is not None:
                self.snapshot_error_count += 1
                if self.snapshot_error_count >= self.snapshot_error_limit:
                    if last_exception is not None:
                        raise last_exception
                    else:
                        raise Exception('{} failed snapshots. fail out'.format(self.snapshot_error_count))

            if did_snapshot:
                # wait a full period
                time.sleep(self.snapshot_period_seconds)
            else:
                # wait a half period
                time.sleep(self.snapshot_period_seconds/2.0)

    def maybe_periodic_snapshot(self):
        # Call this any time it might be a good time to snapshot, like
        # right after doing some other operation.
        if self.snapshot_path_format is None:
            return False
        if self.snapshot_period_seconds is None:
            return False
        if self.last_snapshot_time is None:
            # the snapshot period starts now
            self.last_snapshot_time = time.time()
            return False
        if not self._dirty:
            # no changes to snapshot.
            logger.debug('nothing to snapshot')
            return False
        now = time.time()
        if (self.last_snapshot_time + self.snapshot_period_seconds) < now:
            self.snapshot()
            endtime = time.time()
            self.last_snapshot_time = now
            return True

    def snapshot(self, snap_path=None):
        if snap_path is None:
            if self.snapshot_path_format is None:
                return
            newts = timestamp()
            snap_path = self.snapshot_path_format.format(timestamp=newts)
        start_time = time.time()
        # TODO: output might be stream to s3 or kvlayer?
        zout = zopenw(snap_path)
        fout = Md5Writer(zout)
        self._snapshot(fout, snap_path)
        assert snap_path, 'need snap_path to write out associated .md5 file'
        zout.close()
        dotmd5path = snap_path + '.md5'
        with open(dotmd5path, 'wb') as dotmd5fd:
            dotmd5fd.write(fout.hexdigest())
            dotmd5fd.write('\n')
        end_time = time.time()
        logger.info('wrote snapshot to %r in %s seconds', snap_path, end_time - start_time)

        if self.storage:
            self.storage.vacuum()

        self.old_snapshot_paths.append(snap_path)
        logger.info('dsb %s old snap paths %r', self.delete_snapshots_beyond, self.old_snapshot_paths)
        while self.delete_snapshots_beyond and (len(self.old_snapshot_paths) > self.delete_snapshots_beyond):
            old_snap_path = self.old_snapshot_paths.pop(0)
            logger.info('deleting old snapshot %r', old_snap_path)
            try:
                if os.path.exists(old_snap_path + '.md5'):
                    os.remove(old_snap_path + '.md5')
                os.remove(old_snap_path)
            except:
                logger.error('failed trying to remove old snapshot %r', old_snap_path, exc_info=True)
                return

    def _snapshot(self, fout, snap_path=None):
        self.archive()
#        self._snapshot_pickle(fout, snap_path)
        self._snapshot_cbor(fout, snap_path)

    # def _snapshot_pickle(self, fout, snap_path=None):
    #     # TODO: delete. deprecated.
    #     with self.mutex:
    #         ws_subpickles = dict([
    #             (name_ws[0], name_ws[1]._pickle()) for name_ws in self.work_specs.iteritems()
    #         ])
    #         self._close_logfile()
    #         self._dirty = False
    #     return pickle.dump(ws_subpickles, fout, protocol=pickle.HIGHEST_PROTOCOL)

    def _snapshot_cbor(self, fout, snap_path=None):
        with self.mutex:
            for name, ws in self.work_specs.iteritems():
                ws._cbor_dump(fout)
            self._close_logfile()
            self._dirty = False

    def _close_logfile(self):
        if self.logfile is not None:
            lf = self.logfile
            self.logfile = None
            # TODO: some closing record into the logfile to signify that it is complete and whole
            closer_thread = threading.Thread(target=lf.close)
            closer_thread.start()

    def load_snapshot(self, stream=None, snap_path=None):
        if stream is None:
            if snap_path is None:
                raise Exception('need stream or snap_path to load_snapshot()')
            stream = zopen(snap_path)
        assert stream is not None
        #self._load_snapshot_pickled(stream)
        self._load_snapshot_cbor(stream)
        self.scheduler.update()

    # def _load_snapshot_pickled(self, stream):
    #     # TODO: delete. deprecated.
    #     ws_subpickles = pickle.load(stream)
    #     with self.mutex:
    #         for name, subpickle in ws_subpickles.iteritems():
    #             ws = pickle.loads(subpickle)
    #             ws.jobq = self
    #             self.work_specs[name] = ws

    def _load_snapshot_cbor(self, stream):
        with self.mutex:
            try:
                while True:
                    if self.storage:
                        ws = SqliteWorkSpec.__new__(SqliteWorkSpec)
                        ws.storage = self.storage
                    else:
                        ws = WorkSpec.__new__(WorkSpec)
                    ws._cbor_load(stream)
                    ws.jobq = self
                    self.work_specs[ws.name] = ws
            except EOFError:
                # okay. done.
                pass

    def _log_action(self, action, args):
        # TODO: make this abstract to backends other than file.
        # e.g. kvlayer, or to slave coordinated cluster nodes
        self._dirty = True
        if self.log_disabled:
            return
        if self.logfile is None:
            if self.log_path_format is not None:
                next_logpath = self.log_path_format.format(timestamp=timestamp())
                self.logfile = zopenw(next_logpath)
        if self.logfile is not None:
            cbor.dump((action, args), self.logfile)

    def _run_log(self, logf, error_limit=0):
        errcount = 0
        #errlist = []
        for ob in _cbor_load_iter(logf):
            logger.debug('action log: %r', ob)
            action, args = ob
            try:
                self._run_log_action(action, args)
            except Exception as e:
                errcount += 1
                if errcount > error_limit:
                    raise
                else:
                    logger.error('log replay error, but continuing', exc_info=True)
                    #errlist.append(e)

    def _run_log_action(self, action, args):
        log_disabled_stack = self.log_disabled
        self.log_disabled = True

        try:
            if action == 'set_work_spec':
                self.set_work_spec(*args)
            elif action == 'del_work_spec':
                self.del_work_spec(*args)
            elif action == 'add_work_unit':
                work_spec_name, wu_key, wu_data = args
                self.add_work_units(work_spec_name, [(wu_key, wu_data)])
            elif action == 'update_work_unit':
                work_spec_name, wu_key, options = args
                self.update_work_unit(work_spec_name, wu_key, options)
            elif action == 'del_work_units':
                work_spec_name, options = args
                self.del_work_units(work_spec_name, options)
            else:
                raise Exception('uknnown log entry action {!r}, args={!r}'.format(action, args))
        finally:
            self.log_disabled = log_disabled_stack

    def _do_recover(self):
        # try to find the latest snapshot, and any log files after it, and load
        if self.snapshot_path_format is not None:
            start_time = time.time()
            globpat = self.snapshot_path_format.format(timestamp=_TIMESTAMP_GLOB)
            self.old_snapshot_paths = sorted(glob.glob(globpat))
            logger.debug('found snapshots: %r, (from %r)', self.old_snapshot_paths, globpat)
            if not self.old_snapshot_paths:
                return
            by_mtime = []
            for snap_path in self.old_snapshot_paths:
                dotmd5path = snap_path + '.md5'
                if not os.path.exists(dotmd5path):
                    logger.debug('skipping for lack of %r', dotmd5path)
                    continue
                try:
                    mtime = os.path.getmtime(snap_path)
                    by_mtime.append( (mtime, snap_path) )
                except:
                    logger.info('failed getting mtime for %r', snap_path, exc_info=True)
                    # meh, drop it
            # highest mtime (newest) first
            by_mtime.sort(reverse=True)
            for mtime, snap_path in by_mtime:
                dotmd5path = snap_path + '.md5'
                try:
                    with open(dotmd5path, 'rb') as md5in:
                        md5str = md5in.read()
                    if not md5str:
                        logger.info('no md5 text in %s', dotmd5path)
                        continue
                    md5str = md5str.strip()
                    if not md5str:
                        logger.info('no md5 text in %s', dotmd5path)
                        continue
                except:
                    logger.info('failed to read %s', dotmd5path, exc_info=True)
                    continue
                zin = zopen(snap_path)
                inhash = hashlib.md5()
                while True:
                    data = zin.read(32*1024)
                    if not data:
                        break
                    inhash.update(data)
                if md5str != inhash.hexdigest():
                    logger.error('md5 hash mismatch .md5 %s != %s', md5str, inhash.hexdigest())
                    continue

                # else, okay, go with this mtime,snap_path
                try:
                    self.load_snapshot(snap_path=snap_path)
                    end_time = time.time()
                    logger.info('loaded snapshot in %s sec from %r', end_time - start_time, snap_path)
                    # done. return.
                    # TODO: load log files after the snapshot and apply them.
                    return
                except:
                    logger.error('failed loading snapshot %r', snap_path, exc_info=True)

            logger.info('no snapshot successfully loaded')

    def wsgi(self):
        '''Return a wsgi compatible application object'''
        try:
            from .job_httpd import JobHttpd
            return JobHttpd(self)
        except:
            logger.error('could not initialize wsgi handler', exc_info=True)
            return None

    def set_work_spec(self, work_spec):
        '''Create or alter a work spec.

        `work_spec` is the work spec dictionary.  If the work spec
        already exists, this does not change any of its control
        information, only the actual dictionary that is available to
        work units.  Some of that information such as the
        configuration and work function is still used within
        :mod:`coordinate`.

        The return value is a pair where the first value is a
        :const:`True` or :const:`False` success status, and the second
        value is an error message on failure.

        '''
        name = work_spec['name']
        ws = None
        with self.mutex:
            self._log_action('set_work_spec', (work_spec,))
            ws = self.work_specs.get(name)
            if ws is None:
                if self.storage:
                    ws = SqliteWorkSpec.from_dict(work_spec, self, storage=self.storage)
                else:
                    ws = WorkSpec.from_dict(work_spec, self)
                self.work_specs[name] = ws
                self.scheduler.update()
                return True, None
        ws.update_data(work_spec)
        return True, None

    def control_work_spec(self, work_spec_name, options):
        '''Change control information (e.g., is it runnable) for a spec.

        `work_spec_name` is the name of the work spec.

        `options` contains:

        * `status`: new status of the work spec, should be
          :data:`RUNNABLE` or :data:`PAUSED`
        * `continuous`: boolean indicating whether to do continuous
          job creation
        * `interval`: if the job is continuous, minimum number of
          seconds before creating new jobs
        * `max_running`: maximum number of concurrent work units
        * `weight`: relative weight of the work spec

        The return value is a pair where the first value is a
        :const:`True` or :const:`False` success status, and the
        second value is an error message on failure.

        '''
        ws = self.work_specs.get(work_spec_name)
        if ws is None:
            return (False, 'no such work spec {}'.format(work_spec_name))
        with ws.mutex:
            if 'continuous' in options:
                if ((options['continuous'] and
                     not ws.data.get('continuous', False))):
                    return (False, 'cannot make non-continuous work spec '
                            '{} continuous'.format(work_spec_name))
                ws.continuous = options['continuous']
            if 'status' in options:
                ws.status = options['status']
            if 'weight' in options:
                ws.weight = options['weight']
            if 'interval' in options:
                ws.interval = options['interval']
            if 'max_running' in options:
                ws.max_running = options['max_running']
            self.scheduler.update()
        return (True, None)

    def get_work_spec(self, work_spec_name):
        '''Get the dictionary for a work spec.

        `work_spec_name` is the name of the work spec.

        The return value is the definition of the work spec that could
        be resubmitted to :meth:`set_work_spec`, or :const:`None` if
        the work spec does not exist.

        '''
        # no locks to read
        ws = self.work_specs.get(work_spec_name)
        if ws is None:
            return None
        return ws.data

    def get_work_spec_meta(self, work_spec_name):
        '''Get control information (e.g., is it runnable) for a spec.

        `work_spec_name` is the name of the work spec.

        On failure, returns a pair of :const:`None` and an error
        message.  On success, returns a pair of a metadata dictionary
        and :const:`None`.  The keys in the metadata dictionary are
        the same as the `options` parameter to
        :meth:`control_work_spec`.

        '''
        ws = self.work_specs.get(work_spec_name)
        if ws is None:
            return (None, 'no such work spec {}'.format(work_spec_name))
        with ws.mutex:
            res = {'status': ws.status,
                   'continuous': ws.continuous,
                   'interval': ws.interval,
                   'max_running': ws.max_running,
                   'weight': ws.weight}
        return (res, None)

    def list_work_specs(self, options):
        limit = options.get('limit')
        if limit is not None:
            limit = int(limit)
        start = options.get('start')
        out = []
        next = None
        with self.mutex:
            keys = list(self.work_specs.keys())
            keys.sort()
            pos = 0
            if start is not None:
                while (pos < len(keys)) and (start < keys[pos]):
                    pos += 1
            while pos < len(keys):
                out.append(self.work_specs[keys[pos]].data)
                pos += 1
                if (limit is not None) and (len(out) >= limit):
                    if pos < len(keys):
                        next = keys[pos]
                    break
        return out, next

    def clear(self):
        with self.mutex:
            oldspecs = self.work_specs
            self.work_specs = {}
            delcount = 0
            for work_spec_name, ws in oldspecs.iteritems():
                delcount += 1
                ws.del_work_units({'all':True})
            self.scheduler.update()
            return delcount

    def del_work_spec(self, work_spec_name):
        "return ok, message"
        with self.mutex:
            self._log_action('del_work_spec', (work_spec_name,))
            oldws = self.work_specs.get(work_spec_name)
            if oldws is not None:
                oldws.del_work_units({'all':True})
                del self.work_specs[work_spec_name]
                self.scheduler.update()
                return True, None
            return False, 'no such work_spec {!r}'.format(work_spec_name)

    def archive(self, options={}):
        '''Purge old completed work units.

        `options` contains the following keys:

        * `work_spec_names`: only consider these work specs;
          otherwise do all

        Returns :const:`True`.  Invalid `work_spec_names` are ignored.

        '''
        work_spec_names = options.get('work_spec_names', None)
        if work_spec_names is None:
            work_spec_names = self.work_specs.keys()
        for wsn in work_spec_names:
            spec = self.work_specs.get(wsn, None)
            if spec is not None:
                spec.archive_work_units(self.limit_completed_count,
                                        self.limit_completed_age)

    def add_work_units(self, work_spec_name, work_unit_key_vals):
        "return ok, message"
        ws = self.work_specs.get(work_spec_name)
        if ws is None:
            return False, 'no such work_spec {!r}'.format(work_spec_name)
        new_wu = {}
        for x in work_unit_key_vals:
            wu = WorkUnit(*x)
            new_wu[wu.key] = wu  # in case of duplicate, last writer wins
        ws.add_work_units(new_wu.values())
        return True, None

    def count_work_units(self, work_spec_name):
        "return {status:count,...}, message"
        ws = self.work_specs.get(work_spec_name)
        if ws is None:
            return False, 'no such work_spec {!r}'.format(work_spec_name)
        return ws.count_work_units()

    def get_work_units(self, work_spec_name, options):
        '''
        options={work_unit_keys=None, state=None, limit=None, start=None}
        return [(key, data), ...], message
        '''
        ws = self.work_specs.get(work_spec_name)
        if ws is None:
            return None, 'no such work_spec {!r}'.format(work_spec_name)
        return ws.get_work_units(options)

    def prioritize_work_units(self, work_spec_name, options):
        '''Move some work items to the top of the queue.

        `work_spec_name` is the name of the work spec.

        `options` contains:

        * `work_unit_keys`: list of strings that are the work
          unit keys (required)
        * `priority`: new desired priority (default=:const:`None`)
        * `adjustment`: change in priority (default=:const:`None`)

        If `priority` is given, the priority of all of
        `work_unit_keys` is set to this value; otherwise, if
        `adjustment` is given, that value is added to the current
        priorities of all of the specified work units.  If neither is
        given this call does nothing.  Higher priority means run
        sooner in the queue.  The priority scores may the special
        constants :data:`~coordinate.constants.PRI_GENERATED`,
        :data:`~coordinate.constants.PRI_STANDARD`, or
        :data:`~coordinate.constants.PRI_PRIORITY`; or they may be
        any numeric value.

        The return value is a pair where the first value is a
        :const:`True` or :const:`False` success status, and the second
        value is an error message on failure.  If some of the
        `work_unit_keys` are not present or work has already started
        on them, this is not reported as an error.

        '''
        ws = self.work_specs.get(work_spec_name)
        if ws is None:
            return False, 'no such work_spec {!r}'.format(work_spec_name)
        if 'work_unit_keys' not in options:
            return False, 'missing work_unit_keys'
        ws.prioritize_work_units(
            options['work_unit_keys'],
            priority=options.get('priority', None),
            adjustment=options.get('adjustment', None))
        return True, None

    def del_work_units(self, work_spec_name, options):
        '''Delete work units from a work spec.

        `work_spec_name` is the name of the work spec.

        `options` contains:

        * `all`: if true, delete everything and ignore all other options
          (default=:const:`False`)
        * `work_unit_keys`: if present, a list of specific work unit
          keys to delete (default=:const:`None`, delete all)
        * `state`: if present integer state code (default=:const:`None`)

        If both `work_unit_keys` and `state` are given, only work
        units with the requested name in the requested state are
        deleted.

        The return value is a pair where the first value is the number
        of work units deleted, and the second value is an error
        message on failure.

        '''
        ws = self.work_specs.get(work_spec_name)
        if ws is None:
            return 0, 'no such work_spec {!r}'.format(work_spec_name)
        return ws.del_work_units(options)

    def get_work_unit_status(self, work_spec_name, work_unit_keys):
        "return status, message"
        ws = self.work_specs.get(work_spec_name)
        if ws is None:
            return None, 'no such work_spec {!r}'.format(work_spec_name)
        return ws.get_statuses(work_unit_keys), None

    def update_work_unit(self, work_spec_name, work_unit_key, options):
        '''Record activity on a work unit.

        This can include forward progress or succesful or unsuccessful
        completion.

        `work_spec_name` and `work_unit_key` identify the work unit.

        `options` contains:

        * `lease_time`: requested time extension on the job; if
          :const:`None`, keep the current expiration
        * `status`: new job status; if :const:`None`, stay unchanged
        * `data`: new work unit data
        * `worker_id`: worker that is doing the job

        The return value is a pair where the first value is a
        :const:`True` or :const:`False` success status, and the second
        value is an error message on failure.

        '''
        ws = self.work_specs.get(work_spec_name)
        if ws is None:
            return False, 'no such work_spec {!r}'.format(work_spec_name)
        ok, msg = ws.update_work_unit(work_unit_key, options)
        if ok:
            # This *must* exist for us to get here
            wu = ws.work_units_by_key[work_unit_key]
            if wu.status == FINISHED or wu.status == FAILED:
                # It's done; who was working on it?
                worker = self.workers.workers.get(wu.worker_id)
                if ((worker and
                     worker.work_spec is ws and
                     worker.work_unit and
                     (wu in worker.work_unit)
                 )):
                    # Okay, we've finished the job.  Awesome.
                    worker.work_spec = None
                    worker.work_unit = None
        return (ok, msg)

    def get_work(self, worker_id, options):
        '''Get some unit of work.

        `worker_id` is an opaque string recording who is doing the work.

        `options` contains:

        * `lease_time`: requested number of seconds to do the job
          (default=300)
        * `work_spec_names`: list of work spec names to consider
          (default=:const:`None`, meaning all work specs)
        * `worker_capabilities`
        * `num_units`: number of work units to return (default=1)

        The return value is a tuple with length that is a multiple
        of three.  Each group of three items in the tuple is a
        work spec name, a work unit key, and the corresponding
        work unit data.

        '''
        # TODO: use options['available_gb']
        max_jobs = int(options.get('max_jobs', 1))
        work_spec_names = options.get('work_spec_names')
        # lease_time is seconds into the future
        lease_time = float(options.get('lease_time', DEFAULT_LEASE_SECONDS))
        if lease_time > MAX_LEASE_SECONDS:
            logger.warn('lease_time too high, %s > %s, clamping to %s',
                        lease_time, MAX_LEASE_SECONDS,
                        DEFAULT_LEASE_SECONDS)
            lease_time = DEFAULT_LEASE_SECONDS
        if lease_time < MIN_LEASE_SECONDS:
            logger.warn('lease_time too low, %s < %s, defaulting to %s',
                        lease_time, MIN_LEASE_SECONDS,
                        DEFAULT_LEASE_SECONDS)
            lease_time = DEFAULT_LEASE_SECONDS
        # convert to absolute time (at least within this server)
        lease_time += time.time()

        worker = self.workers.workers.get(worker_id)
        if worker is None:
            # some test code doesn't worker_heartbeat before diving in
            pass
        elif worker.work_unit:
            logger.warn('worker %s is already working on work spec '
                        '%s work unit %s',
                        worker_id, [x.key for x in worker.work_unit],
                        worker.work_spec.name)

        def valid(spec):
            if work_spec_names and spec.name not in work_spec_names:
                return False
            return True

        while True:
            spec = self.scheduler.choose_work_spec(valid)
            if spec is None:
                return ((None, None, None),
                        'no work specs with available work')
            ws = self.work_specs[spec]
            work_units = ws.get_work(worker_id, lease_time, max_jobs)
            if not work_units:
                # Try the scheduler loop again.
                # Another request dried up this workspec between when
                # we chose it and when we got work from it?
                continue
            if worker is not None:
                worker.work_spec = ws
                worker.work_unit = work_units
            if max_jobs == 1:
                # old style single return
                wu = work_units[0]
                return ((ws.name, wu.key, wu.data), None)
            else:
                return ([(ws.name, wu.key, wu.data) for wu in work_units], None)

    def get_child_work_units(self, worker_id):
        '''Get work units assigned to a worker's children.

        `worker_id` is the parent worker ID.

        The return value is a dictionary mapping child worker IDs
        to lists of work unit metadata dictionaries.  Each of the inner
        dictionaries has keys ``work_spec_name``, ``work_unit_key``,
        ``work_unit_data``, ``worker_id``, and ``expires``, which
        can be used to reconstruct the work unit client-side.

        '''
        return self.workers.get_child_work_units(worker_id)

    def worker_heartbeat(self, worker_id, mode, expire_seconds, data, parent):
        return self.workers.update(worker_id, mode, expire_seconds, data,
                                   parent)

    def worker_unregister(self, worker_id):
        return self.workers.unregister(worker_id)

    def list_worker_modes(self):
        return self.workers.list_worker_modes()

    def get_worker_info(self, worker_id):
        return self.workers.get_worker_info(worker_id)

    def mode_counts(self):
        return self.workers.mode_counts()

    def worker_stats(self):
        return self.workers.stats()

    def get_config(self):
        '''Get the server's global configuration.

        This takes no parameters, and returns the configuration dictionary.

        '''
        return (yakonfig.get_global_config(), None)


def _cbor_load_iter(stream):
    try:
        while True:
            ob = cbor.load(stream)
            yield ob
    except EOFError:
        return


class WorkSpecScheduler(object):
    '''Scheduler that decides which work spec should run next.

    This is in many ways part of :class:`JobQueue`, but it is a
    logically separate part.

    This maintains a dictionary mapping work spec name to
    :class:`WorkSpec` objects.  The dictionary must be passed to
    either the constructor or :meth:`update` (or both).  This
    class will not modify the dictionary.

    This object constructs a graph from work specs and their
    successor jobs, and classifies each work spec in one of three
    ways.  It can be a _source_ with no work specs feeding it;
    it can be in a _loop_ of work specs; or it can be on a non-loop
    _path_ (possibly it can be a sink, but this is not treated
    specially).  If work specs on paths have work to do, these are
    considered first, before considering sources and loops; then
    sources and loops are considered together.

    .. automethod:: __init__
    .. automethod:: update
    .. automethod:: schedule

    '''
    def __init__(self, work_specs=None, source_weight=20, loop_weight=10,
                 continuous_weight=1):
        '''Create a new scheduler.

        If `work_specs` is given, then it is used as the default
        work spec dictionary for subsequent calls to :meth:`update`,
        but this constructor will not itself build the work spec
        graph; you need to manually call :meth:`update`.

        The three "weight" parameters control the relative probability
        of choosing a new inbound job, a loop of jobs that feed into
        each other, or a job marked "continuous" if there are no
        ingest jobs to run.

        '''
        super(WorkSpecScheduler, self).__init__()
        #: Dictionary of work spec name to :class:`WorkSpec`
        self.work_specs = work_specs
        #: List of work spec names that are only fed manually
        self.sources = []
        #: List of lists of work spec names in loops
        self.loops = []
        #: List of lists of work spec names in continuous paths
        self.paths = []
        #: List of work spec names that are marked continuous
        self.continuous = []
        #: Relative weight of source jobs
        self.source_weight = source_weight
        #: Relative weight of loop jobs
        self.loop_weight = loop_weight
        #: Relative weight of continuous jobs
        self.continuous_weight = continuous_weight

    def update(self, work_specs=None):
        '''Rebuild the work spec queue.

        This completely rebuilds the internal state of the scheduler.
        If `work_specs` is :const:`None`, then a work spec dictionary
        must have been provided in a previous call to either
        :meth:`__init__` or :meth:`update`, and that dictionary will
        be reused.

        '''
        assert work_specs is not None or self.work_specs is not None
        if work_specs is not None:
            self.work_specs = work_specs

        # Run depth-first search on the graph.
        # The DFS algorithm can run in arbitrary order, but we
        # may need to do some fixup afterwards.
        predecessors = {}
        t = [0]
        entries = {}
        exits = {}
        loop_nodes = set()
        self.continuous = []

        def dfs_visit(name, spec):
            entries[name] = t[0]  # node becomes "gray"
            t[0] += 1
            if ((spec.next_work_spec and
                 spec.next_work_spec in self.work_specs)):
                # This differs slightly from the CLR DFS algorithm.
                # Always record some predecessor for a node if it
                # has one (even if it's not strictly in the DFS tree).
                if spec.next_work_spec not in predecessors:
                    predecessors[spec.next_work_spec] = name
                if spec.next_work_spec not in entries:
                    # It is "white", visit it
                    dfs_visit(spec.next_work_spec,
                              self.work_specs[spec.next_work_spec])
                elif spec.next_work_spec not in exits:
                    # It is "gray" and this is a back edge
                    loop_nodes.add(spec.next_work_spec)
                # Otherwise it is "black" and this is a forward or
                # cross edge
            exits[name] = t[0]  # node becomes "black"
            t[0] += 1

        for name, spec in self.work_specs.iteritems():
            if spec.continuous:
                self.continuous.append(name)
            if name in entries:
                continue  # node is not "white"
            dfs_visit(name, spec)

        # Now go through and classify the nodes.
        names = set(self.work_specs.keys())

        self.loops = []
        any_loop = set()
        for loop_head in loop_nodes:
            # loop_head is the name of a node that is at the far end
            # of a back edge.  Nodes only have one outgoing edge in
            # the flow graph, so we can follow edges forward until
            # we find loop_head again.
            the_loop = []
            node_name = loop_head
            while True:
                the_loop.append(node_name)
                any_loop.add(node_name)
                names.remove(node_name)
                node_name = self.work_specs[node_name].next_work_spec
                if node_name == loop_head:
                    break
            self.loops.append(the_loop)

        self.sources = [name for name in names
                        if name not in predecessors]
        for name in self.sources:
            names.remove(name)

        # Everything remaining in names is on a path, and that
        # path necessarily starts from a source.  Build paths
        # as a dictionary from the most-downstream node.
        self.paths = []
        pathq = dict([(name, (((name not in self.continuous) and [name]) or [])) for name in self.sources])
        while pathq:
            name, path = pathq.popitem()
            spec = self.work_specs[name]
            next_name = spec.next_work_spec
            if next_name is None:
                # End of the road.
                if path:
                    self.paths.append(path)
            elif next_name in any_loop:
                # Also end of the road.
                if path:
                    self.paths.append(path)
            elif next_name in names:
                # We haven't gone there yet, so it is part of
                # the current path
                path.append(next_name)
                names.remove(next_name)
                pathq[next_name] = path
            else:
                # It must be in some existing path, either a
                # completed one or an incomplete one.
                for p in self.paths:
                    if next_name in p:
                        self.paths.remove(p)
                        p = path + p
                        self.paths.append(p)
                for (n, p) in pathq.iteritems():
                    if next_name in p:
                        p = path + p
                        pathq[n] = p

    def choose_work_spec(self, valid=None):
        '''Select a unit of work to do.
        Returns the name of the work_spec from which the next work_unit
        should come.

        `valid` is a function of a single argument, the work spec,
        which returns a boolean value indicating whether or not the
        work spec can be considered; if not provided any can be
        considered.  Work specs that are invalid and work specs that
        are empty at the point of examination are not considered,
        regardless of the value of `valid`.  This returns the name of
        a work spec, or :const:`None`.

        '''
        # If there is a "path" work item, always use it; otherwise
        # try a "source" or "loop" work item, preferring "source"
        # at a 2:1 ratio if there are both.
        # TODO: should 'path' items always preempt loop items?
        # (pretty sure preempting generator continuous specs is fine)
        wu = self._choose_spec_from(self.paths, valid, pick_last=True)
        if not wu:
            # Pick something from each of the lists
            wu_source = self._choose_spec_from(
                [[n] for n in self.sources], valid)
            wu_loop = self._choose_spec_from(self.loops, valid)
            wu_continuous = self._choose_spec_from(
                [[n] for n in self.continuous], valid, allow_empty=True)

            # Come up with the correct relative weight
            w_source = self.source_weight
            if wu_source is None:
                w_source = 0
            w_loop = self.loop_weight
            if wu_loop is None:
                w_loop = 0
            w_continuous = self.continuous_weight
            if wu_continuous is None:
                w_continuous = 0

            # If none of the paths produced an item (or if they're
            # disabled in the scheduler weights) return nothing
            w_total = w_source + w_loop + w_continuous
            if w_total == 0:
                wu = None
            else:
                # Pick one of the three with correct weight
                score = random.randrange(w_source + w_loop + w_continuous)
                if score < w_source:
                    wu = wu_source
                else:
                    score -= w_source
                    if score < w_loop:
                        wu = wu_loop
                    else:
                        wu = wu_continuous
        return wu

    def _choose_spec_from(self, list_of_lists, valid, pick_last=False,
                          allow_empty=False):
        '''Choose a work spec name from a list of lists of spec names.
        Returns the name of the work_spec from which the next work_unit
        should come.'''
        # Score every work unit we're considering
        scores = {}
        priority = 0
        min_lscore = None
        min_l = None

        # super noisy debug of what all the scheduler options are
        # logger.debug('%r', [(name, ws.num_pending(), ws.weight, (1 + ws.num_pending()) / ws.weight) for name,ws in self.work_specs.iteritems()])

        for l in list_of_lists:
            lscore = None
            for name in l:
                spec = self.work_specs.get(name)
                spec_score = _spec_score(spec, allow_empty, valid)
                if spec_score is None:
                    continue
                spec_priority = spec.priority()
                if spec_priority < priority:
                    continue
                if spec_priority > priority:
                    # invalidate everything else of lesser priority
                    scores = {}
                    lscore = None
                    priority = spec_priority
                    min_lscore = None
                    min_l = None
                scores[name] = spec_score
                lscore = nmin(lscore, spec_score)
            if (min_lscore is None) or ((lscore is not None) and (lscore < min_lscore)):
                min_lscore = lscore
                min_l = l

        if min_l is None:
            return None

        l = min_l
        # Pick the (last) best scored thing in winning list
        min_score = None
        min_name = None
        prev_defers = False
        last_usable = None
        for name in l:
            ns = scores.get(name)
            if ns is not None:
                # we might follow a chain of deferrals to a last spec
                # which isn't usable (empty or something), so keep the
                # last usable one as alternate
                last_usable = name
                if prev_defers or (min_score is None) or (ns < min_score):
                    min_score = nmin(ns, min_score)
                    min_name = name
            prev_defers = pick_last and self.work_specs[name].next_work_spec_preempts
        return min_name or last_usable


def _spec_score(spec, allow_empty, valid):
    # LOWER SCORE BETTER
    # returns num_pending / weight
    # for two specs of equal weight, having fewer running jobs should run next
    # for two specs of equal jobs pending, higher weight should run next
    if ((spec is None or
         spec.status != RUNNABLE)):
        return None
    if valid is not None and not valid(spec):
        return None
    will_get_work, has_queue, num_pending = spec.sched_data()
    if not will_get_work:
        return None
    if (not has_queue) and (not allow_empty):
        return None
    # (+1) because 0 pending would leave all things equal weight
    # or trump any weight. This should still preserve ordering
    # between spec scores.
    return (1 + num_pending) / spec.weight


class Md5Writer(object):
    def __init__(self, fd):
        self.fd = fd
        self.md5 = hashlib.md5()

    def write(self, data):
        self.md5.update(data)
        return self.fd.write(data)

    def hexdigest(self):
        return self.md5.hexdigest()
