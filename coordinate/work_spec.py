'''Command-line coordinated client.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import
from __future__ import division
import collections
import logging
import time

import cbor

from .constants import AVAILABLE, FINISHED, FAILED, PENDING, \
    WORK_UNIT_STATUS_NAMES_BY_NUMBER, \
    RUNNABLE, PAUSED, \
    PRI_GENERATED, PRI_STANDARD, \
    MAX_LEASE_SECONDS, MIN_LEASE_SECONDS, DEFAULT_LEASE_SECONDS
from .fifolock import FifoLock
# special improved version of standard library's heapq
from . import heapq
from .server_work_unit import WorkUnit


logger = logging.getLogger(__name__)
jlog = logging.getLogger('coordinate.server.job')


class WorkSpec(object):
    '''Server-side data for a work spec.'''

    def __init__(self, name, jobq, data=None,
                 next_work_spec=None, next_work_spec_preempts=True,
                 weight=20, status=RUNNABLE, continuous=False,
                 interval=None, max_running=None, max_getwork=None):
        '''Create a new work spec.

        All information needed by the work spec is passed
        explicitly.  `data` can be any content that will be
        passed along with work units, though most work units
        will expect it to be in the dictionary format processed
        by :meth:`from_dict`.

        :param str name: name of the work spec
        :param jobq: containing job queue
        :param dict data: arbitrary work spec data
        :type jobq: :class:`JobQueue`
        :param str next_work_spec: name of following work spec
          for automatic job generation, or :const:`None`
        :param bool next_work_spec_preempts: if True then any work
          in the named next_work_spec preemts this one.
        :param int weight: relative weight of this job
          (higher is more important, default=20)
        :param int status: initial status of this job
        :param bool continuous: if true, the scheduler will
          automatically create jobs for this work spec
        :param int interval: if `continuous`, the minimum time
          between creating continuous work units; if :const:`None`
          allow more work units to be created immediately
        :param int max_running: if not :const:`None`, allow only
          this many "pending" jobs
        :param int max_getwork: hand out at most this many WorkUnits
          to a single client in one request.

        '''
        #: core data
        self.data = data
        if data is None:
            self.data = {}
        #: name of this work spec
        self.name = name
        #: name of chained next work spec to run on completion
        self.next_work_spec = next_work_spec
        self.next_work_spec_preempts = next_work_spec_preempts

        #: pointer to parent container
        self.jobq = jobq

        # metadata
        self.weight = weight
        self.status = status
        self.continuous = continuous
        #: minimum time interval before launching another continuous job;
        #: if :const:`None`, launch another immediately
        self.interval = interval
        #: earliest time another continuous job can be launched
        self.next_continuous = 0  # e.g. the Unix epoch
        #: maximum number of concurrent pending jobs, or :const:`None`
        #: for unlimited
        self.max_running = max_running

        self.max_getwork = max_getwork

        # work units:
        #: available queue, ordered by key
        self.queue = []
        #: units pending, ordered by timeout
        self.pending_queue = []
        #: all units, by key
        self.work_units_by_key = {}
        #: number of work units that are no longer in memory
        self.archived_counts_by_status = {}
        self.mutex = FifoLock()

    @classmethod
    def from_dict(cls, data, jobq, **kwargs):
        '''Create a work spec from a dictionary.

        Parameters will be extracted from `data` as follows:

        * `name` is passed directly as the job `name`
        * `then` is passed directly as the `next_work_spec`
        * `nice` is subtracted from 20 to become `weight`; the
          default niceness is 0, higher niceness is lower weight,
          effective maximum niceness is 20
        * `disabled` is a boolean flag which may change the status
          from :data:`RUNNABLE` to :data:`PAUSED`
        * `continuous`, `interval`, and `max_running` are passed
           directly from `data` as corresponding constructor arguments

        '''
        nkwargs = cls.spec_dict_to_kwargs(data)
        nkwargs.update(kwargs)
        return cls(name=data['name'], jobq=jobq, **nkwargs)

    @classmethod
    def spec_dict_to_kwargs(cls, data):
        status = RUNNABLE
        if data.get('disabled', False):
            status = PAUSED

        interval = data.get('interval', None)
        if interval is not None:
            if isinstance(interval, basestring):
                interval = float(interval)
            if not isinstance(interval, (int,long,float)):
                raise ValueError('bad interval {!r}'.format(data['interval']))

        weight = data.get('weight')
        if weight is not None:
            weight = int(weight)
        else:
            weight = max(1, 20 - int(data.get('nice', 0)))

        max_running = data.get('max_running', None)
        if max_running is not None:
            max_running = int(max_running)

        max_getwork = data.get('max_getwork', None)
        if max_getwork is not None:
            max_getwork = int(max_getwork)

        return dict(
            data = data,
            status = status,
            interval = interval,
            weight = weight,
            max_running = max_running,
            max_getwork = max_getwork,
            next_work_spec = data.get('then'),
            next_work_spec_preempts = data.get('then_preempts', True),
            continuous = data.get('continuous', False)
        )

    def __getstate__(self):
        return (self.data, self.weight, self.status, self.continuous,
                self._reduce_work_units_by_key(),
                self.archived_counts_by_status, self.interval,
                self.next_continuous, self.max_running)

    def __setstate__(self, state):
        # run instead of __init__ on de-pickle
        self.data = state[0]
        self._update_data(self.data)
        self.name = self.data['name']
        self.next_work_spec = self.data.get('then')

        self.weight = state[1]
        self.status = state[2]
        self.continuous = state[3]

        self.queue = []
        self.pending_queue = []
        self.work_units_by_key = state[4]
        self.mutex = FifoLock()

        if isinstance(self.work_units_by_key, list):
            self._rebuild_work_units_by_key(self.work_units_by_key)

        if len(state) >= 6:
            self.archived_counts_by_status = state[5]
        else:
            self.archived_counts_by_status = {}

        if len(state) >= 9:
            self.interval = state[6]
            self.next_continuous = state[7]
            self.max_running = state[8]
        else:
            self.interval = None
            self.next_continuous = 0
            self.max_running = None

        # put things in queues
        for k, wu in self.work_units_by_key.iteritems():
            if wu.status == AVAILABLE:
                heapq.heappush(self.queue, wu)
            elif wu.status == PENDING:
                self._pq_push(wu)

        self.jobq = None

    # def _pickle(self):
    #     with self.mutex:
    #         return pickle.dumps(self, protocol=pickle.HIGHEST_PROTOCOL)

    def _reduce_work_units_by_key(self):
        return [
            (kv[0], kv[1].__dict__)
            for kv in self.work_units_by_key.iteritems()
        ]

    def _rebuild_work_units_by_key(self, wubklist):
        work_units_by_key = {}
        for key, wu_dict in wubklist:
            work_units_by_key[key] = _WorkUnit_rebuild(wu_dict)
        self.work_units_by_key = work_units_by_key
        return work_units_by_key

    def _cbor_dump(self, fileo):
        with self.mutex:
            state = self.__getstate__()
            try:
                return cbor.dump(state, fileo)
            except:
                logger.error('could not cbor serialize state for spec %s', self.name, exc_info=True)
                raise

    def _cbor_load(self, filei):
        # no mutex yet, we're making a new object
        # with self.mutex:
        state = cbor.load(filei)
        self.__setstate__(state)

    def _pq_push(self, item):
        heapq.heappush(self.pending_queue, item, cmp_lt=WorkUnit_lease_less)

    def _pq_pop(self, index=0):
        return heapq.heappop(self.pending_queue, index=index,
                             cmp_lt=WorkUnit_lease_less)

    def _pq_replace(self, item, index=0):
        return heapq.heapreplace(self.pending_queue, item, index=index,
                                 cmp_lt=WorkUnit_lease_less)

    def _update_lease(self, wu):
        '''
        move the work unit in the pending queue
        wu.lease_time should have already been updated externally
        '''
        try:
            pos = self.pending_queue.index(wu)
        except ValueError:
            raise Exception('work unit {} expected in pending_queue '
                            'but not found'.format(wu.key))
        # replace wu with itself, and re-heapify it
        self._pq_replace(wu, index=pos)

    def _expire_stale_leases(self):
        # for expired-lease work units, make available again
        # run inside self.mutex context
        now = time.time()
        while self.pending_queue and (self.pending_queue[0].lease_time < now):
            wu = self._pq_pop()
            if self.jobq.do_joblog:
                jlog.info('spec %r unit %r worker %r %s -> AVAILABLE (expired)',
                          self.name, wu.key, wu.worker_id,
                          WORK_UNIT_STATUS_NAMES_BY_NUMBER[wu.status])
            wu.status = AVAILABLE
            wu.lease_time = None
            wu.worker_id = None
            heapq.heappush(self.queue, wu)

    def _repopulate_queue(self):
        '''If this has an alternate backing store, get more jobs.

        Always runs under :attr:`mutex`.

        '''
        pass

    def _log_action(self, action, args):
        self.jobq._log_action(action, args)

    def priority(self):
        "peek at next work unit, return its priority, thus the priority of this queue"
        with self.mutex:
            if not self.queue:
                return 0
            return self.queue[0].priority

    def update_data(self, data):
        with self.mutex:
            assert data['name'] == self.name
            self._update_data(data)

    def _update_data(self, data):
        # must operate within threadsafe context
        kwargs = self.spec_dict_to_kwargs(data)
        for k,v in kwargs.iteritems():
            setattr(self, k, v)

    def add_work_units(self, they):
        '''Add some number of work units to this work spec.

        :param they: work unit(s) to add
        :type they: iterable of :class:`WorkUnit`

        '''
        with self.mutex:
            self._add_work_units(they)

    def _add_work_units(self, they, push=True):
        '''Add work units under the work spec mutex.

        :param they: work unit(s) to add
        :type they: iterable of :class:`WorkUnit`
        :param bool push: add `they` to the available queue

        '''
        for wu in they:
            self._log_action('add_work_unit', (self.name, wu.key, wu.data))
            assert wu.status == AVAILABLE
            if wu.key in self.work_units_by_key:
                logger.warn('adding dup key %r, updating...', wu.key)
                self._update_work_unit(wu.key, status=wu.status, data=wu.data)
                continue

            if push:
                heapq.heappush(self.queue, wu)
            self.work_units_by_key[wu.key] = wu

    def prioritize_work_units(self, work_unit_keys, priority=None,
                              adjustment=None):
        """Adjust the priority of work units"""
        needs_reheap = False
        with self.mutex:
            for wuk in work_unit_keys:
                wu = self.work_units_by_key.get(wuk)
                if wu is not None:
                    if priority is not None:
                        wu.priority = priority
                    elif adjustment is not None:
                        wu.priority += adjustment
                    if wu.status == AVAILABLE:
                        needs_reheap = True
            if needs_reheap:
                # It's quite likely at the wrong place in the
                # priority queue and needs to be fixed;
                # heapify takes O(n) time but so does lookup
                heapq.heapify(self.queue)

    def get_statuses(self, work_unit_keys):
        out = []
        with self.mutex:
            self._expire_stale_leases()
            for wuk in work_unit_keys:
                wu = self.work_units_by_key.get(wuk)
                if wu is None:
                    out.append(None)
                else:
                    sdict = {
                        'status': wu.status,
                        'expiration': wu.lease_time,
                    }
                    if wu.worker_id:
                        sdict['worker_id'] = wu.worker_id
                    if 'traceback' in wu.data:
                        sdict['traceback'] = wu.data['traceback']
                    out.append(sdict)
        return out

    def get_work(self, worker_id, lease_time, max_jobs):
        '''Get an available work unit from this work spec.

        If there are no available work units to do, returns
        :const:`None`.  This can also return :const:`None` if
        there are already :attr:`max_running` pending work units.
        If there is no work but this work spec is :attr:`continuous`,
        could return a new synthetic work unit.

        In all cases, if a work unit is returned, `worker_id` will
        own it and it will move to :data:`PENDING` state.

        :param str worker_id: worker that will own the work unit
        :param long lease_time: latest time the worker will own the
          work unit
        :return: list of :class:`WorkUnit`, maybe empty

        '''
        with self.mutex:
            self._expire_stale_leases()
            self._repopulate_queue()
            max_jobs = nmin(max_jobs, self.max_getwork)
            out = []
            notdone = True
            while notdone:
                work_unit = None
                if ((self.max_running is not None and
                     len(self.pending_queue) >= self.max_running)):
                    # Hit maximum number of concurrent pending jobs
                    break
                elif self.queue:
                    # There is work to do, pull something from it
                    work_unit = heapq.heappop(self.queue)
                elif self.continuous:
                    # No work to do, but we can create sythetic work units
                    now = time.time()
                    if now >= self.next_continuous:
                        work_unit = WorkUnit(str(time.time()), {},
                                             priority=PRI_GENERATED)
                        self._add_work_units([work_unit], push=False)
                        if self.interval is not None:
                            self.next_continuous = now + self.interval
                        notdone = True # only generate one at a time

                # We get no work out of this if
                # (a) max number of concurrent pending jobs; or
                # (b1) the available queue is empty, and
                # (b2a) the work spec is not continuous, or
                # (b2b) the work spec is continuous with an interval and
                #       the interval hasn't passed since the last time
                #       we returned a continuous job

                if work_unit is None:
                    break
                out.append(self._update_work_wu(worker_id, lease_time, work_unit))
                if len(out) >= max_jobs:
                    break
            return out

    def _update_work_wu(self, worker_id, lease_time, wu):
        '''Update a work unit's metadata in :data:`PENDING` status.

        :param str worker_id: worker doing the work
        :param long lease_time: expiration time of the work unit
        :param wu: work unit to update
        :type wu: :class:`WorkUnit`

        '''
        if wu.worker_id or wu.lease_time:
            logger.warn('wu was in avail queue with worker_id=%r '
                        'lease=%r', wu.worker_id, wu.lease_time)
        if self.jobq.do_joblog:
            jlog.debug('spec %r unit %r worker %r %s -> PENDING',
                       self.name, wu.key, worker_id,
                       WORK_UNIT_STATUS_NAMES_BY_NUMBER[wu.status])
        wu.worker_id = worker_id
        wu.lease_time = lease_time
        wu.status = PENDING
        self._pq_push(wu)
        # Note that we log the *result* of get_work(), which is to
        # change the state and lease_time of a work unit.
        self._log_action('update_work_unit',
                         (self.name, wu.key,
                          {'status': PENDING,
                           'lease_time': wu.lease_time,
                           'worker_id': worker_id}))
        # logger.debug('worker %r leased until %s (now %s): %r',
        #              worker_id, lease_time, time.time(), wu.key)
        return wu

    def _available_queue_remove(self, wu):
        pos = 0
        qwu = None
        while pos < len(self.queue):
            if self.queue[pos] == wu:
                qwu = heapq.heappop(self.queue, pos)
                break
            pos += 1
        assert qwu is not None, 'work unit {} expected in queue ' \
            'but not found'.format(wu.key)

    def _pending_queue_remove(self, wu):
        pos = 0
        qwu = None
        while pos < len(self.pending_queue):
            if self.pending_queue[pos] == wu:
                qwu = self._pq_pop(index=pos)
                break
            pos += 1
        assert qwu is not None, 'work unit {} expected in pending_queue ' \
            'but not found'.format(wu.key)

    def get_work_units(self, options):
        '''
        options={work_unit_keys=None, state=None, limit=None, start=None}
        options['state'] accepts either a single state or a list/tuple of states

        return [(key, data), ...], message
        '''
        work_unit_keys = options.get('work_unit_keys')
        states = options.get('state')
        if (states is not None) and (not isinstance(states, (list, tuple))):
            states = (states,)
        start = options.get('start', None)
        limit = int(options.get('limit', 1000))
        if limit > 10000:
            logging.warn('clamping limit to 10000 (got %s)', limit)
            limit = 10000

        with self.mutex:
            # Get for specified keys?
            if work_unit_keys:
                return self._get_work_units_by_keys(work_unit_keys)

            # Get filtered on status?
            if states is not None:
                return self._get_work_units_by_states(states, start, limit)

            # otherwise, all work units
            return self._get_work_units_all(start, limit)

    # overridden by SqliteWorkSpec
    def _get_work_units_by_keys(self, work_unit_keys):
        # Note that if asked for specific keys, we don't
        # bother with 'limit' and just try to return
        # everything that was asked for.
        out = []
        for key in work_unit_keys:
            wu = self.work_units_by_key.get(key)
            wu_data = ((wu is not None) and wu.data) or None
            out.append( (key, wu_data) )
        return out, None

    # overridden by SqliteWorkSpec
    def _get_work_units_by_states(self, states, start, limit):
        # return WorkUnits filtered on set of states
        out = []
        all_wu_of_state = sorted(filter(lambda x: x.status in states, self.work_units_by_key.itervalues()))
        for wu in all_wu_of_state:
            if (start is not None) and (wu.key < start):
                continue
            out.append( (wu.key, wu.data) )
            if (limit is not None) and (len(out) >= limit):
                break
        return out, None

    # overridden by SqliteWorkSpec
    def _get_work_units_all(self, start, limit):
        out = []
        all_wu_keys = sorted(self.work_units_by_key.keys())
        for wukey in all_wu_keys:
            if (start is not None) and (wukey < start):
                continue
            wu = self.work_units_by_key.get(wukey)
            wu_data = ((wu is not None) and wu.data) or None
            out.append( (wukey, wu_data) )
            if (limit is not None) and (len(out) >= limit):
                break
        return out, None

    def update_work_unit(self, work_unit_key, options):
        lease_time = options.get('lease_time')
        status = options.get('status')
        data = options.get('data')
        worker_id = options.get('worker_id')
        wu = None
        finishing = False
        if (lease_time is None) and (status is None):
            return False, 'nothing to do'
        lease_time = self._normalize_lease_time(lease_time)
        with self.mutex:
            self._log_action('update_work_unit',
                             (self.name, work_unit_key, options))
            wu, finishing = self._update_work_unit(work_unit_key, lease_time, status, data, worker_id)

        if wu is None:
            return False, 'no such work unit key={!r}'.format(
                work_unit_key)

        if wu and finishing:
            self._trigger_flow(wu)

        return True, None

    def _normalize_lease_time(self, lease_time):
        if lease_time is not None:
            # lease time is seconds into the future,
            # convert to absolute time.
            if lease_time > MAX_LEASE_SECONDS:
                logger.warn('bogus lease_time %s > %s, clamped to %s',
                            lease_time, MAX_LEASE_SECONDS,
                            DEFAULT_LEASE_SECONDS)
                lease_time = DEFAULT_LEASE_SECONDS
            lease_time = time.time() + float(lease_time)
        return lease_time

    def _update_work_unit(self, work_unit_key, lease_time=None, status=None, data=None, worker_id=None):
        # runs inside self.mutex context, returns updated WorkUnit
        finishing = False
        wu = self.work_units_by_key.get(work_unit_key)
        if wu is None:
            # TODO: if in SqliteWorkSpec, update record
            return None, None
        if data is not None:
            wu.data = data
        if status is not None:
            if self.jobq.do_joblog:
                jlog.debug('spec %r unit %r worker %r %s -> %s',
                           self.name, wu.key, wu.worker_id,
                           WORK_UNIT_STATUS_NAMES_BY_NUMBER[wu.status],
                           WORK_UNIT_STATUS_NAMES_BY_NUMBER[status])
            if (wu.status == AVAILABLE) and (status != AVAILABLE):
                # out of available queue
                self._available_queue_remove(wu)
            if (wu.status != AVAILABLE) and (status == AVAILABLE):
                # into available queue
                wu.worker_id = None
                wu.lease_time = None
                heapq.heappush(self.queue, wu)
            if (wu.status == PENDING) and (status != PENDING):
                # out of pending queue
                self._pending_queue_remove(wu)
            elif wu.status == PENDING:  # and status == PENDING
                if lease_time is not None:
                    # was pending, still pending, update lease_time
                    wu.lease_time = lease_time
                    self._update_lease(wu)
                if worker_id is not None:
                    if (((wu.worker_id is not None) and
                         (wu.worker_id != worker_id))):
                        logger.warn('old worker_id = %r, '
                                    'new worker_id = %r, '
                                    'double assignment?',
                                    wu.worker_id, worker_id)
                    wu.worker_id = worker_id
            if (wu.status != PENDING) and (status == PENDING):
                # into pending queue
                if lease_time is not None:
                    wu.lease_time = lease_time
                self._pq_push(wu)
            if (wu.status != FINISHED) and (status == FINISHED):
                # record the actual completion time
                wu.lease_time = time.time()
                # we will need to trigger jobs but can't do it
                # under the mutex
                finishing = True
            if (wu.status == FINISHED) and (status == FAILED):
                # don't fail an already-finished job
                status = wu.status
            if (wu.status != FAILED) and (status == FAILED):
                # record the completion time; do not trigger
                wu.lease_time = time.time()
            wu.status = status

        elif lease_time is not None:
            wu.lease_time = lease_time
            if wu.status == PENDING:
                self._update_lease(wu)

        return wu, finishing

    def _trigger_flow(self, wu):
        '''Potentially create new work units from the output of 'wu'.

        This requires ``wu.data['output']`` to be a dictionary,
        and it requires :attr:`next_work_spec` to exist.  If both
        of these are true, then submits a request to start the next
        work units.

        Any errors are silently ignored.

        :param wu: work unit that is completing
        :type wu: :class:`WorkUnit`

        '''
        if (not wu.data) or (not hasattr(wu.data, 'get')):
            return
        work_units = wu.data.get('output', None)
        if not work_units:
            logger.debug('no output, not triggering')
            return
        if not self.next_work_spec:
            logger.debug('%s done with no then:', self.name)
            return

        if isinstance(work_units, collections.Mapping):
            work_units = work_units.items()

        # upgrade plain strings to work unit tuples.
        # cause priority inheritance from source wu to next wu.
        def solidify_wu(x):
            if isinstance(x, basestring):
                return (x, {}, {'priority': wu.priority})
            if len(x) == 2:
                return (x[0], x[1], {'priority': wu.priority})
            if 'priority' not in x[2]:
                x[2]['priority'] = wu.priority
            return x

        work_units = [solidify_wu(x) for x in work_units]
        self.jobq.add_work_units(self.next_work_spec, work_units)

    def del_work_units(self, options):
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
        with self.mutex:
            self._log_action('del_work_units', (self.name, options))
            if options.get('all', False):
                return self._del_work_units_all()

            filterstate = options.get('state', None)
            delkeys = options.get('work_unit_keys', None)

            if delkeys is not None:
                return self._del_work_units_keys(delkeys, filterstate)

            if filterstate is not None:
                return self._del_work_units_state(filterstate)

        return 0, 'no usable options to del_work_units'

    # override in SqliteWorkSpec
    def _del_work_units_all(self):
        count = len(self.work_units_by_key)
        self.queue = []
        self.pending_queue = []
        self.work_units_by_key = {}

        # clear archive
        for st, stc in self.archived_counts_by_status.iteritems():
            count += stc
        self.archived_counts_by_status = {}

        return count, None

    # override in SqliteWorkSpec
    def _del_work_units_state(self, filterstate):
        delkeys = []
        for wu_key, wu in self.work_units_by_key.iteritems():
            if wu.status != filterstate:
                # not this wu
                continue

            # yes, delete this wu
            if wu.status == AVAILABLE:
                self._available_queue_remove(wu)
            if wu.status == PENDING:
                self._pending_queue_remove(wu)
            delkeys.append(wu_key)
        for wu_key in delkeys:
            del self.work_units_by_key[wu_key]
        count = len(delkeys)
        count += self.archived_counts_by_status.pop(filterstate, 0)
        return count, None

    # override in SqliteWorkSpec
    def _del_work_units_keys(self, delkeys, filterstate=None):
        '''Delete work units with specific names.

        All work units whose names are in `delkeys` are deleted.
        If `filterstate` is not :const:`None`, they must be in that
        state to be deleted.

        Runs under :attr:`mutex`.

        :param delkeys: work unit keys to delete
        :type delkeys: list of str
        :param int filterstate: required state, or :const:`None`
        :return: pair of number of work units deleted and error message

        '''
        count = 0
        for wu_key in delkeys:
            wu = self.work_units_by_key.get(wu_key)
            if wu is None:
                continue
            if filterstate is not None and wu.status != filterstate:
                continue
            self.work_units_by_key.pop(wu_key)
            count += 1
            if wu.status == AVAILABLE:
                self._available_queue_remove(wu)
            if wu.status == PENDING:
                self._pending_queue_remove(wu)
        return count, None

    def archive_work_units(self, max_count, max_age):
        '''Remove record of old finished work units.

        Their existence is preserved only in
        :attr:`archived_counts_by_status`.  Both finished and
        failed work units are archived; available and pending
        work units are not.

        :param int max_count: keep no more than this many
          finished and failed work units (each)
        :param int max_age: discard finished and failed work
          units older than this (in seconds)

        '''
        with self.mutex:
            self._archive_status(FINISHED, max_count, max_age)
            self._archive_status(FAILED, max_count, max_age)

    def _archive_status(self, status, max_count, max_age):
        # Take a pass through all of the work units; ignore if they don't
        # have the right status; discard immediately if they're too old;
        # else queue up with start time and name
        to_remove = []
        to_consider = []
        now = time.time()

        for name, wu in self.work_units_by_key.iteritems():
            if wu.status != status:
                continue
            age = now - wu.lease_time
            #print("wu={} lease_time={} now={} age={} max_age={}"
            #      .format(name, wu.lease_time, now, age, max_age))
            if max_age is not None and age > max_age:
                to_remove.append(wu.key)
            else:
                to_consider.append((wu.lease_time, wu.key))

        if max_count is not None:
            # Sort these so the most recent is first, then chop
            to_consider.sort(reverse=True)
            to_remove += [p[1] for p in to_consider[max_count:]]

        for wu_key in to_remove:
            del self.work_units_by_key[wu_key]
            count = self.archived_counts_by_status.get(status, 0)
            self.archived_counts_by_status[status] = count + 1

    def count_work_units(self):
        "Return dictionary by status which is sum of current and archived work unit counts."
        with self.mutex:
            return self._count_work_units()

    def _count_work_units(self):
        self._expire_stale_leases()
        out = collections.Counter(self.archived_counts_by_status)
        for wu in self.work_units_by_key.itervalues():
            out[wu.status] += 1
        return dict(out), None

    def sched_data(self):
        '''
        One call to do one mutex cycle and get the scheduler what it needs.
        return (will get work, has queue, num pending)
        '''
        with self.mutex:
            return self._will_get_work(), bool(self.queue), len(self.pending_queue)

    def _will_get_work(self):
        '''Determine whether :meth:`get_work` will return anything.

        This will return :const:`True` exactly when :meth:`get_work`
        will return something other than :const:`None`, up to
        concurrency constraints.

        must run inside mutex
        '''
        self._expire_stale_leases()
        self._repopulate_queue()
        if ((self.max_running is not None and
             len(self.pending_queue) >= self.max_running)):
            # Hit maximum number of concurrent pending jobs
            return False
        elif self.queue:
            # There is work to do
            return True
        elif self.continuous:
            # No work to do, but we can create sythetic work units
            now = time.time()
            if now >= self.next_continuous:
                return True
        return False

    def __len__(self):
        with self.mutex:
            self._expire_stale_leases()
            return len(self.queue)


def _WorkUnit_rebuild(wu_dict):
    # this is kinda what pickle does behind the scenes...
    wu = WorkUnit.__new__(WorkUnit)
    wu.__dict__ = wu_dict
    return wu


# cmp_lt for improved heapq.* functions
def WorkUnit_lease_less(a, b):
    if a is None:
        return False
    if b is None:
        return True
    if a.lease_time is None:
        return False
    if b.lease_time is None:
        return True
    return a.lease_time < b.lease_time


def nmin(*args):
    # non-None min()
    xm = None
    for x in args:
        if (xm is None) or ((x is not None) and (x < xm)):
            xm = x
    return xm
