'''Client to jobd work queue server

This provides :class:`TaskMaster` and :class:`WorkUnit` whith are the
primary public interface to the coordinate server.
:class:`coordinate.job_client.TaskMaster`
communicates with the coordinated server.
:class:`WorkUnit` is received by code running under the :class:`Worker`
framework.

The implementations in this module contain the most common use
patterns for :mod:`coordinate`.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

.. autoclass:: WorkUnit
   :members:

.. autoclass:: TaskMaster
   :members:

'''
from __future__ import absolute_import
from __future__ import division  # makes / default to float/float=>float
from __future__ import print_function
import collections
from copy import deepcopy
import logging
import traceback

import yaml

from coordinate.exceptions import ProgrammerError
import yakonfig
from .base_client import CborRpcClient
from .constants import AVAILABLE, BLOCKED, PENDING, FINISHED, FAILED, \
    RUN, IDLE, TERMINATE, RUNNABLE, PAUSED, PRI_PRIORITY
from .workunit_run import run as workunit_run

logger = logging.getLogger(__name__)


# You may not lease a work unit for more than one day
MAX_LEASE_SECONDS = (24 * 3600)
# It is inadvisable to lease for less than 10 seconds. Lag happens.
MIN_LEASE_SECONDS = 10


class WorkUnit(object):
    '''Client-side implementation of a single unit of work.

    This has a reference to a work spec, a name (often a file name),
    and a dictionary of data.  This object is passed to the
    `run_function` named in the work spec.

    '''
    def __init__(self, taskmaster, work_spec_name, key, data, worker_id=None,
                 expires=None, default_lease_time=900):
        #: :class:`TaskMaster` connected to this unit
        self.taskmaster = taskmaster
        #: Name of the work spec
        self.work_spec_name = work_spec_name
        #: Name of the work unit
        self.key = key
        #: Data provided for the work unit
        self.data = data
        #: Worker currently responsible for the work unit
        self.worker_id = worker_id
        #: Expiration time for the work unit
        self.expires = expires
        # storage for lazy getter property
        self._spec_cache = None
        # storage to notice data updates
        self._old_data = deepcopy(data)
        #: How long update() extends leases by default
        self.default_lease_time = default_lease_time

    def __str__(self):
        return 'work unit {!r} {!r} {!r}'.format(self.work_spec_name,
                                                 self.key, self.data)

    def __repr__(self):
        return ('WorkUnit('
                'work_spec_name={self.work_spec_name}, '
                'key={key}, '
                'worker_id={self.worker_id}, '
                'expires={self.expires}, '
                'default_lease_time={self.default_lease_time}, '
                'data={data})'.format(self=self, key=repr(self.key),
                                      data=repr(self.data)))

    @property
    def spec(self):
        '''Work spec object (fetched on first use).'''
        if self._spec_cache is None:
            if self.taskmaster is not None:
                self._spec_cache = self.taskmaster.get_work_spec(
                    self.work_spec_name)
            assert self._spec_cache is not None, \
                'need work spec value for spec={} unit={}'.format(
                    self.work_spec_name, self.key)
        return self._spec_cache

    @spec.setter
    def spec(self, value):
        self._spec_cache = value

    def _do_update(self, **kwargs):
        if self.data != self._old_data:
            kwargs['data'] = self.data
        rc = self.taskmaster.update_work_unit(self.work_spec_name,
                                              self.key, **kwargs)
        if self.data != self._old_data:
            self._old_data = deepcopy(self.data)
        return rc

    def update(self, lease_time=None):
        '''Check in with the server and extend the lease.'''
        if lease_time is None:
            lease_time = self.default_lease_time
        if lease_time > MAX_LEASE_SECONDS:
            lease_time = MAX_LEASE_SECONDS
        return self._do_update(lease_time=lease_time)

    def finish(self):
        '''Mark this job as completed.'''
        return self._do_update(status=FINISHED)

    def fail(self, exc=None):
        '''Mark this job as failed.'''
        if exc:
            self.data['traceback'] = traceback.format_exc(exc) or exc
        return self._do_update(status=FAILED)

    def run(self):
        return workunit_run(self, self.spec)


class TaskMaster(CborRpcClient):
    '''Client-side interface to the job system.
    '''

    # states for the system as a whole
    RUN = RUN
    IDLE = IDLE
    TERMINATE = TERMINATE

    # states for a work unit
    AVAILABLE = AVAILABLE
    BLOCKED = BLOCKED
    PENDING = PENDING
    FINISHED = FINISHED
    FAILED = FAILED

    def __init__(self, config):
        self.config = config
        self.default_lifetime = config.get('default_lifetime', 900)
        super(TaskMaster, self).__init__(config)

    @classmethod
    def validate_work_spec(cls, work_spec):
        '''Check that `work_spec` is valid.

        It must at the very minimum contain a ``name`` and ``min_gb``.

        :raise coordinate.exceptions.ProgrammerError: if it isn't valid

        '''
        if 'name' not in work_spec:
            raise ProgrammerError('work_spec lacks "name"\n' +
                                  yaml.dump(work_spec))
        if 'min_gb' not in work_spec or \
                not isinstance(work_spec['min_gb'], (float, int, long)):
            raise ProgrammerError('work_spec["min_gb"] must be a number')

    def set_work_spec(self, work_spec):
        '''Add or a replace a work spec contents.

        `work_spec` is a dictionary.  Its `name` is used as the
        `work_spec_name` parameters in other calls that refer
        to this.  It must pass :meth:`validate_work_spec`.
        `nice` is used for prioritization, if set.

        :param dict work_spec: work spec definition
        :return: :const:`True` if the work spec was successfully
          added or updated
        :raise coordinate.exceptions.ProgrammerError: if it isn't valid

        '''
        self.validate_work_spec(work_spec)
        ok, msg = self._rpc('set_work_spec', (work_spec,))
        if msg:
            logger.debug('%s', msg)
        return ok

    def get_work_spec(self, work_spec_name):
        '''Get a work spec back from the server.

        :param str work_spec_name: name of the work spec
        :return: dictionary corresponding to `work_spec_name`, or
          :const:`None` if it is undefined

        '''
        # TODO: caching? (with LRU and TTL?)
        work_spec = self._rpc('get_work_spec', (work_spec_name,))
        return work_spec

    def pause_work_spec(self, work_spec_name, paused=True):
        '''Pause or resume a work spec.

        :param str work_spec_name: name of the work spec
        :param paused: :const:`True` to pause the work spec, or
          :const:`False` to resume it

        '''
        options = {'status': RUNNABLE}
        if paused:
            options['status'] = PAUSED
        self._rpc('control_work_spec', (work_spec_name, options))

    def list_work_specs(self, limit=None, start=None):
        '''Get a listing of (some of) the work spec dictionaries.

        This interface is compatible with
        :class:`coordinate.TaskMaster`.  Use :meth:`iter_work_specs`
        to get dictionaries back directly in a streaming fashion.

        :param int start: zero-based index of first spec to retrieve;
          if :const:`None`, start at the beginning
        :param int limit: number of work specs to retrieve; if
          :const:`None`, get all of the remainder
        :return: pair of list of pairs of (work spec name, work spec)
          and next start value

        '''
        options = {}
        if limit is not None:
            options['limit'] = limit
        if start is not None:
            options['start'] = start
        ws_list, next = self._rpc('list_work_specs', (options,))
        return [(spec['name'], spec) for spec in ws_list], next

    def iter_work_specs(self, limit=None, start=None):
        '''Iterate over (some of) the work spec dictionaries.

        This is a generator function yielding work spec dictionaries.

        :param int start: zero-based index of first spec to retrieve;
          if :const:`None`, start at the beginning
        :param int limit: number of work specs to retrieve; if
          :const:`None`, get all of the remainder

        '''
        # TODO: set a smaller limit and make this more streaming
        # (not that there are usually that many work specs)
        count = 0
        options = {}
        if limit is not None:
            options['limit'] = limit
        if start is not None:
            options['start'] = start
        ws_list, start = self._rpc('list_work_specs', (options,))
        while True:
            for ws in ws_list:
                yield ws
                count += 1
                if (limit is not None) and (count >= limit):
                    break
            if not start:
                break
            if limit is not None:
                limit -= count
                options['limit'] = limit
            options['start'] = start
            ws_list, start = self._rpc('list_work_specs', (options,))

    def clear(self):
        '''Delete everything.
        Deletes all work units and all work specs.
        '''
        delcount = self._rpc('clear', ())
        return delcount

    def del_work_spec(self, work_spec_name):
        '''Delete a work spec.

        :param str work_spec_name: name of work spec to delete
        :return: :const:`True` if the work spec was successfully
          deleted

        '''
        ok, message = self._rpc('del_work_spec', (work_spec_name,))
        if message:
            logger.debug('del_work_spec: %s', message)
        return ok

    def add_work_units(self, work_spec_name, work_unit_key_vals):
        '''Add work units to an existing work spec.

        `work_unit_key_vals` may be either a dictionary mapping
        work unit names to work unit definitions, or a list of
        pairs of names and definitions.  The provided list is
        sent to the server in one batch. The list may also be
        a *triple* (key, data, metadata). metadata should be a
        dict, currently supporting {'priority':int}.

        :param str work_spec_name: name of work spec
        :param work_unit_key_vals: work units to add
        :type work_unit_key_vals: list or dict
        :return: :const:`True` if all of the work units were added

        '''
        if isinstance(work_unit_key_vals, collections.Mapping):
            work_unit_key_vals = work_unit_key_vals.items()
        ok, message = self._rpc('add_work_units',
                                (work_spec_name, work_unit_key_vals))
        if message:
            logger.debug('add_work_units: %s', message)
        return ok

    def count_work_units(self, work_spec_name):
        '''Get the number of work units in all states.

        :param str work_spec_name: name of work spec
        :return: dictionary of status string to count

        '''
        counts, message = self._rpc('count_work_units', (work_spec_name,))
        if message:
            logger.debug('count_work_units: %s', message)
        return counts

    def status(self, work_spec_name):
        '''Get the number of work units in all states.

        This method is for compatibility with
        :class:`coordinate.TaskMaster`.  The return value is a
        dictionary with keys `num_available`, `num_pending`,
        `num_blocked`, `num_finished`, `num_failed`, and `num_tasks`
        as a sum of all of these.

        Code that is aware that it is using
        :class:`coordinate.TaskMaster` should call
        :meth:`count_work_units` instead.

        :param str work_spec_name: name of work spec
        :return: dictionary of status label to count

        '''
        # logger.warn('deprecated old interface TaskMaster.status')
        counts = self.count_work_units(work_spec_name)
        return {
            'num_available': counts.get(AVAILABLE, 0),
            'num_pending': counts.get(PENDING, 0),
            'num_blocked': counts.get(BLOCKED, 0),
            'num_finished': counts.get(FINISHED, 0),
            'num_failed': counts.get(FAILED, 0),
            'num_tasks': sum(counts.itervalues())
        }

    def prioritize_work_units(self, work_spec_name, work_unit_keys,
                              priority=None, adjustment=None):
        '''Reprioritize work units within a work spec.

        Every work unit has a priority associated with it.  This may
        be one of the constants
        :data:`~coordinate.constants.PRI_GENERATED`,
        :data:`~coordinate.constants.PRI_STANDARD`, or
        :data:`~coordinate.constants.PRI_PRIORITY`, or it may be any
        other numeric value.  In most cases work unit priorities
        default to :data:`~coordinate.constants.PRI_STANDARD` (0).
        Higher-priority work units will be done first, before other
        work units get to run.

        If `priority` is given, change the priority of
        `work_unit_keys` to that priority.  Otherwise if `adjustment`
        is given, add that amount to the priority of `work_unit_keys`.
        If neither is given, set the priority of `work_unit_keys` to
        :data:`~coordinate.constants.PRI_PRIORITY`.  If any of the
        keys are not present or have already been started, this
        failure is ignored.

        :param str work_spec_name: name of the work spec
        :param list work_unit_keys: names of the work unit(s) to
          prioritize
        :return: :const:`True` on success, :const:`False` if
          `work_spec_name` is invalid

        '''
        if priority is None and adjustment is None:
            priority = PRI_PRIORITY
        rc, msg = self._rpc('prioritize_work_units',
                            (work_spec_name,
                             {'work_unit_keys': work_unit_keys,
                              'priority': priority,
                              'adjustment': adjustment}))
        if msg:
            logger.debug('prioritize_work_units: %s', msg)
        return rc

    def get_work_units(self, work_spec_name, work_unit_keys=None,
                       state=None, limit=None, start=None):
        '''Get (some of) the work units for a work spec.

        If `work_unit_keys` is provided, retrieve all of those keys,
        ignoring the other parameters.

        If `state` is provided, retrieve only work units in that state
        (or any named state, if `state` is a list or tuple); otherwise
        retrieve work units in any state.  Then return a window of
        those work units specified by `start` and `limit`.

        :param str work_spec_name: name of the work spec
        :param list work_unit_keys: if not :const:`None`, get all of
          these work units and no others
        :param state: if :const:`None`, get all work units; if a list
          or tuple, work units in any of these states; if a string,
          work units in that specific state
        :param int start: zero-based index of first work unit
        :param int limit: number of work units to retrieve
        :return: list of pairs of work unit name and work unit data,
          or :const:`None` if `work_spec_name` is invalid

        '''
        options = {}
        if work_unit_keys is not None:
            options['work_unit_keys'] = work_unit_keys
        if state is not None:
            options['state'] = state
        if limit is not None:
            options['limit'] = limit
        if start is not None:
            options['start'] = start
        key_wudata_list, message = self._rpc('get_work_units',
                                             (work_spec_name, options))
        if message:
            logger.debug('get_work_units: %s', message)
        return key_wudata_list

    def _list_work_units(self, work_spec_name, start, limit, state):
        # get_work_units() (and the job_server equivalent) takes start
        # as the *name* of a work unit, not the count.  Is this wrong?
        # Work around it for now
        if limit is not None:
            limit += start
        many_work_units = self.get_work_units(
            work_spec_name, limit=limit, state=state)
        work_unit_pairs = many_work_units[start:]
        return dict([(k, v) for (k, v) in work_unit_pairs])

    def list_work_units(self, work_spec_name, start=0, limit=None):
        '''Get a dictionary of unfinished work units for some work spec.

        This is compatible with :class:`coordinate.TaskMaster`.

        .. seealso:: :meth:`get_work_units`

        '''
        return self._list_work_units(work_spec_name, start, limit,
                                     (AVAILABLE, PENDING))

    def list_finished_work_units(self, work_spec_name, start=0, limit=None):
        return self._list_work_units(work_spec_name, start, limit,
                                     FINISHED)

    def del_work_units(self, work_spec_name, work_unit_keys=None,
                       state=None, all=False):
        '''Delete work units from a work spec.

        If `all` is :const:`True`, then all work units in
        `work_spec_name` are deleted.

        If `all` is :const:`False`, and a list of `work_unit_keys`
        is given, then only those work units are deleted; if
        `state` is also given, then only work units in that state
        are deleted.

        If `all` is :const:`False`, `work_unit_keys` is :const:`None`,
        and `state` is one of the state constants, then all work units
        in `work_spec_name` with that state are deleted.

        If no non-default parameters are given, nothing is deleted.

        :param str work_spec_name: name of the work spec
        :param list work_unit_keys: if not :const:`None`, only delete
          these specific keys
        :param str state: only delete work units in this state
        :param bool all: if true, delete all work units
        :return: number of work units deleted

        '''
        options = {}
        if work_unit_keys is not None:
            options['work_unit_keys'] = work_unit_keys
        if state is not None:
            options['state'] = state
        if all is True:
            options['all'] = True
        count, message = self._rpc('del_work_units',
                                   (work_spec_name, options))
        if message:
            logger.debug('del_work_units: %s', message)
        return count

    def get_work_unit_status(self, work_spec_name, work_unit_keys):
        '''Get the status for some number of work units.

        The return value is a list of statuses in the same order as
        `work_unit_keys`.  If any work unit is undefined, its status
        in the list is :const:`None`.  If the work spec is undefined
        then the return value of the function is :const:`None`.

        Status returned is a dict with string keys:

        `status`
          An integer constant, one of :attr:`AVAILABLE`, :attr:`PENDING`,
          :attr:`BLOCKED`, :attr:`FAILED`, or :attr:`FINISHED`
        `expiration`
          Time number, in seconds since the Unix epoch
        `worker_id`
          Worker ID string if leased
        `traceback`
          Error traceback if failed

        :param str work_spec_name: name of the work spec
        :param list work_unit_keys: names of keys to retrieve
        :return: list of corresponding statuses

        '''
        one = False
        if isinstance(work_unit_keys, basestring):
            one = True
            work_unit_keys = [work_unit_keys]
        statuses, message = self._rpc('get_work_unit_status',
                                      (work_spec_name, work_unit_keys))
        if message:
            logger.debug('get_work_unit_status: %s', message)
        if one:
            return statuses[0]
        return statuses

    def update_work_unit(self, work_spec_name, work_unit_key,
                         lease_time=None, status=None, data=None):
        '''Record progress on a work unit or change its status.

        In most cases this will be called from corresponding functions
        on :class:`WorkUnit`.

        :param str work_spec_name: name of the work spec
        :param str work_unit_key: name of the work unit
        :param int lease_time: extend lease by this long
        :param str status: change status to this
        :param dict data: change work unit data to this
        :return: :const:`True` if the change was succesful

        '''
        options = {}
        if lease_time is not None:
            assert lease_time < MAX_LEASE_SECONDS, \
                'lease_time too big {} > {}'.format(
                    lease_time, MAX_LEASE_SECONDS)
            options['lease_time'] = lease_time
        if status is not None:
            options['status'] = status
        if data is not None:
            options['data'] = data
        ok, message = self._rpc('update_work_unit',
                                (work_spec_name, work_unit_key, options))
        if message:
            logger.debug('update_work_unit: %s', message)
        return ok

    def get_work(self, worker_id, available_gb=None, lease_time=None,
                 work_spec_names=None, max_jobs=1):
        '''Get one or more WorkUnit for this client to work on.

        If there is no work to do, returns :const:`None`.  Otherwise, if
        `max_jobs` is exactly 1, returns the single :class:`WorkUnit`
        object; and otherwise returns a list of :class:`WorkUnit`.  In
        this last case the worker owns a lease on all of the work units
        until the `lease_time` expires.

        :param str worker_id: worker ID that is doing work
        :param int available_gb: maximum memory size for the job
        :param int lease_time: requested time to do the job
        :param int max_jobs: maximum number of work units to return (default 1)
        :param list work_spec_names: only consider these work specs
        :return: a single :class:`WorkUnit` if max_jobs is 1; or a list
           of :class:`WorkUnit`; or :const:`None` if there is no
           (eligible) work to do

        '''
        options = {}
        if lease_time is None:
            lease_time = self.default_lifetime
        if lease_time is not None:
            assert lease_time < MAX_LEASE_SECONDS, \
                'lease_time too big {} > {}'.format(
                    lease_time, MAX_LEASE_SECONDS)
            if lease_time < MIN_LEASE_SECONDS:
                logger.warn('get_work lease_seconds too small %s < %s',
                            lease_time, MIN_LEASE_SECONDS)
            options['lease_time'] = lease_time
        if work_spec_names is not None:
            options['work_spec_names'] = work_spec_names
        if available_gb is not None:
            if not isinstance(available_gb, (int, long, float)):
                raise ValueError('available_gb must be a number, got {0!r}'
                                 .format(type(available_gb)))
            options['available_gb'] = available_gb
        if max_jobs is not None:
            if not isinstance(max_jobs, (int, long)):
                raise ValueError('max_jobs must be int or long, got {0!r}'
                                 .format(type(max_jobs)))
            options['max_jobs'] = max_jobs
        keydata, message = self._rpc('get_work', (worker_id, options))
        if message:
            logger.debug('get_work: %s', message)
        if (max_jobs is None) or (max_jobs == 1):
            if (keydata[0] is None) or (keydata[1] is None):
                return None
            # TODO: get lease timeout back from server?
            return WorkUnit(self, keydata[0], keydata[1], keydata[2],
                            worker_id, default_lease_time=lease_time)
        else:
            return [WorkUnit(self, kd[0], kd[1], kd[2], worker_id,
                             default_lease_time=lease_time)
                    for kd in keydata]

    def retry(self, work_spec_name, *work_unit_names):
        '''Move work units back into the available queue.

        This is principally for compatibility with
        :meth:`coordinate.TaskMaster.retry`, which requires that
        the work units in question have failed.  This implementation
        allows a work unit in any status to be retried; if it is
        currently :attr:`PENDING` then other workers can start working
        on it immediately.

        Code that is aware that it is using
        :class:`coordinate.TaskMaster` should call
        :meth:`update_work_unit` instead, setting its state to
        :attr:`AVAILABLE`.

        :param str work_spec_name: name of the work spec
        :param str work_unit_names: work units to retry
        :return: :const:`True` if all of the work units could be retried.

        '''
        allok = True
        for wukey in work_unit_names:
            ok = self.update_work_unit(work_spec_name, wukey,
                                       lease_time=0, status=AVAILABLE)
            if not ok:
                logger.warn('failed to update for retry key %r', wukey)
                allok = False
        return allok

    def update_bundle(self, work_spec, work_units, nice=None):
        '''Reset a work spec and add units together.

        This method is for compatibility with
        :meth:`coordinate.TaskMaster.update_bundle`.  New code written
        specifically for :mod:`coordinate` should use
        :meth:`set_work_spec` and :meth:`add_work_units` directly.
        This always overwrites the work specification with `work_spec`
        (even if it is unchanged), but in the traditional
        :class:`coordinate.TaskMaster` implementation it is the only way
        to add new work units to a work spec.

        :param dict work_spec: work specification
        :param dict work_units: map of work unit name to definition

        '''
        if nice is not None:
            logger.warn('coordinated job server does not understand workspec '
                        'nice value, ignored value=%r', nice)
        self.set_work_spec(work_spec)
        self.add_work_units(work_spec['name'], work_units)

    def set_mode(self, mode):
        '''Do nothing.

        TODO: Delete. This was relevant to an early version of the API.
        '''
        pass

    def get_mode(self):
        '''Returns ``RUN``.

        TODO: Delete. This was relevant to an early version of the API.
        '''
        return RUN

    def get_child_work_units(self, worker_id):
        '''Get work units assigned to a worker's children.

        Returns a dictionary mapping worker ID to :class:`WorkUnit`.
        If a child exists but is idle, that worker ID will map to
        :const:`None`.  The work unit may already be expired or
        assigned to a different worker; this will be reflected in
        the returned :class:`WorkUnit`.

        '''
        wudict, msg = self._rpc('get_child_work_units', (worker_id,))
        if msg:
            logger.debug('get_child_work_units: %r', msg)
        out = {}
        for k, partsl in wudict.iteritems():
            if partsl:
                out[k] = [
                    WorkUnit(self,
                             parts.get('work_spec_name', None),
                             parts.get('work_unit_key', None),
                             parts.get('work_unit_data', None),
                             worker_id=parts.get('worker_id', None),
                             expires=parts.get('expires', None))
                    for parts in partsl]
        return out

    def worker_register(self, worker_id, mode=None, lifetime=6000,
                        environment=None, parent=None):
        # actually the same as heartbeat, just, "hello, I'm here"
        self.worker_heartbeat(worker_id, mode, lifetime, environment, parent)

    def worker_heartbeat(self, worker_id, mode=None, lifetime=6000,
                         environment=None, parent=None):
        ok, msg = self._rpc('worker_heartbeat',
                            (worker_id, mode, lifetime, environment, parent))
        if not ok:
            logger.debug('worker_heartbeat: %r', msg)

    def worker_unregister(self, worker_id, parent=None):
        ok, msg = self._rpc('worker_unregister', (worker_id,))
        if not ok:
            logger.debug('worker_unregister: %r', msg)

    def workers(self, alive=True):
        '''Get a listing of all workers.

        This returns a dictionary mapping worker ID to the mode
        constant for their last observed mode.

        :param bool alive: if true (default), only include workers
          that have called :meth:`Worker.heartbeat` sufficiently recently

        '''
        # TODO: this isn't a very useful API. Add a mode flag or make
        # a new API which returns fused list of workers with data like
        # from get_heartbeat() ?
        data, msg = self._rpc('list_worker_modes', ())
        if msg:
            logger.debug('list_worker_modes: %r', msg)
        return data

    def get_heartbeat(self, worker_id):
        '''Get the last known state of some worker.

        If the worker never existed, or the worker's lifetime has
        passed without it heartbeating, this will return an empty
        dictionary.

        The return value is a dictionary with string keys:

        `worker_id`
          String containing `worker_id`
        `host`
          String with the worker's host name
        `fqdn`
          String with the worker's fully-qualified domain name
        `version`
          String with the :mod:`coordinate` package version
        `working_set`
          List of installed packages from :mod:`pkg_resources`
        `memory`
          Float
        `age_seconds`
          Integer time since the server last heard from this worker

        :param str worker_id: worker ID
        :return: dictionary of worker state, or empty dictionary
        :see: :meth:`Worker.heartbeat`

        '''
        data, msg = self._rpc('get_worker_info', (worker_id,))
        if msg:
            logger.debug('get_worker_info: %r', msg)
        return data

    def mode_counts(self):
        '''Get the number of workers in each mode.

        This returns a dictionary where the keys are mode constants
        and the values are a simple integer count of the number of
        workers in that mode.

        '''
        data, msg = self._rpc('mode_counts', ())
        if msg:
            logger.debug('mode_counts: %r', msg)
        return data

    def add_flow(self, flow, config=None):
        '''Add a series of related work specs.

        `flow` is a dictionary, where the keys are work spec names and
        the values are either abbreviated work spec definitions or
        flow dictionaries that could be recursively passed into this
        function.  Each work spec is amended by adding `name` based on
        its key and ancestry, and by adding `config` as either the
        `config` parameter or the current global configuration if
        :const:`None`.

        If a given work spec contains a `config` parameter, that
        parameter is overlaid over the provided configuration for that
        specific work spec.  Thus, a work spec may contain a partial
        configuration for a specific setup of
        :mod:`streamcorpus_pipeline`, and the global configuration
        can contain shared settings for :mod:`kvlayer`.

        :param dict flow: work spec or dictionary of work specs
        :param dict config: global config to save with work specs
        :see: :func:`yakonfig.overlay_config`

        '''
        # default to the global config
        if config is None:
            config = yakonfig.get_global_config()

        # collect all work specs and trap errors before submitting any
        work_specs = {}
        worklist = flow.items()
        while worklist:
            (k, v) = worklist.pop()
            if not isinstance(v, collections.Mapping):
                raise ProgrammerError('invalid work spec flow definition')
            if 'min_gb' in v:
                # d is a work spec
                v['name'] = k
                if 'config' in v:
                    v['config'] = yakonfig.overlay_config(config, v['config'])
                else:
                    v['config'] = config
                work_specs[k] = v
            else:
                for kk, vv in v.iteritems():
                    worklist.append((k + '.' + kk, vv))

        # check that chaining is correct
        for k, v in work_specs.iteritems():
            if 'then' in v:
                if v['then'] not in work_specs:
                    raise ProgrammerError(
                        'work spec {} chained to invalid work spec {}'
                        .format(k, v['then']))

        # all good, submit them all
        for d in work_specs.itervalues():
            self.set_work_spec(d)

    def get_server_config(self):
        '''Get the server's global configuration.'''
        data, msg = self._rpc('get_config', ())
        if msg:
            logger.debug('get_config: %r', msg)
        return data
