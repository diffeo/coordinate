'''Command-line coordinated client.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import
from __future__ import division
import logging


from .constants import AVAILABLE, FINISHED, FAILED, PENDING, \
    WORK_UNIT_STATUS_NAMES_BY_NUMBER, \
    RUNNABLE, PAUSED, \
    PRI_GENERATED, PRI_STANDARD
from .work_spec import WorkSpec, _WorkUnit_rebuild
# special improved version of standard library's heapq
from . import heapq
from .server_work_unit import WorkUnit


logger = logging.getLogger(__name__)


class SqliteWorkSpec(WorkSpec):
    _to_store_batch = 20
    _queue_max = 600
    _queue_min = 200
    _put_size = 20
    _get_size = 200

    def __init__(self, name, jobq, data=None,
                 next_work_spec=None, next_work_spec_preempts=True,
                 weight=20, status=RUNNABLE, continuous=False, interval=None,
                 max_running=None, max_getwork=None, storage=None):
        super(SqliteWorkSpec, self).__init__(
            name, jobq, data,
            next_work_spec, next_work_spec_preempts,
            weight, status, continuous,
            interval, max_running, max_getwork)

        # SqliteJobStorage
        assert storage is not None, "SqliteWorkSpec must be initialized with SqliteJobStorage"
        self.storage = storage

        # thinks that should be flushed to storage
        self.to_store = []
        # avoid storage.get_work_units() disk hit if we know it's empty
        self.db_has_more = True

    def __getstate__(self):
        return (
            'sws',
            super(SqliteWorkSpec, self).__getstate__(),
            [wu.__dict__ for wu in self.to_store]
        )

    def __setstate__(self, state):
        if state[0] == 'sws':
            super(SqliteWorkSpec, self).__setstate__(state[1])
            self.to_store = [_WorkUnit_rebuild(wud) for wud in state[2]]
        else:
            super(SqliteWorkSpec, self).__setstate__(state)
            self.to_store = []
        self.db_has_more = True

    def _add_work_units(self, they, push=True):
        super(SqliteWorkSpec, self)._add_work_units(they, push)
        self._maybe_overflow_to_disk()

    def _maybe_overflow_to_disk(self):
        # NOTE! Run this already inside `with self.mutex` context
        #logger.debug('to store %s; queue %s', len(self.to_store), len(self.queue))
        if (len(self.to_store) > self._to_store_batch) or (len(self.queue) > self._queue_max):
            # put stuff to disk
            if len(self.queue) > self._queue_max:
                putsize = max(self._put_size, len(self.queue) - self._queue_max)
                logger.debug('putting %s from queue and %s from to_store', putsize, len(self.to_store))
                head = self.queue[:-putsize]
                # I don't think we need to precisely put the last
                # sorted elements. If we draw down far enough we
                # will precisely pull back the first sorted
                # elements. There _does_ exist a window for some
                # not precisely sorted elements to get processed,
                # but I don't think we care that much.
                tail = self.queue[-putsize:]
                assert len(head) + len(tail) == len(self.queue)
                out = tail + self.to_store
                self.queue = head
            else:
                out = self.to_store
            self.to_store = []
            self.storage.put_work_units(self.name, out)
            self.db_has_more = True
            for wu in out:
                # these wu are no longer available in RAM
                assert wu.status == AVAILABLE
                # TODO: FIXME? The `if` line here is a workaround for
                # some complex interaction I don't fully understand. I
                # _think_ that everything should be in
                # work_units_by_key, but actually this `del` was
                # failing due to some things missing. It's kinda not a
                # problem for something to already be gone when we go
                # to `del` it, so hopefully this workaround isnt't
                # hiding anything very bad.
                if wu.key in self.work_units_by_key:
                    del self.work_units_by_key[wu.key]

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
                else:
                    # check disk for it ... later
                    self._adjust_stored_priority(wuk, priority, adjustment)
            if needs_reheap:
                # It's quite likely at the wrong place in the
                # priority queue and needs to be fixed;
                # heapify takes O(n) time but so does lookup
                heapq.heapify(self.queue)

    def _adjust_stored_priority(self, wuk, priority=None, adjustment=None):
        pd = self.storage.get_work_unit_by_key(self.name, wuk)
        if pd is not None:
            dirty = False
            oldprio = pd[0]
            if priority is not None:
                if oldprio != priority:
                    oldprio = priority
                    dirty = True
            elif adjustment is not None:
                oldprio += adjustment
                dirty = True
            if dirty:
                self.storage.put_work_unit(self.name, wuk, oldprio, pd[1])

    def get_statuses(self, work_unit_keys):
        out = []
        with self.mutex:
            self._expire_stale_leases()
            for wuk in work_unit_keys:
                wu = self.work_units_by_key.get(wuk)
                if wu is None:
                    pd = self.storage.get_work_unit_by_key(self.name, wuk)
                    if pd is not None:
                        out.append({'status': AVAILABLE, 'expiration': None})
                    else:
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

    def _repopulate_queue(self):
        if not self.db_has_more:
            return
        if (not self.queue) or (len(self.queue) < self._queue_min):
            # pull from disk
            added_keys = []
            for key, prio, data in self.storage.get_work_units(self.name, self._get_size):
                wu = WorkUnit(key, data, priority=prio)
                heapq.heappush(self.queue, wu)
                self.work_units_by_key[wu.key] = wu
                added_keys.append(key)
            if added_keys:
                self.storage.del_work_units(self.name, added_keys)
            else:
                # we tried to get some, and got none, db is empty.
                self.db_has_more = False

    def _get_work_units_by_keys(self, work_unit_keys):
        out = []
        for key in work_unit_keys:
            wu = self.work_units_by_key.get(key)
            if wu is not None:
                out.append( (key, wu) )
            else:
                # try sqlite
                prio_data = self.storage.get_work_unit_by_key(self.name, key)
                if prio_data is not None:
                    wu_prio = prio_data[0]
                    wu_data = prio_data[1]
                    # new WU() defaults to AVAILABLE, which everything in sqlite is.
                    out.append( (key, WorkUnit(key, wu_data, priority=wu_prio)) )
                else:
                    out.append( (key, None) )
        return out, None

    def _get_work_units_by_states(self, states, start, limit):
        out, err = super(SqliteWorkSpec, self)._get_work_units_by_states(states, start, limit)
        if err is not None:
            return out, err
        if (len(out) < limit) and (AVAILABLE in states):
            # get more from sql
            more = limit - len(out)
            for kpd in self.storage.get_work_units(self.name, more):
                key, prio, data = kpd
                out.append( (key, WorkUnit(key, data, priority=prio)) )
        return out, None

    def _get_work_units_all(self, start, limit):
        out, err = super(SqliteWorkSpec, self)._get_work_units_all(start, limit)
        if err is not None:
            return out, err
        if len(out) < limit:
            # get more from sql
            more = limit - len(out)
            for kpd in self.storage.get_work_units_start(self.name, start, more):
                key, prio, data = kpd
                out.append( (key, WorkUnit(key, data, priority=prio)) )
        return out, None

## If we never update a work unit in the AVAILABLE queue, this is fine
#    def update_work_unit(self, work_unit_key, options):
#        logger.warn("TODO: extend update_work_unit() for SQL backed WorkSpec")
#        return super(SqliteWorkSpec, self).update_work_unit(work_unit_key, options)

    def _del_work_units_all(self):
        count, err = super(SqliteWorkSpec, self)._del_work_units_all()
        self.storage.del_work_units_all(self.name)
        # warning, count will be low. not counting items deleted from sqlite
        return count, err

    def _del_work_units_state(self, filterstate):
        count, err = super(SqliteWorkSpec, self)._del_work_units_state(filterstate)
        if filterstate == AVAILABLE:
            self.storage.del_work_units_all(self.name)
        # warning, count will be low. not counting items deleted from sqlite
        return count, err

    def _del_work_units_keys(self, delkeys, filterstate=None):
        super(SqliteWorkSpec, self)._del_work_units_keys(
            delkeys, filterstate)
        if filterstate is None or filterstate == AVAILABLE:
            self.storage.del_work_units(self.name, delkeys)
        # warning, not returning count of how many things actually
        # existed and were deleted. shrug?
        return len(delkeys), None

    def count_work_units(self):
        with self.mutex:
            out, msg = self._count_work_units()
            out[AVAILABLE] = out.get(AVAILABLE, 0) + self.storage.count(self.name)
            return out, msg

    def __len__(self):
        with self.mutex:
            self._expire_stale_leases()
            return len(self.queue) + self.storage.count(self.name)
