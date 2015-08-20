'''Command-line coordinated client.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import, print_function, division
import contextlib
import logging
import time

import cbor
import psycopg2
import psycopg2.pool

from .constants import AVAILABLE, FINISHED, FAILED, PENDING, \
    WORK_UNIT_STATUS_NAMES_BY_NUMBER, \
    RUNNABLE, PAUSED, \
    PRI_GENERATED, PRI_STANDARD
from .fifolock import FifoLock
from .server_work_unit import WorkUnit
from .work_spec import WorkSpec, nmin

logger = logging.getLogger(__name__)

setup_sql = '''
CREATE OR REPLACE FUNCTION {schema}.init_coordinate_tables() RETURNS void AS $$
DECLARE
  myrec pg_catalog.pg_class%ROWTYPE;
BEGIN
CREATE TABLE IF NOT EXISTS {schema}.wu (
  spec varchar(100),
  wukey bytea,
  wudata bytea,
  prio int,
  timeout bigint, -- int64 ms since 1970, or NULL
  status int,
  finishtime bigint, -- int64 ms since 1970, or NULL
  worker bytea,
  PRIMARY KEY (spec, wukey));
SELECT * INTO myrec FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = 'wu_timeout' AND n.nspname = '{schema}';
IF NOT FOUND THEN
  CREATE INDEX wu_timeout ON {schema}.wu (spec, timeout);
END IF;
SELECT * INTO myrec FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = 'wu_status' AND n.nspname = '{schema}';
IF NOT FOUND THEN
  CREATE INDEX wu_status ON {schema}.wu (spec, status);
END IF;
SELECT * INTO myrec FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = 'wu_getwork' AND n.nspname = '{schema}';
IF NOT FOUND THEN
  CREATE INDEX wu_getwork ON {schema}.wu (spec, prio DESC, wukey ASC);
END IF;
CREATE TABLE IF NOT EXISTS {schema}.old_counts (
  spec varchar(100),
  status int,
  count bigint,
  PRIMARY KEY (spec, status)
);
END;
$$ LANGUAGE plpgsql;

-- SELECT {schema}_init_coordinate_tables();

CREATE OR REPLACE FUNCTION {schema}.get_work(qspec varchar(100), nlim int, nworker bytea, expire_time bigint) RETURNS TABLE(wukey bytea, wudata bytea, prio int) AS $$
DECLARE
  --myt TABLE;--RECORD;--TABLE(wukey bytea, wudata bytea, prio int);
BEGIN
  CREATE TEMPORARY TABLE myt (wukey bytea, wudata bytea, prio int) ON COMMIT DROP;
  INSERT INTO myt (wukey, wudata, prio) SELECT {schema}.wu.wukey, {schema}.wu.wudata, {schema}.wu.prio FROM {schema}.wu WHERE {schema}.wu.spec = qspec AND {schema}.wu.status = 1 ORDER BY {schema}.wu.prio DESC, {schema}.wu.wukey ASC LIMIT nlim;
  UPDATE {schema}.wu SET worker = nworker, timeout = expire_time, status = 3 FROM myt WHERE {schema}.wu.spec = qspec AND myt.wukey = wu.wukey;
  RETURN QUERY SELECT * FROM myt;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION {schema}.archive_counts(expire_time bigint) RETURNS VOID AS $$
BEGIN
  -- init things to 0
  INSERT INTO {schema}.old_counts (spec, status, count) SELECT xwu.xp spec, xwu.xs status, 0 count FROM {schema}.old_counts RIGHT JOIN (SELECT spec xp, status xs, COUNT(wu.status) xc FROM {schema}.wu WHERE {schema}.wu.finishtime < expire_time GROUP BY xp, xs) xwu ON {schema}.old_counts.spec = xwu.xp AND {schema}.old_counts.status = xwu.xs WHERE {schema}.old_counts.count IS NULL;

  -- update them
  UPDATE {schema}.old_counts SET count = count + xc FROM (SELECT spec xp, status xs, COUNT(wu.status) xc FROM {schema}.wu WHERE {schema}.wu.finishtime < expire_time GROUP BY xp, xs) sq WHERE {schema}.old_counts.spec = sq.xp AND {schema}.old_counts.status = sq.xs;

  -- delete old work unit records
  DELETE FROM {schema}.wu WHERE finishtime < expire_time;
END;
$$ LANGUAGE plpgsql;


'''
# SELECT init_coordinate_tables()
# INSERT INTO wu (spec, wukey, wudata, prio, status) VALUES ('a', 'a1', '', 1, 1), ('a', 'b2', '', 1, 1), ('a', 'c3', '', 2, 1);
# SELECT * FROM get_work('a', 1, 'w1', 12345);
# UPDATE wu SET status = 1, timeout = NULL WHERE status = 3 AND timeout < 99999;
# TODO: if postgres coordinate becomes prevalent, restructure timeout to one global query for timeout across all specs


class PostgresWorkSpec(WorkSpec):
    '''Keep all records in PostgreSQL'''

    def __init__(self, name, jobq, data=None,
                 next_work_spec=None, next_work_spec_preempts=True,
                 weight=20, status=RUNNABLE, continuous=False,
                 interval=None, max_running=None, max_getwork=None,
                 connect_string=None, schema=None):
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

        self.connect_string = connect_string

        self.mutex = FifoLock()

        # value for lazy getter property .connection_pool
        self._connection_pool = None
        self._dbinitted = False
        self._schema = schema or 'public' # default schema for postgres

        # when did we last expire stale leases?
        self._lastexpire = None

    @property
    def connection_pool(self):
        if self._connection_pool is None:
            self._connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, # min_connections
                10, # max_connections
                self.connect_string
            )
        return self._connection_pool

    @contextlib.contextmanager
    def _conn(self):
        '''Produce a PostgreSQL connection from the pool.

        This also runs a single transaction on that connection.  On
        successful completion, the transaction is committed; if any
        exception is thrown, the transaction is aborted.

        On successful completion the connection is returned to the
        pool for reuse.  If any exception is thrown, the connection
        is closed.

        '''
        tries = 5
        for _ in xrange(tries):
            conn = self.connection_pool.getconn()
            try:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute('SELECT 1')
                except (psycopg2.DatabaseError, psycopg2.InterfaceError):
                    logging.warn('connection is gone, maybe retrying...',
                                 exc_info=True)
                    time.sleep(0.5)
                    continue
                with conn:
                    yield conn
                    break
            finally:
                # This has logic to test whether the connection is closed
                # and/or failed and correctly manages returning it to the
                # pool (or not).
                self.connection_pool.putconn(conn)

    @contextlib.contextmanager
    def _cursor(self, name=None):
        '''Produce a cursor from a connection.

        This is a helper for the common case of wanting a single
        cursor on a single connection to do a single operation.

        '''
        with self._conn() as conn:
            self._initdb(conn)
            with conn.cursor(name=name) as cursor:
                yield cursor

    def _initdb(self, conn):
        # MUST be called from within lock on self.mutex
        if self._dbinitted:
            return
        with conn.cursor() as cursor:
            cursor.execute('CREATE SCHEMA IF NOT EXISTS {schema}'
                           .format(schema=self._schema))
            cursor.execute(setup_sql.format(schema=self._schema))
            cursor.execute('SELECT {schema}.init_coordinate_tables()'
                           .format(schema=self._schema))
            self._dbinitted = True

    def delete_all_storage(self):
        with self.mutex:
            with self._cursor() as cursor:
                if self._schema != 'public':
                    cursor.execute('DROP SCHEMA IF EXISTS {schema} CASCADE'.format(schema=self._schema))
                else:
                    cursor.execute('DROP TABLE {schema}.wu'.format(schema=self._schema))
                    cursor.execute('DROP TABLE {schema}.old_counts'.format(schema=self._schema))
                    cursor.execute('DROP FUNCTION IF EXISTS {schema}.init_coordinate_tables()'.format(schema=self._schema))
                    cursor.execute('DROP FUNCTION IF EXISTS {schema}.get_work(qspec varchar(100), nlim int, nworker bytea, expire_time bigint)'.format(schema=self._schema))

    def __getstate__(self):
        return (
            'pws',
            self.name,
            self.data,
            self.weight,
            self.status,
            self.continuous,
            self.interval,
            self.next_continuous,
            self.max_running,
            self.max_getwork,
            self.next_work_spec,
            self.next_work_spec_preempts,
            self.connect_string,
            self._schema,
        )
    def __setstate__(self, state):
        assert state[0] == 'pws'
        (
            _,
            self.name,
            self.data,
            self.weight,
            self.status, # :5
            self.continuous,
            self.interval,
            self.next_continuous,
            self.max_running,
            self.max_getwork, # :10
            self.next_work_spec,
            self.next_work_spec_preempts,
            self.connect_string,
            self._schema, # :14
        ) = state[:14]
        self.jobq = None
        self.mutex = FifoLock()
        self._connection_pool = None
        self._dbinitted = False
        self._lastexpire = None

    def _expire_stale_leases(self, cursor):
        # must run inside self.mutex and self._cursor()
        now = time.time()
        if (self._lastexpire is not None) and ((now - self._lastexpire) < 5.0):
            # don't re-expire more often than every 5 seconds
            return
        # set AVAILABLE where PENDING and expiry < now
        cursor.execute('UPDATE {schema}.wu SET status = 1, timeout = NULL, worker = NULL WHERE status = 3 AND timeout < %s'.format(schema=self._schema), (now,))
        logger.debug('expired %s leases', cursor.rowcount)
        self._lastexpire = now

    def priority(self):
        "peek at next work unit, return its priority, thus the priority of this queue"
        with self.mutex:
            with self._cursor() as cursor:
                self._expire_stale_leases(cursor)
                cursor.execute('SELECT {schema}.wu.prio FROM {schema}.wu WHERE {schema}.wu.spec = %s ORDER BY {schema}.wu.prio DESC LIMIT 1'.format(schema=self._schema), (self.name,))
                for row in cursor:
                    return row[0]
        return 0

    def add_work_units(self, they):
        for wu in they:
            assert wu.status == AVAILABLE
        with self.mutex:
            kdps = [(self.name, psycopg2.Binary(wu.key), psycopg2.Binary(cbor.dumps(wu.data)), wu.priority) for wu in they]
            logger.debug('pg adding %s wu', len(kdps))
            with self._cursor() as cursor:
                self._expire_stale_leases(cursor)
                cursor.execute('CREATE TEMPORARY TABLE awut (spec varchar(100), wukey bytea, wudata bytea, prio int) ON COMMIT DROP')
                cursor.executemany('INSERT INTO awut (spec, wukey, wudata, prio) VALUES (%s, %s, %s, %s)'.format(schema=self._schema), kdps)

                cursor.execute('UPDATE {schema}.wu SET wudata = pg_temp.awut.wudata, prio = pg_temp.awut.prio, status = 1 FROM pg_temp.awut WHERE {schema}.wu.spec = pg_temp.awut.spec AND {schema}.wu.wukey = pg_temp.awut.wukey'.format(schema=self._schema))
                cursor.execute(
'INSERT INTO {schema}.wu (spec, wukey, wudata, prio, status) '
'SELECT awut.spec, awut.wukey, awut.wudata, awut.prio, 1 FROM pg_temp.awut LEFT JOIN {schema}.wu ON {schema}.wu.wukey = awut.wukey AND {schema}.wu.spec = awut.spec WHERE {schema}.wu.spec IS NULL'.format(schema=self._schema))

    def update_work_unit(self, work_unit_key, options):
        lease_time = options.get('lease_time')
        status = options.get('status')
        data = options.get('data')
        worker_id = options.get('worker_id')
        if (lease_time is None) and (status is None):
            return False, 'nothing to do'
        lease_time = self._normalize_lease_time(lease_time)
        with self.mutex:
            with self._cursor() as cursor:
                self._expire_stale_leases(cursor)
                # TODO: how inefficient is this? how tedious is it to construct one UPDATE statement?

                if data is not None:
                    cursor.execute('UPDATE {schema}.wu SET wudata = %s WHERE spec = %s AND wukey = %s'.format(schema=self._schema), (psycopg2.Binary(cbor.dumps(data)), self.name, psycopg2.Binary(work_unit_key)))

                if worker_id is not None:
                    cursor.execute('UPDATE {schema}.wu SET worker = %s WHERE spec = %s AND wukey = %s'.format(schema=self._schema), (psycopg2.Binary(worker_id), self.name, psycopg2.Binary(work_unit_key)))

                if (status is not None) and (lease_time is not None):
                    if (status == FINISHED) or (status == FAILED):
                        # special logic for finishing a work unit
                        cursor.execute('UPDATE {schema}.wu SET status = %s, finishtime = %s WHERE spec = %s AND wukey = %s'.format(schema=self._schema), (status, lease_time, self.name, psycopg2.Binary(work_unit_key)))
                    else:
                        cursor.execute('UPDATE {schema}.wu SET status = %s, timeout = %s WHERE spec = %s AND wukey = %s'.format(schema=self._schema), (status, lease_time, self.name, psycopg2.Binary(work_unit_key)))
                    # don't do either of the following single set updates
                    status = None
                    lease_time = None
                if status is not None:
                    cursor.execute('UPDATE {schema}.wu SET status = %s WHERE spec = %s AND wukey = %s'.format(schema=self._schema), (status, self.name, psycopg2.Binary(work_unit_key)))
                if lease_time is not None:
                    cursor.execute('UPDATE {schema}.wu SET timeout = %s WHERE spec = %s AND wukey = %s'.format(schema=self._schema), (lease_time, self.name, psycopg2.Binary(work_unit_key)))
        return True, None

    def prioritize_work_units(self, work_unit_keys, priority=None,
                              adjustment=None):
        assert (priority is not None) or (adjustment is not None)
        with self.mutex:
            with self._cursor() as cursor:
                self._expire_stale_leases(cursor)
                if priority is not None:
                    cursor.executemany('UPDATE {schema}.wu SET prio = %s WHERE spec = %s AND wukey = %s'.format(schema=self._schema), [(priority, self.name, psycopg2.Binary(wuk)) for wuk in work_unit_keys])
                else:
                    # adjustment
                    cursor.executemany('UPDATE {schema}.wu SET prio = prio + %s WHERE spec = %s AND wukey = %s'.format(schema=self._schema), [(adjustment, self.name, psycopg2.Binary(wuk)) for wuk in work_unit_keys])

    def _collect_wu_kdps(self, out, cursor):
        # Common iteration over query results for _get_work_units_* below
        for row in cursor:
            key = bytes(row[0])
            data = cbor.loads(bytes(row[1]))
            prio = row[2]
            status = row[3]
            nwu = WorkUnit(key, data, priority=prio)
            nwu.status = status
            out.append( (key, nwu) )

    def _get_work_units_by_keys(self, work_unit_keys):
        # runs inside self.mutex
        # return [(key, WorkUnit), ...], message
        out = []
        with self._cursor() as cursor:
            for wuk in work_unit_keys:
                # TODO: many queries is slow, make big query, but
                # can't return results from cursor.executemany()
                cursor.execute('SELECT wukey, wudata, prio, status FROM {schema}.wu WHERE spec = %s AND wukey = %s'.format(schema=self._schema), (self.name, psycopg2.Binary(wuk)))
                self._collect_wu_kdps(out, cursor)
        return out, None

    def _get_work_units_by_states(self, states, start, limit):
        # runs inside self.mutex
        # return [(key, WorkUnit), ...], message
        stateqtemplate = ' OR '.join(['status = %s'] * len(states))
        query = 'SELECT wukey, wudata, prio, status FROM {schema}.wu WHERE spec = %s AND (' + stateqtemplate + ')'
        args = [self.name]
        args.extend(states)
        if start is not None:
            query += ' OFFSET %s'
            args.append(start)
        if limit is not None:
            query += ' LIMIT %s'
            args.append(limit)
        out = []
        with self._cursor() as cursor:
            self._expire_stale_leases(cursor)
            cursor.execute(query.format(schema=self._schema), args)
            self._collect_wu_kdps(out, cursor)
        return out, None

    def _get_work_units_all(self, start, limit):
        # runs inside self.mutex
        # return [(key, WorkUnit), ...], message
        out = []
        with self._cursor() as cursor:
            if start is not None:
                if limit is not None:
                    cursor.execute('SELECT wukey, wudata, prio, status FROM {schema}.wu WHERE spec = %s OFFSET %s LIMIT %s'.format(schema=self._schema), (self.name, start, limit))
                else:
                    cursor.execute('SELECT wukey, wudata, prio, status FROM {schema}.wu WHERE spec = %s OFFSET %s'.format(schema=self._schema), (self.name, start))
            else:
                if limit is not None:
                    cursor.execute('SELECT wukey, wudata, prio, status FROM {schema}.wu WHERE spec = %s LIMIT %s'.format(schema=self._schema), (self.name, limit))
                else:
                    cursor.execute('SELECT wukey, wudata, prio, status FROM {schema}.wu WHERE spec = %s'.format(schema=self._schema), (self.name,))
            self._collect_wu_kdps(out, cursor)
        return out, None

    def del_work_units(self, options):
        # options={'all':True,
        # work_unit_keys: [...],
        # state: int}
        # return (num deleted, msg)
        with self.mutex:
            with self._cursor() as cursor:
                self._expire_stale_leases(cursor)
                if options.get('all'):
                    cursor.execute('DELETE FROM {schema}.wu'.format(schema=self._schema))
                    return cursor.rowcount, None
                xstate = options.get('state')
                if xstate is not None:
                    cursor.execute('DELETE FROM {schema}.wu WHERE status = %s'.format(schema=self._schema), (xstate,))
                    return cursor.rowcount, None
                wukeys = options.get('work_unit_keys')
                if wukeys:
                    cursor.executemany('DELETE FROM {schema}.wu WHERE wukey = %s'.format(schema=self._schema), [(psycopg2.Binary(key),) for key in wukeys])
                    return cursor.rowcount, None
                return 0, 'no valid option "all", "state", or "work_unit_keys"'

    def get_statuses(self, work_unit_keys):
        # return [{'status':int, 'expration':bigint, 'worker_id':, 'traceback':}, ...]
        out = []
        with self.mutex:
            with self._cursor() as cursor:
                self._expire_stale_leases(cursor)
                for wuk in work_unit_keys:
                    # TODO: many queries is slow, make big query, but
                    # can't return results from cursor.executemany()
                    cursor.execute('SELECT wudata, status, timeout, worker '
                                   'FROM {schema}.wu '
                                   'WHERE spec = %s AND wukey = %s'
                                   .format(schema=self._schema),
                                   (self.name, psycopg2.Binary(wuk)))
                    found = False
                    for row in cursor:
                        found = True
                        data = cbor.loads(bytes(row[0]))
                        status = row[1]
                        timeout = row[2]
                        worker = row[3] and bytes(row[3])
                        sdict = {'status': status, 'expration': timeout}
                        if worker:
                            sdict['worker_id'] = worker
                        if 'traceback' in data:
                            sdict['traceback'] = data['traceback']
                        out.append(sdict)
                    if not found:
                        out.append(None)
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
        out = []
        max_jobs = nmin(max_jobs, self.max_getwork)
        with self.mutex:
            with self._cursor() as cursor:
                self._expire_stale_leases(cursor)
                cursor.execute('SELECT * '
                               'FROM {schema}.get_work(%s, %s, %s, %s)'
                               .format(schema=self._schema),
                               (self.name, max_jobs,
                                psycopg2.Binary(worker_id), long(lease_time)))
                for row in cursor:
                    wukey = bytes(row[0])
                    wudata = cbor.loads(bytes(row[1]))
                    prio = row[2]
                    wu = WorkUnit(wukey, wudata, priority=prio)
                    out.append(wu)
        return out

    def archive_work_units(self, max_count, max_age):
        '''Drop data of FINISHED and FAILED work units. Keep count of them.'''
        if max_age is None:
            logger.warn('TODO: implement keeping a maximum count of records, '
                        'currently only has cutoff time')
            return
        with self.mutex:
            with self._cursor() as cursor:
                # things that finished before cutoff_time will be forgotten
                cutoff_time = time.time() - max_age
                cursor.execute('SELECT {schema}.archive_counts(%s)'
                               .format(schema=self._schema),
                               (long(cutoff_time),))

    def count_work_units(self):
        "Return dictionary by status which is sum of current and archived work unit counts."
        out = {}
        with self.mutex:
            with self._cursor() as cursor:
                self._expire_stale_leases(cursor)
                cursor.execute('SELECT status, COUNT(status) FROM {schema}.wu WHERE spec = %s GROUP BY status'.format(schema=self._schema), (self.name,))
                for row in cursor:
                    out[row[0]] = row[1]
                cursor.execute('SELECT status, count FROM {schema}.old_counts WHERE spec = %s'.format(schema=self._schema), (self.name,))
                for row in cursor:
                    out[row[0]] = out.get(row[0], 0) + row[1]
        return out, None

    def sched_data(self):
        '''
        One call to do one mutex cycle and get the scheduler what it needs.
        return (will get work, has queue, num pending)
        '''
        with self.mutex:
            with self._cursor() as cursor:
                self._expire_stale_leases(cursor)
                counts = {}
                # This is like count_work_units, except we only care
                # about AVAILABLE and PENDING so we don't need to add
                # in archived counts of finished and failed.
                cursor.execute('SELECT status, COUNT(status) FROM {schema}.wu WHERE spec = %s GROUP BY status'.format(schema=self._schema), (self.name,))
                for row in cursor:
                    counts[row[0]] = row[1]
                num_available = counts.get(AVAILABLE, 0)
                num_pending = counts.get(PENDING, 0)
                will_get_work = False
                if (self.max_running is not None) and (num_pending >= self.max_running):
                    will_get_work = False
                elif num_available:
                    will_get_work = True
                elif self.continuous:
                    # No work to do, but we can create sythetic work units
                    now = time.time()
                    if now >= self.next_continuous:
                        will_get_work = True
                return will_get_work, num_available > 0, num_pending

    def __len__(self):
        with self.mutex:
            with self._cursor() as cursor:
                cursor.execute('SELECT COUNT(*) FROM {schema}.wu WHERE spec = %s'.format(schema=self._schema), (self.name,))
                for row in cursor:
                    return row[0]
        return 0



if __name__ == '__main__':
    print(setup_sql.format(schema='public'))
