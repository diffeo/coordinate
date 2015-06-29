'''Command-line coordinated client.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import, print_function, division
import contextlib

import cbor
import psycopg2
import psycopg2.pool

from .constants import AVAILABLE, FINISHED, FAILED, PENDING, \
    WORK_UNIT_STATUS_NAMES_BY_NUMBER, \
    RUNNABLE, PAUSED, \
    PRI_GENERATED, PRI_STANDARD
from .work_spec import WorkSpec


setup_sql = '''
CREATE TABLE IF NOT EXISTS wu (
  spec varchar(100),
  wukey bytea,
  wudata bytea,
  prio int,
  timeout bigint, -- int64 ms since 1970, or NULL
  status int,
  finishtime bigint, -- int64 ms since 1970, or NULL
  worker bytea,
  PRIMARY KEY (spec, wukey));
-- TODO: SELECT * FROM pg_catalog.pg_class c WHERE c.relname = 'wu_timeout';
CREATE INDEX wu_timeout ON wu (spec, timeout);
CREATE INDEX wu_status ON wu (spec, status);
CREATE TABLE IF NOT EXISTS old_counts (
  spec varchar(100) PRIMARY KEY,
  finished bigint,
  failed bigint);
'''
# TODO: if postgres coordinate becomes prevalent, restructure timeout to one global query for timeout across all specs


class PostgresWorkSpec(WorkSpec):
    '''Keep all records in PostgreSQL'''

    def __init__(self, name, jobq, data=None,
                 next_work_spec=None, next_work_spec_preempts=True,
                 weight=20, status=RUNNABLE, continuous=False,
                 interval=None, max_running=None, max_getwork=None,
                 connect_string=None):
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
            with conn.cursor(name=name) as cursor:
                yield cursor

    def __getstate__(self):
        pass
    def __setstate__(self, state):
        pass
    def priority(self):
        pass
    def update_data(self, data):
        pass
    def add_work_units(self, they):
        pass
    def prioritize_work_units(self, work_unit_keys, priority=None,
                              adjustment=None):
        pass
    def get_statuses(self, work_unit_keys):
        pass
    def get_work(self, worker_id, lease_time, max_jobs):
        pass
    def get_work_units(self, options):
        pass
    def update_work_unit(self, work_unit_key, options):
        pass
    def del_work_units(self, options):
        pass
    def archive_work_units(self, max_count, max_age):
        pass
    def count_work_units(self):
        pass
    def sched_data(self):
        pass
    def __len__(self):
        pass
