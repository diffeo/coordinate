'''python server logic for coordinate daemon

.. Your use of this software is governed by your license agreement.
   Copyright 2012-2015 Diffeo, Inc.
'''
from __future__ import absolute_import
import logging
import sqlite3
import time

import cbor

from .fifolock import FifoLock


logger = logging.getLogger(__name__)


def synchronized(func):
    def _(self, *args, **kwargs):
        with self.mutex:
            return func(self, *args, **kwargs)
    return _


def lowermpk(mpk, key, priority):
    "(key, priority) should come before mpk"
    if mpk is None:
        return True
    if priority > mpk[1]:
        return True
    if priority < mpk[1]:
        return False
    if key < mpk[0]:
        return True
    return False


_CREATE_TABLE_NEWER = 'CREATE TABLE IF NOT EXISTS jobs (name TEXT, key BLOB, prio INT, data BLOB, PRIMARY KEY (name, key)) WITHOUT ROWID'
# "WITHOUT ROWID" started in sqlite 3.8.2
_CREATE_TABLE_PRE_3_8_2 = 'CREATE TABLE IF NOT EXISTS jobs (name TEXT, key BLOB, prio INT, data BLOB, PRIMARY KEY (name, key))'


class SqliteJobStorage(object):
    '''Wrapper for sqlite to provide crud we want for work units by key or by sort order'''

    _min_vacuum_seconds = 3600

    def __init__(self, path):
        self.path = path
        self.mutex = FifoLock()
        # storage for lazy getter property self.db
        self._db = None
        # map[work_spec_name] = (key, prio, data)
        self._min_prio_key = {}
        self._should_vacuum = False
        self._last_vacuum = None

    @property
    def db(self):
        if self._db is None:
            self._db = sqlite3.connect(self.path, check_same_thread=False)
            self._db.text_factory = str  # this is strange
            c = self._db.cursor()
            # >= 3.8.2 ?
            svi = sqlite3.sqlite_version_info
            if (svi[0] > 3) or ((svi[0] == 3) and ((svi[1] > 8) or ((svi[1] == 8) and (svi[2] >= 2)))):
                create_table = _CREATE_TABLE_NEWER
            else:
                create_table = _CREATE_TABLE_PRE_3_8_2
            c.execute(create_table)
            c.execute('CREATE INDEX IF NOT EXISTS jobs_q ON jobs (name, prio DESC, key ASC)')
            self._db.commit()
        else:
            self._maybe_vacuum()
        return self._db

    def _maybe_vacuum(self):
        if self._should_vacuum:
            self._should_vacuum = False
            now = time.time()
            if (self._last_vacuum is not None) and ((new - self._last_vacuum) < self._min_vacuum_seconds):
                # nah, wait for the next time we should maybe vacuum
                return
            try:
                c = self._db.cursor()
                c.execute('VACUUM')
                self._db.commit()
            except:
                logger.error('problem vacuuming database %s', self.path, exc_info=True)

    @synchronized
    def get_min_prio_key(self, work_spec_name):
        """A fast cache of the next entry stored in the database.

        returns (key, prio, data) like get_work_units()"""
        mpk = self._min_prio_key.get(work_spec_name)
        if mpk is None:
            for x in self._get_work_units(work_spec_name, 1):
                mpk = x
                break
            if mpk:
                self._min_prio_key[work_spec_name] = mpk
        return mpk

    @synchronized
    def put_work_unit(self, work_spec_name, key, priority, data):
        assert isinstance(work_spec_name, basestring), 'bad work_spec_name type={} {!r}'.format(type(work_spec_name), work_spec_name)
        cdata = cbor.dumps(data)
        db = self.db
        c = db.cursor()
        c.execute('INSERT OR REPLACE INTO jobs (name, key, prio, data) VALUES (?, ?, ?, ?)', (work_spec_name, key, priority, cdata))
        db.commit()
        mpk = self._min_prio_key.get(work_spec_name)
        if lowermpk(mpk, key, priority):
            mpk = (key, priority, data)
            self._min_prio_key[work_spec_name] = mpk

    @synchronized
    def put_work_units(self, work_spec_name, they):
        "they: WorkUnit[]; saves wu.key, wu.priority, wu.data"
        assert isinstance(work_spec_name, basestring), 'bad work_spec_name type={} {!r}'.format(type(work_spec_name), work_spec_name)
        def dgen():
            mpk = self._min_prio_key.get(work_spec_name)
            for wu in they:
                cdata = cbor.dumps(wu.data)
                if lowermpk(mpk, wu.key, wu.priority):
                    mpk = (wu.key, wu.priority, wu.data)
                    self._min_prio_key[work_spec_name] = mpk
                yield (work_spec_name, wu.key, wu.priority, cdata)
        db = self.db
        c = db.cursor()
        c.executemany('INSERT OR REPLACE INTO jobs (name, key, prio, data) VALUES (?, ?, ?, ?)', dgen())
        db.commit()

    @synchronized
    def get_work_unit_by_key(self, work_spec_name, key):
        "yields (prio, data)"
        assert isinstance(work_spec_name, basestring), 'bad work_spec_name type={} {!r}'.format(type(work_spec_name), work_spec_name)
        db = self.db
        c = db.cursor()
        c.execute('SELECT prio, data FROM jobs WHERE name = ? AND key = ?', (work_spec_name, key))
        retval = None
        x = c.fetchone()
        while x is not None:
            if retval is None:
                retval = x
            else:
                raise Exception("get by key should return exactly one result, got {!r} and {!r}".format(retval, x))
            x = c.fetchone()
        if retval is None:
            return None
        return retval[0], cbor.loads(retval[1])

    # NOT syncronized, for internal use only
    def _get_work_units(self, work_spec_name, num):
        "yields (key, prio, data)"
        assert isinstance(work_spec_name, basestring), 'bad work_spec_name type={} {!r}'.format(type(work_spec_name), work_spec_name)
        assert isinstance(num, (int, long)), 'bad limit num type={} {!r}'.format(type(num), num)
        db = self.db
        c = db.cursor()
        c.execute('SELECT key, prio, data FROM jobs WHERE name = ? ORDER BY name, prio DESC, key ASC LIMIT ?', (work_spec_name, num))
        x = c.fetchone()
        while x is not None:
            yield x[0], x[1], cbor.loads(x[2])
            x = c.fetchone()

    @synchronized
    def get_work_units(self, work_spec_name, num):
        "yields (key, prio, data)"
        for x in self._get_work_units(work_spec_name, num):
            yield x

    @synchronized
    def get_work_units_start(self, work_spec_name, start, num):
        "yields (key, prio, data)"
        if not start:
            for kpd in self._get_work_units(work_spec_name, num):
                yield kpd
            return

        assert isinstance(work_spec_name, basestring), 'bad work_spec_name type={} {!r}'.format(type(work_spec_name), work_spec_name)
        assert isinstance(start, basestring), 'bad start type={} {!r}'.format(type(start), start)
        assert isinstance(num, (int, long)), 'bad limit num type={} {!r}'.format(type(num), num)
        db = self.db
        c = db.cursor()
        c.execute('SELECT key, prio, data FROM jobs WHERE name = ? AND key > ? ORDER BY name, prio DESC, key ASC LIMIT ?', (work_spec_name, start, num))
        x = c.fetchone()
        while x is not None:
            yield x[0], x[1], cbor.loads(x[2])
            x = c.fetchone()

    def _maybe_clear_mpk(self, work_spec_name, key):
        mpk = self._min_prio_key.get(work_spec_name)
        if (mpk is not None) and (mpk[0] == key):
            del self._min_prio_key[work_spec_name]

    @synchronized
    def del_work_unit(self, work_spec_name, key):
        assert isinstance(work_spec_name, basestring), 'bad work_spec_name type={} {!r}'.format(type(work_spec_name), work_spec_name)
        self._maybe_clear_mpk(work_spec_name, key)
        db = self.db
        c = db.cursor()
        c.execute('DELETE FROM jobs WHERE name = ? AND key = ?', (work_spec_name, key))
        db.commit()

    @synchronized
    def del_work_units(self, work_spec_name, keys):
        assert isinstance(work_spec_name, basestring), 'bad work_spec_name type={} {!r}'.format(type(work_spec_name), work_spec_name)
        def tgen():
            for key in keys:
                self._maybe_clear_mpk(work_spec_name, key)
                yield (work_spec_name, key)
        db = self.db
        c = db.cursor()
        c.executemany('DELETE FROM jobs WHERE name = ? AND key = ?', tgen())
        db.commit()

    @synchronized
    def del_work_units_all(self, work_spec_name):
        assert isinstance(work_spec_name, basestring), 'bad work_spec_name type={} {!r}'.format(type(work_spec_name), work_spec_name)
        db = self.db
        c = db.cursor()
        c.execute('DELETE FROM jobs WHERE name = ?', (work_spec_name,))
        db.commit()
        if work_spec_name in self._min_prio_key:
            del self._min_prio_key[work_spec_name]

    @synchronized
    def count(self, work_spec_name):
        assert isinstance(work_spec_name, basestring), 'bad work_spec_name type={} {!r}'.format(type(work_spec_name), work_spec_name)
        db = self.db
        c = db.cursor()
        c.execute('SELECT COUNT(*) FROM jobs WHERE name = ?', (work_spec_name,))
        x = c.fetchone()
        return x[0]

    def vacuum(self):
        self._should_vacuum = True
