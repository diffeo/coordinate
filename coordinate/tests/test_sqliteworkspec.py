from __future__ import absolute_import

import logging
import time

import coordinate
import coordinate.job_server
from coordinate.constants import AVAILABLE, PENDING, FINISHED, \
    RUNNABLE, PAUSED
from coordinate.job_server import SqliteWorkSpec, WorkUnit, JobQueue
from coordinate.job_sqlite import SqliteJobStorage

#JobQueue, WorkSpecScheduler, \
#    WorkSpec, WorkUnit


def test_storage_simple(tmpdir):
    dbpath = tmpdir.join('ss.sqlite').realpath().strpath
    src = [
        WorkUnit('aa', {}, priority=1),
        WorkUnit('zz', {}, priority=2),
        WorkUnit('ac', {}, priority=1),
        WorkUnit('ab', {}, priority=1),
    ]
    # setup
    sto = SqliteJobStorage(dbpath)
    sto.put_work_units('a', src)

    # all there in order?
    they = list(sto.get_work_units('a', 100))
    assert len(they) == len(src)
    assert map(lambda x: x[0], they) == ['zz', 'aa', 'ab', 'ac']
    assert sto.count('a') == 4
    assert sto.get_min_prio_key('a')[0] == 'zz'

    # delete one out of the middle
    sto.del_work_units('a', ['ab'])
    they = list(sto.get_work_units('a', 100))
    assert map(lambda x: x[0], they) == ['zz', 'aa', 'ac']
    assert sto.count('a') == 3
    assert sto.get_min_prio_key('a')[0] == 'zz'

    # update priority, bump to top
    sto.put_work_unit('a', 'ac', 3, {})
    assert sto.get_min_prio_key('a')[0] == 'ac'
    they = list(sto.get_work_units('a', 100))
    assert map(lambda x: x[0], they) == ['ac', 'zz', 'aa']
    assert sto.count('a') == 3
    assert sto.get_min_prio_key('a')[0] == 'ac'

    # del head
    sto.del_work_unit('a', 'ac')
    assert sto.get_min_prio_key('a')[0] == 'zz'
    they = list(sto.get_work_units('a', 100))
    assert map(lambda x: x[0], they) == ['zz', 'aa']
    assert sto.count('a') == 2

    prio, data = sto.get_work_unit_by_key('a', 'zz')
    assert prio == 2
    assert data == {}

    pd = sto.get_work_unit_by_key('a', 'QQ')
    assert pd is None

def test_workspec_simple(tmpdir):
    jobq = JobQueue({'config':'lol'})
    dbpath = tmpdir.join('ss.sqlite').realpath().strpath
    sto = SqliteJobStorage(dbpath)

    ws = SqliteWorkSpec('a', jobq, {}, storage=sto)
    # for test purposes, be rediculously small
    ws._queue_max = 6
    ws._queue_min = 2
    ws._put_size = 2
    ws._get_size = 2

    twu = [WorkUnit('{:3d}'.format(x), {}, priority=1) for x in xrange(1,100)]
    ws.add_work_units(twu)

    # we did some storage
    assert sto.count('a') > 90
    # and reclaimed some RAM
    assert len(ws.queue) < 10
    assert len(ws.work_units_by_key) < 10
    # the sum is consistent so far
    assert sto.count('a') + len(ws.queue) == 99

    lease_time = time.time() + 123456

    # get all the work units
    they = []
    bobi = 1
    wul = ws.get_work('bob{}'.format(bobi), lease_time, 1)
    while wul:
        assert len(wul) == 1
        wu = wul[0]
        they.append(wu)
        assert len(they) < 105
        bobi += 1
        wul = ws.get_work('bob{}'.format(bobi), lease_time, 1)

    # check that we still have exactly all the work units
    assert map(lambda x: x.key, they) == ['{:3d}'.format(x) for x in xrange(1,100)]
    assert map(lambda x: x.key, they) == map(lambda x: x.key, twu)
    def wueq(wua, wub):
        return (wua.key == wub.key) and (wua.data == wub.data) and (wua.priority == wub.priority)
    def wut(abtuple):
        return wueq(*abtuple)
    assert all(map(wut, zip(twu, they)))

