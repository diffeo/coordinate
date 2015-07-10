from __future__ import absolute_import
from collections import Counter
import logging
import os
import random
import time

# can't use cStringIO and subclass it
from StringIO import StringIO

import pytest
import yakonfig
import yaml

import coordinate
import coordinate.job_server
from coordinate.constants import AVAILABLE, PENDING, FINISHED, \
    RUNNABLE, PAUSED
from coordinate.job_server import JobQueue, WorkSpecScheduler, \
    WorkSpec, SqliteWorkSpec, WorkUnit
from coordinate.postgres_work_spec import PostgresWorkSpec
from coordinate.job_sqlite import SqliteJobStorage

logger = logging.getLogger(__name__)

wu1v = {'wu1v':1}
wu2v = {'wu2v':2}
wu3v = {'wu3v':3}


try:
    connect_string = open(os.path.join(os.path.dirname(__file__), 'postgres_connect_string.txt'), 'rb').read().strip()
except:
    connect_string = None


@pytest.yield_fixture
def xconfig():
    with yakonfig.defaulted_config(
            [coordinate]) as config:
        yield config


@pytest.fixture(scope='module')
def random_schema():
    random_schema = [chr(x) for x in xrange(ord('a'), ord('z'))]
    random.shuffle(random_schema)
    random_schema = ''.join(random_schema)
    return random_schema

def _make_WorkSpec(*args, **kwargs):
    return WorkSpec(*args, **kwargs)

def _make_SqliteWorkSpec(*args, **kwargs):
    storage = SqliteJobStorage(':memory:')
    kwargs['storage'] = storage
    sws = SqliteWorkSpec(*args, **kwargs)
    # redicilously constrained to exercise things in test
    sws._to_store_batch = 2
    sws._queue_max = 6
    sws._queue_min = 2
    sws._put_size = 2
    sws._get_size = 2
    return sws

def pwsfactory(random_schema):
    def ff(*args, **kwargs):
        return PostgresWorkSpec(
            connect_string=connect_string,
            schema=random_schema,
            *args, **kwargs)
    return ff

@pytest.yield_fixture(params=[_make_WorkSpec, _make_SqliteWorkSpec, 'PostgresWorkSpec'], scope='function')
def work_spec_class(request, random_schema):
    if request.param == 'PostgresWorkSpec':
        if not connect_string:
            pytest.skip()
            return
        pf = pwsfactory(random_schema)
        yield pf
        ws = pf('', None)
        ws.delete_all_storage()
    else:
        yield request.param


@pytest.fixture(params=[
{},  # defaults
{'sqlite_path':':memory:'}, # use SqliteJobStorage with in-memory sqlite
])
def jobqueue_conf(request):
    return request.param


def test_job_server(monkeypatch, xconfig, jobqueue_conf):
    monkeypatch.setattr(coordinate.job_server.time, 'time', lambda: 100.0)

    jq = JobQueue(jobqueue_conf)

    assert jq.get_work_spec('aoeu') is None
    assert jq.list_work_specs({}) == ([], None)

    # can't add work units to a work spec that doesn't exist
    assert (jq.add_work_units('aoeu', [('wu1', wu1v)]) ==
            (False, "no such work_spec 'aoeu'"))

    assert jq.get_work_spec('aoeu') is None
    assert jq.list_work_specs({}) == ([], None)

    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts == (None, None, None)

    ws1 = {'name': 'ws1'}
    jq.set_work_spec(ws1)

    # put a work unit
    jq.add_work_units('ws1', [('wu1', wu1v)])

    # get it back
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts == ('ws1', 'wu1', wu1v)

    # ... but don't get it twice
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts == (None, None, None)

    monkeypatch.setattr(coordinate.job_server.time, 'time', lambda: 500.0)

    # get it back if the prior lease expired
    wu_parts, msg = jq.get_work('id2', {})
    assert wu_parts == ('ws1', 'wu1', wu1v)

    # mark it finished
    jq.update_work_unit('ws1', 'wu1', {'status': FINISHED})

    monkeypatch.setattr(coordinate.job_server.time, 'time', lambda: 900.0)

    # ... and check that it stays finished
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts == (None, None, None)

    # push two, wu2 should come back first
    jq.add_work_units('ws1', [('wu3', wu3v), ('wu2', wu2v)])

    wu_parts, msg = jq.get_work('id2', {})
    assert wu_parts == ('ws1', 'wu2', wu2v)

    jq.update_work_unit('ws1', 'wu2', {'lease_time': 900})

    monkeypatch.setattr(coordinate.job_server.time, 'time', lambda: 1300.0)
    # a normal lease time has expried, but not our SUPER LEASE!

    statuses, msg = jq.get_work_unit_status(
        'ws1', ['wu1', 'wu2', 'wu3', 'wuBogus'])
    statuses = map(lambda x: x and x['status'], statuses)
    assert statuses == [FINISHED, PENDING, AVAILABLE, None]

    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts == ('ws1', 'wu3', wu3v)


def test_max_running(xconfig, jobqueue_conf):
    jq = JobQueue(jobqueue_conf)

    ws1 = {'name': 'ws1', 'max_running': 1}
    jq.set_work_spec(ws1)
    jq.add_work_units('ws1', [('wu1', wu1v), ('wu2', wu2v)])

    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts == ('ws1', 'wu1', wu1v)

    # While this work unit is still running we shouldn't get more
    # work
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts[0] is None

    jq.update_work_unit('ws1', 'wu1', {'status': FINISHED})

    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts == ('ws1', 'wu2', wu2v)
    jq.update_work_unit('ws1', 'wu2', {'status': FINISHED})


class NonClosingStringIO(StringIO):
    def close(self):
        pass


def test_job_server_snapshot(monkeypatch, xconfig):
    monkeypatch.setattr(coordinate.job_server.time, 'time', lambda: 1.0)
    # Don't use jobqueue_conf with this, it would lose in-memory sqlite between instances.
    jq = JobQueue()

    ws1 = {'name': 'ws1'}
    jq.set_work_spec(ws1)
    jq.add_work_units('ws1', [('wu3', wu3v), ('wu2', wu2v)])

    # move wu2 to PENDING
    wu_parts, msg = jq.get_work('id2', {})
    assert wu_parts == ('ws1', 'wu2', wu2v)

    snapf = NonClosingStringIO()
    jq._snapshot(snapf, snap_path=None)

    jq = JobQueue()
    # reset for reading
    snapblob = snapf.getvalue()
    logger.info('snapblob len=%s', len(snapblob))
    snapf = StringIO(snapblob)

    jq.load_snapshot(stream=snapf, snap_path=None)

    wu_parts, msg = jq.get_work('id3', {})
    assert wu_parts == ('ws1', 'wu3', wu3v)

    jq.update_work_unit('ws1', 'wu3', {'status': FINISHED})

    monkeypatch.setattr(coordinate.job_server.time, 'time', lambda: 400.0)

    # work unit that was PENDING on suspend properly times out and
    # becomes available again.
    wu_parts, msg = jq.get_work('id2', {})
    assert wu_parts == ('ws1', 'wu2', wu2v)


def test_job_server_logrecover(monkeypatch, xconfig):
    monkeypatch.setattr(coordinate.job_server.time, 'time', lambda: 1.0)
    jq = JobQueue()
    jq.logfile = StringIO()

    ws1 = {'name': 'ws1'}
    jq.set_work_spec(ws1)
    jq.add_work_units('ws1', [('wu3', wu3v), ('wu2', wu2v)])
    # move wu2 to PENDING
    wu_parts, msg = jq.get_work('id2', {})
    assert wu_parts == ('ws1', 'wu2', wu2v)

    logblob = jq.logfile.getvalue()

    logger.info('logblob %s', len(logblob))

    jq = JobQueue()
    jq._run_log(StringIO(logblob))


def test_job_server_update_data(xconfig, jobqueue_conf):
    jq = JobQueue(jobqueue_conf)

    # Create a job
    ws1 = {'name': 'ws1'}
    jq.set_work_spec(ws1)
    jq.add_work_units('ws1', [('wu1', {'x': 1})])
    wu_parts, msg = jq.get_work_units('ws1', {'work_unit_keys': ['wu1']})
    assert wu_parts == [('wu1', {'x': 1})]
    statuses, msg = jq.get_work_unit_status('ws1', ['wu1'])
    assert msg is None
    assert statuses[0]['status'] == AVAILABLE

    # Get the job back
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts == ('ws1', 'wu1', {'x': 1})
    statuses, msg = jq.get_work_unit_status('ws1', ['wu1'])
    assert msg is None
    assert statuses[0]['status'] == PENDING

    # Finish the job
    jq.update_work_unit('ws1', 'wu1', {'status': FINISHED,
                                       'data': {'x': 1, 'output': {}}})
    wu_parts, msg = jq.get_work_units('ws1', {'work_unit_keys': ['wu1']})
    assert wu_parts == [('wu1', {'x': 1, 'output': {}})]
    statuses, msg = jq.get_work_unit_status('ws1', ['wu1'])
    assert msg is None
    assert statuses[0]['status'] == FINISHED


def test_job_server_continuous(xconfig, jobqueue_conf):
    jq = JobQueue(jobqueue_conf)

    # Create a continuous job
    ws1 = {'name': 'ws1', 'continuous': True}
    jq.set_work_spec(ws1)

    # Get synthetic work from the continuous job
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts[0] == 'ws1'
    (sec, frac) = wu_parts[1].split('.', 1)
    assert sec.isdigit()
    assert frac is None or frac.isdigit()
    assert wu_parts[2] == {}
    jq.update_work_unit('ws1', wu_parts[1], {'status': FINISHED})

    # Get synthetic work from the continuous job (again)
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts[0] == 'ws1'
    (sec, frac) = wu_parts[1].split('.', 1)
    assert sec.isdigit()
    assert frac is None or frac.isdigit()
    assert wu_parts[2] == {}
    jq.update_work_unit('ws1', wu_parts[1], {'status': FINISHED})

    # Put real work on the continuous job; we should get it back
    jq.add_work_units('ws1', [('wu1', {'x': 1})])
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts == ('ws1', 'wu1', {'x': 1})
    jq.update_work_unit('ws1', wu_parts[1], {'status': FINISHED})

    # Get synthetic work from the continuous job (again)
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts[0] == 'ws1'
    (sec, frac) = wu_parts[1].split('.', 1)
    assert sec.isdigit()
    assert frac is None or frac.isdigit()
    assert wu_parts[2] == {}
    jq.update_work_unit('ws1', wu_parts[1], {'status': FINISHED})


def test_job_server_continuous_interval(xconfig, monkeypatch, jobqueue_conf):
    jq = JobQueue(jobqueue_conf)
    now = 10000000
    monkeypatch.setattr(time, 'time', lambda: now)
    ws1 = {'name': 'ws1', 'continuous': True, 'interval': 60,
           'max_running': 1}
    jq.set_work_spec(ws1)

    # Calling this the first time should always produce a work unit
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts[0] == 'ws1'
    jq.update_work_unit('ws1', wu_parts[1], {'status': FINISHED})

    # Calling this again at the same "now" shouldn't
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts[0] is None

    # If we wait 59 seconds still get nothing
    now = 10000059
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts[0] is None

    # ...but advancing to 60 produces something
    now = 10000060
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts[0] == 'ws1'
    
    # The finish time doesn't affect the next start time
    now = 10000090
    jq.update_work_unit('ws1', wu_parts[1], {'status': FINISHED})

    now = 10000119
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts[0] is None

    now = 10000120
    wu_parts, msg = jq.get_work('id1', {})
    assert wu_parts[0] == 'ws1'
    jq.update_work_unit(wu_parts[0], wu_parts[1], {'status': FINISHED})


def test_archive_by_count(xconfig, jobqueue_conf):
    config = dict(yakonfig.get_global_config('coordinate', 'job_queue'))
    config['limit_completed_count'] = 2
    config.update(jobqueue_conf)
    jq = JobQueue(config)
    jq.set_work_spec({'name': 'ws1'})
    jq.add_work_units('ws1', [('wu1', {'x': 1}),
                              ('wu2', {'x': 1}),
                              ('wu3', {'x': 1})])
    # Bump all three work units to "finished"
    for wu in ['wu1', 'wu2', 'wu3']:
        wu_parts, msg = jq.get_work('id1', {})
        assert wu_parts[0] == 'ws1'
        assert wu_parts[1] == wu
        jq.update_work_unit(wu_parts[0], wu_parts[1], {'status': FINISHED})

    # Archiving hasn't happened, so we should see the finished count
    # is 3, and all three work units are there
    counts, msg = jq.count_work_units('ws1')
    assert counts[FINISHED] == 3
    wus, msg = jq.get_work_units('ws1', {})
    assert [wu[0] for wu in wus] == ['wu1', 'wu2', 'wu3']

    jq.archive()

    # Now we should still see the same count, but the one that ran
    # first (wu1) is off the list
    counts, msg = jq.count_work_units('ws1')
    assert counts[FINISHED] == 3
    wus, msg = jq.get_work_units('ws1', {})
    assert [wu[0] for wu in wus] == ['wu2', 'wu3']


def test_archive_by_age(monkeypatch, xconfig, jobqueue_conf):
    config = dict(yakonfig.get_global_config('coordinate', 'job_queue'))
    config['limit_completed_age'] = 15
    config.update(jobqueue_conf)
    jq = JobQueue(config)
    jq.set_work_spec({'name': 'ws1'})
    jq.add_work_units('ws1', [('wu1', {'x': 1}),
                              ('wu2', {'x': 1}),
                              ('wu3', {'x': 1})])
    # Bump all three work units to "finished"
    now = 100.0
    monkeypatch.setattr(coordinate.job_server.time, 'time', lambda: now)
    for wu in ['wu1', 'wu2', 'wu3']:
        wu_parts, msg = jq.get_work('id1', {})
        assert wu_parts[0] == 'ws1'
        assert wu_parts[1] == wu
        now += 10
        jq.update_work_unit(wu_parts[0], wu_parts[1], {'status': FINISHED})

    # Archiving hasn't happened, so we should see the finished count
    # is 3, and all three work units are there
    counts, msg = jq.count_work_units('ws1')
    assert counts[FINISHED] == 3
    wus, msg = jq.get_work_units('ws1', {})
    assert [wu[0] for wu in wus] == ['wu1', 'wu2', 'wu3']

    jq.archive()

    # The policy archives jobs that finished over 15 seconds ago.
    # So this archive should have purged wu1:
    counts, msg = jq.count_work_units('ws1')
    assert counts[FINISHED] == 3
    wus, msg = jq.get_work_units('ws1', {})
    assert [wu[0] for wu in wus] == ['wu2', 'wu3']

    # Advance 10 more seconds, and wu2 should go:
    now += 10
    jq.archive()
    counts, msg = jq.count_work_units('ws1')
    assert counts[FINISHED] == 3
    wus, msg = jq.get_work_units('ws1', {})
    assert [wu[0] for wu in wus] == ['wu3']

    # Again:
    now += 10
    jq.archive()
    counts, msg = jq.count_work_units('ws1')
    assert counts[FINISHED] == 3
    wus, msg = jq.get_work_units('ws1', {})
    assert [wu[0] for wu in wus] == []


class MockJobQ(object):
    do_joblog = True
    def _log_action(self, action, args):
        pass


def test_scheduler_empty():
    '''test that the scheduler doesn't break with nothing in it'''
    work_specs = {}
    scheduler = WorkSpecScheduler(work_specs)
    scheduler.update()
    assert scheduler.sources == []
    assert scheduler.loops == []
    assert scheduler.paths == []
    assert scheduler.continuous == []
    ws = scheduler.choose_work_spec()
    assert ws is None


def test_scheduler_one(work_spec_class):
    '''the scheduler should behave correctly with a single work spec'''
    jq = MockJobQ()
    work_specs = {'a': work_spec_class('a', jq)}
    scheduler = WorkSpecScheduler(work_specs)
    scheduler.update()
    assert scheduler.sources == ['a']
    assert scheduler.loops == []
    assert scheduler.paths == [['a']]
    assert scheduler.continuous == []
    ws = scheduler.choose_work_spec()
    assert ws is None  # because 'a' is empty
    work_specs['a'].add_work_units([WorkUnit('k', {})])
    ws = scheduler.choose_work_spec()
    assert ws == 'a'


def test_scheduler_two(work_spec_class):
    '''the scheduler should behave correctly with two chained work specs'''
    jq = MockJobQ()
    work_specs = {'a': work_spec_class('a', jq, next_work_spec='b'),
                  'b': work_spec_class('b', jq)}
    scheduler = WorkSpecScheduler(work_specs)
    scheduler.update()
    assert scheduler.sources == ['a']
    assert scheduler.loops == []
    assert scheduler.paths == [['a', 'b']]
    assert scheduler.continuous == []
    ws = scheduler.choose_work_spec()
    assert ws is None  # because both 'a' and 'b' are empty
    work_specs['a'].add_work_units([WorkUnit('k', {})])
    ws = scheduler.choose_work_spec()
    assert ws == 'a'  # because only 'a' has work
    work_specs['b'].add_work_units([WorkUnit('l', {})])
    ws = scheduler.choose_work_spec()
    assert ws == 'b'  # because it is in a path


def test_scheduler_three(work_spec_class):
    '''the scheduler should behave correctly with three chained work specs'''
    jq = MockJobQ()
    work_specs = {'a': work_spec_class('a', jq, next_work_spec='b'),
                  'b': work_spec_class('b', jq, next_work_spec='c'),
                  'c': work_spec_class('c', jq)}
    scheduler = WorkSpecScheduler(work_specs)
    scheduler.update()
    assert scheduler.sources == ['a']
    assert scheduler.loops == []
    assert scheduler.paths == [['a', 'b', 'c']]
    assert scheduler.continuous == []
    ws = scheduler.choose_work_spec()
    assert ws is None
    work_specs['a'].add_work_units([WorkUnit('k', {})])
    ws = scheduler.choose_work_spec()
    assert ws == 'a'  # because only 'a' has work
    work_specs['b'].add_work_units([WorkUnit('l', {})])
    ws = scheduler.choose_work_spec()
    assert ws == 'b'  # because it is in a path
    work_specs['c'].add_work_units([WorkUnit('m', {})])
    # Get a bunch of work specs.  b and c are both in the path, so one
    # will be picked over a, which isn't in the path; c is later in the
    # path so it should always get chosen
    wss = [scheduler.choose_work_spec() for _ in xrange(1000)]
    assert all(ws == 'c' for ws in wss)
    # Even if we add another work unit, it should always pick the later one
    work_specs['b'].add_work_units([WorkUnit('n', {})])
    wss = [scheduler.choose_work_spec() for _ in xrange(1000)]
    assert all(ws == 'c' for ws in wss)


def test_scheduler_three_paused(work_spec_class):
    '''the scheduler shouldn't schedule paused work specs'''
    jq = MockJobQ()
    work_specs = {'a': work_spec_class('a', jq, next_work_spec='b'),
                  'b': work_spec_class('b', jq, next_work_spec='c'),
                  'c': work_spec_class('c', jq)}
    scheduler = WorkSpecScheduler(work_specs)
    scheduler.update()
    assert scheduler.sources == ['a']
    assert scheduler.loops == []
    assert scheduler.paths == [['a', 'b', 'c']]
    assert scheduler.continuous == []
    ws = scheduler.choose_work_spec()
    assert ws is None
    work_specs['a'].add_work_units([WorkUnit('k', {})])
    work_specs['b'].add_work_units([WorkUnit('l', {})])
    work_specs['c'].add_work_units([WorkUnit('m', {})])

    # So from the previous test:
    assert scheduler.choose_work_spec() == 'c'
    # But if we pause things:
    work_specs['c'].status = PAUSED
    assert scheduler.choose_work_spec() == 'b'
    work_specs['b'].status = PAUSED
    assert scheduler.choose_work_spec() == 'a'
    work_specs['a'].status = PAUSED
    assert scheduler.choose_work_spec() is None
    work_specs['c'].status = RUNNABLE
    assert scheduler.choose_work_spec() == 'c'
    work_specs['b'].status = RUNNABLE
    assert scheduler.choose_work_spec() == 'c'
    work_specs['a'].status = RUNNABLE
    assert scheduler.choose_work_spec() == 'c'


def test_scheduler_trivial_loop(work_spec_class):
    '''one work unit that feeds itself'''
    jq = MockJobQ()
    work_specs = {'a': work_spec_class('a', jq, next_work_spec='a')}
    scheduler = WorkSpecScheduler(work_specs)
    scheduler.update()
    assert scheduler.sources == []
    assert scheduler.loops == [['a']]
    assert scheduler.paths == []
    assert scheduler.continuous == []
    ws = scheduler.choose_work_spec()
    assert ws is None
    work_specs['a'].add_work_units([WorkUnit('k', {})])
    ws = scheduler.choose_work_spec()
    assert ws is 'a'


# A note on probability starting here:
# Many of the following tests look at the probability distribution
# of what comes out of the scheduler.  If the odds of picking choice
# 'a' are P(a), then the expected number in n runs is E(a)=n*P(a),
# and the standard deviation is sigma(a)=sqrt(n*P(a)*(1-P(a))).
# The tests are tuned so that we'll accept E(a) +/- 3*sigma(a).
#
# For the standard cases with n=1000:
#
# Only source/loop jobs:
# P(source)=20/30, E(source)=667, sigma(source)=15
# P(loop)=10/30, E(loop)=333, sigma(loop)=15
#
# One each source/loop/continuous jobs:
# P(source)=10/31, E(source)=323, sigma(source)=15
# P(loop)=10/31, E(loop)=323, sigma(loop)=15
# P(continuous)=11/31, E(continuous)=355, sigma(continuous)=15
#
# One each source/loop jobs, empty continuous spec present:
# P(source)=20/31, E(source)=645, sigma(source)=15
# P(loop)=10/31, E(loop)=323, sigma(loop)=15
# P(continuous)=1/31, E(continuous)=32, sigma(continuous)=6


def test_scheduler_one_chain(work_spec_class):
    '''some of everything'''
    jq = MockJobQ()
    work_specs = {'a': work_spec_class('a', jq, next_work_spec='b'),
                  'b': work_spec_class('b', jq, next_work_spec='c'),
                  'c': work_spec_class('c', jq, next_work_spec='d'),
                  'd': work_spec_class('d', jq, next_work_spec='e'),
                  'e': work_spec_class('e', jq, next_work_spec='d')}
    scheduler = WorkSpecScheduler(work_specs)
    scheduler.update()
    assert scheduler.sources == ['a']
    assert scheduler.loops == [['d', 'e']] or scheduler.loops == [['e', 'd']]
    assert scheduler.paths == [['a', 'b', 'c']]
    assert scheduler.continuous == []
    ws = scheduler.choose_work_spec()
    assert ws is None
    # Let's simulate a real load job:
    work_specs['a'].add_work_units([WorkUnit('a1', {}), WorkUnit('a2', {})])
    assert scheduler.choose_work_spec() == 'a'
    work_specs['a'].update_work_unit('a1', {'status': FINISHED})
    work_specs['b'].add_work_units([WorkUnit('b1', {})])
    assert scheduler.choose_work_spec() == 'b'
    work_specs['b'].update_work_unit('b1', {'status': FINISHED})
    work_specs['c'].add_work_units([WorkUnit('c1', {})])
    assert scheduler.choose_work_spec() == 'c'
    work_specs['c'].update_work_unit('c1', {'status': FINISHED})
    work_specs['d'].add_work_units([WorkUnit('d1', {})])
    # Now there is one work unit in a and one in d.  We should get both,
    # with 2/3 of the choices being a (the source).
    counts = Counter()
    for _ in xrange(1000):
        counts[scheduler.choose_work_spec()] += 1
    #assert counts['a'] > 620 and counts['a'] < 715
    assert counts['a'] == 1000
    assert counts['b'] == 0
    assert counts['c'] == 0
    #assert counts['d'] > 285 and counts['d'] < 380
    assert counts['d'] == 0
    assert counts['e'] == 0
    # Pick a (and leave the work unit for the loop in d).  We should
    # see the same sequence:
    work_specs['a'].update_work_unit('a2', {'status': FINISHED})
    work_specs['b'].add_work_units([WorkUnit('b2', {})])
    assert scheduler.choose_work_spec() == 'b'
    work_specs['b'].update_work_unit('b2', {'status': FINISHED})
    work_specs['c'].add_work_units([WorkUnit('c2', {})])
    assert scheduler.choose_work_spec() == 'c'
    work_specs['c'].update_work_unit('c2', {'status': FINISHED})
    work_specs['d'].add_work_units([WorkUnit('d2', {})])
    # Now there are two things in d and we must pick d (and go
    # through the loop)
    assert scheduler.choose_work_spec() == 'd'


def test_scheduler_continuous(work_spec_class):
    '''add a continuous job into the mix'''
    jq = MockJobQ()
    work_specs = {'source': work_spec_class('source', jq, next_work_spec='path'),
                  'path': work_spec_class('path', jq),
                  'loop': work_spec_class('loop', jq, next_work_spec='loop'),
                  'continuous': work_spec_class('continuous', jq,
                                         next_work_spec='path',
                                         continuous=True)}
    scheduler = WorkSpecScheduler(work_specs)
    scheduler.update()
    assert (scheduler.sources == ['source', 'continuous'] or
            scheduler.sources == ['continuous', 'source'])
    assert scheduler.paths == [['source', 'path']]
    assert scheduler.loops == [['loop']]
    assert scheduler.continuous == ['continuous']

    # Since there's nothing else to do at all, this should always
    # pick "continuous"
    wss = [scheduler.choose_work_spec() for _ in xrange(1000)]
    assert all(ws == 'continuous' for ws in wss)

    # If we insert a work unit on the path job, that should always
    # get picked
    work_specs['path'].add_work_units([WorkUnit('p', {})])
    wss = [scheduler.choose_work_spec() for _ in xrange(1000)]
    assert all(ws == 'path' for ws in wss)

    # Put one work unit on everything; the path job should still
    # always win
    work_specs['source'].add_work_units([WorkUnit('s', {})])
    work_specs['loop'].add_work_units([WorkUnit('l', {})])
    work_specs['continuous'].add_work_units([WorkUnit('c', {})])
    wss = [scheduler.choose_work_spec() for _ in xrange(1000)]
    assert all(ws == 'path' for ws in wss)

    # If the path job finishes, the source job is still available to
    # get all work.
    work_specs['path'].update_work_unit('p', {'status': FINISHED})
    wss = [scheduler.choose_work_spec() for _ in xrange(1000)]
    counts = Counter()
    for ws in wss:
        counts[ws] += 1
    assert counts['source'] == 1000
    assert counts['path'] == 0
    assert counts['loop'] == 0
    assert counts['continuous'] == 0

    # With the source job finished, the continuous and loop jobs should share
    # at 2:1 weight.
    work_specs['source'].update_work_unit('s', {'status': FINISHED})
    counts = Counter()
    for _ in xrange(1000):
        counts[scheduler.choose_work_spec()] += 1
    assert abs(counts['loop'] - 333) < 50
    assert abs(counts['continuous'] - 667) < 50
    assert counts['source'] == 0
    assert counts['path'] == 0

    # If the continuous job finishes, it should only get the
    # continuous trigger; weighted against loop at 10:1
    work_specs['continuous'].update_work_unit('c', {'status': FINISHED})
    wss = [scheduler.choose_work_spec() for _ in xrange(1000)]
    counts = Counter()
    for ws in wss:
        counts[ws] += 1
    print counts
    assert counts['source'] == 0
    assert counts['path'] == 0
    assert abs(counts['loop'] - (1000.0*10/11)) < 50
    assert abs(counts['continuous'] - (1000.0*1/11)) < 50


def test_scheduler_weights(work_spec_class):
    '''two independent work specs with weight 4 and 6 should get scheduled in correct proportion'''
    jq = MockJobQ()
    work_specs = {'a': work_spec_class('a', jq, weight=4),
                  'b': work_spec_class('b', jq, weight=6)}
    scheduler = WorkSpecScheduler(work_specs)
    scheduler.update()
    assert scheduler.sources == ['a', 'b']
    assert scheduler.loops == []
    assert scheduler.paths == [['a'], ['b']]
    assert scheduler.continuous == []
    ws = scheduler.choose_work_spec()
    assert ws is None  # because both 'a' and 'b' are empty
    work_specs['a'].add_work_units([WorkUnit('k{}'.format(n), {}) for n in xrange(1,100)])
    ws = scheduler.choose_work_spec()
    assert ws == 'a'  # because only 'a' has work
    work_specs['b'].add_work_units([WorkUnit('l{}'.format(n), {}) for n in xrange(1,100)])
    ws = scheduler.choose_work_spec()
    assert ws == 'b'  # because it is now available and higher weight

    gotwork = []
    lease_time = time.time() + 300
    for i in xrange(10):
        ws_name = scheduler.choose_work_spec()
        wu = work_specs[ws_name].get_work('worker{}'.format(i), lease_time, 1)
        gotwork.append( (ws_name, wu) )
    assert sum([x[0]=='a' for x in gotwork]) == 4
    assert sum([x[0]=='b' for x in gotwork]) == 6
    # approximating 6:4 at each step
    assert [x[0] for x in gotwork] == ['b', 'a', 'b', 'a', 'b', 'b', 'a', 'b', 'a', 'b']


def test_scheduler_weights_vs_priority(work_spec_class):
    '''two independent work specs with weight 4 and 6 should get scheduled in correct proportion'''
    jq = MockJobQ()
    work_specs = {
        'a': work_spec_class('a', jq, weight=4),
        'b': work_spec_class('b', jq, weight=6),
        'c': work_spec_class('c', jq, weight=1),
    }
    scheduler = WorkSpecScheduler(work_specs)
    scheduler.update()
    assert sorted(scheduler.sources) == ['a', 'b', 'c']
    assert scheduler.loops == []
    assert sorted(scheduler.paths) == [['a'], ['b'], ['c']]
    assert scheduler.continuous == []
    ws = scheduler.choose_work_spec()
    assert ws is None  # because both 'a' and 'b' are empty

    work_specs['a'].add_work_units([WorkUnit('k{}'.format(n), {}) for n in xrange(1,100)])
    ws = scheduler.choose_work_spec()
    assert ws == 'a'  # because only 'a' has work

    work_specs['b'].add_work_units([WorkUnit('l{}'.format(n), {}) for n in xrange(1,100)])
    ws = scheduler.choose_work_spec()
    assert ws == 'b'  # because it is now available and higher weight

    work_specs['c'].add_work_units([WorkUnit('m{}'.format(n), {}, priority=9) for n in xrange(1,5)])
    ws = scheduler.choose_work_spec()
    assert ws == 'c'  # because it is now available and higher priority

    gotwork = []
    lease_time = time.time() + 300
    for i in xrange(14):
        ws_name = scheduler.choose_work_spec()
        wu = work_specs[ws_name].get_work('worker{}'.format(i), lease_time, 1)
        gotwork.append( (ws_name, wu) )
    assert sum([x[0]=='c' for x in gotwork]) == 4
    assert sum([x[0]=='a' for x in gotwork]) == 4
    assert sum([x[0]=='b' for x in gotwork]) == 6
    # approximating 6:4 at each step
    assert [x[0] for x in gotwork] == ['c', 'c', 'c', 'c', 'b', 'a', 'b', 'a', 'b', 'b', 'a', 'b', 'a', 'b']


def test_scheduler_two_no_preempt(work_spec_class):
    '''the scheduler should behave correctly with two chained work specs'''
    jq = MockJobQ()
    work_specs = {'a': work_spec_class('a', jq, next_work_spec='b', next_work_spec_preempts=False, weight=100),
                  'b': work_spec_class('b', jq, weight=1)}
    scheduler = WorkSpecScheduler(work_specs)
    scheduler.update()
    assert scheduler.sources == ['a']
    assert scheduler.loops == []
    assert scheduler.paths == [['a', 'b']]
    assert scheduler.continuous == []
    ws = scheduler.choose_work_spec()
    assert ws is None  # because both 'a' and 'b' are empty
    work_specs['a'].add_work_units([WorkUnit('k', {})])
    ws = scheduler.choose_work_spec()
    assert ws == 'a'  # because only 'a' has work
    work_specs['b'].add_work_units([WorkUnit('l', {})])
    ws = scheduler.choose_work_spec()
    assert ws == 'a'  # a should still win
    work_specs['a'].get_work('w1',time.time() + 1000, 1)
    ws = scheduler.choose_work_spec()
    assert ws == 'b'  # now we can b


def test_scheduler_three_no_preempt(work_spec_class):
    '''the scheduler should behave correctly with three chained work specs'''
    jq = MockJobQ()
    work_specs = {'a': work_spec_class('a', jq, next_work_spec='b', weight=40),
                  'b': work_spec_class('b', jq, next_work_spec='c', next_work_spec_preempts=False),
                  'c': work_spec_class('c', jq, weight=20)}
    scheduler = WorkSpecScheduler(work_specs)
    scheduler.update()
    assert scheduler.sources == ['a']
    assert scheduler.loops == []
    assert scheduler.paths == [['a', 'b', 'c']]
    assert scheduler.continuous == []
    ws = scheduler.choose_work_spec()
    assert ws is None

    work_specs['c'].add_work_units([WorkUnit('m', {})])
    ws = scheduler.choose_work_spec()
    assert ws == 'c'  # because only 'c' has work

    work_specs['a'].add_work_units([WorkUnit('k', {})])
    ws = scheduler.choose_work_spec()
    assert ws == 'a'  # because 'a' has higher weight

    work_specs['b'].add_work_units([WorkUnit('l', {})])
    ws = scheduler.choose_work_spec()
    assert ws == 'b'  # because it is preempts 'a'

    # work_specs['c'].add_work_units([WorkUnit('m', {})])
    # # Get a bunch of work specs.  b and c are both in the path, so one
    # # will be picked over a, which isn't in the path; c is later in the
    # # path so it should always get chosen
    # wss = [scheduler.choose_work_spec() for _ in xrange(1000)]
    # assert all(ws == 'c' for ws in wss)
    # # Even if we add another work unit, it should always pick the later one
    # work_specs['b'].add_work_units([WorkUnit('n', {})])
    # wss = [scheduler.choose_work_spec() for _ in xrange(1000)]
    # assert all(ws == 'c' for ws in wss)
