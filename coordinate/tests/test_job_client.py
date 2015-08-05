'''Unit tests for :mod:`coordinate.job_client`.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2015 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function

import pytest

from ..base_client import cbor_return, make_cbor_tagmapper
from ..constants import AVAILABLE, PENDING, FINISHED
from ..job_client import TaskMaster
from ..job_server import JobQueue
from ..server import CborRpcHandler


def run_function(work_unit, *args, **kwargs):
    work_unit.data['output'] = {'foo': {'bar': 'baz'}}
    if args:
        work_unit.data['args'] = args
    if kwargs:
        work_unit.data['kwargs'] = kwargs


@pytest.fixture
def job_queue():
    jq = JobQueue({'do_recover': False})
    jq.do_rpclog = False
    return jq


# Return a TaskMaster that points to local server.
# RPCs go object to object, no networking happens.'''
@pytest.fixture
def task_master(job_queue):
    class Conn(object):
        def __init__(self):
            self.resp = ''
            self.closed = False
            self.tagmapper = make_cbor_tagmapper()
            self.server = CborRpcHandler(self, None, None,
                                         proxy_object=job_queue)

        def send(self, buf):
            message = self.tagmapper.loads(buf)
            rval = self.server.handle_message(message)
            self.resp = cbor_return(self.tagmapper, rval)

        def recv(self, n=-1):
            if n < 0:
                resp = self.resp
                self.resp = ''
            else:
                resp = self.resp[:n]
                self.resp = self.resp[n:]
            return resp

        write = send
        read = recv

        def nop(self, *args, **kwargs):
            pass

        shutdown = nop
        flush = nop
        close = nop

        def makefile(self, mode, size):
            return self

    tm = TaskMaster({'addr_family': 'test'})
    tm._socket = Conn()
    return tm


@pytest.fixture
def work_spec():
    return {
        'name': 'test_job_client',
        'min_gb': 1,
        'module': 'coordinate.tests.test_job_client',
        'run_function': 'run_function'
    }


def do_work(task_master, work_spec, key, data):
    task_master.set_work_spec(work_spec)
    task_master.add_work_units(work_spec['name'], [(key, data)])
    assert (task_master.get_work_units(work_spec['name'],
                                       work_unit_keys=[key]) ==
            [(key, data)])
    assert (task_master.get_work_unit_status(work_spec['name'],
                                             [key])[0]['status'] ==
            AVAILABLE)

    wu = task_master.get_work('test', available_gb=1)
    assert wu is not None
    assert wu.work_spec_name == work_spec['name']
    assert wu.key == key
    assert (task_master.get_work_units(work_spec['name'],
                                       work_unit_keys=[key]) ==
            [(key, data)])
    assert (task_master.get_work_unit_status(work_spec['name'],
                                             [key])[0]['status'] ==
            PENDING)
    wu.run()
    wu.finish()

    assert (task_master.get_work_unit_status(work_spec['name'],
                                             [key])[0]['status'] ==
            FINISHED)
    wus = task_master.get_work_units(work_spec['name'],
                                     work_unit_keys=[key])
    assert len(wus) == 1
    assert wus[0][0] == key
    return wus[0][1]


def do_one_work(task_master, work_spec, key):
    wu = task_master.get_work(work_spec)
    if key is None:
        assert wu is None
    else:
        assert wu is not None
        assert wu.work_spec_name == work_spec
        assert wu.key == key
        wu.finish()


def test_data_updates(task_master, work_spec):
    res = do_work(task_master, work_spec, 'u', {'k': 'v'})
    assert res == {'k': 'v',
                   'output': {'foo': {'bar': 'baz'}}}


def test_args(task_master, work_spec):
    work_spec['run_params'] = ['arg']
    res = do_work(task_master, work_spec, 'u', {'k': 'v'})

    # Since we set run_params to a list, it should get passed to
    # run_function as *args, and we should see it in the output
    # (as a tuple)
    assert res == {'k': 'v',
                   'output': {'foo': {'bar': 'baz'}},
                   'args': ('arg',)}


def test_kwargs(task_master, work_spec):
    work_spec['run_params'] = {'param': 'value'}
    res = do_work(task_master, work_spec, 'u', {'k': 'v'})

    # Since we set run_params to a dictionary, it should get passed to
    # run_function as **kwargs, and we should see it in the output
    assert res == {'k': 'v',
                   'output': {'foo': {'bar': 'baz'}},
                   'kwargs': {'param': 'value'}}


def test_pause(task_master, work_spec):
    task_master.set_work_spec(work_spec)
    task_master.add_work_units(work_spec['name'], [('u', {'k': 'v'})])
    assert (task_master.get_work_units(work_spec['name']) ==
            [('u', {'k': 'v'})])
    assert (task_master.get_work_unit_status(work_spec['name'], ['u'])
            [0]['status'] == AVAILABLE)

    task_master.pause_work_spec(work_spec['name'], paused=True)
    wu = task_master.get_work('test', available_gb=1)
    assert wu is None

    task_master.pause_work_spec(work_spec['name'], paused=False)
    wu = task_master.get_work('test', available_gb=1)
    assert wu.work_spec_name == work_spec['name']
    assert wu.key == 'u'


def test_get_many(task_master, work_spec):
    task_master.set_work_spec(work_spec)
    task_master.add_work_units(
        work_spec['name'],
        [
            ('u{:03d}'.format(x), {'k': 'v{}'.format(x)})
            for x in xrange(1, 100)
        ])
    wus = task_master.get_work('test', available_gb=1, lease_time=300,
                               max_jobs=10)
    assert len(wus) == 10
    assert [wu.key for wu in wus] == ['u{:03d}'.format(x)
                                      for x in xrange(1, 11)]


def test_get_many_max_getwork(task_master, work_spec):
    work_spec = dict(work_spec)
    work_spec['max_getwork'] = 5
    task_master.set_work_spec(work_spec)
    task_master.add_work_units(
        work_spec['name'],
        [
            ('u{:03d}'.format(x), {'k': 'v{}'.format(x)})
            for x in xrange(1, 100)
        ])
    wus = task_master.get_work('test', available_gb=1, lease_time=300,
                               max_jobs=10)
    assert len(wus) == 5
    assert [wu.key for wu in wus] == ['u{:03d}'.format(x)
                                      for x in xrange(1, 6)]


def test_get_too_many(task_master, work_spec):
    # check what happens when we get the last few
    work_spec = dict(work_spec)
    work_spec['weight'] = 1
    task_master.set_work_spec(work_spec)
    task_master.add_work_units(
        work_spec['name'],
        [
            ('u{:03d}'.format(x), {'k': 'v{}'.format(x)})
            for x in xrange(1, 100)
        ])

    other_work_spec = {
        'name': 'ws2',
        'min_gb': 0.1,
        'module': 'coordinate.tests.test_job_client',
        'run_function': 'run_function',
        'weight': 300,  # winner
    }
    task_master.set_work_spec(other_work_spec)
    task_master.add_work_units(
        'ws2',
        [
            ('z{:03d}'.format(x), {'k': 'v{}'.format(x)})
            for x in xrange(1, 5)
        ])

    wus = task_master.get_work('test', available_gb=1, lease_time=300,
                               max_jobs=10)
    # we should get 4 from the higher weight spec queue, but none from
    # the other
    assert len(wus) == 4
    assert [wu.key for wu in wus] == ['z{:03d}'.format(x)
                                      for x in xrange(1, 5)]


def test_prioritize(task_master, work_spec):
    task_master.set_work_spec(work_spec)
    task_master.add_work_units(work_spec['name'],
                               [('a', {'k': 'v'}),
                                ('b', {'k': 'v'}),
                                ('c', {'k': 'v'})])

    # Default order is alphabetical
    do_one_work(task_master, work_spec['name'], 'a')

    # If we prioritize c, it should go first, before b
    task_master.prioritize_work_units(work_spec['name'], ['c'])
    do_one_work(task_master, work_spec['name'], 'c')
    do_one_work(task_master, work_spec['name'], 'b')
    do_one_work(task_master, work_spec['name'], None)


def test_prioritize_adjust(task_master, work_spec):
    task_master.set_work_spec(work_spec)
    task_master.add_work_units(work_spec['name'],
                               [('a', {'k': 'v'}),
                                ('b', {'k': 'v'}),
                                ('c', {'k': 'v'})])

    # Use "adjust" mode to set the priorities
    task_master.prioritize_work_units(work_spec['name'], ['a'],
                                      adjustment=10)
    task_master.prioritize_work_units(work_spec['name'], ['b'],
                                      adjustment=20)
    task_master.prioritize_work_units(work_spec['name'], ['c'],
                                      adjustment=30)

    # Highest priority goes first
    do_one_work(task_master, work_spec['name'], 'c')
    do_one_work(task_master, work_spec['name'], 'b')
    do_one_work(task_master, work_spec['name'], 'a')
    do_one_work(task_master, work_spec['name'], None)


def test_reprioritize(task_master, work_spec):
    task_master.set_work_spec(work_spec)
    task_master.add_work_units(work_spec['name'],
                               [('a', {'k': 'v'}),
                                ('b', {'k': 'v'}),
                                ('c', {'k': 'v'})])

    # Use "priority" mode to set the priorities
    task_master.prioritize_work_units(work_spec['name'], ['a'],
                                      priority=10)
    task_master.prioritize_work_units(work_spec['name'], ['b'],
                                      priority=20)
    task_master.prioritize_work_units(work_spec['name'], ['c'],
                                      priority=30)

    # Highest priority goes first
    do_one_work(task_master, work_spec['name'], 'c')

    # Now adjust "a" to have higher priority
    task_master.prioritize_work_units(work_spec['name'], ['a'],
                                      adjustment=15)  # + 10 = 25
    do_one_work(task_master, work_spec['name'], 'a')
    do_one_work(task_master, work_spec['name'], 'b')
    do_one_work(task_master, work_spec['name'], None)


def test_succeed_fail(task_master, work_spec):
    '''Test that failing a finished work unit is a no-op (DIFFEO-1184)'''
    task_master.set_work_spec(work_spec)
    task_master.add_work_units(work_spec['name'], [('a', {'k': 'v'})])

    wu = task_master.get_work('child')
    assert wu is not None
    assert wu.work_spec_name == work_spec['name']
    assert wu.key == 'a'

    wu.finish()

    # meanwhile the parent nukes us from orbit
    task_master.update_work_unit(work_spec['name'], 'a',
                                 status=task_master.FAILED)

    # the end status should be "succeeded"
    status = task_master.get_work_unit_status(work_spec['name'], 'a')
    assert status['status'] == task_master.FINISHED


def test_fail_succeed(task_master, work_spec):
    '''Test that finishing a failed work unit wins (DIFFEO-1184)'''
    task_master.set_work_spec(work_spec)
    task_master.add_work_units(work_spec['name'], [('a', {'k': 'v'})])

    wu = task_master.get_work('child')
    assert wu is not None
    assert wu.work_spec_name == work_spec['name']
    assert wu.key == 'a'

    # meanwhile the parent nukes us from orbit
    task_master.update_work_unit(work_spec['name'], 'a',
                                 status=task_master.FAILED)

    # but wait!  we actually did the job!
    wu.finish()

    # the end status should be "succeeded"
    status = task_master.get_work_unit_status(work_spec['name'], 'a')
    assert status['status'] == task_master.FINISHED


def test_get_child_units_basic(task_master, work_spec):
    task_master.set_work_spec(work_spec)
    task_master.add_work_units(work_spec['name'], [('a', {'k': 'v'})])
    task_master.worker_register('parent')
    task_master.worker_register('child', parent='parent')

    assert task_master.get_child_work_units('parent') == {}

    wu = task_master.get_work('child')
    assert wu is not None
    assert wu.work_spec_name == work_spec['name']
    assert wu.key == 'a'

    wudict = task_master.get_child_work_units('parent')
    assert len(wudict) == 1
    assert 'child' in wudict
    assert len(wudict['child']) == 1
    assert wu.work_spec_name == wudict['child'][0].work_spec_name
    assert wu.key == wudict['child'][0].key
    assert wu.worker_id == wudict['child'][0].worker_id
    assert wu.data == wudict['child'][0].data

    wu.finish()
    assert task_master.get_child_work_units('parent') == {}


def test_get_child_units_multi(task_master, work_spec):
    task_master.set_work_spec(work_spec)
    task_master.add_work_units(work_spec['name'],
                               [('a', {'k': 'v'}), ('b', {'k': 'v'})])
    task_master.worker_register('parent')
    task_master.worker_register('child', parent='parent')

    assert task_master.get_child_work_units('parent') == {}

    wus = task_master.get_work('child', max_jobs=10)
    assert len(wus) == 2
    assert sorted([wu.key for wu in wus]) == ['a', 'b']
    assert all(wu.work_spec_name == work_spec['name'] for wu in wus)

    wudict = task_master.get_child_work_units('parent')
    assert len(wudict) == 1
    assert 'child' in wudict
    assert len(wudict['child']) == 2
    assert sorted([wu.key for wu in wudict['child']]) == ['a', 'b']
    assert all(wu.work_spec_name == work_spec['name']
               for wu in wudict['child'])
    assert all(wu.worker_id == 'child' for wu in wudict['child'])

    wus[0].finish()

    wudict = task_master.get_child_work_units('parent')
    assert len(wudict) == 1
    assert 'child' in wudict
    assert len(wudict['child']) == 1
    assert wus[1].work_spec_name == wudict['child'][0].work_spec_name
    assert wus[1].key == wudict['child'][0].key
    assert wus[1].worker_id == wudict['child'][0].worker_id
    assert wus[1].data == wudict['child'][0].data

    wus[1].finish()

    assert task_master.get_child_work_units('parent') == {}
