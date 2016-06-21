# coding=utf-8
'''Run coordinate TaskMaster tests under coordinate.

.. Your use of this software is governed by your license agreement.
   Copyright 2012-2015 Diffeo, Inc.

'''
from __future__ import absolute_import
import logging
import random
import subprocess
import sys
import time
import uuid

import pytest

from coordinate.constants import WORK_UNIT_STATUS_BY_NAME
from coordinate.tests.test_job_client import job_queue, task_master  # noqa


logger = logging.getLogger(__name__)

work_spec = {
    'name': 'tbundle',
    'desc': 'a test work bundle',
    'min_gb': 8,
    'config': {'many': '', 'params': ''},
    'module': 'tests.coordinate.test_workers',
    'run_function': 'work_program',
    'terminate_function': 'work_program',
}


@pytest.yield_fixture(scope='module')
def local_server_port():
    port = random.randint(4000, 32000)
    logger.info('starting coordinate localhost:%s', port)
    subp = subprocess.Popen([sys.executable, '-m', 'coordinate.run',
                             '-v', '--port', str(port)], shell=False)
    time.sleep(5.0)
    # TODO: check that it didn't crash on startup, e.g. due to port in use
    logging.info('poll says: %s', subp.poll())
    yield port
    logger.info('killing coordinate server at %s', port)
    try:
        subp.kill()
    except:
        logger.error('error killing coordinate subprocess', exc_info=True)


def test_list_work_specs(task_master):  # noqa
    # Initial state: nothing
    assert task_master.list_work_specs() == ([], None)

    work_units = dict(foo={'length': 3}, foobar={'length': 6})
    task_master.update_bundle(work_spec, work_units)

    specs, next = task_master.list_work_specs()
    specs = dict(specs)
    assert len(specs) == 1
    assert work_spec['name'] in specs
    assert specs[work_spec['name']]['desc'] == work_spec['desc']


def test_clear(task_master):  # noqa
    # Initial state: nothing
    assert task_master.list_work_specs() == ([], None)

    work_units = dict(foo={'length': 3}, foobar={'length': 6})
    task_master.update_bundle(work_spec, work_units)

    specs, next = task_master.list_work_specs()
    specs = dict(specs)
    assert len(specs) == 1
    assert work_spec['name'] in specs
    assert specs[work_spec['name']]['desc'] == work_spec['desc']

    task_master.clear()

    # back to nothing
    assert task_master.list_work_specs() == ([], None)


def test_list_work_units(task_master):  # noqa
    work_units = dict(foo={'length': 3}, foobar={'length': 6})
    task_master.update_bundle(work_spec, work_units)

    # Initial check: both work units are there
    u = task_master.list_work_units(work_spec['name'])
    for k, v in u.iteritems():
        assert k in work_units
        assert 'length' in v
        assert len(k) == v['length']
    assert sorted(work_units.keys()) == sorted(u.keys())

    # Start one unit; should still be there
    work_unit = task_master.get_work('fake_worker_id', available_gb=13)
    assert work_unit.key in work_units
    u = task_master.list_work_units(work_spec['name'])
    assert work_unit.key in u
    assert sorted(u.keys()) == sorted(work_units.keys())

    # Finish that unit; should be gone, the other should be there
    work_unit.finish()
    u = task_master.list_work_units(work_spec['name'])
    assert work_unit.key not in u
    assert all(k in work_units for k in u.iterkeys())
    assert all(k == work_unit.key or k in u for k in work_units.iterkeys())


def test_list_work_units_start_limit(task_master):  # noqa
    work_units = dict(foo={'length': 3}, bar={'length': 6})
    task_master.update_bundle(work_spec, work_units)

    u = task_master.list_work_units(work_spec['name'], start=0, limit=1)
    assert u == {'bar': {'length': 6}}

    u = task_master.list_work_units(work_spec['name'], start=1, limit=1)
    assert u == {'foo': {'length': 3}}

    u = task_master.list_work_units(work_spec['name'], start=2, limit=1)
    assert u == {}


def test_del_work_units_simple(task_master):  # noqa
    work_units = dict(foo={'length': 3}, bar={'length': 6})
    task_master.update_bundle(work_spec, work_units)

    rc = task_master.del_work_units(work_spec['name'],
                                    work_unit_keys=['foo'])
    assert rc == 1
    assert (task_master.get_work_units(work_spec['name']) ==
            [('bar', {'length': 6})])


STATES = ['AVAILABLE', 'PENDING', 'FINISHED', 'FAILED']


def prepare_one_of_each(task_master):  # noqa
    task_master.update_bundle(work_spec, {'FA': {'x': 1}})
    wu = task_master.get_work('worker', available_gb=16)
    assert wu.key == 'FA'
    wu.fail()

    task_master.update_bundle(work_spec, {'FI': {'x': 1}})
    wu = task_master.get_work('worker', available_gb=16)
    assert wu.key == 'FI'
    wu.finish()

    task_master.update_bundle(work_spec, {'PE': {'x': 1}})
    wu = task_master.get_work('worker', available_gb=16)
    assert wu.key == 'PE'

    task_master.update_bundle(work_spec, {'AV': {'x': 1}})


@pytest.mark.parametrize('state', STATES)  # noqa
def test_del_work_units_by_name(task_master, state):
    prepare_one_of_each(task_master)

    rc = task_master.del_work_units(work_spec['name'],
                                    work_unit_keys=[state[:2]])
    assert rc == 1

    expected = set(STATES)
    expected.remove(state)
    work_units = task_master.get_work_units(work_spec['name'])
    work_unit_keys = set(p[0] for p in work_units)
    assert work_unit_keys == set(st[0:2] for st in expected)


@pytest.mark.parametrize('state', STATES)  # noqa
def test_del_work_units_by_state(task_master, state):
    prepare_one_of_each(task_master)

    rc = task_master.del_work_units(work_spec['name'],
                                    state=WORK_UNIT_STATUS_BY_NAME[state])
    assert rc == 1

    expected = set(STATES)
    expected.remove(state)
    work_units = task_master.get_work_units(work_spec['name'])
    work_unit_keys = set(p[0] for p in work_units)
    assert work_unit_keys == set(st[0:2] for st in expected)


@pytest.mark.parametrize('state', STATES)  # noqa
def test_del_work_units_by_name_and_state(task_master, state):
    prepare_one_of_each(task_master)

    rc = task_master.del_work_units(work_spec['name'],
                                    work_unit_keys=[state[:2]],
                                    state=WORK_UNIT_STATUS_BY_NAME[state])
    assert rc == 1

    expected = set(STATES)
    expected.remove(state)
    work_units = task_master.get_work_units(work_spec['name'])
    work_unit_keys = set(p[0] for p in work_units)
    assert work_unit_keys == set(st[0:2] for st in expected)


def prepare_two_of_each(task_master):  # noqa
    task_master.update_bundle(work_spec, {'FA': {'x': 1}, 'IL': {'x': 1}})
    wu = task_master.get_work('worker', available_gb=16)
    wu.fail()
    wu = task_master.get_work('worker', available_gb=16)
    wu.fail()

    task_master.update_bundle(work_spec, {'FI': {'x': 1}, 'NI': {'x': 1}})
    wu = task_master.get_work('worker', available_gb=16)
    wu.finish()
    wu = task_master.get_work('worker', available_gb=16)
    wu.finish()

    task_master.update_bundle(work_spec, {'PE': {'x': 1}, 'ND': {'x': 1}})
    wu = task_master.get_work('worker', available_gb=16)
    wu = task_master.get_work('worker', available_gb=16)

    task_master.update_bundle(work_spec, {'AV': {'x': 1}, 'AI': {'x': 1}})


@pytest.mark.parametrize('state', STATES)  # noqa
def test_del_work_units_by_name2(task_master, state):
    prepare_two_of_each(task_master)

    rc = task_master.del_work_units(work_spec['name'],
                                    work_unit_keys=[state[:2]])
    assert rc == 1

    # This deletes FA but leaves IL
    expected = set(st[0:2]for st in STATES)
    expected.update(set(st[2:4] for st in STATES))
    expected.remove(state[0:2])

    work_units = task_master.get_work_units(work_spec['name'])
    work_unit_keys = set(p[0] for p in work_units)
    assert work_unit_keys == expected


@pytest.mark.parametrize('state', STATES)  # noqa
def test_del_work_units_by_state2(task_master, state):
    prepare_two_of_each(task_master)

    rc = task_master.del_work_units(work_spec['name'],
                                    state=WORK_UNIT_STATUS_BY_NAME[state])
    assert rc == 2

    # These deletes both FA and IL
    expected = set(st[0:2]for st in STATES)
    expected.update(set(st[2:4] for st in STATES))
    expected.remove(state[0:2])
    expected.remove(state[2:4])

    work_units = task_master.get_work_units(work_spec['name'])
    work_unit_keys = set(p[0] for p in work_units)
    assert work_unit_keys == expected


@pytest.mark.parametrize('state', STATES)  # noqa
def test_del_work_units_by_name_and_state2(task_master, state):
    prepare_two_of_each(task_master)

    rc = task_master.del_work_units(work_spec['name'],
                                    work_unit_keys=[state[:2]],
                                    state=WORK_UNIT_STATUS_BY_NAME[state])
    assert rc == 1

    # This deletes FA but leaves IL
    expected = set(st[0:2]for st in STATES)
    expected.update(set(st[2:4] for st in STATES))
    expected.remove(state[0:2])

    work_units = task_master.get_work_units(work_spec['name'])
    work_unit_keys = set(p[0] for p in work_units)
    assert work_unit_keys == expected


def test_task_master_regenerate(task_master):  # noqa
    '''test that getting work lets us resubmit the work spec'''
    task_master.update_bundle(work_spec, {'one': {'number': 1}})

    work_unit1 = task_master.get_work('fake_worker_id1', available_gb=13)
    assert work_unit1.key == 'one'
    task_master.update_bundle(work_unit1.spec, {'two': {'number': 2}})
    work_unit1.finish()

    work_unit2 = task_master.get_work('fake_worker_id1', available_gb=13)
    assert work_unit2.key == 'two'
    work_unit2.finish()

    work_unit3 = task_master.get_work('fake_worker_id1', available_gb=13)
    assert work_unit3 is None


def test_task_master_binary_work_unit(task_master):  # noqa
    work_units = {
        b'\x00': {'k': b'\x00', 't': 'single null'},
        b'\x00\x01\x02\x03': {'k': b'\x00\x01\x02\x03', 't': 'control chars'},
        b'\x00a\x00b': {'k': b'\x00a\x00b', 't': 'UTF-16BE'},
        b'a\x00b\x00': {'k': b'a\x00b\x00', 't': 'UTF-16LE'},
        b'f\xc3\xbc': {'k': b'f\xc3\xbc', 't': 'UTF-8'},
        b'f\xfc': {'k': b'f\xfc', 't': 'ISO-8859-1'},
        b'\xf0\x0f': {'k': b'\xf0\x0f', 't': 'F00F'},
        b'\xff': {'k': b'\xff', 't': 'FF'},
        b'\xff\x80': {'k': b'\xff\x80', 't': 'FF80'},
    }
    task_master.update_bundle(work_spec, work_units)

    assert task_master.list_work_units(work_spec['name']) == work_units

    completed = []
    for _ in xrange(len(work_units)):
        wu = task_master.get_work('fake_worker_id', available_gb=13)
        assert wu.key in work_units
        assert wu.data == work_units[wu.key]
        completed.append(wu.key)
        wu.finish()

    wu = task_master.get_work('fake_worker_id', available_gb=13)
    assert wu is None

    assert sorted(completed) == sorted(work_units.keys())

    assert (task_master.list_finished_work_units(work_spec['name']) ==
            work_units)


def test_task_master_work_unit_value(task_master):  # noqa
    work_units = {
        'k': {'list': [1, 2, 3],
              'tuple': (4, 5, 6),
              'mixed': [1, (2, [3, 4])],
              'uuid': uuid.UUID('01234567-89ab-cdef-0123-456789abcdef'),
              'str': b'foo',
              'unicode': u'foo',
              'unicode2': u'fü'}
    }
    task_master.update_bundle(work_spec, work_units)
    assert task_master.list_work_units(work_spec['name']) == work_units
