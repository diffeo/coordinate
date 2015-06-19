from __future__ import absolute_import

# these imports look unused but are needed for pytest
from coordinate.tests.test_job_client import job_queue, task_master  # noqa


def first_function(work_unit):
    work_unit.data['output'] = {work_unit.key: {}}


def second_function(work_unit, mode):
    work_unit.data['output'] = {work_unit.key: {'mode': mode}}


def list_function(work_unit):
    work_unit.data['output'] = [work_unit.key]


def config_function(work_unit):
    work_unit.data['output'] = work_unit.spec['config']


def test_minimal_flow(task_master):  # noqa
    flow = {
        'first': {
            'min_gb': 1,
            'module': 'coordinate.tests.test_job_flow',
            'run_function': 'first_function',
        },
    }
    task_master.add_flow(flow, config={})
    assert task_master.get_work_spec('first') is not None

    task_master.add_work_units('first', [('u', {'k': 'v'})])

    wu = task_master.get_work('test', available_gb=1)
    assert wu is not None
    assert wu.work_spec_name == 'first'
    wu.run()
    wu.finish()

    res = task_master.get_work_units('first')
    assert res == [('u', {'k': 'v', 'output': {'u': {}}})]


def test_nested_flow(task_master):  # noqa
    flow = {
        'parent': {
            'first': {
                'min_gb': 1,
                'module': 'coordinate.tests.test_job_flow',
                'run_function': 'first_function',
            },
        },
    }
    task_master.add_flow(flow, config={})
    assert task_master.get_work_spec('parent.first') is not None

    task_master.add_work_units('parent.first', [('u', {'k': 'v'})])

    wu = task_master.get_work('test', available_gb=1)
    assert wu is not None
    assert wu.work_spec_name == 'parent.first'
    wu.run()
    wu.finish()

    res = task_master.get_work_units('parent.first')
    assert res == [('u', {'k': 'v', 'output': {'u': {}}})]


def test_simple_flow(task_master):  # noqa
    flow = {
        'first': {
            'min_gb': 1,
            'module': 'coordinate.tests.test_job_flow',
            'run_function': 'first_function',
            'then': 'second',
        },
        'second': {
            'min_gb': 1,
            'module': 'coordinate.tests.test_job_flow',
            'run_function': 'second_function',
            'run_params': ['foo'],
        },
    }
    task_master.add_flow(flow, config={})
    assert task_master.get_work_spec('first') is not None
    assert task_master.get_work_spec('second') is not None

    task_master.add_work_units('first', [('u', {'k': 'v'})])

    wu = task_master.get_work('test', available_gb=1)
    assert wu is not None
    assert wu.work_spec_name == 'first'
    wu.run()
    wu.finish()

    wu = task_master.get_work('test', available_gb=1)
    assert wu is not None
    assert wu.work_spec_name == 'second'
    wu.run()
    wu.finish()

    wu = task_master.get_work('test', available_gb=1)
    assert wu is None


def test_simple_output(task_master):  # noqa
    flow = {
        'first': {
            'min_gb': 1,
            'module': 'coordinate.tests.test_job_flow',
            'run_function': 'list_function',
            'then': 'second',
        },
        'second': {
            'min_gb': 1,
            'module': 'coordinate.tests.test_job_flow',
            'run_function': 'second_function',
            'run_params': ['foo'],
        },
    }
    task_master.add_flow(flow, config={})
    assert task_master.get_work_spec('first') is not None
    assert task_master.get_work_spec('second') is not None

    task_master.add_work_units('first', [('u', {'k': 'v'})])

    wu = task_master.get_work('test', available_gb=1)
    assert wu is not None
    assert wu.work_spec_name == 'first'
    wu.run()
    wu.finish()
    assert wu.data['output'] == ['u']  # that is, just a flat list

    wu = task_master.get_work('test', available_gb=1)
    assert wu is not None
    assert wu.work_spec_name == 'second'
    wu.run()
    wu.finish()
    assert wu.data['output'] == {'u': {'mode': 'foo'}}

    wu = task_master.get_work('test', available_gb=1)
    assert wu is None

def test_config_merge(task_master):  # noqa
    flow = {
        'config': {
            'min_gb': 1,
            'module': 'coordinate.tests.test_job_flow',
            'run_function': 'config_function',
            'config': {
                'will_be_added': {'present': 'yes'},
                'will_be_deleted': None,
                'will_be_replaced': {'origin': 'flow'},
                'will_be_merged': {
                    'add': 1,
                    'delete': None,
                    'replace': 5,
                }
            },
        },
    }
    config = {
        'will_be_deleted': {'present': 'yes'},
        'will_be_replaced': {'origin': 'config'},
        'will_be_kept': {'present': 'yes'},
        'will_be_merged': {
            'delete': 2,
            'replace': 3,
            'keep': 4,
        },
    }
    task_master.add_flow(flow, config=config)

    task_master.add_work_units('config', [('u', {'k': 'v'})])
    wu = task_master.get_work('test', available_gb=1)
    assert wu is not None
    assert wu.work_spec_name == 'config'
    wu.run()
    wu.finish()

    assert wu.data['output'] == {
        'will_be_added': {'present': 'yes'},
        'will_be_replaced': {'origin': 'flow'},
        'will_be_kept': {'present': 'yes'},
        'will_be_merged': {
            'add': 1,
            'keep': 4,
            'replace': 5,
        },
    }
