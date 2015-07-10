
from __future__ import absolute_import

import logging
import time
from StringIO import StringIO

import pytest

import coordinate
import coordinate.job_server
from coordinate.constants import AVAILABLE, PENDING, FINISHED, \
    RUNNABLE, PAUSED
from coordinate.job_server import SqliteWorkSpec, WorkUnit, JobQueue
from coordinate.postgres_work_spec import PostgresWorkSpec
from coordinate.tests.test_job import xconfig, NonClosingStringIO, wu1v, wu2v, wu3v, random_schema

logger = logging.getLogger(__name__)


@pytest.yield_fixture(scope='function')
def jqconfig(random_schema):
    c = {'postgres_connect': 'host=127.0.0.1 user=test dbname=test password=test', 'postgres_schema': random_schema}
    yield c
    ws = PostgresWorkSpec('', None, connect_string=c['postgres_connect'], schema=random_schema)
    ws.delete_all_storage()

def test_job_server_snapshot(monkeypatch, xconfig, jqconfig):
    monkeypatch.setattr(coordinate.job_server.time, 'time', lambda: 1.0)
    jq = JobQueue(jqconfig)

    ws1 = {'name': 'ws1'}
    jq.set_work_spec(ws1)
    jq.add_work_units('ws1', [('wu3', wu3v), ('wu2', wu2v)])

    # move wu2 to PENDING
    wu_parts, msg = jq.get_work('id2', {})
    assert wu_parts == ('ws1', 'wu2', wu2v)

    snapf = NonClosingStringIO()
    jq._snapshot(snapf, snap_path=None)

    jq = JobQueue(jqconfig)
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
