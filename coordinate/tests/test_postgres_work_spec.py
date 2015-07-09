
from __future__ import absolute_import

import logging
import time

import coordinate
import coordinate.job_server
from coordinate.constants import AVAILABLE, PENDING, FINISHED, \
    RUNNABLE, PAUSED
from coordinate.job_server import SqliteWorkSpec, WorkUnit, JobQueue
from coordinate.postgres_work_spec import PostgresWorkSpec


def test_nop():
    pass
