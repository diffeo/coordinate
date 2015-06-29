#!python
'''
.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''

# work unit states
AVAILABLE = 1
BLOCKED = 2
PENDING = 3
FINISHED = 4
FAILED = 5

WORK_UNIT_STATUS_BY_NAME = {
    'AVAILABLE': AVAILABLE,
    'BLOCKED': BLOCKED,
    'PENDING': PENDING,
    'FINISHED': FINISHED,
    'FAILED': FAILED,
}

WORK_UNIT_STATUS_NAMES_BY_NUMBER = dict([
    (v, k) for k, v in WORK_UNIT_STATUS_BY_NAME.iteritems()
])


# work unit priorities
PRI_GENERATED = -1
PRI_STANDARD = 0
PRI_PRIORITY = 1


# work spec states
RUNNABLE = 1
PAUSED = 2

WORK_SPEC_STATUS_BY_NAME = {
    'RUNNABLE': RUNNABLE,
    'PAUSED': PAUSED,
}

WORK_SPEC_STATUS_NAMES_BY_NUMBER = dict([
    (v, k) for k, v in WORK_SPEC_STATUS_BY_NAME.iteritems()
])


# modes for the whole system or for a task queue

#: Mode constant instructing workers to do work
RUN = 'RUN'
#: Mode constant instructing workers to not start new work
IDLE = 'IDLE'
#: Mode constant instructing workers to shut down
TERMINATE = 'TERMINATE'

# You may not lease a work unit for more than one day
MAX_LEASE_SECONDS = (24 * 3600)
MIN_LEASE_SECONDS = 1
DEFAULT_LEASE_SECONDS = 300
