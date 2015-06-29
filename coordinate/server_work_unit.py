'''Command-line coordinated client.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import

from .constants import AVAILABLE, PRI_STANDARD

def safe_get(gettable, key, default=None):
    # get, as if from a dictionary, but the dict itself may be None
    if gettable is None:
        return default
    return gettable.get(key, default)


class WorkUnit(object):
    '''Server-side state for an individual work unit.'''

    def __init__(self, key, data, meta=None, priority=None):
        # core data

        assert isinstance(key, basestring), 'bad WorkUnit key={!r} ({}) data={!r} meta={!r}'.format(key, type(key), data, meta)
        #: String key for this work unit
        self.key = key

        #: Dictionary data for this work unit
        self.data = data

        # metadata
        self.status = AVAILABLE
        self.priority = priority
        if self.priority is None:
            self.priority = safe_get(meta, 'priority', PRI_STANDARD)
        self.lease_time = None
        self.worker_id = None

    def __repr__(self):
        return ('WorkUnit(key={0.key!r}, data={0.data!r}, '
                'status={0.status!r}, priority={0.priority!r}, '
                'lease_time={0.lease_time!r}, worker_id={0.worker_id!r})'
                .format(self))

    def __lt__(self, other):
        "used by heapq -- smallest item is first in queue"
        if self.priority > other.priority:
            return True
        if self.priority < other.priority:
            return False
        return self.key < other.key
