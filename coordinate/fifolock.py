#!python
'''
Like threading.Lock() except it yields to lockers in order they tried to lock.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''

import logging
import random
import threading
import time

logger = logging.getLogger(__name__)


class FifoLock(object):
    '''Handle lockers in order they tried to lock.

    Python standard library threading.Lock() doesn't do anything about
    fairness or starvation.

    '''
    def __init__(self):
        self.outer = threading.Lock()
        self.locked = False
        self.waiters = []

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def acquire(self, blocking=True):
        with self.outer:
            cond = None
            first_pass = True
            # why a loop? because there's a gap in Condition. It's
            # possible for another request to acquire the lock while
            # this thread hasn't woken up from notify yet.
            #
            # If it is our first pass and there are waiters, go to the
            # back of the waiter queue. This makes it so we shouldn't
            # jump to the head if we happened to grab the self.outer
            # Lock between when another waiter was being notified and
            # it got to run.
            while self.locked or (first_pass and self.waiters):
                first_pass = False
                if cond is None:
                    cond = threading.Condition(self.outer)
            
                #logger.info('  waiting %s %s', id(self), id(cond))
                self.waiters.append(cond)
                #wt = time.time()
                cond.wait()
                #wt = time.time() - wt
                #logger.info('waited %s for lock %s, locked=%s', wt, id(cond), self.locked)

            assert not self.locked
            #logger.info('  locking %s', id(self))
            self.locked = True

    def release(self):
        with self.outer:
            assert self.locked
            #logger.info('UNlocking %s', id(self))
            self.locked = False
            if self.waiters:
                ## random selection
                #popi = random.randint(0, len(self.waiters) - 1)
                #next = self.waiters.pop(popi)
                ## fifo selection
                next = self.waiters.pop(0)
                #logger.info('notify %s', id(next))
                next.notify()
                # danger, notify-ee can start running immediately,
                # within that .notify() call
