
import logging
import threading
import time


import pytest

from coordinate.fifolock import FifoLock

logger = logging.getLogger(__name__)

def _action(fl, somelist, arg, c1, c2):
    if c1 is not None:
        logger.info('%s will wait', arg)
        with c1:
            logger.info('%s got cond-mutex, waiting...', arg)
            c1.wait()
            logger.info('%s waited', arg)
    with fl:
        logger.info('%s appending', arg)
        somelist.append(arg)
        if c2 is not None:
            logger.info('%s will wait', arg)
            with c2:
                logger.info('%s got cond-mutex, waiting...', arg)
                c2.wait()
                logger.info('%s waited', arg)
        somelist.append(arg)
        logger.info('%s done', arg)


def test_fifolock():
    somelist = []

    fl = FifoLock()

    c1 = threading.Condition()
    c2 = threading.Condition()
    c3 = threading.Condition()
    c4 = threading.Condition()

    t1 = threading.Thread(target=_action, args=(fl, somelist, 1, c1, c4))
    t2 = threading.Thread(target=_action, args=(fl, somelist, 2, c2, None))
    t3 = threading.Thread(target=_action, args=(fl, somelist, 3, c3, None))

    t1.start()
    time.sleep(0.01)
    t2.start()
    time.sleep(0.01)
    t3.start()
    time.sleep(0.01)

    for xc in (c1, c2, c3, c4):
        with xc:
            logger.info('notify')
            xc.notify()
            time.sleep(0.05)

    t1.join()
    t2.join()
    t3.join()
    assert somelist == [1,1,2,2,3,3]
