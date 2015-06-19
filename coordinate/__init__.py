'''Centralized lock and job queue server and clients.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

:program:`coordinated` is a centralized daemon that provides a job
queue and other services for a cluster of compute systems.

:program:`coordinated` server
=============================

.. automodule:: coordinate.run

:program:`coordinate` client
=============================

.. automodule:: coordinate.coordinatec

Client APIs
===========

Base class
----------

.. automodule:: coordinate.base_client

Job server
----------

.. automodule:: coordinate.job_client

Server APIs
===========

Job server
----------

.. automodule:: coordinate.job_server

Miscellaneous
=============

.. automodule:: coordinate.constants
.. automodule:: coordinate.fifolock
.. automodule:: coordinate.workunit_run

'''
from __future__ import absolute_import

from .job_client import TaskMaster
from .job_server import JobQueue
from .server import CoordinateServer
from .exceptions import CoordinateException, ProgrammerError

# things for yakonfig
config_name = 'coordinate'
sub_modules = [JobQueue, CoordinateServer]
