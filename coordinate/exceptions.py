#!python
'''
.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''

class CoordinateException(Exception):
    pass

class ProgrammerError(CoordinateException):
    "API is getting something unxpected"
    pass

class LostLease(CoordinateException):
    "WorkUnit not owned by current client"
    pass

class NoSuchWorkUnitError(CoordinateException):
    "WorkUnit cannot be found"
    pass

class NoSuchWorkSpecError(CoordinateException):
    "work spec cannot be found"
    pass
