'''Command-line coordinated client.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import
import bz2
import gzip
try:
    # python2_7
    import backports.lzma as lzma
except:
    try:
        # python3
        import lzma
    except:
        lzma = None


def zopen(path):
    if path == '-':
        return sys.stdin
    lpath = path.lower()
    if lpath.endswith('.gz'):
        return gzip.open(path, 'rb')
    elif lpath.endswith('.bz2'):
        return bz2.BZ2File(path, 'rb')
    elif lpath.endswith('.xz'):
        assert lzma, "path ends with .xz but lzma library not available"
        return lzma.open(path, 'rb')
    else:
        return open(path, 'r')


def zopenw(path):
    if path == '-':
        return sys.stdout
    lpath = path.lower()
    # TODO: if prefix is s3: or http:, open some stream to such an interface
    if lpath.endswith('.gz'):
        return gzip.open(path, 'wb')
    elif lpath.endswith('.bz2'):
        return bz2.BZ2File(path, 'wb')
    elif lpath.endswith('.xz'):
        assert lzma, "path ends with .xz but lzma library not available"
        return lzma.open(path, 'wb')
    else:
        return open(path, 'w')
