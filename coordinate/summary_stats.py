#!python
#
# Utility for analyzing the output of `coordinate summary --json`

from __future__ import division
from collections import namedtuple
import json
import logging
import sys


logger = logging.getLogger(__name__)


Datum = namedtuple('Datum', ('data', 'when'))


class JsonFileSource(object):
    def __init__(self, path):
        self.path = path

    def data(self):
        "Yields Datum(data, timestamp)"
        # TODO: add a database or rotating log file class and params
        # for filter and limit
        with open(self.path, 'rb') as fin:
            for line in fin:
                if not line:
                    continue
                line = line.strip()
                if not line:
                    continue
                if line[0] == '#':
                    continue
                ob = json.loads(line)
                then = ob.pop('_NOW', None)
                if then is None:
                    continue
                yield Datum(data, then)


def datumdiff(a, b):
    at = a.get('_NOW')
    if at is None:
        logger.error('need a[_NOW]')
        return None
    bt = b.get('_NOW')
    if bt is None:
        logger.error('need b[_NOW]')
        return None

    dt = bt - at
    if not dt:
        logger.warn('zero dt. skipping')
        return None

    out = {}
    for spec, ad in a.iteritems():
        if spec == '_NOW':
            continue
        bd = b.get(spec)
        if not bd:
            logger.warn('spec %r in a but not b', spec)
            continue
        dd = {}
        keys = set(ad.keys())
        keys.update(bd.keys())
        for k in keys:
            av = float(ad.get(k, 0.0))
            bv = float(bd.get(k, 0.0))
            dv = bv - av
            dd[k] = dv / dt
        out[spec] = dd
    return out


short_names = {
    "num_available": "Avail",
    "num_failed": "Failed",
    "num_blocked": "Blocked",
    "num_finished": "Finished",
    "num_pending": "Pending",
    "num_tasks": "Total",
}


def datastr(xd):
    lines = [
#01234567890123456789 1234567 1234567 1234567 1234567 1234567 1234567
'Work spec              Avail Pending Blocked  Failed Finished  Total'
]
    specnames = sorted(xd.keys())
    for specname in specnames:
        sd = xd[specname]
        lines.append(
            '{0: <20s} {1[num_available]: >7.4f} {1[num_pending]: >7.4f} {1[num_blocked]: >7.4f} {1[num_failed]: >7.4f} {1[num_finished]: >7.4f} {1[num_tasks]: >7.4f}'.format(specname, sd))
    return '\n'.join(lines)


def xread(input, out):
    a = None
    b = None
    for line in input:
        if not line:
            continue
        line = line.strip()
        if not line:
            continue
        if line[0] == '#':
            continue
        ob = json.loads(line)
        a = b
        b = ob
        if a is not None:
            diff = datumdiff(a, b)
            if diff:
                out.write(datastr(diff) + '\n')


if __name__ == '__main__':
    xread(sys.stdin, sys.stdout)
