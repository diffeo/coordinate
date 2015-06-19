#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.
'''

import logging

from jinja2 import Template

from .constants import AVAILABLE, FINISHED, FAILED, PENDING, \
    RUNNABLE, PAUSED, \
    PRI_GENERATED, PRI_STANDARD, PRI_PRIORITY, \
    WORK_UNIT_STATUS_NAMES_BY_NUMBER

logger = logging.getLogger(__name__)

list_template = Template('''<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>coordinate daemon summary</title></head><body bgcolor="#ffffff" text="#000000">
<table>
<tr><th>work spec name</th>{% for h in statuses %}<th>{{ h[1] }}</th>{% endfor %}</tr>
{% for row in rows %}
<tr><td>{{ row.name }}</td>{% for c in row.counts %}<td><a href="?ws={{ row.name }}&wus={{ c[1] }}">{{ c[0] }}</a></td>{% endfor %}</tr>
{% endfor %}
</table>
</body></html>
''')

class JobHttpd(object):
    '''
    callable wsgi app to display JobQueue stats
    '''

    def __init__(self, jq):
        '''
        jq reference to a JobQueue instance
        '''
        self.jq = jq

    def __call__(self, environ, start_response):
        "wsgi entry point"
        return self.list(environ, start_response)
        # TODO: jq.get_work_spec(work_spec_name)
        # TODO: jq.get_work_spec_meta(work_spec_name)

    def list(self, environ, start_response):
        "wsgi compatible function, display summary list"
        # TODO: list jobs by work-spec and status; ?ws={work spec name}&wus={work unit status to filter on}
        they, _ = self.jq.list_work_specs({})
        statuses = WORK_UNIT_STATUS_NAMES_BY_NUMBER.items()
        statuses.sort()
        names = [ws['name'] for ws in they]
        names.sort()
        rows = []
        for name in names:
            counts, err = self.jq.count_work_units(name)
            statn_count = [(counts.get(kv[0], None), kv[0]) for kv in statuses]
            row = {'name': name, 'counts':statn_count}
            rows.append(row)

        status = '200 OK'
        headers = [('Content-Type', 'text/html;charset=utf-8')]
        start_response(status, headers)
        data = list_template.render(statuses=statuses, rows=rows)
        if isinstance(data, unicode):
            data = data.encode('utf8')
        logger.info('returning %s len=%s', type(data), len(data))
        return [data]
