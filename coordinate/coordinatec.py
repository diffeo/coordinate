'''Command-line coordinated client.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import, print_function
import argparse
import collections
from importlib import import_module
import json
import logging
import os
import pprint
import sys
import time

import cbor
import yaml

import coordinate
from coordinate.exceptions import NoSuchWorkSpecError, NoSuchWorkUnitError
from coordinate.job_client import TaskMaster
from coordinate.workers import SingleWorker
import dblogger
import yakonfig
from yakonfig.cmd import ArgParseCmd


logger = logging.getLogger(__name__)


class CoordinateC(ArgParseCmd):
    def __init__(self):
        ArgParseCmd.__init__(self)
        self.prompt = 'coordinatec> '
        self.client = None
        self.exitcode = 0
        #self.job_client = None # TaskMaster instance
        self._config = None
        self._task_master = None

    @property
    def config(self):
        if self._config is None:
            self._config = yakonfig.get_global_config('coordinate')
        return self._config

    @property
    def task_master(self):
        """A `TaskMaster` object for manipulating work"""
        if self._task_master is None:
            self._task_master = TaskMaster(config=self.config)
        return self._task_master

    def _add_work_spec_args(self, parser):
        '''Add ``--work-spec`` to an :mod:`argparse` `parser`.'''
        parser.add_argument('-w', '--work-spec', dest='work_spec_path',
                            metavar='FILE', type=existing_path,
                            required=True,
                            help='path to a YAML or JSON file')
    def _get_work_spec(self, args):
        '''Get the contents of the work spec from the arguments.'''
        with open(args.work_spec_path) as f:
            return yaml.load(f)

    def _add_work_spec_name_args(self, parser):
        '''Add either ``--work-spec`` or ``--work-spec-name`` to an
        :mod:`argparse` `parser`.'''
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('-w', '--work-spec', dest='work_spec_path',
                           metavar='FILE', type=existing_path,
                           help='path to a YAML or JSON file')
        group.add_argument('-W', '--work-spec-name', metavar='NAME',
                           help='name of a work spec for queries')

    def _get_work_spec_name(self, args):
        '''Get the name of the work spec from the arguments.

        This assumes :meth:`_add_work_spec_name_args` has been called,
        but will also work if just :meth:`_add_work_spec_args` was
        called instead.

        '''
        if getattr(args, 'work_spec_name', None):
            return args.work_spec_name
        return self._get_work_spec(args)['name']

    def _read_work_units_file(self, work_units_fh):
        '''work_units_fh is iterable on lines (could actually be ['{}',...])
        '''
        count = 0
        for line in work_units_fh:
            if not line:
                continue
            line = line.strip()
            if not line:
                continue
            if line[0] == '#':
                continue
            try:
                count += 1
                work_unit = json.loads(line)
                #work_units.update(work_unit)
                for k,v in work_unit.iteritems():
                    yield k,v
            except:
                logger.error('failed handling work_unit on line %s: %r', count, line, exc_info=True)
                raise

    def _work_units_fh_from_path(self, work_units_path):
        work_units_fh = None
        if work_units_path == '-':
            work_units_fh = sys.stdin
        elif work_units_path is not None:
            if work_units_path.endswith('.gz'):
                work_units_fh = gzip.open(work_units_path)
            else:
                work_units_fh = open(work_units_path)
        return work_units_fh

    def main(self, args):
        return ArgParseCmd.main(self, args)

    def args_rpc(self, parser):
        parser.add_argument('method', help='RPC to call')
        parser.add_argument('param', nargs='*',
                            help='YAML-syntax parameters')

    def do_rpc(self, args):
        '''Directly call a procedure on the server.'''
        params = [yaml.load(param) for param in args.param]
        try:
            ret = self.task_master._rpc(args.method, params)
            self.stdout.write(repr(ret) + '\n')
        except Exception, exc:
            self.stdout.write(repr(exc) + '\n')

    def args_flow(self, parser):
        parser.add_argument('-c', '--config', metavar='FILE',
                            help='alternate configuration file')
        parser.add_argument('file', help='YAML file describing flow')

    def do_flow(self, args):
        "Load a flow yaml describing a set of work specs"
        with open(args.file, 'r') as f:
            flow_file = yaml.load(f)
        mods = flow_file.get('yakonfig_modules', ['yakonfig', 'coordinate'])
        mods = [import_module(name) for name in mods]

        # This appears to have the semantics that, if filename is not
        # None, load it, and otherwise use the passed-in config.
        # Load and extract this config, but then actually run the
        # job load under our normal config.
        with yakonfig.defaulted_config(
                mods,
                filename=args.config,
                config=yakonfig.get_global_config()) as c:
            config = c

        self.task_master.add_flow(flow_file['flows'], config=config)

    def args_load(self, parser):
        self._add_work_spec_args(parser)
        parser.add_argument('-n', '--nice', default=0, type=int,
                            help='specify a nice level for these jobs')
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('-u', '--work-units', metavar='FILE',
                           dest='work_units_path',
                           type=existing_path_or_minus,
                           help='path to file with one JSON record per line')
        group.add_argument('--no-work', default=False, action='store_true',
                           help='set no work units, just the work spec')

    def do_load(self, args):
        '''loads a work spec and some work_units into it

        --work-units file is JSON, one work unit per line, each line a
        JSON dict with the one key being the work unit key and the
        value being a data dict for the work unit.

        '''
        work_spec = self._get_work_spec(args)
        work_spec['nice'] = args.nice
        self.task_master.set_work_spec(work_spec)

        if args.no_work:
            work_units_fh = []  # it just has to be an iterable with no lines
        else:
            work_units_fh = self._work_units_fh_from_path(args.work_units_path)
            if work_units_fh is None:
                raise RuntimeError('need -u/--work-units or --no-work')
        self.stdout.write('loading work units from {0!r}\n'
                          .format(work_units_fh))
        work_units = dict(self._read_work_units_file(work_units_fh))

        if work_units:
            self.stdout.write('pushing work units\n')
            self.task_master.add_work_units(work_spec['name'], work_units.items())
            self.stdout.write('finished writing {0} work units to work_spec={1!r}\n'
                              .format(len(work_units), work_spec['name']))
        else:
            self.stdout.write('no work units. done.\n')

    def args_addwork(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('-u', '--work-units', metavar='FILE',
                            dest='work_units_path',
                            type=existing_path_or_minus,
                            help='path to file with one JSON record per line')

    def do_addwork(self, args):
        '''Add work units to a work spec's queue.
        Input file is JSON records one per line.
        {'work unit key':{any additional work unit data ...}}
        '''
        work_spec_name = self._get_work_spec_name(args)
        work_units_fh = self._work_units_fh_from_path(args.work_units_path)
        work_units = [kv for kv in self._read_work_units_file(work_units_fh)]
        if work_units_fh is None:
            raise RuntimeError('need -u/--work-units')

        self.task_master.add_work_units(work_spec_name, work_units)

    # def args_add_work_unit(self, parser):
    #     parser.add_argument('spec', help='work spec to update')
    #     parser.add_argument('unit', help='work unit(s) to add (YAML syntax)')

    # def do_add_work_unit(self, args):
    #     units = yaml.load(args.unit)
    #     if isinstance(units, collections.Mapping):
    #         units = units.items()
    #     if isinstance(units, basestring):
    #         units = [(units, {})]

    #     def pairify(x):
    #         if isinstance(x, basestring):
    #             return [x, {}]
    #         return x

    #     units = [pairify(name) for name in units]
    #     self.task_master.add_work_units(args.spec, units)
    #     print('added %d work units' % len(units))

    def args_pause(self, parser):
        parser.add_argument('spec', help='work spec to pause')

    def do_pause(self, args):
        '''Pause a work spec's queue.
        No work units will be handed out to workers.'''
        self.task_master.pause_work_spec(args.spec, paused=True)

    def args_resume(self, parser):
        parser.add_argument('spec', help='work spec to unpause')

    def do_resume(self, args):
        '''Resume a paused work spec's queue.
        Work units from that work spec will again be handed out to workers.'''
        self.task_master.pause_work_spec(args.spec, paused=False)

    def args_prioritize(self, parser):
        parser.add_argument('spec', help='work spec to modify')
        parser.add_argument('unit', nargs='+',
                            help='work unit(s) to prioritize')

    def do_prioritize(self, args):
        '''Move work units to the front of a work spec queue.'''
        self.task_master.prioritize_work_units(args.spec, args.unit)


    def args_config(self, parser):
        parser.add_argument('-o', '--output', metavar='FILE',
                            help='write config to FILE')

    def do_config(self, args):
        '''Retrieve the server's global configuration.'''
        config = self.task_master.get_server_config()
        if args.output:
            with open(args.output, 'w') as f:
                yaml.dump(config, stream=f)
        else:
            yaml.dump(config, stream=self.stdout)

    def args_status(self, parser):
        parser.add_argument('--logfile', help='file to log statuses to')
        parser.add_argument('--repeat-seconds', type=float, default=None, help='get status from server repeatedly every N seconds')

    def do_status(self, args):
        logfile = None
        try:
            while True:
                they = []
                for ws in self.task_master.iter_work_specs():
                    name = ws['name']
                    counts = self.task_master.count_work_units(name)
                    they.append({'name': name, 'data': ws, 'counts': counts})
                they.sort(key=lambda x:x['name'])
                if args.logfile:
                    record = {'time': time.time(), 'ws': they}
                    if logfile is None:
                        logfile = open(args.logfile, 'ab')
                    cbor.dump(record, logfile)
                    logfile.flush()
                else:
                    # write json text to stdout
                    self.stdout.write(json.dumps(they) + '\n')
                    self.stdout.flush()
                if (args.repeat_seconds is None) or not (args.repeat_seconds > 0.0):
                    break
                time.sleep(args.repeat_seconds)
        finally:
            if logfile is not None:
                logfile.close()

    def args_worker_stats(self, parser):
        pass
    def do_worker_stats(self, args):
        wstats = self.task_master._rpc('worker_stats', [])
        self.stdout.write('{!r}\n'.format(wstats))

    def args_delete(self, parser):
        parser.add_argument('-y', '--yes', default=False, action='store_true',
                            dest='assume_yes',
                            help='assume "yes" and require no input for '
                            'confirmation questions.')
    def do_delete(self, args):
        '''delete the entire contents of the current coordinate environment'''
        if not args.assume_yes:
            response = raw_input('Delete everything in the current coordinate environment?')
            if response.lower() not in ('y', 'yes'):
                self.stdout.write('not deleting anything\n')
                return
        self.stdout.write('deleting all\n')
        self.task_master.clear()

    def args_work_specs(self, parser):
        pass
    def do_work_specs(self, args):
        '''print the names of all of the work specs'''
        work_spec_names = [x['name'] for x in self.task_master.iter_work_specs()]
        work_spec_names.sort()
        for name in work_spec_names:
            self.stdout.write('{0}\n'.format(name))

    def args_work_spec(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('--json', default=False, action='store_true',
                            help='write output in json')
        parser.add_argument('--yaml', default=False, action='store_true',
                            help='write output in yaml (default)')
    def do_work_spec(self, args):
        '''dump the contents of an existing work spec'''
        work_spec_name = self._get_work_spec_name(args)
        spec = self.task_master.get_work_spec(work_spec_name)
        if args.json:
            self.stdout.write(json.dumps(spec, indent=4, sort_keys=True) +
                              '\n')
        else:
            yaml.safe_dump(spec, self.stdout)

    def args_status(self, parser):
        self._add_work_spec_name_args(parser)

    def do_status(self, args):
        '''print the number of work units in an existing work spec'''
        work_spec_name = self._get_work_spec_name(args)
        status = self.task_master.status(work_spec_name)
        self.stdout.write(json.dumps(status, indent=4, sort_keys=True) +
                          '\n')

    def args_summary(self, parser):
        parser.add_argument('--json', default=False, action='store_true',
                            help='write output in json')
        parser.add_argument('--text', default=None, action='store_true',
                            help='write output in text')
    def do_summary(self, args):
        '''print a summary of running work'''
        assert args.json or args.text or (args.text is None)
        do_text = args.text

        xd = {}
        for ws in self.task_master.iter_work_specs():
            name = ws['name']
            status = self.task_master.status(name)
            xd[name] = status
        xd['_NOW'] = time.time()

        if args.json:
            self.stdout.write(json.dumps(xd) + '\n')
        else:
            if do_text is None:
                do_text = True

        if do_text:
            self.stdout.write('Work spec                             Avail  Pending  Blocked'
                              '   Failed Finished    Total\n')
            self.stdout.write('================================== ======== ======== ========'
                              ' ======== ======== ========\n')
            for name in sorted(xd.keys()):
                if name == '_NOW':
                    continue
                status = xd[name]
                self.stdout.write('{0:30s} {1[num_available]:12d} '
                                  '{1[num_pending]:8d} {1[num_blocked]:8d} '
                                  '{1[num_failed]:8d} {1[num_finished]:8d} '
                                  '{1[num_tasks]:8d}\n'.format(name, status))

    def args_work_units(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('-n', '--limit', type=int, metavar='N',
                            help='only print N work units')
        parser.add_argument('-s', '--status',
                            choices=['available', 'pending', 'blocked',
                                     'finished', 'failed'],
                            help='print work units in STATUS')
        parser.add_argument('--details', action='store_true',
                            help='also print the contents of the work units')

    def do_work_units(self, args):
        '''list work units that have not yet completed'''
        work_spec_name = self._get_work_spec_name(args)
        if args.status:
            status = args.status.upper()
            statusi = getattr(self.task_master, status, None)
            if statusi is None:
                self.stdout.write('unknown status {0!r}\n'.format(args.status))
                return
        else:
            statusi = None
        work_units = dict(self.task_master.get_work_units(
            work_spec_name, state=statusi, limit=args.limit))
        work_unit_names = sorted(work_units.keys())
        if args.limit:
            work_unit_names = work_unit_names[:args.limit]
        for k in work_unit_names:
            if args.details:
                wu_data = work_units[k]
                if not wu_data:
                    self.stdout.write('{}: no info\n'.format(k))
                    continue

                tback = wu_data.get('traceback', '')
                if tback:
                    tback += '\n'
                    work_units[k]['traceback'] = 'displayed below'
                self.stdout.write(
                    '{0!r}: {1}\n{2}'
                    .format(k, pprint.pformat(work_units[k], indent=4),
                            tback))
            else:
                self.stdout.write('{0}\n'.format(k))

    def args_work_unit(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('unit', nargs='*',
                            help='work unit name(s)')

    def do_work_unit(self, args):
        '''print basic details about work units'''
        work_spec_name = self._get_work_spec_name(args)
        for work_unit_name in args.unit:
            status = self.task_master.get_work_unit_status(work_spec_name,
                                                           work_unit_name)
            self.stdout.write('{0} ({1!r})\n'
                              .format(work_unit_name, status['status']))
            if 'expiration' in status:
                when = time.ctime(status['expiration'])
                if status == 'available':
                    if status['expiration'] == 0:
                        self.stdout.write('  Never scheduled\n')
                    else:
                        self.stdout.write('  Available since: {0}\n'
                                          .format(when))
                else:
                    self.stdout.write('  Expires: {0}\n'.format(when))
            if 'worker_id' in status:
                try:
                    heartbeat = self.task_master.get_heartbeat(status['worker_id'])
                except:
                    heartbeat = None
                if heartbeat:
                    hostname = (heartbeat.get('fqdn', None) or
                                heartbeat.get('hostname', None) or
                                '')
                    ipaddrs = ', '.join(heartbeat.get('ipaddrs', ()))
                    if hostname and ipaddrs:
                        summary = '{0} on {1}'.format(hostname, ipaddrs)
                    else:
                        summary = hostname + ipaddrs
                else:
                    summary = 'No information'
                self.stdout.write('  Worker: {0} ({1})\n'.format(
                    status['worker_id'], summary))
            if 'traceback' in status:
                self.stdout.write('  Traceback:\n{0}\n'.format(
                    status['traceback']))
            if 'depends_on' in status:
                self.stdout.write('  Depends on:\n')
                for what in status['depends_on']:
                    self.stdout.write('    {0!r}\n'.format(what))

    def args_retry(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('-a', '--all', action='store_true',
                            help='retry all failed jobs')
        parser.add_argument('unit', nargs='*',
                            help='work unit name(s) to retry')
    def do_retry(self, args):
        '''retry a specific failed job'''
        work_spec_name = self._get_work_spec_name(args)
        retried = 0
        complained = False
        try:
            if args.all:
                while True:
                    units = self.task_master.get_work_units(
                        work_spec_name, limit=1000,
                        state=self.task_master.FAILED)
                    units = [u[0] for u in units]  # just need wu key
                    if not units: break
                    try:
                        self.task_master.retry(work_spec_name, *units)
                        retried += len(units)
                    except NoSuchWorkUnitError, e:
                        # Because of this sequence, this probably means
                        # something else retried the work unit.  If we
                        # try again, we shouldn't see it in the failed
                        # list...so whatever
                        pass
            else:
                units = args.unit
                try:
                    self.task_master.retry(work_spec_name, *units)
                    retried += len(units)
                except NoSuchWorkUnitError, e:
                    unit = e.work_unit_name
                    self.stdout.write('No such failed work unit {0!r}.\n'
                                      .format(unit))
                    complained = True
                    units.remove(unit)
                    # and try again
        except NoSuchWorkSpecError, e:
            # NB: you are not guaranteed to get this, especially with --all
            self.stdout.write('Invalid work spec {0!r}.\n'
                              .format(work_spec_name))
            return
        if retried == 0 and not complained:
            self.stdout.write('Nothing to do.\n')
        elif retried == 1:
            self.stdout.write('Retried {0} work unit.\n'.format(retried))
        elif retried > 1:
            self.stdout.write('Retried {0} work units.\n'.format(retried))

    def args_clear(self, parser):
        self._add_work_spec_name_args(parser)
        parser.add_argument('-s', '--status',
                            choices=['available', 'pending', 'blocked',
                                     'finished', 'failed'],
                            help='print work units in STATUS')
        parser.add_argument('unit', nargs='*',
                            help='work unit name(s) to remove')
    def do_clear(self, args):
        '''remove work units from a work spec'''
        # Which units?
        work_spec_name = self._get_work_spec_name(args)
        units = args.unit or None
        # What to do?
        count = 0
        if args.status is None:
            all = units is None
            count += self.task_master.del_work_units(work_spec_name, work_unit_keys=units, all=all)
        elif args.status == 'available':
            count += self.task_master.del_work_units(
                work_spec_name, work_unit_keys=units, state=self.task_master.AVAILABLE)
        elif args.status == 'pending':
            count += self.task_master.del_work_units(
                work_spec_name, work_unit_keys=units, state=self.task_master.PENDING)
        elif args.status == 'blocked':
            count += self.task_master.del_work_units(
                work_spec_name, work_unit_keys=units, state=self.task_master.BLOCKED)
        elif args.status == 'finished':
            count += self.task_master.del_work_units(
                work_spec_name, work_unit_keys=units, state=self.task_master.FINISHED)
        elif args.status == 'failed':
            count += self.task_master.del_work_units(
                work_spec_name, work_unit_keys=units, state=self.task_master.FAILED)
        self.stdout.write('Removed {0} work units.\n'.format(count))

    def args_workers(self, parser):
        parser.add_argument('--all', action='store_true',
                            help='list all workers (even dead ones)')
        parser.add_argument('--details', action='store_true',
                            help='include more details if available')
    def do_workers(self, args):
        '''list all known workers'''
        workers = self.task_master.workers(alive=not args.all)
        for k in sorted(workers.iterkeys()):
            self.stdout.write('{0} ({1})\n'.format(k, workers[k]))
            if args.details:
                heartbeat = self.task_master.get_heartbeat(k)
                for hk, hv in heartbeat.iteritems():
                    self.stdout.write('  {0}: {1}\n'.format(hk, hv))

    def args_run_one(self, parser):
        parser.add_argument('--from-work-spec', action='append', default=[], help='workspec name to accept work from, may be repeated.')
        parser.add_argument('--limit-seconds', default=None, type=int, metavar='N', help='stop after running for N seconds')
        parser.add_argument('--limit-count', default=None, type=int, metavar='N', help='stop after running for N work units')
        parser.add_argument('--max-jobs', default=None, type=int, metavar='N', help='fetch up to N work units at once')
    def do_run_one(self, args):
        '''run a single job'''
        work_spec_names = args.from_work_spec or None
        worker = SingleWorker(self.config, task_master=self.task_master, work_spec_names=work_spec_names, max_jobs=args.max_jobs)
        worker.register()
        rc = False
        starttime = time.time()
        count = 0
        try:
            while True:
                rc = worker.run()
                if not rc:
                    break
                count += 1
                if (args.limit_seconds is None) and (args.limit_count is None):
                    # only do one
                    break
                if (args.limit_seconds is not None) and ((time.time() - starttime) >= args.limit_seconds):
                    break
                if (args.limit_count is not None) and (count >= args.limit_count):
                    break
        finally:
            worker.unregister()
        if not rc:
            self.exitcode = 2

# utils for arg parsing
def existing_path(string):
    '''"Convert" a string to a string that is a path to an existing file.'''
    if not os.path.exists(string):
        msg = 'path {0!r} does not exist'.format(string)
        raise argparse.ArgumentTypeError(msg)
    return string

def existing_path_or_minus(string):
    '''"Convert" a string like :func:`existing_path`, but also accept "-".'''
    if string == '-': return string
    return existing_path(string)



def main():
    parser = argparse.ArgumentParser()
    app = CoordinateC()
    app.add_arguments(parser)
    args = yakonfig.parse_args(parser, [yakonfig, dblogger, coordinate])
    app.main(args)
    sys.exit(app.exitcode)

if __name__ == '__main__':
    main()
