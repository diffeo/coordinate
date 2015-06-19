'''Dirty mechanics of running a work unit.

.. Your use of this software is governed by your license agreement.
   Copyright 2014 Diffeo, Inc.

.. autofunction:: run
.. autofunction:: run_module_func_for_work_unit

'''
from __future__ import absolute_import
import collections
import logging

from coordinate.exceptions import LostLease, ProgrammerError


logger = logging.getLogger(__name__)


def run(work_unit, work_spec_dict):
    '''Run a work unit given its :mod:`coordinate` metadata.

    `work_spec_dict` must have keys `module` and `run_function`,
    and it may optionally also have `run_params`.  This information
    is extracted and passed to :func:`run_module_func_for_work_unit`.

    :param work_unit: work unit to run
    :type work_unit: :class:`coordinate.job_client.WorkUnit`
    :param dict work_spec_dict: work spec
    :raises coordinate.exceptions.ProgrammerError: if the required
      metadata is missing

    '''
    module_name = work_spec_dict.get('module')
    if module_name is None:
        # ProgrammerError feels wrong here, but we should probably trap
        # invalid work specs earlier; but if you're not using a standard
        # worker it doesn't actually matter
        raise ProgrammerError('work spec {!r} does not specify a "module" '
                              'to run from'
                              .format(work_spec_dict.get('name')))
    run_function_name = work_spec_dict.get('run_function')
    if run_function_name is None:
        raise ProgrammerError('work spec {!r} does not specify a '
                              '"run_function"'
                              .format(work_spec_dict.get('name')))
    run_params = work_spec_dict.get('run_params')  # may be None

    return run_module_func_for_work_unit(work_unit, module_name,
                                         run_function_name, run_params)


def run_module_func_for_work_unit(work_unit, module_name, func_name,
                                  params=None):
    '''Import and run a function for a given work unit.

    `module_name` is imported and `func_name` looked up within that.
    That function is run with `work_unit` as a single parameter.  If
    `params` is a list, the function is called with `*params`; if
    `params` is a dictionary, with `**params`.

    The function generally should not call
    :meth:`~coordinate._task_master.WorkUnit.finish` or
    :meth:`~coordinate._task_master.WorkUnit.fail` on the work unit; the
    worker system will call these on successful completion or an
    exception.  However, it may call
    :meth:`~coordinate._task_master.WorkUnit.update` and update
    :attr:`~coordinate._task_master.WorkUnit.data` on the work unit to
    show progress.

    :param work_unit: work unit to run
    :type work_unit: :class:`coordinate.job_client.WorkUnit`
    :param str module_name: name of module to import
    :param str func_name: name of function to run
    :param params: additional parameters to the function
    :raises exceptions.ImportError: if `module_name` cannot be loaded
    :raises exceptions.AttributeError: if `func_name` is not present
      in `module_name`

    '''
    xmodule = __import__(module_name, globals(), (), (func_name,), -1)
    run_function = getattr(xmodule, func_name)
    args = []
    kwargs = {}
    if isinstance(params, collections.Sequence):
        args = params
    elif isinstance(params, collections.Mapping):
        kwargs = params

    logger.info('running work unit %s:%r', work_unit.work_spec_name, work_unit.key)
    logger.debug('calling %s.%s(%r, *%r, **%r)', module_name, func_name,
                 work_unit, args, kwargs)
    try:
        ret_val = run_function(work_unit, *args, **kwargs)
        work_unit.finish()
        logger.info('finished work unit %r', work_unit.key)
        return ret_val
    except LostLease:
        logger.warning('work unit %r timed out', work_unit.key)
        raise
    except Exception:
        logger.error('work unit %r failed', work_unit.key, exc_info=True)
        raise
