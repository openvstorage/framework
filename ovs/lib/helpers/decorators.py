# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
Contains various decorators
"""

import os
import json
import time
import random
import string
import inspect
import logging
import threading
from functools import wraps
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.lib.helpers.exceptions import EnsureSingleTimeoutReached

ENSURE_SINGLE_KEY = 'ovs_ensure_single'


class Decorators(object):
    """
    Decorators class currently only used by the unittests
    """
    # Keep individual state for each thread in here
    unittest_thread_info_by_name = {}
    # Keep order in which threads enter certain states
    unittest_thread_info_by_state = {'WAITING': [],
                                     'FINISHED': []}

    @staticmethod
    def _clean():
        Decorators.unittest_thread_info_by_name = {}
        Decorators.unittest_thread_info_by_state = {'WAITING': [],
                                                    'FINISHED': []}


def log(event_type):
    """
    Task logger
    :param event_type: Event type
    :return: Pointer to function
    """
    def wrap(f):
        """
        Wrapper function
        :param f: Function to log something about
        """
        @wraps(f)
        def new_function(*args, **kwargs):
            """
            Wrapped function
            :param args: Arguments without default values
            :param kwargs: Arguments with default values
            """
            # Log the call
            if event_type == 'VOLUMEDRIVER_TASK' and 'storagedriver_id' in kwargs:
                metadata = {'storagedriver': StorageDriverList.get_by_storagedriver_id(kwargs['storagedriver_id']).guid}
            else:
                metadata = {}
            _logger = logging.getLogger(event_type.lower())
            _logger.info('[{0}.{1}] - {2} - {3} - {4}'.format(
                f.__module__,
                f.__name__,
                json.dumps(list(args)),
                json.dumps(kwargs),
                json.dumps(metadata)
            ))

            # Call the function
            return f(*args, **kwargs)

        new_function.__name__ = f.__name__
        new_function.__module__ = f.__module__
        return new_function

    return wrap


def ovs_task(**kwargs):
    """
    Decorator to execute celery tasks in OVS
    These tasks can be wrapped additionally in the ensure single decorator
    """
    def wrapper(f):
        """
        Wrapper function
        """
        from ovs.celery_run import celery

        ensure_single_info = kwargs.pop('ensure_single_info', {})
        if ensure_single_info != {}:
            f = _ensure_single(task_name=kwargs['name'], **ensure_single_info)(f)
            kwargs['bind'] = True
        return celery.task(**kwargs)(f)
    return wrapper


def _ensure_single(task_name, mode, extra_task_names=None, global_timeout=300, callback=None):
    """
    Decorator ensuring a new task cannot be started in case a certain task is
    running, scheduled or reserved.

    Keep also in mind that validation will be executed by the worker itself, so if the task is scheduled on
    a worker currently processing a "duplicate" task, it will only get validated after the first
    one completes, which will result in the fact that the task will execute normally.

    Allowed modes:
     - DEFAULT: De-duplication based on the task's name. If any new task with the same name is scheduled it will be
                discarded
     - DEDUPED: De-duplication based on the task's name and arguments. If a new task with the same name and arguments
                is scheduled while the first one is currently being executed, it will be allowed on the queue (to make
                sure there will be at least one new execution). All subsequent identical tasks will be discarded.
                 - Tasks with identical arguments will be executed in serial (Subsequent tasks with same params will be discarded if 1 waiting task with these params already in queue)
                 - Tasks with different arguments will be executed in parallel
     - CHAINED: Identical as DEDUPED with the exception that tasks will be executed in serial.
                 - Tasks with identical arguments will be executed in serial (Subsequent tasks with same params will be discarded if 1 waiting task with these params already in queue)
                 - Tasks with different arguments will be executed in serial

    :param task_name: Name of the task to ensure its singularity
    :type task_name: str
    :param extra_task_names: Extra tasks to take into account
    :type extra_task_names: list
    :param mode: Mode of the ensure single. Allowed values: DEFAULT, CHAINED
    :type mode: str
    :param global_timeout: Timeout before raising error (Only applicable in CHAINED mode)
    :type global_timeout: int
    :param callback: Call back function which will be executed if identical task in progress
    :type callback: func
    :return: Pointer to function
    :rtype: func
    """
    logger = logging.getLogger(__name__)

    def wrap(f):
        """
        Wrapper function
        :param f: Function to check
        """
        @wraps(f)
        def new_function(self, *args, **kwargs):
            """
            Wrapped function
            :param self: With bind=True, the celery task result itself is passed in
            :param args: Arguments without default values
            :param kwargs: Arguments with default values
            """
            def log_message(message, level='info'):
                """
                Log a message with some additional information
                :param message: Message to log
                :param level:   Log level
                :return:        None
                """
                if level not in ('info', 'warning', 'debug', 'error', 'exception'):
                    raise ValueError('Unsupported log level "{0}" specified'.format(level))
                if unittest_mode is False:
                    complete_message = 'Ensure single {0} mode - ID {1} - {2}'.format(mode, now, message)
                else:
                    complete_message = 'Ensure single {0} mode - ID {1} - {2} - {3}'.format(mode, now, threading.current_thread().getName(), message)
                getattr(logger, level)(complete_message)

            def update_value(key, append, value_to_update=None):
                """
                Store the specified value in the PersistentFactory
                :param key:             Key to store the value for
                :param append:          If True, the specified value will be appended else element at index 0 will be popped
                :param value_to_update: Value to append to the list or remove from the list
                :return:                Updated value
                """
                with volatile_mutex(name=key, wait=5):
                    vals = list(persistent_client.get_multi([key], must_exist=False))
                    if vals[0] is not None:
                        val = vals[0]
                        if append is True and value_to_update is not None:
                            val['values'].append(value_to_update)
                        elif append is False and value_to_update is not None:
                            for value_item in val['values']:
                                if value_item == value_to_update:
                                    val['values'].remove(value_item)
                                    break
                        elif append is False and len(val['values']) > 0:
                            val['values'].pop(0)
                    else:
                        log_message('Setting initial value for key {0}'.format(key))
                        val = {'mode': mode,
                               'values': []}
                    persistent_client.set(key, val)
                return val

            if not hasattr(self, 'request'):
                raise RuntimeError('The decorator ensure_single can only be applied to bound tasks (with bind=True argument)')

            now = '{0}_{1}'.format(int(time.time()), ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(10)))
            task_id = self.request.id
            async_task = task_id is not None  # Async tasks have an ID, inline executed tasks have None as ID
            task_names = [task_name] if extra_task_names is None else [task_name] + extra_task_names
            thread_name = threading.current_thread().getName()
            unittest_mode = os.environ.get('RUNNING_UNITTESTS') == 'True'
            persistent_key = '{0}_{1}'.format(ENSURE_SINGLE_KEY, task_name)
            persistent_client = PersistentFactory.get_client()

            if mode == 'DEFAULT':
                with volatile_mutex(persistent_key, wait=5):
                    for task in task_names:
                        key_to_check = '{0}_{1}'.format(ENSURE_SINGLE_KEY, task)
                        if persistent_client.exists(key_to_check):
                            if async_task is True or callback is None:
                                log_message('Execution of task {0} discarded'.format(task_name))
                                if unittest_mode is True:
                                    Decorators.unittest_thread_info_by_name[thread_name] = ('DISCARDED', None)
                                return None
                            else:
                                log_message('Execution of task {0} in progress, executing callback function'.format(task_name))
                                if unittest_mode is True:
                                    Decorators.unittest_thread_info_by_name[thread_name] = ('CALLBACK', None)
                                return callback(*args, **kwargs)

                    log_message('Setting key {0}'.format(persistent_key))
                    persistent_client.set(persistent_key, {'mode': mode,
                                                           'values': [{'task_id': task_id}]})

                try:
                    if unittest_mode is True:
                        Decorators.unittest_thread_info_by_name[thread_name] = ('EXECUTING', None)
                    output = f(*args, **kwargs)
                    if unittest_mode is True:
                        Decorators.unittest_thread_info_by_name[thread_name] = ('FINISHED', None)
                        Decorators.unittest_thread_info_by_state['FINISHED'].append(thread_name)
                    log_message('Task {0} finished successfully'.format(task_name))
                    return output
                finally:
                    with volatile_mutex(persistent_key, wait=5):
                        log_message('Deleting key {0}'.format(persistent_key))
                        persistent_client.delete(persistent_key, must_exist=False)

            elif mode == 'DEDUPED':
                if extra_task_names is not None:
                    log_message('Extra tasks are not allowed in this mode',
                                level='error')
                    raise ValueError('Ensure single {0} mode - ID {1} - Extra tasks are not allowed in this mode'.format(mode, now))

                # Update kwargs with args
                sleep = 1 if unittest_mode is False else 0.1
                timeout = kwargs.pop('ensure_single_timeout', 10 if unittest_mode is True else global_timeout)
                function_info = inspect.getargspec(f)
                kwargs_dict = {}
                for index, arg in enumerate(args):
                    kwargs_dict[function_info.args[index]] = arg
                kwargs_dict.update(kwargs)
                params_info = 'with params {0}'.format(kwargs_dict) if kwargs_dict else 'with default params'

                # Set the key in arakoon if non-existent
                value = update_value(key=persistent_key,
                                     append=True)

                # Validate whether another job with same params is being executed
                job_counter = 0
                for item in value['values']:
                    if item['kwargs'] == kwargs_dict:
                        job_counter += 1
                        if job_counter == 2:  # 1st job with same params is being executed, 2nd is scheduled for execution ==> Discard current
                            if async_task is True:  # Not waiting for other jobs to finish since asynchronously
                                log_message('Execution of task {0} {1} discarded because of identical parameters'.format(task_name, params_info))
                                if unittest_mode is True:
                                    Decorators.unittest_thread_info_by_name[thread_name] = ('DISCARDED', None)
                                return None

                            # If executed inline (sync), execute callback if any provided
                            if callback is not None:
                                log_message('Execution of task {0} {1} in progress, executing callback function'.format(task_name, params_info))
                                if unittest_mode is True:
                                    Decorators.unittest_thread_info_by_name[thread_name] = ('CALLBACK', None)
                                return callback(*args, **kwargs)

                            # Let's wait for 2nd job in queue to have finished if no callback provided
                            slept = 0
                            while slept < timeout:
                                log_message('Task {0} {1} is waiting for similar tasks to finish - ({2})'.format(task_name, params_info, slept + sleep))
                                values = list(persistent_client.get_multi([persistent_key], must_exist=False))
                                if values[0] is None:
                                    if unittest_mode is True:
                                        Decorators.unittest_thread_info_by_name[thread_name] = ('WAITED', None)
                                    return None  # All pending jobs have been deleted in the meantime, no need to wait
                                if item['timestamp'] not in [value['timestamp'] for value in values[0]['values']]:
                                    if unittest_mode is True:
                                        Decorators.unittest_thread_info_by_name[thread_name] = ('WAITED', None)
                                    return None  # Similar tasks have been executed, so sync task currently waiting can return without having been executed
                                slept += sleep
                                time.sleep(sleep)
                                if slept >= timeout:
                                    log_message('Task {0} {1} waited {2}s for similar tasks to finish, but timeout was reached'.format(task_name, params_info, slept),
                                                level='error')
                                    if unittest_mode is True:
                                        Decorators.unittest_thread_info_by_name[thread_name] = ('EXCEPTION', 'Could not start within timeout of {0}s while waiting for other tasks'.format(timeout))
                                    raise EnsureSingleTimeoutReached('Ensure single {0} mode - ID {1} - Task {2} could not be started within timeout of {3}s'.format(mode,
                                                                                                                                                                     now,
                                                                                                                                                                     task_name,
                                                                                                                                                                     timeout))
                                if unittest_mode is True:
                                    if thread_name not in Decorators.unittest_thread_info_by_state['WAITING']:
                                        Decorators.unittest_thread_info_by_state['WAITING'].append(thread_name)

                log_message('New task {0} {1} scheduled for execution'.format(task_name, params_info))
                update_value(key=persistent_key,
                             append=True,
                             value_to_update={'kwargs': kwargs_dict,
                                              'task_id': task_id,
                                              'timestamp': now})

                # Poll the arakoon to see whether this call is the only in list, if so --> execute, else wait
                slept = 0
                while slept < timeout:
                    values = list(persistent_client.get_multi([persistent_key], must_exist=False))
                    if values[0] is not None:
                        queued_jobs = [v for v in values[0]['values'] if v['kwargs'] == kwargs_dict]
                        if len(queued_jobs) != 1:
                            if unittest_mode is True:
                                Decorators.unittest_thread_info_by_name[thread_name] = ('WAITING', None)
                                if thread_name not in Decorators.unittest_thread_info_by_state['WAITING']:
                                    Decorators.unittest_thread_info_by_state['WAITING'].append(thread_name)
                        else:
                            try:
                                if slept != 0:
                                    log_message('Task {0} {1} had to wait {2} seconds before being able to start'.format(task_name,
                                                                                                                         params_info,
                                                                                                                         slept))
                                if unittest_mode is True:
                                    Decorators.unittest_thread_info_by_name[thread_name] = ('EXECUTING', None)
                                output = f(*args, **kwargs)
                                if unittest_mode is True:
                                    Decorators.unittest_thread_info_by_name[thread_name] = ('FINISHED', None)
                                    Decorators.unittest_thread_info_by_state['FINISHED'].append(thread_name)
                                log_message('Task {0} finished successfully'.format(task_name))
                                return output
                            finally:
                                update_value(key=persistent_key,
                                             append=False,
                                             value_to_update=queued_jobs[0])
                    slept += sleep
                    time.sleep(sleep)
                    if slept >= timeout:
                        update_value(key=persistent_key,
                                     append=False,
                                     value_to_update={'kwargs': kwargs_dict,
                                                      'task_id': task_id,
                                                      'timestamp': now})
                        log_message('Could not start task {0} {1}, within expected time ({2}s). Removed it from queue'.format(task_name, params_info, timeout),
                                    level='error')
                        if unittest_mode is True:
                            Decorators.unittest_thread_info_by_name[thread_name] = ('EXCEPTION', 'Could not start within timeout of {0}s while queued'.format(timeout))
                        raise EnsureSingleTimeoutReached('Ensure single {0} mode - ID {1} - Task {2} could not be started within timeout of {3}s'.format(mode,
                                                                                                                                                         now,
                                                                                                                                                         task_name,
                                                                                                                                                         timeout))

            elif mode == 'CHAINED':
                if extra_task_names is not None:
                    log_message('Extra tasks are not allowed in this mode',
                                level='error')
                    raise ValueError('Ensure single {0} mode - ID {1} - Extra tasks are not allowed in this mode'.format(mode, now))

                # Update kwargs with args
                sleep = 1 if unittest_mode is False else 0.1
                timeout = kwargs.pop('ensure_single_timeout', 10 if unittest_mode is True else global_timeout)
                function_info = inspect.getargspec(f)
                kwargs_dict = {}
                for index, arg in enumerate(args):
                    kwargs_dict[function_info.args[index]] = arg
                kwargs_dict.update(kwargs)
                params_info = 'with params {0}'.format(kwargs_dict) if kwargs_dict else 'with default params'

                # Set the key in arakoon if non-existent
                value = update_value(key=persistent_key,
                                     append=True)

                # Validate whether another job with same params is being executed, skip if so
                for item in value['values'][1:]:  # 1st element is processing job, we check all other queued jobs for identical params
                    if item['kwargs'] == kwargs_dict:
                        if async_task is True:  # Not waiting for other jobs to finish since asynchronously
                            log_message('Execution of task {0} {1} discarded because of identical parameters'.format(task_name, params_info))
                            if unittest_mode is True:
                                Decorators.unittest_thread_info_by_name[thread_name] = ('DISCARDED', None)
                            return None

                        # If executed inline (sync), execute callback if any provided
                        if callback is not None:
                            log_message('Execution of task {0} {1} in progress, executing callback function'.format(task_name, params_info))
                            if unittest_mode is True:
                                Decorators.unittest_thread_info_by_name[thread_name] = ('CALLBACK', None)
                            return callback(*args, **kwargs)

                        # Let's wait for 2nd job in queue to have finished if no callback provided
                        slept = 0
                        while slept < timeout:
                            log_message('Task {0} {1} is waiting for similar tasks to finish - ({2})'.format(task_name, params_info, slept + sleep))
                            values = list(persistent_client.get_multi([persistent_key], must_exist=False))
                            if values[0] is None:
                                if unittest_mode is True:
                                    Decorators.unittest_thread_info_by_name[thread_name] = ('WAITED', None)
                                return None  # All pending jobs have been deleted in the meantime, no need to wait
                            if item['timestamp'] not in [value['timestamp'] for value in values[0]['values']]:
                                if unittest_mode is True:
                                    Decorators.unittest_thread_info_by_name[thread_name] = ('WAITED', None)
                                return None  # Similar tasks have been executed, so sync task currently waiting can return without having been executed
                            slept += sleep
                            time.sleep(sleep)
                            if slept >= timeout:
                                log_message('Task {0} {1} waited {2}s for similar tasks to finish, but timeout was reached'.format(task_name, params_info, slept),
                                            level='error')
                                if unittest_mode is True:
                                    Decorators.unittest_thread_info_by_name[thread_name] = ('EXCEPTION', 'Could not start within timeout of {0}s while waiting for other tasks'.format(timeout))
                                raise EnsureSingleTimeoutReached('Ensure single {0} mode - ID {1} - Task {2} could not be started within timeout of {3}s'.format(mode,
                                                                                                                                                                 now,
                                                                                                                                                                 task_name,
                                                                                                                                                                 timeout))
                            if unittest_mode is True:
                                if thread_name not in Decorators.unittest_thread_info_by_state['WAITING']:
                                    Decorators.unittest_thread_info_by_state['WAITING'].append(thread_name)

                log_message('New task {0} {1} scheduled for execution'.format(task_name, params_info))
                update_value(key=persistent_key,
                             append=True,
                             value_to_update={'kwargs': kwargs_dict,
                                              'task_id': task_id,
                                              'timestamp': now})

                # Poll the arakoon to see whether this call is the first in list, if so --> execute, else wait
                first_element = None
                slept = 0
                while slept < timeout:
                    values = list(persistent_client.get_multi([persistent_key], must_exist=False))
                    if values[0] is not None:
                        value = values[0]
                        first_element = value['values'][0]['timestamp'] if len(value['values']) > 0 else None

                    if first_element == now:
                        try:
                            if slept > 0:
                                log_message('Task {0} {1} had to wait {2} seconds before being able to start'.format(task_name,
                                                                                                                     params_info,
                                                                                                                     slept))
                            if unittest_mode is True:
                                Decorators.unittest_thread_info_by_name[thread_name] = ('EXECUTING', None)
                            output = f(*args, **kwargs)
                            if unittest_mode is True:
                                Decorators.unittest_thread_info_by_name[thread_name] = ('FINISHED', None)
                                Decorators.unittest_thread_info_by_state['FINISHED'].append(thread_name)
                            log_message('Task {0} finished successfully'.format(task_name))
                            return output
                        finally:
                            update_value(key=persistent_key,
                                         append=False)
                    else:
                        if unittest_mode is True:
                            if thread_name not in Decorators.unittest_thread_info_by_state['WAITING']:
                                Decorators.unittest_thread_info_by_name[thread_name] = ('WAITING', None)
                                Decorators.unittest_thread_info_by_state['WAITING'].append(thread_name)

                    slept += sleep
                    time.sleep(sleep)
                    if slept >= timeout:
                        update_value(key=persistent_key,
                                     append=False,
                                     value_to_update={'kwargs': kwargs_dict,
                                                      'task_id': task_id,
                                                      'timestamp': now})
                        log_message('Could not start task {0} {1}, within expected time ({2}s). Removed it from queue'.format(task_name, params_info, timeout),
                                    level='error')
                        if unittest_mode is True:
                            Decorators.unittest_thread_info_by_name[thread_name] = ('EXCEPTION', 'Could not start within timeout of {0}s while queued'.format(timeout))
                        raise EnsureSingleTimeoutReached('Ensure single {0} mode - ID {1} - Task {2} could not be started within timeout of {3}s'.format(mode,
                                                                                                                                                         now,
                                                                                                                                                         task_name,
                                                                                                                                                         timeout))
            else:
                raise ValueError('Unsupported mode "{0}" provided'.format(mode))

        new_function.__name__ = f.__name__
        new_function.__module__ = f.__module__
        return new_function
    return wrap


def add_hooks(hook_type, hooks):
    """
    This decorator marks the decorated function to be interested in a certain hook
    :param hook_type: Type of hook
    :param hooks: Hooks to add to function
    """
    def wrap(f):
        """
        Wrapper function
        :param f: Function to add hooks on
        """
        if not hasattr(f, 'hooks'):
            f.hooks = {}
        if hook_type not in f.hooks:
            f.hooks[hook_type] = []
        if isinstance(hooks, list):
            f.hooks[hook_type].extend(hooks)
        else:
            f.hooks[hook_type].append(hooks)
        return f
    return wrap
