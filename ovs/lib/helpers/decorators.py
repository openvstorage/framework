# Copyright 2016 iNuron NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Contains various decorators
"""

import inspect
import json
import random
import string
import time
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.extensions.generic.volatilemutex import VolatileMutex
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.lib.helpers.exceptions import EnsureSingleTimeoutReached
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('lib', name='scheduled tasks')
ENSURE_SINGLE_KEY = 'ovs_ensure_single'


def log(event_type):
    """
    Task logger
    :param event_type: Event type
    :return: Pointer to function
    """

    def wrap(function):
        """
        Wrapper function
        :param function: Function to log something about
        """

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
            _logger = LogHandler.get('log', name=event_type.lower())
            _logger.info('[{0}.{1}] - {2} - {3} - {4}'.format(
                function.__module__,
                function.__name__,
                json.dumps(list(args)),
                json.dumps(kwargs),
                json.dumps(metadata)
            ))

            # Call the function
            return function(*args, **kwargs)

        new_function.__name__ = function.__name__
        new_function.__module__ = function.__module__
        return new_function

    return wrap


def ensure_single(task_name, extra_task_names=None, mode='DEFAULT', global_timeout=300):
    """
    Decorator ensuring a new task cannot be started in case a certain task is
    running, scheduled or reserved.

    The task using this decorator on. Keep also in
    mind that validation will be executed by the worker itself, so if the task is scheduled on
    a worker currently processing a "duplicate" task, it will only get validated after the first
    one completes, which will result in the fact that the task will execute normally.

    Allowed modes:
     - DEFAULT: If any of the specified task names is being executed, the calling function will not be executed
     - CHAINED: If a task is being executed, the new task will be appended for later execution

    :param task_name:        Name of the task to ensure its singularity
    :type task_name:         String

    :param extra_task_names: Extra tasks to take into account
    :type extra_task_names:  List

    :param mode:             Mode of the ensure single. Allowed values: DEFAULT, CHAINED
    :type mode:              String

    :param global_timeout:   Timeout before raising error (Only applicable in CHAINED mode)
    :type global_timeout:    Integer

    :return:                 Pointer to function
    """
    def wrap(function):
        """
        Wrapper function
        :param function: Function to check
        """
        def new_function(*args, **kwargs):
            """
            Wrapped function
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
                if level not in ('info', 'warning', 'debug', 'error'):
                    raise ValueError('Unsupported log level "{0}" specified'.format(level))
                complete_message = 'Ensure single {0} mode - ID {1} - {2}'.format(mode, now, message)
                getattr(logger, level)(complete_message)

            def update_value(key, append, value_to_update=None):
                """
                Store the specified value in the PersistentFactory
                :param key:             Key to store the value for
                :param append:          If True, the specified value will be appended else element at index 0 will be popped
                :param value_to_update: Value to append to the list or remove from the list
                :return:                Updated value
                """
                with VolatileMutex(name=key, wait=5):
                    if persistent_client.exists(key):
                        val = persistent_client.get(key)
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
                        log_message('Setting initial value for key {0}'.format(persistent_key))
                        val = {'mode': mode,
                               'values': []}
                    persistent_client.set(key, val)
                return val

            now = '{0}_{1}'.format(int(time.time()), ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(10)))
            task_names = [task_name] if extra_task_names is None else [task_name] + extra_task_names
            persistent_key = '{0}_{1}'.format(ENSURE_SINGLE_KEY, task_name)
            persistent_client = PersistentFactory.get_client()

            if mode == 'DEFAULT':
                with VolatileMutex(persistent_key, wait=5):
                    for task in task_names:
                        key_to_check = '{0}_{1}'.format(ENSURE_SINGLE_KEY, task)
                        if persistent_client.exists(key_to_check):
                            log_message('Execution of task {0} discarded'.format(task_name))
                            return None
                    log_message('Setting key {0}'.format(persistent_key))
                    persistent_client.set(persistent_key, {'mode': mode})

                try:
                    output = function(*args, **kwargs)
                    log_message('Task {0} finished successfully'.format(task_name))
                    return output
                finally:
                    with VolatileMutex(persistent_key, wait=5):
                        if persistent_client.exists(persistent_key):
                            log_message('Deleting key {0}'.format(persistent_key))
                            persistent_client.delete(persistent_key)
            elif mode == 'DEDUPED':
                with VolatileMutex(persistent_key, wait=5):
                    if extra_task_names is not None:
                        for task in extra_task_names:
                            key_to_check = '{0}_{1}'.format(ENSURE_SINGLE_KEY, task)
                            if persistent_client.exists(key_to_check):
                                log_message('Execution of task {0} discarded'.format(task_name))
                                return None
                    log_message('Setting key {0}'.format(persistent_key))

                # Update kwargs with args
                timeout = kwargs.pop('ensure_single_timeout') if 'ensure_single_timeout' in kwargs else global_timeout
                function_info = inspect.getargspec(function)
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
                            log_message('Execution of task {0} {1} discarded because of identical parameters'.format(task_name, params_info))
                            return None
                log_message('New task {0} {1} scheduled for execution'.format(task_name, params_info))
                update_value(key=persistent_key,
                             append=True,
                             value_to_update={'kwargs': kwargs_dict})

                # Poll the arakoon to see whether this call is the only in list, if so --> execute, else wait
                counter = 0
                while counter < timeout:
                    if persistent_client.exists(persistent_key):
                        values = persistent_client.get(persistent_key)['values']
                        queued_jobs = [v for v in values if v['kwargs'] == kwargs_dict]
                        if len(queued_jobs) == 1:
                            try:
                                if counter != 0:
                                    current_time = int(time.time())
                                    starting_time = int(now.split('_')[0])
                                    log_message('Task {0} {1} had to wait {2} seconds before being able to start'.format(task_name,
                                                                                                                         params_info,
                                                                                                                         current_time - starting_time))
                                output = function(*args, **kwargs)
                                log_message('Task {0} finished successfully'.format(task_name))
                                return output
                            finally:
                                update_value(key=persistent_key,
                                             append=False,
                                             value_to_update={'kwargs': kwargs_dict})
                        counter += 1
                        time.sleep(1)
                        if counter == timeout:
                            update_value(key=persistent_key,
                                         append=False,
                                         value_to_update={'kwargs': kwargs_dict})
                            log_message('Could not start task {0} {1}, within expected time ({2}s). Removed it from queue'.format(task_name, params_info, timeout),
                                        level='error')
                            raise EnsureSingleTimeoutReached('Ensure single {0} mode - ID {1} - Task {2} could not be started within timeout of {3}s'.format(mode,
                                                                                                                                                             now,
                                                                                                                                                             task_name,
                                                                                                                                                             timeout))
            elif mode == 'CHAINED':
                if extra_task_names is not None:
                    log_message('Extra tasks are not allowed in this mode',
                                level='error')
                    raise ValueError('Ensure single {0} mode - ID {1} - Extra tasks are not allowed in this mode'.format(mode, now))

                # Create key to be stored in arakoon and update kwargs with args
                timeout = kwargs.pop('ensure_single_timeout') if 'ensure_single_timeout' in kwargs else global_timeout
                function_info = inspect.getargspec(function)
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
                        log_message('Execution of task {0} {1} discarded because of identical parameters'.format(task_name, params_info))
                        return None
                log_message('New task {0} {1} scheduled for execution'.format(task_name, params_info))
                update_value(key=persistent_key,
                             append=True,
                             value_to_update={'kwargs': kwargs_dict,
                                              'timestamp': now})

                # Poll the arakoon to see whether this call is the first in list, if so --> execute, else wait
                first_element = None
                counter = 0
                while first_element != now and counter < timeout:
                    if persistent_client.exists(persistent_key):
                        value = persistent_client.get(persistent_key)
                        first_element = value['values'][0]['timestamp']

                    if first_element == now:
                        try:
                            if counter != 0:
                                current_time = int(time.time())
                                starting_time = int(now.split('_')[0])
                                log_message('Task {0} {1} had to wait {2} seconds before being able to start'.format(task_name,
                                                                                                                     params_info,
                                                                                                                     current_time - starting_time))
                            output = function(*args, **kwargs)
                            log_message('Task {0} finished successfully'.format(task_name))
                        finally:
                            update_value(key=persistent_key,
                                         append=False)
                        return output
                    counter += 1
                    time.sleep(1)
                    if counter == timeout:
                        update_value(key=persistent_key,
                                     append=False)
                        log_message('Could not start task {0} {1}, within expected time ({2}s). Removed it from queue'.format(task_name, params_info, timeout),
                                    level='error')
                        raise EnsureSingleTimeoutReached('Ensure single {0} mode - ID {1} - Task {2} could not be started within timeout of {3}s'.format(mode,
                                                                                                                                                         now,
                                                                                                                                                         task_name,
                                                                                                                                                         timeout))
            else:
                raise ValueError('Unsupported mode "{0}" provided'.format(mode))

        new_function.__name__ = function.__name__
        new_function.__module__ = function.__module__
        return new_function
    return wrap


def add_hooks(hook_type, hooks):
    """
    This decorator marks the decorated function to be interested in a certain hook
    :param hook_type: Type of hook
    :param hooks: Hooks to add to function
    """
    def wrap(function):
        """
        Wrapper function
        :param function: Function to add hooks on
        """
        if not hasattr(function, 'hooks'):
            function.hooks = {}
        if hook_type not in function.hooks:
            function.hooks[hook_type] = []
        if isinstance(hooks, list):
            function.hooks[hook_type] += hooks
        else:
            function.hooks[hook_type].append(hooks)
        return function
    return wrap
