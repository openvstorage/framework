# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
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


def ensure_single(task_name, extra_task_names=None, mode='REVOKE'):
    """
    Decorator ensuring a new task cannot be started in case a certain task is
    running, scheduled or reserved.

    The task using this decorator on. Keep also in
    mind that validation will be executed by the worker itself, so if the task is scheduled on
    a worker currently processing a "duplicate" task, it will only get validated after the first
    one completes, which will result in the fact that the task will execute normally.

    Allowed modes:
     - REVOKE: If any of the specified task names is being executed, the calling function will not be executed
     - DELAY: If a task is in queue, it will be revoked and a new one will be launched with the specified delay
     - CHAIN: If a task is being executed, the new task will be appended for later execution

    :param task_name:        Name of the task to ensure its singularity
    :type task_name:         String

    :param extra_task_names: Extra tasks to take into account
    :type extra_task_names:  List

    :param mode:             Mode of the ensure single. Allowed values: REVOKE, DELAY, CHAIN
    :type mode:              String

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
            def update_value(key, append, value_to_store=None):
                """
                Store the specified value in the PersistentFactory
                :param key:            Key to store the value for
                :param append:         If True, the specified value will be appended else element at index 0 will be popped
                :param value_to_store: Value to append to the list
                :return:               None
                """
                with VolatileMutex(name=key, wait=5) as volatile_mutex:
                    volatile_mutex.acquire()
                    if persistent_client.exists(key):
                        val = persistent_client.get(key)
                        if append is True and value_to_store is not None:
                            val['values'].append(value_to_store)
                        elif append is False:
                            val['values'].pop(0)
                    else:
                        logger.info('Ensure single {0} mode: Setting initial value for key {1}'.format(mode, persistent_key))
                        val = {'mode': mode,
                               'values': []}
                    persistent_client.set(key, val)
                    volatile_mutex.release()

            now = '{0}_{1}'.format(int(time.time()), ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(10)))
            task_names = [task_name] if extra_task_names is None else [task_name] + extra_task_names
            persistent_key = '{0}_{1}'.format(ENSURE_SINGLE_KEY, task_name)
            persistent_client = PersistentFactory.get_client()

            if mode == 'REVOKE':
                with VolatileMutex(persistent_key, wait=5) as mutex:
                    mutex.acquire()
                    for task in task_names:
                        key_to_check = '{0}_{1}'.format(ENSURE_SINGLE_KEY, task)
                        if persistent_client.exists(key_to_check):
                            logger.info('Ensure single {0} mode: Execution of task "{1}" discarded'.format(mode, task_name))
                            mutex.release()
                            return None
                    logger.info('Ensure single {0} mode: Setting key "{1}"'.format(mode, persistent_key))
                    persistent_client.set(persistent_key, {'mode': mode})
                    mutex.release()

                try:
                    output = function(*args, **kwargs)
                finally:
                    with VolatileMutex(persistent_key, wait=5) as mutex:
                        mutex.acquire()
                        if persistent_client.exists(persistent_key):
                            logger.info('Ensure single {0} mode: Deleting key "{1}" from persistent client'.format(mode, persistent_key))
                            persistent_client.delete(persistent_key)
                        mutex.release()
                return output

            elif mode == 'DELAY':
                pass

            elif mode == 'CHAIN':
                if extra_task_names is not None:
                    raise ValueError('Ensure single {0} mode: Extra tasks are not allowed in this mode'.format(mode))

                # 1. Create key to be stored in arakoon and update kwargs with args
                timeout = kwargs.pop('chain_timeout') if 'chain_timeout' in kwargs else 60
                function_info = inspect.getargspec(function)
                kwargs_dict = {}
                for index, arg in enumerate(args):
                    kwargs_dict[function_info.args[index]] = arg
                kwargs_dict.update(kwargs)

                # 2. Set the key in arakoon if non-existent
                update_value(key=persistent_key,
                             append=True)

                # 3. Validate whether another job with same params is being executed, skip if so
                value = persistent_client.get(persistent_key)
                for item in value['values'][1:]:  # 1st element is processing job, we check all other queued jobs for identical params
                    if item['kwargs'] == kwargs_dict:
                        logger.info('Ensure single {0} mode: Execution of task {1} discarded because of identical parameters. {2}'.format(mode, task_name, kwargs_dict))
                        return None
                update_value(key=persistent_key,
                             append=True,
                             value_to_store={'kwargs': kwargs_dict,
                                             'timestamp': now})

                # 4. Poll the arakoon to see whether this call is the first in list, if so --> execute, else wait
                first_element = None
                counter = 0
                while first_element != now and counter < timeout:
                    value = persistent_client.get(persistent_key)
                    first_element = value['values'][0]['timestamp']

                    if first_element == now:
                        try:
                            if counter != 0:
                                current_time = int(time.time())
                                starting_time = int(now.split('_')[0])
                                logger.info('Ensure single {0} mode: Task had to wait {1} seconds before being able to start'.format(now, current_time - starting_time))
                            output = function(*args, **kwargs)
                        finally:
                            update_value(key=persistent_key,
                                         append=False)
                        return output
                    counter += 1
                    time.sleep(1)
                    if counter == timeout:
                        update_value(key=persistent_key,
                                     append=False)
                        raise RuntimeError('Ensure single {0} mode: Could not start job within expected time. Removed it from queue'.format(mode))
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
