# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Contains various decorators
"""

import json
from ovs.dal.lists.storagedriverlist import StorageDriverList
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


def ensure_single(task_names, mode='REVOKE'):
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

    :param task_names: List of names to check
    :type task_names: List
    :param mode: Mode of the ensure single. Allowed values: REVOKE, DELAY, CHAIN
    :type mode: String
    :return: Pointer to function
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
            cache = PersistentFactory.get_client()
            if not task_names[0].endswith(function.__name__):
                raise ValueError('First task name in ensure_single decorator must be identical to function name')
            if mode == 'REVOKE':
                for task_name in task_names:
                    if cache.exists('{0}_{1}'.format(ENSURE_SINGLE_KEY, task_name)):
                        logger.debug('Execution of task {0} discarded'.format(function.__name__))
                        return None
                cache.set('{0}_{1}'.format(ENSURE_SINGLE_KEY, task_names[0]), 'revoke_task')
                return function(*args, **kwargs)
            elif mode == 'DELAY':
                pass
            elif mode == 'CHAIN':
                pass
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
