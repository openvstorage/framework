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
from celery.task.control import inspect
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('lib', name='scheduled tasks')


def log(event_type):
    """
    Task logger
    """

    def wrap(f):
        """
        Wrapper function
        """

        def new_function(*args, **kwargs):
            """
            Wrapped function
            """
            # Log the call
            if event_type == 'VOLUMEDRIVER_TASK':
                metadata = {'storagedriver': StorageDriverList.get_by_storagedriver_id(kwargs['storagedriver_id']).guid}
            else:
                metadata = {}
            _logger = LogHandler.get('log', name=event_type.lower())
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


def ensure_single(tasknames):
    """
    Decorator ensuring a new task cannot be started in case a certain task is
    running, scheduled or reserved.

    The task using this decorator on, must be a bound task (with bind=True argument). Keep also in
    mind that validation will be executed by the worker itself, so if the task is scheduled on
    a worker currently processing a "duplicate" task, it will only get validated after the first
    one completes, which will result in the fact that the task will execute normally.

    @param tasknames: list of names to check
    @type tasknames: list
    """
    def wrap(function):
        """
        Wrapper function
        """
        def wrapped(self=None, *args, **kwargs):
            """
            Wrapped function
            """
            if not hasattr(self, 'request'):
                raise RuntimeError('The decorator ensure_single can only be applied to bound tasks (with bind=True argument)')
            task_id = self.request.id

            reason = ''

            def can_run():
                global reason
                """
                Checks whether a task is running/scheduled/reserved.
                The check is executed in stages, as querying the inspector is a slow call.
                """
                if tasknames:
                    inspector = inspect()
                    active = inspector.active()
                    if active:
                        for taskname in tasknames:
                            for worker in active.values():
                                for task in worker:
                                    if task['id'] != task_id and taskname == task['name']:
                                        reason = 'active'
                                        return False
                    scheduled = inspector.scheduled()
                    if scheduled:
                        for taskname in tasknames:
                            for worker in scheduled.values():
                                for task in worker:
                                    request = task['request']
                                    if request['id'] != task_id and taskname == request['name']:
                                        reason = 'scheduled'
                                        return False
                    reserved = inspector.reserved()
                    if reserved:
                        for taskname in tasknames:
                            for worker in reserved.values():
                                for task in worker:
                                    if task['id'] != task_id and taskname == task['name']:
                                        reason = 'reserved'
                                        return False
                return True

            if can_run():
                return function(*args, **kwargs)
            else:
                logger.debug('Execution of task {0}[{1}] discarded'.format(
                    self.name, self.request.id
                ))
                return None

        wrapped.__name__ = function.__name__
        wrapped.__module__ = function.__module__
        return wrapped
    return wrap


def add_hooks(hook_type, hooks):
    """
    This decorator marks the decorated function to be interested in a certain hook
    """
    def wrap(function):
        """
        Wrapper function
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
