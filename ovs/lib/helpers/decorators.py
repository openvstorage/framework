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
import threading
from functools import wraps
from contextlib import contextmanager
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs_extensions.storage.exceptions import KeyNotFoundException, AssertException
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.lib.helpers.exceptions import EnsureSingleTimeoutReached, EnsureSingleDoCallBack, EnsureSingleTaskDiscarded, EnsureSingleNoRunTimeInfo
from ovs.lib.helpers.toolbox import Schedule
from ovs.log.log_handler import LogHandler

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


def ovs_task(name, schedule=None, ensure_single_info=None):
    """
    Decorator to execute celery tasks in OVS
    These tasks can be wrapped additionally in the ensure single decorator
    :param name: Name of the task
    :type name: str
    :param schedule: Optional: task schedule to use.
    Discovering the scheduled tasks by the celery_beat is done using reflection.
    The schedule passed in this decorator is passed on to celery which will make it part of the tasks attributes
    The reflection discovery will check for these 'schedule' attributes.
    :type schedule: Schedule
    :param ensure_single_info: Optional: ensure single information. Options are:
    {'callback': The callback function to call when the task is already running somewhere,
     'extra_task_names': Extra task names that this task should bear.
     These names can be used to prevent Task B with name B to run if task A is already running. extra_task_names would be 'A' in this example.
     'mode': ensure single mode to operate in
     'global_timeout' Timeout before raising error (Only applicable in CHAINED mode)
     'ignored_arguments': Arguments to ignore when CHAINED or DEDUPED. These arguments will not be accounted for when checking if a task is already queued up
     'hooks': hooks to execute on certain points. Used for unittesting only

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
    :type ensure_single_info: dict

    Allowed hooks:
        Hooks are invoked without any arguments
        All hooks have both a before_ and after_ hooking point
        - validation: Validation logic to either run/discard or call the callback
        - execution: Execution of the underlying task. Hook will not operate if either callback or discard happenend
        - callback: Execution of the passed callback function (if any)
        - discard: The task will be discarded
    """
    if ensure_single_info is None:
        ensure_single_info = {}

    def wrapper(f):
        """
        Wrapper function
        """
        from ovs.celery_run import celery

        if ensure_single_info != {}:
            ensure_single_container = EnsureSingleContainer(task_name=name, **ensure_single_info)
            if ensure_single_container.mode == 'DEFAULT':
                f = ensure_single_default(ensure_single_container)(f)
            else:
                f = _ensure_single(task_name=name, **ensure_single_info)(f)
        return celery.task(name=name, schedule=schedule, bind=True)(f)
    return wrapper


def ensure_single_default(ensure_single_container):
    """
    Decorator ensuring a new task cannot be started in case a certain task is
    running, scheduled or reserved.

    Keep also in mind that validation will be executed by the worker itself, so if the task is scheduled on
    a worker currently processing a "duplicate" task, it will only get validated after the first
    one completes, which will result in the fact that the task will execute normally.

    Mode: DEFAULT: De-duplication based on the task's name. If any new task with the same name is scheduled it will be discarded

    :param ensure_single_container: Container past on by the initial decorator
    :type ensure_single_container: EnsureSingleContainer
    :return: Pointer to function
    :rtype: func
    """

    def ensure_single_default_wrap(f):
        """
        Wrapper function
        :param f: Function to wrap
        """

        @wraps(f)
        def ensure_single_default_inner(self, *args, **kwargs):
            """
            Wrapped function
            :param self: With bind=True, the celery task result itself is passed in
            :param args: Arguments without default values
            :param kwargs: Arguments with default values
            """
            ensure_single = EnsureSingle(ensure_single_container, self)

            try:
                with ensure_single.lock_default_mode():
                    return f(*args, **kwargs)
            except EnsureSingleTaskDiscarded:
                return ensure_single.do_discard()
            except EnsureSingleDoCallBack:
                return ensure_single.do_callback(*args, **kwargs)

        return ensure_single_default_inner
    return ensure_single_default_wrap


def ensure_single_deduped(ensure_single_container):
    # type: (EnsureSingleContainer) -> callable
    """
    Decorator ensuring a new task cannot be started in case a certain task is
    running, scheduled or reserved.

    Keep also in mind that validation will be executed by the worker itself, so if the task is scheduled on
    a worker currently processing a "duplicate" task, it will only get validated after the first
    one completes, which will result in the fact that the task will execute normally.

    Mode: DEDUPED: De-duplication based on the task's name and arguments. If a new task with the same name and arguments
        is scheduled while the first one is currently being executed, it will be allowed on the queue (to make
        sure there will be at least one new execution). All subsequent identical tasks will be discarded.
         - Tasks with identical arguments will be executed in serial (Subsequent tasks with same params will be discarded if 1 waiting task with these params already in queue)
         - Tasks with different arguments will be executed in parallel

    :param ensure_single_container: EnsureSingle container instance
    :type ensure_single_container: EnsureSingleContainer
    :return: Pointer to function
    :rtype: func
    """

    def ensure_single_deduped_wrap(f):
        """
        Wrapper function
        :param f: Function to wrap
        """

        @wraps(f)
        def ensure_single_deduped_inner(self, *args, **kwargs):
            """
            Wrapped function
            :param self: With bind=True, the celery task result itself is passed in
            :param args: Arguments without default values
            :param kwargs: Arguments with default values
            """

            ensure_single = EnsureSingle(ensure_single_container, self)
            raise NotImplementedError()
        return ensure_single_deduped_inner
    return ensure_single_deduped_wrap


def ensure_run_time_info():
    """
    Ensure that the runtime information is present
    """
    def ensure_run_time_info_wrap(f):
        @wraps(f)
        def ensure_run_time_info_inner(self, *args, **kwargs):
            # type: (EnsureSingle, *any, **any) -> any
            if any(arg is None for arg in [self.now, self.thread_name, self.unittest_mode, self.message]):
                raise EnsureSingleNoRunTimeInfo('Please gather all runtime information using \'gather_run_time_info\'')
            return f(self, *args, **kwargs)
        return ensure_run_time_info_inner
    return ensure_run_time_info_wrap


class EnsureSingleContainer(object):
    """
    Container object which holds the arguments of the function
    """
    def __init__(self, task_name, mode, extra_task_names=None, global_timeout=300, callback=None, ignore_arguments=None, hooks=None):
        """
        Initialize a EnsureSingleContainer
        Does not keep track of any state
        :param task_name: Name of the task to ensure its singularity
        :type task_name: str
        :param extra_task_names: Extra tasks to take into account
        :type extra_task_names: list
        :param mode: Mode of the ensure single. Allowed values: DEFAULT, CHAINED, 'DEDUPED'
        :type mode: str
        :param global_timeout: Timeout before raising error (Only applicable in CHAINED mode)
        :type global_timeout: int
        :param callback: Call back function which will be executed if identical task in progress
        :type callback: func
        :param ignore_arguments: Arguments to ignore when CHAINED or DEDUPED. These arguments will not be accounted for
        when checking if a task is already queued up
        :type ignore_arguments: list
        :param hooks: Optional: hooks to execute on certain points. Used for unittesting only
        Hooks are invoked without any arguments
        Optional hooks: all hooks have both a before_ and after_ hooking point
        - validation: Validation logic to either run/discard or call the callback
        - execution: Execution of the underlying task. Hook will not operate if either callback or discard happenend
        - callback: Execution of the passed callback function (if any)
        - discard: The task will be discarded
        :type hooks: dict
        """
        if hooks is None:
            hooks = {}
        self.hooks = hooks
        self.mode = mode
        self.task_name = task_name
        self.extra_task_names = extra_task_names
        self.task_names = [task_name] + extra_task_names if extra_task_names else [task_name]
        self.global_timeout = global_timeout
        self.callback = callback
        self.ignore_arguments = ignore_arguments or []

        # Locks
        self.log_lock = threading.Lock()

    
class EnsureSingle(object):

    """
    Container class to help ensuring that a single function is being executed across the cluster
    """

    def __init__(self, ensure_single_container, task):
        """
        Initialize a EnsureSingle container
        :param ensure_single_container: Ensure single arguments container
        :type ensure_single_container: EnsureSingleContainer
        :param task: Task instance
        :type task: celery.AsyncResult
        """
        self.ensure_single_container = ensure_single_container
        # Storage
        self.persistent_key = '{0}_{1}'.format(ENSURE_SINGLE_KEY, ensure_single_container.task_name)
        self.persistent_client = PersistentFactory.get_client()
        self.task_id, self.async_task = self.get_task_id_and_async(task)

        # Logging
        self.logger = LogHandler.get('lib', name='ensure single')

        # Runtime
        self.now = None
        self.thread_name = None
        self.unittest_mode = None
        self.message = None
        self.gather_run_time_info()

    def set_task(self, task):
        # type: (celery.AsyncResult) -> None
        """
        Set the task context in which this ensure single is running
        :param task: Task to set
        :type task: celery.AsyncResult
        :return: None
        """
        self.task_id, self.async_task = self.get_task_id_and_async(task)

    def log_message(self, message, level='info'):
        """
        Log a message with some additional information
        :param message: Message to log
        :param level:   Log level
        :return:        None
        """
        if not hasattr(self.logger, level):
            raise ValueError('Unsupported log level "{0}" specified'.format(level))

        getattr(self.logger, level)(self.message.format(message))

    @staticmethod
    def get_task_id_and_async(task):
        """
        Retrieve the task ID of the current task
        :param task: Task to retrieve ID for
        :type task: celery.AsyncResult
        """
        if not hasattr(task, 'request'):
            raise RuntimeError('The decorator ensure_single can only be applied to bound tasks (with bind=True argument)')

        task_id = task.request.id
        # Async tasks have an ID, inline executed tasks have None as ID
        async_task = task_id is not None
        return task_id, async_task

    def validate_no_extra_names(self):
        # type: () -> None
        """
        Certain chaining modes do not accept multiple names
        """
        if self.ensure_single_container.extra_task_names:
            self.log_message('Extra tasks are not allowed in this mode', level='error')
            raise ValueError('Ensure single {0} mode - ID {1} - Extra tasks are not allowed in this mode'.format(self.ensure_single_container.mode, self.now))

    @staticmethod
    def get_all_arguments_as_kwargs(func, args, kwargs):
        # type: (callable, tuple, dict) -> dict
        """
        Retrieve all arguments passed to a function as key-word arguments
        :param func: Function to get all arguments as key-word arguments for
        :type func: callable
        :param args: Arguments passed to the function
        :type args: tuple
        :param kwargs: Key words passed to the functoin
        :type kwargs: dict
        :return: Key word arguments
        :rtype: dict
        """
        function_info = inspect.getargspec(func)
        kwargs_dict = {}
        for index, arg in enumerate(args):
            kwargs_dict[function_info.args[index]] = arg
        kwargs_dict.update(kwargs)
        return kwargs_dict

    # def poll_task_completion(self, params_info, sleep_time, timeout):
    #     # type: (threading.Event) -> None
    #     """
    #     Poll for the task to complete
    #     :param event: Event to set once the polling has completed
    #     :type event: threading.Event
    #     :return:
    #     """
    #     # Let's wait for 2nd job in queue to have finished if no callback provided
    #     slept = 0
    #     while slept < timeout:
    #         self.logger.info('Task {0} {1} is waiting for similar tasks to finish - ({2})'.format(self.ensure_single_container.task_name,
    #                                                                                               params_info,
    #                                                                                               slept + sleep_time))
    #         values = list(persistent_client.get_multi([persistent_key], must_exist=False))
    #         if values[0] is None or item['timestamp'] not in [value['timestamp'] for value in
    #                                                           values[0]['values']]:
    #             # values[0] is None -> All pending jobs have been deleted in the meantime, no need to wait
    #             # Otherwise similar tasks have been executed, so sync task currently waiting can return without having been executed
    #             ensure_single_container.unittest_set_state_waited()
    #             return None  # Similar tasks have been executed, so sync task currently waiting can return without having been executed
    #         slept += sleep
    #         time.sleep(sleep)
    #         if slept >= timeout:
    #             self.logger.error('Task {0} {1} waited {2}s for similar tasks to finish, but timeout was reached'.format(self.ensure_single_container.task_name,
    #                                                                                                                      params_info,
    #                                                                                                                      slept)
    #
    #             exception_message = 'Could not start within timeout of {0}s while waiting for other tasks'.format(timeout)
    #             self.unittest_set_state_exception(exception_message)
    #             raise EnsureSingleTimeoutReached('Ensure single {0} mode - ID {1} - Task {2} could not be started within timeout of {3}s'.format(mode,now,task_name,timeout))
    #         if self.unittest_mode:
    #             if thread_name not in Decorators.unittest_thread_info_by_state['WAITING']:
    #                 Decorators.unittest_thread_info_by_state['WAITING'].append(thread_name)

    # @ensure_run_time_info()
    # @contextmanager
    # def lock_deduped_mode(self, kwargs_dict, timeout=None):
    #     # type: (dict) -> None
    #     """
    #     Lock the function in deduped mode
    #     """
    #     mode = 'DEDUPED'
    #     persistent_key = '{}_{}'.format(self.persistent_key, mode.lower())
    #
    #     params_info = 'with params {0}'.format(kwargs_dict) if kwargs_dict else 'with default params'
    #
    #     def update_value(key, append, value_to_update=None):
    #         """
    #         Store the specified value in the PersistentFactory
    #         :param key:             Key to store the value for
    #         :param append:          If True, the specified value will be appended else element at index 0 will be popped
    #         :param value_to_update: Value to append to the list or remove from the list
    #         :return:                Updated value
    #         """
    #         with volatile_mutex(name=key, wait=5):
    #             vals = list(persistent_client.get_multi([key], must_exist=False))
    #             if vals[0] is not None:
    #                 val = vals[0]
    #                 if append is True and value_to_update is not None:
    #                     val['values'].append(value_to_update)
    #                 elif append is False and value_to_update is not None:
    #                     for value_item in val['values']:
    #                         if value_item == value_to_update:
    #                             val['values'].remove(value_item)
    #                             break
    #                 elif append is False and len(val['values']) > 0:
    #                     val['values'].pop(0)
    #             else:
    #                 log_message('Setting initial value for key {0}'.format(key))
    #                 val = {'mode': mode,
    #                        'values': []}
    #             persistent_client.set(key, val)
    #         return val
    #
    #     if timeout is None:
    #         timeout = self.ensure_single_container.global_timeout
    #
    #     # Acquire
    #     initial_registrations = None  # Used for asserting
    #     try:
    #         current_registration = self.persistent_client.get(persistent_key)
    #         initial_registrations = current_registration
    #     except KeyNotFoundException:
    #         current_registration = []
    #     # Set the key in arakoon if non-existent
    #     value = update_value(key=persistent_key,
    #                          append=True)
    #
    #     # Validate whether another job with same params is being executed
    #     job_counter = 0
    #     for task_data in current_registration:
    #         if task_data['kwargs'] != kwargs_dict:
    #             continue
    #         # Another job with the same registration
    #         job_counter += 1
    #         if job_counter == 2:  # 1st job with same params is being executed, 2nd is scheduled for execution ==> Discard current
    #             if self.async_task:  # Not waiting for other jobs to finish since asynchronously
    #                 discard_message = 'Execution of task {0} {1} discarded because of identical parameters'.format(self.task_id, params_info)
    #                 self.discard_task(discard_message)
    #
    #             # If executed inline (sync), execute callback if any provided
    #             if self.ensure_single_container.callback:
    #                 callback_message = 'Execution of task {0} {1} in progress, execute callback function'.format(self.ensure_single_container.task_name, params_info)
    #                 self.callback_task(callback_message)
    #
    #             # Let's wait for 2nd job in queue to have finished if no callback provided
    #             slept = 0
    #             while slept < timeout:
    #                 log_message('Task {0} {1} is waiting for similar tasks to finish - ({2})'.format(task_name,
    #                                                                                                  params_info,
    #                                                                                                  slept + sleep))
    #                 values = list(persistent_client.get_multi([persistent_key], must_exist=False))
    #                 if values[0] is None or item['timestamp'] not in [value['timestamp'] for value in
    #                                                                   values[0]['values']]:
    #                     # values[0] is None -> All pending jobs have been deleted in the meantime, no need to wait
    #                     # Otherwise similar tasks have been executed, so sync task currently waiting can return without having been executed
    #                     ensure_single_container.unittest_set_state_waited()
    #                     return None  # Similar tasks have been executed, so sync task currently waiting can return without having been executed
    #                 slept += sleep
    #                 time.sleep(sleep)
    #                 if slept >= timeout:
    #                     log_message(
    #                         'Task {0} {1} waited {2}s for similar tasks to finish, but timeout was reached'.format(
    #                             task_name, params_info, slept),
    #                         level='error')
    #
    #                     exception_message = 'Could not start within timeout of {0}s while waiting for other tasks'.format(
    #                         timeout)
    #                     ensure_single_container.unittest_set_state_exception(exception_message)
    #                     raise EnsureSingleTimeoutReached(
    #                         'Ensure single {0} mode - ID {1} - Task {2} could not be started within timeout of {3}s'.format(
    #                             mode,
    #                             now,
    #                             task_name,
    #                             timeout))
    #                 if unittest_mode is True:
    #                     if thread_name not in Decorators.unittest_thread_info_by_state['WAITING']:
    #                         Decorators.unittest_thread_info_by_state['WAITING'].append(thread_name)
    #
    #     log_message('New task {0} {1} scheduled for execution'.format(task_name, params_info))
    #     update_value(key=persistent_key,
    #                  append=True,
    #                  value_to_update={'kwargs': kwargs_dict,
    #                                   'task_id': task_id,
    #                                   'timestamp': now})

    @ensure_run_time_info()
    @contextmanager
    def lock_default_mode(self):
        # type: () -> None
        """
        Lock function racing for default ensure single mode
        Checks all possible task names
        :raises: EnsureSingleTaskDiscarded: If the task was discarded
        :raises: EnsureSingleDoCallBack: If the task is required to call the callback function instead of the task function
        """
        # @todo instead of volatile mutex, do asserts within transaction
        # Acquire
        try:
            self.run_hook('before_validation')
            with volatile_mutex(self.persistent_key, wait=5):
                for task in self.ensure_single_container.task_names:
                    key_to_check = '{0}_{1}'.format(ENSURE_SINGLE_KEY, task)
                    if self.persistent_client.exists(key_to_check):
                        # Raising errors as potential long functions should not be invoked in the acquire phase
                        if self.async_task or not self.ensure_single_container.callback:
                            self.discard_task()
                        else:
                            self.callback_task()
            self.logger.info(self.message.format('Setting key {0}'.format(self.persistent_key)))
            self.persistent_client.set(self.persistent_key, {'mode': self.ensure_single_container.mode, 'values': [{'task_id': self.task_id}]})
        finally:
            self.run_hook('after_validation')

        success = True
        self.unittest_set_state_executing()
        self.run_hook('before_execution')
        try:
            yield
        except:
            success = False
            raise
        finally:
            if success:
                self.unittest_set_state_finished()
                self.logger.info(self.message.format('Task {0} finished successfully'.format(self.ensure_single_container.task_name)))
            # Release
            # @todo remove mutex and use asserts instead
            with volatile_mutex(self.persistent_key, wait=5):
                self.logger.info(self.message.format('Deleting key {0}'.format(self.persistent_key)))
                self.persistent_client.delete(self.persistent_key, must_exist=False)
            self.run_hook('after_execution')

    def discard_task(self, message=''):
        # type: (str) -> None
        """
        Discard the execution of a task by raising EnsureSingleTaskDiscarded
        :return: None
        :rtype: NoneType
        :raises: EnsureSingleTaskDiscarded
        """
        discard_message = message or 'Execution of task {0} discarded'.format(self.ensure_single_container.task_name)
        self.logger.info(self.message.format(discard_message))
        self.unittest_set_state_discarded()
        self.run_hook('before_discard')
        raise EnsureSingleTaskDiscarded(discard_message)

    def callback_task(self, message=''):
        # type: (str) -> None
        """
        Discard the execution of a task by raising EnsureSingleDoCallBack
        :return: None
        :rtype: NoneType
        :raises: EnsureSingleDoCallBack
        """
        self.unittest_set_state_callback()
        self.run_hook('before_callback')
        callback_message = message or 'Execution of task {0} in progress, execute callback function instead'.format(self.ensure_single_container.task_name)
        self.logger.info(self.message.format(callback_message))
        raise EnsureSingleDoCallBack()

    def do_discard(self):
        # type: () -> None
        """
        Discard the task
        The easiest way to discard is to never allow the celery task to run by returning prematurely
        :return: None
        """
        self.run_hook('after_discard')
        return None

    def do_callback(self, *args, **kwargs):
        # type: (*any, **any) -> any
        """
        Execute the given callback (if any)
        """
        if not self.ensure_single_container.callback:
            error_message = 'No callback passed and callback invocation is requested!'
            self.logger.error(error_message)
            raise ValueError(error_message)

        # This log message might be off if when running it outside the decorators, which isn't supposed to be done
        callback_message = 'Execution of task {0} in progress, executing callback function'.format(self.ensure_single_container.task_name)
        self.logger.info(self.message.format(callback_message))
        result = self.ensure_single_container.callback(*args, **kwargs)
        self.run_hook('after_callback')
        return result

    def run_hook(self, hook):
        # type: (str) -> None
        """
        Run a hook
        :param hook: Hook identifier to run
        :type hook: str
        :return: None
        :rtype: NoneType
        """
        if self.unittest_mode:
            hook_func = self.ensure_single_container.hooks.get(hook)
            if not hook_func:
                return
            hook_func()

    def gather_run_time_info(self):
        # type: () -> None
        """
        Retrieve some information only acquirable when going to execute the code
        """
        self.now = '{0}_{1}'.format(int(time.time()), ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(10)))
        self.thread_name = threading.current_thread().getName()
        self.unittest_mode = os.environ.get('RUNNING_UNITTESTS') == 'True'
        if self.unittest_mode:
            self.message = 'Ensure single {0} mode - ID {1} - {2} - {{0}}'.format(self.ensure_single_container.mode, self.now, self.thread_name)
        else:
            self.message = 'Ensure single {0} mode - ID {1} - {{0}}'.format(self.ensure_single_container.mode, self.now)

    def unittest_set_state(self, state, value):
        # type: (str, any) -> None
        """
        Set the function state to executing
        Only used for unittesting
        :param state: State to set
        :type state: str
        :param value: Value to add
        :type value: any
        :return: None
        """
        if self.unittest_mode:
            # @todo remove
            self.logger.info('Setting {} to {}'.format(self.thread_name, (state, value)))
            Decorators.unittest_thread_info_by_name[self.thread_name] = (state, value)

    def unittest_set_state_executing(self):
        # type: () -> None
        """
        Set the function state to executing
        Only used for unittesting
        :return: None
        """
        return self.unittest_set_state('EXECUTING', None)

    def unittest_set_state_callback(self):
        # type: () -> None
        """
        Set the function state to callback
        Only used for unittesting
        :return: None
        """
        return self.unittest_set_state('CALLBACK', None)

    def unittest_set_state_discarded(self):
        # type: () -> None
        """
        Set the function state to discarded
        Only used for unittesting
        :return: None
        """
        return self.unittest_set_state('DISCARDED', None)

    def unittest_set_state_waited(self):
        # type: () -> None
        """
        Set the function state to waited
        Only used for unittesting
        :return: None
        """
        return self.unittest_set_state('WAITED', None)

    def unittest_set_state_waiting(self):
        # type: () -> None
        """
        Set the function state to waiting
        Only used for unittesting
        :return: None
        """
        return self.unittest_set_state('WAITING', None)

    def unittest_set_state_finished(self):
        # type: () -> None
        """
        Set the function state to finished
        Only used for unittesting
        :return: None
        """
        self.unittest_set_state('FINISHED', None)
        if self.unittest_mode:
            Decorators.unittest_thread_info_by_state['FINISHED'].append(self.thread_name)

    def unittest_set_state_exception(self, exception_message):
        # type: (str) -> None
        """
        Set the function state to waiting
        Only used for unittesting
        :param exception_message: Exception message to set
        :type exception_message: str
        :return: None
        """
        return self.unittest_set_state('EXCEPTION', exception_message)


def _ensure_single(task_name, mode, extra_task_names=None, global_timeout=300, callback=None, ignore_arguments=None):
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
    :param ignore_arguments: Arguments to ignore when CHAINED or DEDUPED. These arguments will not be accounted for
    when checking if a task is already queued up
    :type ignore_arguments: list
    :return: Pointer to function
    :rtype: func
    """
    logger = LogHandler.get('lib', name='ensure single')

    def wrap(f):
        """
        Wrapper function
        :param f: Function to check
        """
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

            if mode == 'DEDUPED':
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
