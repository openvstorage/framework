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
from ovs_extensions.storage.exceptions import KeyNotFoundException
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.lib.helpers.exceptions import EnsureSingleTimeoutReached, EnsureSingleDoCallBack, EnsureSingleTaskDiscarded,\
    EnsureSingleNoRunTimeInfo, EnsureSingleSimilarJobsCompleted
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

        bind = False
        if ensure_single_info != {}:
            bind = True
            ensure_single_container = EnsureSingleContainer(task_name=name, **ensure_single_info)
            if ensure_single_container.mode == 'DEFAULT':
                f = ensure_single_default(ensure_single_container)(f)
            elif ensure_single_container.mode == 'DEDUPED':
                f = ensure_single_deduped(ensure_single_container)(f)
            elif ensure_single_container.mode == 'CHAINED':
                f = ensure_single_chained(ensure_single_container)(f)
            else:
                raise ValueError('Unsupported mode "{0}" provided'.format(ensure_single_container.mode))
        return celery.task(name=name, schedule=schedule, bind=bind)(f)
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
            ensure_single_kwargs = ensure_single.get_ensure_single_runtime_kwargs(kwargs)
            ensure_single.update_runtime_arguments(**ensure_single_kwargs)
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

    Note: the 'ensure_single_timeout' keyword is reserved on decorator function their arguments.

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
            default_timeout = 10 if ensure_single.unittest_mode else ensure_single_container.global_timeout
            timeout = kwargs.pop('ensure_single_timeout', default_timeout)
            kwargs_dict, ensure_single_kwargs = ensure_single.get_all_arguments_as_kwargs(f, args, kwargs)
            ensure_single.update_runtime_arguments(**ensure_single_kwargs)
            try:
                with ensure_single.lock_deduped_mode(kwargs_dict, timeout):
                    return f(*args, **kwargs)
            except EnsureSingleTaskDiscarded:
                return ensure_single.do_discard()
            except EnsureSingleDoCallBack:
                return ensure_single.do_callback(*args, **kwargs)
            except EnsureSingleSimilarJobsCompleted:
                # Nothing to do here
                return None

        return ensure_single_deduped_inner
    return ensure_single_deduped_wrap


def ensure_single_chained(ensure_single_container):
    # type: (EnsureSingleContainer) -> callable
    """
    Decorator ensuring a new task cannot be started in case a certain task is
    running, scheduled or reserved.

    Keep also in mind that validation will be executed by the worker itself, so if the task is scheduled on
    a worker currently processing a "duplicate" task, it will only get validated after the first
    one completes, which will result in the fact that the task will execute normally.

    Mode: CHAINED: Identical as DEDUPED with the exception that tasks will be executed in serial.
                 - Tasks with identical arguments will be executed in serial (Subsequent tasks with same params will be discarded if 1 waiting task with these params already in queue)
                 - Tasks with different arguments will be executed in serial

    Note: the 'ensure_single_timeout' keyword is reserved on decorator function their arguments.

    :param ensure_single_container: EnsureSingle container instance
    :type ensure_single_container: EnsureSingleContainer
    :return: Pointer to function
    :rtype: func
    """

    def ensure_single_chained_wrap(f):
        """
        Wrapper function
        :param f: Function to wrap
        """

        @wraps(f)
        def ensure_single_chained_inner(self, *args, **kwargs):
            """
            Wrapped function
            :param self: With bind=True, the celery task result itself is passed in
            :param args: Arguments without default values
            :param kwargs: Arguments with default values
            """
            ensure_single = EnsureSingle(ensure_single_container, self)
            default_timeout = 10 if ensure_single.unittest_mode else ensure_single_container.global_timeout
            timeout = kwargs.pop('ensure_single_timeout', default_timeout)
            kwargs_dict, ensure_single_kwargs = ensure_single.get_all_arguments_as_kwargs(f, args, kwargs)
            ensure_single.update_runtime_arguments(**ensure_single_kwargs)
            try:
                with ensure_single.lock_chained_mode(kwargs_dict, timeout):
                    return f(*args, **kwargs)
            except EnsureSingleTaskDiscarded:
                return ensure_single.do_discard()
            except EnsureSingleDoCallBack:
                return ensure_single.do_callback(*args, **kwargs)
            except EnsureSingleSimilarJobsCompleted:
                # Nothing to do here
                return None

        return ensure_single_chained_inner
    return ensure_single_chained_wrap


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
        self.persistent_key = self.generate_key_for_task(ensure_single_container.task_name)
        self.persistent_client = PersistentFactory.get_client()
        self.task_id, self.async_task = self.get_task_id_and_async(task)

        # Logging
        self.logger = LogHandler.get('lib', name='ensure single')

        # Runtime
        self.now = None
        self.thread_name = None
        self.unittest_mode = None
        self.message = None
        self.runtime_hooks = {}
        self.gather_run_time_info()

    @property
    def poll_sleep_time(self):
        # type: () -> float
        """
        Polling sleep time
        :return: The sleep time
        :rtype: float
        """
        if self.unittest_mode:
            return 0.1
        return 1.0

    @property
    def task_registration_key(self):
        # type: () -> str
        """
        Key to register tasks under
        Combines the persistent key together with the mode
        :return: The created key
        :rtype: str
        """
        return self.generate_key_for_task_with_mode(self.ensure_single_container.task_name)

    def set_task(self, task):
        # type: (celery.AsyncResult) -> None
        """
        Set the task context in which this ensure single is running
        :param task: Task to set
        :type task: celery.AsyncResult
        :return: None
        """
        self.task_id, self.async_task = self.get_task_id_and_async(task)

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
            self.logger.error('Extra tasks are not allowed in this mode')
            raise ValueError('Ensure single {0} mode - ID {1} - Extra tasks are not allowed in this mode'.format(self.ensure_single_container.mode, self.now))

    def update_runtime_arguments(self, ensure_single_runtime_hooks=None):
        # type: (dict) -> None
        """
        Update the state of the object with arguments fetched at runtime
        :param ensure_single_runtime_hooks: Hooks to run at runtime. Same behaviour as the hooks of the ensure
        single container
        :return: None
        """
        if ensure_single_runtime_hooks:
            # Do not update the ensure single container hooks as they will be updated for every function
            self.runtime_hooks.update(ensure_single_runtime_hooks)

    @classmethod
    def get_all_arguments_as_kwargs(cls, func, args, kwargs):
        # type: (callable, tuple, dict) -> Tuple[dict, dict]
        """
        Retrieve all arguments passed to a function as key-word arguments
        Will pop reserved keywords
        - ensure_single_runtime_hooks: Hooks given at runtime for the ensure single
        :param func: Function to get all arguments as key-word arguments for
        :type func: callable
        :param args: Arguments passed to the function
        :type args: tuple
        :param kwargs: Key words passed to the functoin
        :type kwargs: dict
        :return: Tuple with Key word arguments of the function and extra arguments for the ensure single
        :rtype: Tuple[dict, dict]
        """
        function_info = inspect.getargspec(func)
        kwargs_dict = {}
        for index, arg in enumerate(args):
            kwargs_dict[function_info.args[index]] = arg
        kwargs_dict.update(kwargs)
        ensure_single_kwargs = cls.get_ensure_single_runtime_kwargs(kwargs_dict)
        # Pop all keys returned by ensure_single
        for key in ensure_single_kwargs:
            kwargs.pop(key, None)

        return kwargs_dict, ensure_single_kwargs

    @staticmethod
    def get_ensure_single_runtime_kwargs(kwargs):
        # type: (dict) -> dict
        """
        Retrieve a dict with all ensure_single runtime kwargs
        Note: will mutate the passed on kwargs
        :param kwargs: Kwargs passed ot the function
        :type kwargs: dict
        :return:
        """
        # ensure_single_timeout is an argument that could be here but it does not fit because of the default value
        reserved_keywords = ['ensure_single_runtime_hooks']
        ensure_single_kwargs = {}
        for reserved_keyword in reserved_keywords:
            if reserved_keyword in kwargs:
                ensure_single_kwargs[reserved_keyword] = kwargs.pop(reserved_keyword)
        return ensure_single_kwargs

    def get_task_registrations(self):
        # type: () -> Tuple[List[any], any]
        """
        Retrieve all current task registrations. When the key is not present, it will return an empty list
        :return: The current registrations and the original value of the fetched items
        :rtype: Tuple[List[any], any]
        """
        initial_registrations = None  # Used for asserting
        try:
            current_registration = self.persistent_client.get(self.task_registration_key)
            initial_registrations = current_registration
        except KeyNotFoundException:
            current_registration = []
        return current_registration, initial_registrations

    def is_task_still_registered(self, timestamp_identifier=None):
        # type: (str) -> bool
        """
        Checks if the current task is still amongst the registration
        :param timestamp_identifier: Identifier with a timestamp. Format <timestamp>_<identifier>
        :type timestamp_identifier: str
        :return: True if the task is still registered else False
        :rtype: bool
        """
        timestamp_identifier = timestamp_identifier or self.now
        current_registrations, initial_registrations = self.get_task_registrations()
        # No registrations means that all pending jobs have been deleted in the meantime, no need to wait
        # Timestamp no longer in the registrations mean that all similar tasks have been executed
        # In case of a sync task, that means it does not need to be executed anymore
        return len(current_registrations) > 0 and next((t_d for t_d in current_registrations if timestamp_identifier == t_d['timestamp']), None) is not None

    @staticmethod
    def generate_key_for_task(task_name):
        # type: (str) -> str
        """
        Generate a persistent key for a task
        :param task_name: Name of the task to generate the key for
        :type task_name: str
        :return: The generated key
        :rtype: str
        """
        return '{0}_{1}'.format(ENSURE_SINGLE_KEY, task_name)

    def generate_key_for_task_with_mode(self, task_name):
        # type: (str) -> str
        """
        Generate a persistent key for a task with mode in it
        :param task_name: Name of the task to generate the key for
        :type task_name: str
        :return: The generated key
        :rtype: str
        """
        return '{0}_{1}'.format(self.generate_key_for_task(task_name), self.ensure_single_container.mode.lower())

    def poll_task_completion(self, timeout, timestamp_identifier, task_log=None):
        # type: (float, str, str) -> None
        """
        Poll for the task to complete
        :param timeout: Stop polling after a set timeout
        :type timeout: float
        :param task_log: Task logging string. Default to 'Task <task_name>'
        :type task_log: str
        :param timestamp_identifier: Identifier with a timestamp. Format <timestamp>_<identifier>
        Used to check if the task to poll still exists
        :type timestamp_identifier: str
        :return:
        """
        # @todo better to use a thread for this?
        task_log = task_log or 'Task {0}'.format(self.ensure_single_container.task_name)

        # Let's wait for 2nd job in queue to have finished if no callback provided
        slept = 0
        while slept < timeout:
            self.logger.info('{0} is waiting for similar tasks to finish - ({1})'.format(task_log, slept + self.poll_sleep_time))
            if not self.is_task_still_registered(timestamp_identifier):
                self.discard_task_similar_jobs()
            slept += self.poll_sleep_time
            time.sleep(self.poll_sleep_time)
            if slept >= timeout:
                self.logger.error('{0} waited {1}s for similar tasks to finish, but timeout was reached'.format(task_log, slept))
                exception_message = 'Could not start within timeout of {0}s while waiting for other tasks'.format(timeout)
                self.unittest_set_state_exception(exception_message)
                timeout_message = '{0} - {1} could not be started within timeout of {2}s'.format(self.message, task_log, timeout)
                raise EnsureSingleTimeoutReached(timeout_message)

            self.unittest_set_state_waiting()

    def _ensure_job_limit(self, kwargs_dict, timeout, task_logs, job_limit=2):
        # type: (dict, float, Tuple[str, str], int) -> None
        """
        Ensure that the number of jobs don't exceed the limit
        - Test if the current job limit is not reached
        - If job limit is reached: check if the task should be discarded, callbacked or waiting for a job to start
        :param kwargs_dict: Dict containing all arguments as key word arguments
        :type kwargs_dict: dict
        :param timeout: Polling timeout in seconds
        :type timeout: float
        :param task_logs: Log identification for the tasks
        :type task_logs: Tuple[str, str]
        :param job_limit: Number of jobs to keep running at any given time
        :type job_limit: int
        :return: None
        :raises: EnsureSingleTaskDiscarded: If the task was discarded
        :raises: EnsureSingleDoCallBack: If the task is required to call the callback function instead of the task function
        :raises: EnsureSingleSimilarJobsCompleted: If the task was waiting on a similar task to end but those tasks have finished
        """
        task_log_name, task_log_id = task_logs

        current_registrations, initial_registrations = self.get_task_registrations()

        job_counter = 0
        for task_data in current_registrations:
            if task_data['kwargs'] != kwargs_dict:
                continue
            # Another job with the same registration
            job_counter += 1
            if job_counter == job_limit:
                if self.async_task:  # Not waiting for other jobs to finish since asynchronously
                    discard_message = 'Execution of {0} discarded because of identical parameters'.format(task_log_id)
                    self.discard_task(discard_message)

                # If executed inline (sync), execute callback if any provided
                if self.ensure_single_container.callback:
                    callback_message = 'Execution of {0} in progress, execute callback function'.format(task_log_name)
                    self.callback_task(callback_message)

                # Let's wait for 2nd job in queue to have finished if no callback provided
                self.poll_task_completion(timeout=timeout, timestamp_identifier=task_data['timestamp'])

    def _register_task(self, registration_data):
        # type: (any) -> list
        """
        Registers the execution of the task
        :param registration_data: Dict containing all arguments as key word arguments
        :type registration_data: any
        :return: All registrations
        :rtype: list
        """
        # @todo use transactions instead
        with volatile_mutex(self.persistent_key, wait=5):
            current_registrations, initial_registrations = self.get_task_registrations()
            current_registrations.append(registration_data)
            self.persistent_client.set(self.task_registration_key, current_registrations)
        return current_registrations

    def _unregister_task(self, task_data=None, delete=False):
        # type: (dict, bool) -> None
        """
        Unregisters the execution of the task
        :param task_data: Dict containing all information about the task to unregister
        :type task_data: dict
        :param delete: Delete the registration key
        :type delete: bool
        :return: None
        """
        # @todo use transaction
        with volatile_mutex(self.persistent_key, wait=5):
            if task_data:
                current_registrations, initial_registrations = self.get_task_registrations()
                try:
                    current_registrations.remove(task_data)
                    self.persistent_client.set(self.task_registration_key, current_registrations)
                except ValueError:
                    # Registration was already removed
                    pass
            if delete:
                self.logger.info(self.message.format('Deleting key {0}'.format(self.task_registration_key)))
                self.persistent_client.delete(self.task_registration_key, must_exist=False)

    def _filter_ignorable_arguments(self, kwargs_dict):
        # type: (dict) -> dict
        """
        Filter out all ignorable arguments from the passed keywords
        :param kwargs_dict: Dict containing all arguments as key word arguments
        :type kwargs_dict: dict
        :return: The filtered dict
        :rtype: dict
        """
        filtered_kwargs = kwargs_dict.copy()
        for key in self.ensure_single_container.ignore_arguments:
            filtered_kwargs.pop(key, None)
        return filtered_kwargs

    @ensure_run_time_info()
    @contextmanager
    def lock_chained_mode(self, kwargs, timeout):
        """
        Lock the function in chained mode
        :param kwargs: Dict containing all arguments as key word arguments
        :type kwargs: dict
        :param timeout: Polling timeout in seconds
        :type timeout: float
        :raises: EnsureSingleTaskDiscarded: If the task was discarded
        :raises: EnsureSingleDoCallBack: If the task is required to call the callback function instead of the task function
        :raises: EnsureSingleSimilarJobsCompleted: If the task was waiting on a similar task to end but those tasks have finished
        """
        self.validate_no_extra_names()

        # Update kwargs with args
        kwargs_dict = self._filter_ignorable_arguments(kwargs)
        params_info = 'with params {0}'.format(kwargs_dict) if kwargs_dict else 'with default params'
        task_log_format = 'task {0} {1}'
        task_log_name = task_log_format.format(self.ensure_single_container.task_name, params_info)
        task_log_id = task_log_format.format(self.task_id, params_info)

        try:
            self.run_hook('before_validation')
            # Validate whether another job with same params is being executed, skip if so
            # 1st registration is a running job, we check all other queued jobs for identical params
            self._ensure_job_limit(kwargs_dict, timeout, (task_log_name, task_log_id), job_limit=2)

            self.logger.info('New {0} scheduled for execution'.format(task_log_name))
            new_task_data = {'kwargs': kwargs_dict, 'task_id': self.task_id, 'timestamp': self.now}
            self._register_task(new_task_data)

            # Poll the arakoon to see whether this call is the first in list, if so --> execute, else wait
            first_registration = None
            slept = 0
            while slept < timeout:
                current_registrations, initial_registrations = self.get_task_registrations()
                if current_registrations:
                    first_registration = current_registrations[0]['timestamp']
                if first_registration == self.now:
                    break

                self.unittest_set_state_waiting()
                slept += self.poll_sleep_time
                time.sleep(self.poll_sleep_time)
        finally:
            self.run_hook('after_validation')

        successful = True
        try:
            if slept:
                if slept >= timeout:
                    self.logger.error('Could not start {0}, within expected time ({1}s). Removed it from queue'.format(task_log_name, timeout))
                    self.unittest_set_state_exception('Could not start within timeout of {0}s while queued'.format(timeout))
                    timeout_message = '{0} - Task {1} could not be started within timeout of {2}s'.format(self.message, self.ensure_single_container.task_name, timeout)
                    raise EnsureSingleTimeoutReached(timeout_message)
                else:
                    self.logger.info('{0} had to wait {1} seconds before being able to start'.format(task_log_name, slept))
            self.unittest_set_state_executing()
            self.run_hook('before_execution')
            yield
        # Release
        except:
            successful = False
            raise
        finally:
            if successful:
                self.unittest_set_state_finished()
                self.logger.info('Task {0} finished successfully'.format(self.ensure_single_container.task_name))
            self._unregister_task(new_task_data)
            self.run_hook('after_execution')

    @ensure_run_time_info()
    @contextmanager
    def lock_deduped_mode(self, kwargs, timeout):
        # type: (dict, float) -> None
        """
        Lock the function in deduped mode
        :param kwargs: Dict containing all arguments as key word arguments
        :type kwargs: dict
        :param timeout: Polling timeout in seconds
        :type timeout: float
        :raises: EnsureSingleTaskDiscarded: If the task was discarded
        :raises: EnsureSingleDoCallBack: If the task is required to call the callback function instead of the task function
        :raises: EnsureSingleSimilarJobsCompleted: If the task was waiting on a similar task to end but those tasks have finished
        """
        self.validate_no_extra_names()

        kwargs_dict = self._filter_ignorable_arguments(kwargs)
        params_info = 'with params {0}'.format(kwargs_dict) if kwargs_dict else 'with default params'
        task_log_format = 'task {0} {1}'
        task_log_name = task_log_format.format(self.ensure_single_container.task_name, params_info)
        task_log_id = task_log_format.format(self.task_id, params_info)

        # Acquire
        try:
            self.run_hook('before_validation')
            # Validate whether another job with same params is being executed
            # 1st job with same params is being executed, 2nd is scheduled for execution ==> Discard current
            self._ensure_job_limit(kwargs_dict, timeout, (task_log_name, task_log_id), job_limit=2)

            self.logger.info('New {0} scheduled for execution'.format(task_log_name))
            new_task_data = {'kwargs': kwargs_dict, 'task_id': self.task_id, 'timestamp': self.now}
            self._register_task(new_task_data)

            # Poll the arakoon to see whether this call is the only in list, if so --> execute, else wait
            slept = 0
            while slept < timeout:
                current_registrations, initial_registrations = self.get_task_registrations()
                queued_jobs = [t_d for t_d in current_registrations if t_d['kwargs'] == kwargs_dict]
                if len(queued_jobs) == 1:
                    # The only queued job. No more need to poll
                    break
                self.unittest_set_state_waiting()
                slept += self.poll_sleep_time
                time.sleep(self.poll_sleep_time)
        finally:
            self.run_hook('after_validation')

        successful = True
        try:
            if slept:
                if slept >= timeout:
                    self.logger.error('Could not start {0}, within expected time ({1}s). Removed it from queue'.format(task_log_name, timeout))
                    self.unittest_set_state_exception('Could not start within timeout of {0}s while queued'.format(timeout))
                    timeout_message = '{0} - Task {1} could not be started within timeout of {2}s'.format(self.message, self.ensure_single_container.task_name, timeout)
                    raise EnsureSingleTimeoutReached(timeout_message)
                else:
                    self.logger.info('{0} had to wait {1} seconds before being able to start'.format(task_log_name, slept))
            self.run_hook('before_execution')
            self.unittest_set_state_executing()
            yield
        # Release
        except:
            successful = False
            raise
        finally:
            if successful:
                self.unittest_set_state_finished()
                self.logger.info('Task {0} finished successfully'.format(self.ensure_single_container.task_name))
            self._unregister_task(new_task_data)
            self.run_hook('after_execution')

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
                    key_to_check = self.generate_key_for_task_with_mode(task)
                    if self.persistent_client.exists(key_to_check):
                        # Raising errors as potential long functions should not be invoked in the acquire phase
                        if self.async_task or not self.ensure_single_container.callback:
                            self.discard_task()
                        else:
                            self.callback_task()
            self.logger.info(self.message.format('Setting key {0}'.format(self.persistent_key)))
            self._register_task({'task_id': self.task_id})
        finally:
            self.run_hook('after_validation')

        success = True
        self.unittest_set_state_executing()
        self.run_hook('before_execution')
        try:
            yield
        # Release
        except:
            success = False
            raise
        finally:
            if success:
                self.unittest_set_state_finished()
                self.logger.info(self.message.format('Task {0} finished successfully'.format(self.ensure_single_container.task_name)))
            # Release
            self._unregister_task(delete=True)
            self.run_hook('after_execution')

    def discard_task_similar_jobs(self):
        """
        Discard the execution of a task by raising EnsureSingleSimilarJobsCompleted
        :return: None
        :rtype: NoneType
        :raises: EnsureSingleSimilarJobsCompleted
        """
        self.unittest_set_state_waited()
        self.run_hook('before_discard')
        raise EnsureSingleSimilarJobsCompleted()

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
            hook_func = self.runtime_hooks.get(hook) or self.ensure_single_container.hooks.get(hook)
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
            self.logger.info('Setting {0} to {1}'.format(self.thread_name, (state, value)))
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
        if self.unittest_mode:
            if self.thread_name not in Decorators.unittest_thread_info_by_state['WAITING']:
                Decorators.unittest_thread_info_by_state['WAITING'].append(self.thread_name)
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
