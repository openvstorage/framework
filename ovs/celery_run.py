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
Celery entry point module
"""

from __future__ import absolute_import

import os
import uuid
import yaml
import threading
import traceback
from celery import Celery
from celery.backends import BACKEND_ALIASES
from celery.signals import task_postrun, worker_process_init, after_setup_logger, after_setup_task_logger
from celery.task.control import inspect
from kombu import Queue
from kombu.serialization import register
from threading import Thread
from ovs.extensions.celery.extendedyaml import YamlExtender
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.system import System
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.lib.helpers.exceptions import EnsureSingleTimeoutReached
from ovs.lib.messaging import MessageController
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs_extensions.storage.exceptions import KeyNotFoundException


class CeleryMockup(object):
    """
    Mockup class for Celery
    """
    def task(self, *args, **kwargs):
        """
        Celery task mockup
        Is used as a decorator, so should return a function, which does nothing (for now)
        """
        _ = self

        def _wrapper(func):
            def _wrapped(*arguments, **kwarguments):
                _ = arguments, kwarguments
                if kwargs.get('bind'):
                    return func(type('Task', (), {'request': type('Request', (), {'id': None})}), *arguments, **kwarguments)
                return func(*arguments, **kwarguments)

            def _delayed(*arguments, **kwarguments):
                async_result = {'name': kwargs.get('name', args[0] if len(args) > 0 else None),
                                'id': str(uuid.uuid4()),
                                'thread': None,
                                'exception': None}

                def _catch_errors_in_function(*more_args, **more_kwargs):
                    try:
                        InspectMockup.states['active'].append(async_result)
                        async_result['result'] = func(*more_args, **more_kwargs)
                        InspectMockup.states['active'].remove(async_result)
                    except EnsureSingleTimeoutReached as ex:
                        async_result['exception'] = ex
                    except Exception as ex:
                        traceback.print_exc()
                        async_result['exception'] = ex

                if 'bind' in kwargs:
                    arguments = tuple([type('Task', (), {'request': type('Request', (), {'id': async_result['id']})})] + list(arguments))
                if '_thread_name' in kwarguments:
                    thread_name = kwarguments.pop('_thread_name')
                else:
                    thread_name = threading.current_thread().getName()
                thread = Thread(target=_catch_errors_in_function, name='{0}_delayed'.format(thread_name), args=arguments, kwargs=kwarguments)
                async_result['thread'] = thread
                thread.start()
                return async_result

            _wrapped.delay = _delayed
            _wrapped.__name__ = func.__name__
            _wrapped.__module__ = func.__module__
            return _wrapped
        return _wrapper


class InspectMockup(object):
    """
    Mockup class for the inspect module
    """
    states = {'active': []}
    state_keys = ['active']

    def __init__(self):
        pass

    @staticmethod
    def clean():
        for key in InspectMockup.state_keys:
            InspectMockup.states[key] = []

    def __getattr__(self, item):
        return lambda: {'unittests': InspectMockup.states[item]}


ovs_logger = Logger('celery')
if os.environ.get('RUNNING_UNITTESTS') == 'True':
    inspect = InspectMockup
    celery = CeleryMockup()
else:
    # Update the BACKEND_ALIASES when this item is loaded in (to support the Arakoon backend)
    BACKEND_ALIASES.update({'arakoon': 'ovs.extensions.celery.arakoonresult:ArakoonResultBackend'})
    # Register the YAML encoder
    register('ovsyaml', YamlExtender.ordered_dump, yaml.safe_load, content_type='application/x-yaml', content_encoding='utf-8')
    memcache_servers = Configuration.get('/ovs/framework/memcache|endpoints')
    rmq_servers = Configuration.get('/ovs/framework/messagequeue|endpoints')

    unique_id = System.get_my_machine_id()

    include = []
    path = '/'.join([os.path.dirname(__file__), 'lib'])
    for filename in os.listdir(path):
        if os.path.isfile('/'.join([path, filename])) and filename.endswith('.py') and filename != '__init__.py':
            name = filename.replace('.py', '')
            include.append('ovs.lib.{0}'.format(name))

    celery = Celery('ovs', include=include)

    # http://docs.celeryproject.org/en/latest/configuration.html#cache-backend-settings
    celery.conf.CELERY_RESULT_BACKEND = 'arakoon'
    celery.conf.BROKER_URL = ';'.join(['{0}://{1}:{2}@{3}//'.format(Configuration.get('/ovs/framework/messagequeue|protocol'),
                                                                    Configuration.get('/ovs/framework/messagequeue|user'),
                                                                    Configuration.get('/ovs/framework/messagequeue|password'),
                                                                    server)
                                       for server in rmq_servers])
    celery.conf.BROKER_CONNECTION_MAX_RETRIES = 5
    celery.conf.BROKER_HEARTBEAT = 10
    celery.conf.BROKER_HEARTBEAT_CHECKRATE = 2
    celery.conf.CELERY_DEFAULT_QUEUE = 'ovs_generic'
    celery.conf.CELERY_QUEUES = tuple([Queue('ovs_generic', routing_key='generic.#'),
                                       Queue('ovs_masters', routing_key='masters.#'),
                                       Queue('ovs_{0}'.format(unique_id), routing_key='sr.{0}.#'.format(unique_id))])
    celery.conf.CELERY_DEFAULT_EXCHANGE = 'generic'
    celery.conf.CELERY_DEFAULT_EXCHANGE_TYPE = 'topic'
    celery.conf.CELERY_DEFAULT_ROUTING_KEY = 'generic.default'
    celery.conf.CELERYD_PREFETCH_MULTIPLIER = 1  # This makes sure that the workers won't be pre-fetching tasks, this to prevent deadlocks
    celery.conf.CELERYBEAT_SCHEDULE = {}
    celery.conf.CELERY_TRACK_STARTED = True  # http://docs.celeryproject.org/en/latest/configuration.html#std:setting-CELERY_TRACK_STARTED
    celery.conf.CELERYD_HIJACK_ROOT_LOGGER = False
    celery.conf.CELERY_RESULT_SERIALIZER = 'ovsyaml'  # Change default pickle to ovsyaml as it support more typing than JSON


@task_postrun.connect
def task_postrun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, **kwds):
    """
    Hook for celery post-run event
    The front-end uses these events to resolve the tasks instead of longpolling on the task api
    """
    _ = sender, task, args, kwargs, kwds
    try:
        MessageController.fire(MessageController.Type.TASK_COMPLETE, task_id)
    except Exception:
        ovs_logger.exception('Caught error during post-run handler')


@worker_process_init.connect
def worker_process_init_handler(args=None, kwargs=None, **kwds):
    """
    Hook for process init
    """
    _ = args, kwargs, kwds
    VolatileFactory.store = None
    PersistentFactory.store = None


@after_setup_task_logger.connect
@after_setup_logger.connect
def load_ovs_logger(**kwargs):
    """Load a logger."""
    if 'logger' in kwargs:
        kwargs['logger'].handlers = [ovs_logger.handlers[0]]  # Overrule the default celery handlers with OVSes custom handler


def _get_registration_update_transaction():
    """
    Gets the transaction to execute
    - Checks if the current task registrations are still active
    - All registrations that can be discarded will be discarded
    - If no active registrations are found, the task registration key will get removed
    :return: Transaction guid
    :rtype: str
    """
    from ovs.lib.helpers.decorators import ENSURE_SINGLE_KEY

    active = inspect().active()
    active_task_ids = []
    # Retrieve active tasks from celery
    if active:
        for tasks in active.itervalues():
            active_task_ids += [task['id'] for task in tasks]

    persistent = PersistentFactory.get_client()
    transaction = persistent.begin_transaction()

    for key in persistent.prefix(ENSURE_SINGLE_KEY):
        # Yield task registration keys which are <ensure_single_key>_<task_name>_<ensure_single_mode>
        try:
            initial_registrations = persistent.get(key)
            if not initial_registrations:
                continue
            # Filter out all the tasks are are no longer running within celery
            # @todo might be better to do in update code?
            # Transition from dict to list. Key must also change
            if isinstance(initial_registrations, dict):
                mode = initial_registrations['mode']
                registrations = initial_registrations['values']
                key_to_save = '{0}_{1}'.format(key, mode.lower())
            else:
                registrations = initial_registrations
                key_to_save = key

            running_registrations = []
            for registration in registrations:
                task_id = registration.get('task_id')
                if task_id and task_id in active_task_ids:
                    running_registrations.append(registration)
            if running_registrations:
                if running_registrations == registrations:
                    # No changes required to be made
                    continue
                persistent.assert_value(key, initial_registrations, transaction=transaction)
                if key_to_save != key:
                    # Delete the old key
                    persistent.delete(key, transaction=transaction)
                persistent.set(key_to_save, running_registrations, transaction=transaction)
                ovs_logger.info('Updating key {0}'.format(key))
            elif not running_registrations:
                persistent.assert_value(key, initial_registrations, transaction=transaction)
                persistent.delete(key, transaction=transaction)
                ovs_logger.info('Deleting key {0}'.format(key))
        except KeyNotFoundException:
            pass
    return transaction


def _clean_cache():

    ovs_logger.info('Executing celery "clear_cache" startup script...')
    persistent = PersistentFactory.get_client()

    persistent.apply_callback_transaction(_get_registration_update_transaction, max_retries=5)
    ovs_logger.info('Executing celery "clear_cache" startup script... done')


if __name__ == '__main__':
    import sys
    if len(sys.argv) == 2 and sys.argv[1] == 'clear_cache':
        _clean_cache()
