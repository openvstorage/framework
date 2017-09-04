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
Celery beat module
"""

import os
import imp
import time
import cPickle
import inspect
from celery import current_app
from celery.beat import Scheduler
from celery.schedules import crontab, timedelta
from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonSockNotReadable
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.system import System
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs_extensions.storage.exceptions import KeyNotFoundException
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.lib.helpers.toolbox import Schedule


class DistributedScheduler(Scheduler):
    """
    Distributed scheduler that can run on multiple nodes at the same time.
    """
    TIMEOUT = 60 * 30

    def __init__(self, *args, **kwargs):
        """
        Initializes the distributed scheduler
        """
        self._mutex = volatile_mutex('celery_beat', 10)
        self._logger = Logger('celery')
        self._has_lock = False
        self._lock_name = 'ovs_celery_beat_lock'
        self._entry_name = 'ovs_celery_beat_entries'
        self._persistent = PersistentFactory.get_client()
        self._schedule_info = {}
        super(DistributedScheduler, self).__init__(*args, **kwargs)
        self._logger.debug('DS init')

    def setup_schedule(self):
        """
        Setups the schedule
        """
        self._logger.debug('DS setting up schedule')
        self._load_schedule()
        self.merge_inplace(self._discover_schedule())
        self.install_default_entries(self.schedule)
        for schedule, source in self._schedule_info.iteritems():
            self._logger.debug('* {0} ({1})'.format(schedule, source))
        self._logger.debug('DS setting up schedule - done')

    def _discover_schedule(self):
        schedules = {}
        self._schedule_info = {}
        path = '/'.join([os.path.dirname(__file__), 'lib'])
        for filename in os.listdir(path):
            if os.path.isfile('/'.join([path, filename])) and filename.endswith('.py') and filename != '__init__.py':
                name = filename.replace('.py', '')
                mod = imp.load_source(name, '/'.join([path, filename]))
                for member in inspect.getmembers(mod):
                    if inspect.isclass(member[1]) \
                            and member[1].__module__ == name \
                            and 'object' in [base.__name__ for base in member[1].__bases__]:
                        for submember in inspect.getmembers(member[1]):
                            if hasattr(submember[1], 'schedule') and (isinstance(submember[1].schedule, crontab) or
                                                                      isinstance(submember[1].schedule, timedelta) or
                                                                      isinstance(submember[1].schedule, Schedule)):
                                if isinstance(submember[1].schedule, Schedule):
                                    schedule, source = submember[1].schedule.generate_schedule(submember[1].name)
                                else:
                                    schedule = submember[1].schedule
                                    source = 'crontab or timedelta from code'
                                if schedule is not None:
                                    schedules[submember[1].name] = {'task': submember[1].name,
                                                                    'schedule': schedule,
                                                                    'args': []}
                                self._schedule_info[submember[1].name] = source
        return schedules

    def _load_schedule(self):
        """
        Loads the most recent schedule from the persistent store
        """
        self.schedule = {}
        try:
            self._logger.debug('DS loading schedule entries')
            self._mutex.acquire(wait=10)
            try:
                self.schedule = cPickle.loads(str(self._persistent.get(self._entry_name)))
            except:
                # In case an exception occurs during loading the schedule, it is ignored and the default schedule
                # will be used/restored.
                pass
        finally:
            self._mutex.release()

    def sync(self):
        if self._has_lock is True:
            try:
                self._logger.debug('DS syncing schedule entries')
                self._mutex.acquire(wait=10)
                self._persistent.set(key=self._entry_name,
                                     value=cPickle.dumps(self.schedule))
            except ArakoonSockNotReadable:
                self._logger.exception('Syncing the schedule failed this iteration')
            finally:
                self._mutex.release()
        else:
            self._logger.debug('DS skipping sync: lock is not ours')

    def tick(self):
        """
        Runs one iteration of the scheduler. This is guarded with a distributed lock
        """
        self._logger.debug('DS executing tick')
        try:
            self._has_lock = False
            with self._mutex:
                # noinspection PyProtectedMember
                node_now = current_app._get_current_object().now()
                node_timestamp = time.mktime(node_now.timetuple())
                node_name = System.get_my_machine_id()
                try:
                    lock = self._persistent.get(self._lock_name)
                except KeyNotFoundException:
                    lock = None
                if lock is None:
                    # There is no lock yet, so the lock is acquired
                    self._has_lock = True
                    self._logger.debug('DS there was no lock in tick')
                else:
                    if lock['name'] == node_name:
                        # The current node holds the lock
                        self._logger.debug('DS keeps own lock')
                        self._has_lock = True
                    elif node_timestamp - lock['timestamp'] > DistributedScheduler.TIMEOUT:
                        # The current lock is timed out, so the lock is stolen
                        self._logger.debug('DS last lock refresh is {0}s old'.format(node_timestamp - lock['timestamp']))
                        self._logger.debug('DS stealing lock from {0}'.format(lock['name']))
                        self._load_schedule()
                        self._has_lock = True
                    else:
                        self._logger.debug('DS lock is not ours')
                if self._has_lock is True:
                    lock = {'name': node_name,
                            'timestamp': node_timestamp}
                    self._logger.debug('DS refreshing lock')
                    self._persistent.set(self._lock_name, lock)

            if self._has_lock is True:
                self._logger.debug('DS executing tick workload')
                remaining_times = []
                try:
                    for entry in self.schedule.itervalues():
                        next_time_to_run = self.maybe_due(entry, self.publisher)
                        if next_time_to_run:
                            remaining_times.append(next_time_to_run)
                except RuntimeError:
                    pass
                self._logger.debug('DS executing tick workload - done')
                return min(remaining_times + [self.max_interval])
            else:
                return self.max_interval
        except Exception as ex:
            self._logger.debug('DS got error during tick: {0}'.format(ex))
            return self.max_interval
