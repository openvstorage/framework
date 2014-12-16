# Copyright 2014 CloudFounders NV
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
Celery beat module
"""

import time
import cPickle
from celery.beat import Scheduler
from celery import current_app
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.exceptions import KeyNotFoundException
from ovs.extensions.generic.volatilemutex import VolatileMutex
from ovs.plugin.provider.configuration import Configuration
from ovs.log.logHandler import LogHandler

logger = LogHandler('celery', name='celery beat')


class DistributedScheduler(Scheduler):

    """
    Distributed scheduler that can run on multiple nodes at the same time.
    """

    TIMEOUT = 60 * 30

    def __init__(self, *args, **kwargs):
        """
        Initializes the distributed scheduler
        """
        self._persistent = PersistentFactory.get_client()
        self._namespace = 'ovs_celery_beat'
        self._mutex = VolatileMutex('celery_beat')
        self._has_lock = False
        super(DistributedScheduler, self).__init__(*args, **kwargs)
        logger.debug('DS init')

    def setup_schedule(self):
        """
        Setups the schedule
        """
        logger.debug('DS setting up schedule')
        self._load_schedule()
        self.merge_inplace(self.app.conf.CELERYBEAT_SCHEDULE)
        self.install_default_entries(self.schedule)
        logger.debug('DS setting up schedule - done')

    def _load_schedule(self):
        """
        Loads the most recent schedule from the persistent store
        """
        self.schedule = {}
        try:
            logger.debug('DS loading schedule entries')
            self._mutex.acquire(wait=10)
            try:
                self.schedule = cPickle.loads(
                    str(self._persistent.get('{0}_entries'.format(self._namespace))))
            except:
                # In case an exception occurs during loading the schedule, it is ignored and the default schedule
                # will be used/restored.
                pass
        finally:
            self._mutex.release()

    def sync(self):
        if self._has_lock is True:
            try:
                logger.debug('DS syncing schedule entries')
                self._mutex.acquire(wait=10)
                self._persistent.set('{0}_entries'.format(
                    self._namespace), cPickle.dumps(self.schedule))
            finally:
                self._mutex.release()
        else:
            logger.debug('DS skipping sync: lock is not ours')

    def tick(self):
        """
        Runs one iteration of the scheduler. This is guarded with a distributed lock
        """
        self._has_lock = False
        try:
            logger.debug('DS executing tick')
            self._mutex.acquire(wait=10)
            node_now = current_app._get_current_object().now()
            node_timestamp = time.mktime(node_now.timetuple())
            node_name = Configuration.get('ovs.core.uniqueid')
            try:
                lock = self._persistent.get('{0}_lock'.format(self._namespace))
            except KeyNotFoundException:
                lock = None
            if lock is None:
                # There is no lock yet, so the lock is acquired
                self._has_lock = True
                logger.debug('DS there was no lock in tick')
            else:
                if lock['name'] == node_name:
                    # The current node holds the lock
                    logger.debug('DS keeps own lock')
                    self._has_lock = True
                elif node_timestamp - lock['timestamp'] > DistributedScheduler.TIMEOUT:
                    # The current lock is timed out, so the lock is stolen
                    logger.debug('DS last lock refresh is {0}s old'.format(
                        node_timestamp - lock['timestamp']))
                    logger.debug(
                        'DS stealing lock from {0}'.format(lock['name']))
                    self._load_schedule()
                    self._has_lock = True
                else:
                    logger.debug('DS lock is not ours')
            if self._has_lock is True:
                lock = {'name': node_name,
                        'timestamp': node_timestamp}
                logger.debug('DS refreshing lock')
                self._persistent.set('{0}_lock'.format(self._namespace), lock)
        finally:
            self._mutex.release()

        if self._has_lock is True:
            logger.debug('DS executing tick workload')
            remaining_times = []
            try:
                for entry in self.schedule.itervalues():
                    next_time_to_run = self.maybe_due(entry, self.publisher)
                    if next_time_to_run:
                        remaining_times.append(next_time_to_run)
            except RuntimeError:
                pass
            logger.debug('DS executing tick workload - done')
            return min(remaining_times + [self.max_interval])
        else:
            return self.max_interval
