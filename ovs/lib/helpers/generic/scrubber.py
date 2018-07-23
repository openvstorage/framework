# Copyright (C) 2017 iNuron NV
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
Scrubber Module
"""
import os
import copy
import json
import time
import uuid
from datetime import datetime
from Queue import Empty, Queue
from random import randint
from threading import Thread
from ovs.dal.dataobject import DataObject
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.generic.filemutex import file_mutex
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.generic.volatilemutex import volatile_mutex, NoLockAvailableException
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs_extensions.storage.exceptions import AssertException
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs_extensions.generic.repeatingtimer import RepeatingTimer
from ovs.lib.mdsservice import MDSServiceController
from ovs.log.log_handler import LogHandler


class ScrubShared(object):
    """
    Class which has functions to ensure consistency
    """
    # Test hooks for unit tests
    _test_hooks = {}
    _unittest_data = {'setup': False}

    _logger = LogHandler.get('lib', name='generic tasks scrub')

    _SCRUB_KEY = '/ovs/framework/jobs/scrub'  # Parent key for all scrub related jobs
    _SCRUB_NAMESPACE = 'ovs_jobs_scrub'
    _SCRUB_VDISK_KEY = '{0}_{{0}}_vdisks'.format(_SCRUB_NAMESPACE)  # Second format should be the vpool name
    _SCRUB_VDISK_ACTIVE_KEY = '{0}_active_scrub'.format(_SCRUB_VDISK_KEY)  # Second format should be the vpool name
    _SCRUB_PROXY_KEY = '{0}_{{0}}'.format(_SCRUB_NAMESPACE)  # Second format should be the proxy name
    _SCRUB_JOB_TRACK_COUNT = Configuration.get('{0}/generic|vdisk_scrub_track_count'.format(_SCRUB_KEY), default=5)  # Track the last X scrub jobs on the vdisk object

    def __init__(self, job_id):
        self.job_id = job_id
        self._persistent = PersistentFactory.get_client()
        self._volatile = VolatileFactory.get_client()
        self._service_manager = ServiceFactory.get_manager()

        # Required to be overruled
        self.worker_contexts = None

        self._key = None
        self._log = None  # To be overruled
        # Used to lock SSHClient usage. SSHClient should be locked as Paramiko will fork the SSHProcess causing some issues in multithreading context
        self._client_lock = file_mutex('{0}_client_lock'.format(self._SCRUB_NAMESPACE))
        self._client_cluster_lock = volatile_mutex('{0}_client_cluster_lock'.format(self._SCRUB_NAMESPACE), wait=5 * 60)

    @property
    def worker_context(self):
        """
        Return the own worker context
        :return: Worker context
        :rtype: dict
        """
        if self.worker_contexts is None:
            raise NotImplementedError('No worker_contexts have yet been implemented')
        return self.worker_contexts[System.get_my_storagerouter()]

    def _safely_store(self, key, get_value_and_expected_value, logging_start, key_not_exist=False, max_retries=20):
        """
        Safely store a key/value pair within the persistent storage
        :param key: Key to store
        :type key: str
        :param get_value_and_expected_value: Function which returns the value and expected value
        :type get_value_and_expected_value: callable
        :param logging_start: Start of the logging line
        :type logging_start: str
        :param max_retries: Number of retries to attempt
        :type max_retries: int
        :param key_not_exist: Only store if the key does not exist
        :type key_not_exist: bool
        :return: Stored value or the current value if key_not_exists is True and the key is already present
        :rtype: any
        :raises: AssertException:
        - When the save could not happen
        """
        reference = {}

        def build_transaction():
            self._logger.info('{0} - Executing the passed function'.format(logging_start))
            value, expected_value = get_value_and_expected_value()
            transaction = self._persistent.begin_transaction()
            self._persistent.assert_value(key, expected_value, transaction=transaction)
            self._persistent.set(key, value, transaction=transaction)
            reference['value'] = value
            return transaction

        def retry_callback(tries):
            self._logger.warning('{0} - Asserting failed for key {1}. Retrying {2} more times'.format(logging_start, key, max_retries - tries))
            time.sleep(randint(0, 25) / 100.0)

        self._persistent.apply_callback_transaction(build_transaction, max_retries=max_retries, retry_wait_function=retry_callback)
        return reference.get('value')

    def _get_relevant_items(self, relevant_values, relevant_keys, key=None):
        """
        Retrieves all scrub work currently being done based on relevant values and the relevant format
        - Filters out own data
        - Filters out relevant data
        - Removes obsolete data
        :param relevant_values: The values to check relevancy on (Only supporting dict types)
        :type relevant_values: list[dict]
        :param relevant_keys: The keys that are relevant for checking relevancy
        (found items will strip keys to match to this format) (this format will be used to check in relevant values)
        :type relevant_keys: list
        :param key: Key to fetch from. Defaults to the own key
        :return: All relevant items and all fetched items
        :rtype: tuple(list, list)
        :raises: ValueError: When an irregular item has been detected
        """
        key = key or self._key
        if any(not isinstance(v, dict)for v in relevant_values):
            raise ValueError('Not all relevant values are a dict')
        if not isinstance(relevant_keys, list):
            raise ValueError('The relevant keys should be a list of keys that are relevant')
        if any(set(v.keys()) != set(relevant_keys) for v in relevant_values):
            raise ValueError('The relevant values do not match the relevant format')
        fetched_items = self._fetch_registered_items(key=key)
        relevant_work_items = []
        # Filter out the relevant items
        try:
            for item in fetched_items or []:  # Fetched items could be None
                # Extract relevant context. Note this is just a shallow copy and when retrieving items with lists/dicts we should not modify these in any way
                # because the reference is kept in _relevant_work_items
                relevant_context = dict((k, v) for k, v in item.iteritems() if k in relevant_keys)
                if relevant_context in relevant_values:
                    relevant_work_items.append(item)
                else:
                    # Not a item for the current scrubbing context. Possible remnant of an aborted scrub job so it will be removed when re-saving all total work items
                    self._logger.info('{0} - Will be removing {1} on the next save as it is no longer relevant'.format(self._log, item))
        except KeyError:
            raise ValueError('{0} - Someone is registering keys to this namespace'.format(self._log))
        return relevant_work_items, fetched_items

    def _fetch_registered_items(self, key):
        """
        Fetches all items currently registered on the key
        Saves them under _fetched_work_items for caching purposes. When None is returned, an empty list is set
        :return: All current items (None if the key has not yet been registered)
        """
        if key is None:
            raise ValueError('key has no value. Nothing to fetch')
        if self._persistent.exists(key) is True:
            items = self._persistent.get(key)
        else:
            items = None
        return items

    @classmethod
    def _covert_data_objects(cls, item):
        # Change all data objects to their GUID
        if isinstance(item, list):
            return [cls._covert_data_objects(i) for i in item]
        elif isinstance(item, dict):
            return dict((cls._covert_data_objects(k), cls._covert_data_objects(v)) for k, v in item.iteritems())
        elif isinstance(item, DataObject):
            return item.guid
        return item

    def _format_message(self, message):
        """
        Formats a message with the set _log property in front (to remove redundancy)
        :param message: The message to format
        :return: Newly formatted message
        """
        if self._log is None:
            raise ValueError('_log property has no value. Nothing to format with')
        return '{0} - {1}'.format(self._log, message)


class StackWorkHandler(ScrubShared):
    """
    Handles generation, unregistering and saving of vpool stack work
    """

    def __init__(self, job_id, vpool, vdisks, worker_contexts):
        """
        Initialize
        :param vpool: vPool to generate work for
        :type vpool: ovs.dal.hybrids.vpool.VPool
        :param vdisks: vDisks to include in the work
        :type vdisks: list
        :param worker_contexts: Contexts about the workers
        :type worker_contexts: dict
        """
        super(StackWorkHandler, self).__init__(job_id)

        self.vpool = vpool
        self.vdisks = vdisks
        self.worker_contexts = worker_contexts

        self._key = self._SCRUB_VDISK_KEY.format(self.vpool.name)  # Key to register items under
        self._key_active_scrub = self._SCRUB_VDISK_ACTIVE_KEY.format(self.vpool.name)  # Key to register items that are actively scrubbed under
        self._log = 'Scrubber {0} - vPool {1}'.format(self.job_id, self.vpool.name)

    def generate_save_scrub_work(self):
        """
        Generates applicable scrub work and saves the scrub work consistently
        :return: Queue of work items
        """
        work_queue = self._generate_save_work()
        self._logger.info(self._format_message('Following items will be scrubbed: {0}'.format(list(work_queue.queue))))
        return work_queue

    def _generate_scrub_work(self):
        """
        Generates scrub work to be done
        :return: Queue of work items, value to store in persistent, the current expected value
        :rtype: tuple(Queue, list, list)
        """
        # Fetch data
        relevant_work_items, fetched_work_items = self._get_pending_scrub_work()
        registered_vdisks = [item['vdisk_guid'] for item in relevant_work_items]
        work_queue = Queue()
        for vd in self.vdisks:
            logging_start_vd = '{0} - vDisk {1} with guid {2}'.format(self._log, vd.name, vd.guid)
            if vd.guid in registered_vdisks:
                self._logger.info('{0} has already been registered to get scrubbed, not queueing again'.format(logging_start_vd))
                continue
            if vd.is_vtemplate is True:
                self._logger.info('{0} is a template, not scrubbing'.format(logging_start_vd))
                continue
            vd.invalidate_dynamics('storagedriver_id')
            if not vd.storagedriver_id:
                self._logger.warning('{0} no StorageDriver ID found'.format(logging_start_vd))
                continue
            work_queue.put(vd.guid)
        total_items = relevant_work_items + list(self._wrap_data(item) for item in work_queue.queue)
        return work_queue, total_items, fetched_work_items

    def _wrap_data(self, vdisk_guid):
        """
        Wrap the vdisk guid in a dict with some other metadata
        Saving the worker context allows the other scrub jobs to determine what is still valid scrub data
        :param vdisk_guid: The guid of the vDisk to store
        :return: The wrapped data
        :rtype: dict
        """
        data = {'vdisk_guid': vdisk_guid}
        data.update(self.worker_context)
        data.update({'job_id': self.job_id})
        return data

    def _get_pending_scrub_work(self):
        """
        Retrieves all scrub work currently being done
        - Filters out own data
        - Filters out relevant data
        - Removes obsolete data
        :return: All relevant items and all fetched items
        :rtype: tuple(list, list)
        :raises: ValueError: When an irregular item has been detected
        """
        # This will strip out the vdisk_guid to check the relevancy of the item (keeping the worker context)
        return self._get_relevant_items(relevant_values=self.worker_contexts.values(), relevant_keys=self.worker_context.keys())

    def _get_current_scrubbed_items(self):
        """
        Retrieves all scrub work currently being done
        - Filters out own data
        - Filters out relevant data
        - Removes obsolete data
        :return: All relevant items and all fetched items
        :rtype: tuple(list, list)
        :raises: ValueError: When an irregular item has been detected
        """
        # This will strip out the vdisk_guid to check the relevancy of the item (keeping the worker context)
        return self._get_relevant_items(relevant_values=self.worker_contexts.values(), relevant_keys=self.worker_context.keys(), key=self._key_active_scrub)

    def _generate_save_work(self):
        """
        Consistently save the generated scrub work
        :return: Current work to be done
        :rtype: Queue
        """
        # Keep it pure
        special = {'queue': None}

        def _get_value_and_expected_value():
            work_queue, total_items, fetched_work_items = self._generate_scrub_work()
            special['queue'] = work_queue
            return total_items, fetched_work_items

        self._safely_store(self._key, get_value_and_expected_value=_get_value_and_expected_value, logging_start=self._log)
        # The queue might be different of when the function got called due to the hooking in the save
        return special['queue']

    def _unregister_vdisk(self, vdisk_guid):
        """
        Removes a certain vdisk from the current data list
        :param vdisk_guid: Guid of the vDisk to remove
        :type vdisk_guid: basestring
        :return: List of values to save and a list of expected values for saving
        :rtype tuple(list, list)
        """
        # Fetch data
        relevant_work_items, fetched_work_items = self._get_pending_scrub_work()
        item_to_remove = self._wrap_data(vdisk_guid)
        try:
            self._logger.info(self._format_message('Unregister vDisk {0}'.format(vdisk_guid)))
            relevant_work_items.remove(item_to_remove)
        except ValueError:
            # Indicates a race condition
            message = 'The registering data ({0}) is not in the list. Something must have removed it!'.format(item_to_remove)
            self._logger.exception(self._format_message(message))
            raise ValueError(message)
        return relevant_work_items, fetched_work_items

    def unregister_vdisk(self, vdisk_guid, retries=25):
        """
        Safely removes a vdisk from the pool
        :param vdisk_guid: Guid of the vDisk to remove
        :type vdisk_guid: basestring
        :param retries: Amount of retries to do
        :type retries: int
        :return: Remaining items
        :rtype: list
        """
        # Keep it pure
        special = {'total_items': None}

        def _get_value_and_expected_value():
            relevant_work_items, fetched_work_items = self._unregister_vdisk(vdisk_guid)
            special['relevant_work_items'] = relevant_work_items
            return relevant_work_items, fetched_work_items

        self._logger.info('{0} - Unregistering vDisk {1}'.format(self._log, vdisk_guid))
        # Attempt to save with all fetched data during work generation, expect the current key to not have changed
        self._safely_store(self._key,
                           get_value_and_expected_value=_get_value_and_expected_value,
                           logging_start='{0} - Unregistering vDisk {1}'.format(self._log, vdisk_guid),
                           max_retries=retries)
        self._logger.info(self._format_message('Successfully unregistered vDisk {0}'.format(vdisk_guid)))
        return special['relevant_work_items']

    def register_vdisk_for_scrub(self, vdisk_guid, location_data):
        """
        Register that a vDisk is being actively scrubbed
        :param vdisk_guid: Guid of the vDisk to register
        :type vdisk_guid: basestring
        :param location_data: Data about the scrub location
        :type location_data: dict
        :return: The loop instance keeping the information up to date
        :rtype: RepeatingTimer
        """
        def _update_vdisk_scrubbing_info():
            now = time.time()
            expires = now + 30
            data = {'job_id': self.job_id,
                    'expires': expires,
                    'ongoing': True,
                    'location': location_data}
            current_scrub_info = vdisk.scrubbing_information or {}
            if not current_scrub_info.get('start_time'):
                data.update({'start_time': now,
                             'start_time_readable': datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S.%f%z")})
            current_scrub_info.update(data)
            vdisk.scrubbing_information = current_scrub_info
            vdisk.save()

        vdisk = VDisk(vdisk_guid)
        rt = RepeatingTimer(5, _update_vdisk_scrubbing_info, wait_for_first_run=True)
        rt.start()
        return rt

    def unregister_vdisk_for_scrub(self, vdisk_guid, registering_thread, possible_exception=None):
        """
        Register that a vDisk is being actively scrubbed
        :param vdisk_guid: Guid of the vDisk to register
        :type vdisk_guid: basestring
        :param registering_thread: The thread registering scrub data onto the vdisk
        :type registering_thread: RepeatingTimer
        :param possible_exception: Possible exception that occurred while scrubbing
        :type possible_exception: Excepion
        :return: The loop instance keeping the information up to date
        :rtype: RepeatedTimer
        """
        registering_thread.cancel()
        registering_thread.join()
        vdisk = VDisk(vdisk_guid)
        if not vdisk.scrubbing_information:
            scrub_info = {}
        else:
            scrub_info = vdisk.scrubbing_information
        now = time.time()
        # Updating for storing it under previous runs
        scrub_info.update({'end_time': time.time(),
                           # For Operations. Easier to grep in the logs
                           'end_time_readable': datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S.%f%z"),
                           'exception': str(possible_exception) if possible_exception else possible_exception,
                           'ongoing': False})
        scrub_info_copy = scrub_info.copy()
        # Reset copy
        scrub_info_copy.update(dict.fromkeys(['job_id', 'expires', 'exception', 'end_time', 'end_time_readable', 'start_time', 'start_time_readable', 'location']))
        if possible_exception:
            previous_run_key = 'previous_failed_runs'
            previous_runs = scrub_info_copy.get('previous_failed_runs', [])
        else:
            previous_run_key = 'previous_successful_runs'
            previous_runs = scrub_info_copy.get('previous_successful_runs', [])
        updated_previous_runs = [dict((k, v) for k, v in scrub_info.iteritems() if k not in ['previous_failed_runs', 'previous_successful_runs'])] + previous_runs[0:self._SCRUB_JOB_TRACK_COUNT - 1]

        scrub_info_copy[previous_run_key] = updated_previous_runs
        vdisk.scrubbing_information = scrub_info_copy
        vdisk.save()


class StackWorker(ScrubShared):
    """
    This class represents a worker of the scrubbing stack
    """

    def __init__(self, job_id, queue, vpool, scrub_info, error_messages, stack_work_handler, worker_contexts, stacks_to_spawn, stack_number):
        """
        :param queue: a Queue with vDisk guids that need to be scrubbed (they should only be member of a single vPool)
        :type queue: Queue
        :param vpool: the vPool object of the vDisks
        :type vpool: VPool
        :param scrub_info: A dict containing scrub information:
                           `scrub_path` with the path where to scrub
                           `storage_router` with the StorageRouter that needs to do the work
        :type scrub_info: dict
        :param error_messages: A list of error messages to be filled (by reference)
        :type error_messages: list
        :param stack_work_handler: A stack work handler instance
        :type stack_work_handler: StackWorkHandler
        :param worker_contexts: Context of all ovs-worker services (dict with storagerouter guid, worker_pid and worker_start)
        :type worker_contexts: dict
        :param stacks_to_spawn: Amount of stacks that were spawned. Required to resolve racing with retries
        :type stacks_to_spawn: int
        :param stack_number: Number given by the stack spawner
        :type stack_number: int
        """
        super(StackWorker, self).__init__(job_id)
        self.queue = queue
        self.vpool = vpool
        self.error_messages = error_messages
        self.worker_contexts = worker_contexts
        self.stack_number = stack_number
        self.stacks_to_spawn = stacks_to_spawn
        self.stack_work_handler = stack_work_handler
        self.scrub_info = scrub_info  # Stored for testing purposes

        self.queue_size = self.queue.qsize()
        self.stack_id = str(uuid.uuid4())
        self.storagerouter = scrub_info['storagerouter']
        self.partition_guid = scrub_info['partition_guid']

        self._log = 'Scrubber {0} - vPool {1} - StorageRouter {2} - Stack {3}'.format(self.job_id, self.vpool.name,
                                                                                      self.storagerouter.name,
                                                                                      self.stack_number)

        # Both the proxy and scrub directory are bound to the scrub location and both will be re-used
        self.backend_connection_manager_config = Configuration.get('ovs/vpools/{0}/hosts/{1}/config|backend_connection_manager'.format(self.vpool.guid, self.vpool.storagedrivers[0].storagedriver_id))
        # Allow a custom one to be inserted
        self.backend_connection_manager_config.update(Configuration.get('ovs/vpools/{0}/scrub/tweaks|backend_connection_manager'.format(self.vpool.guid), default={}))
        proxy_amount_key = '/ovs/framework/hosts/{0}/config|scrub_proxy_amount'.format(self.storagerouter.machine_id)
        proxy_amount = Configuration.get(key=proxy_amount_key, default=1)
        if proxy_amount <= 0:
            raise ValueError('{0} - Incorrect amount of proxies. Expected at least 1, found {1}'.format(self._log, proxy_amount))
        if self.backend_connection_manager_config.get('backend_type') != 'MULTI':
            self._logger.warning('{0} - {1} proxies were request but the backend_connection_manager config only supports 1 proxy'.format(self._log, proxy_amount))
            proxy_amount = 1
        self.alba_proxy_services = ['ovs-albaproxy_{0}_{1}_{2}_scrub_{3}'.format(self.vpool.name, self.storagerouter.name, self.partition_guid, i)
                                    for i in xrange(0, proxy_amount)]
        self.scrub_config_key = 'ovs/vpools/{0}/proxies/scrub/scrub_config_{1}_{{0}}'.format(vpool.guid, self.partition_guid)  # Formatted with the number of the proxy
        self.scrub_directory = '{0}/scrub_work_{1}_{2}_{3}'.format(scrub_info['scrub_path'], vpool.name, self.storagerouter.name, self.partition_guid)
        self.backend_config_key = 'ovs/vpools/{0}/proxies/scrub/backend_config_{1}'.format(vpool.guid, self.partition_guid)
        self.lock_time = 5 * 60
        self.registered_proxy = False
        self.registering_data = {'job_id': self.job_id, 'stack_id': self.stack_id}  # Data to register within Arakoon about this StackWorker
        self.registering_data.update(self.worker_context)

        # Always at least 1 proxy service. Using that name to register items under
        self._key = self._SCRUB_PROXY_KEY.format(self.alba_proxy_services[0])
        self._state_key = '{0}_state'.format(self._key)  # Key with the combined state for all proxies

    def deploy_stack_and_scrub(self):
        """
        Executes scrub work for a given vDisk queue and vPool, based on scrub_info
        - Sets up a scrubbing proxy (if the proxy is not present yet)
        - Launches scrubbing threads (up to 20)
        - Cleans up the scrubbing proxy (if the proxy is no longer used)
        :return: None
        :rtype: NoneType
        """
        if len(self.vpool.storagedrivers) == 0 or not self.vpool.storagedrivers[0].storagedriver_id:
            self.error_messages.append('vPool {0} does not have any valid StorageDrivers configured'.format(self.vpool.name))
            return

        # Deploy a proxy
        self._deploy_proxies()

        # Execute the actual scrubbing
        threads = []
        threads_key = '/ovs/framework/hosts/{0}/config|scrub_stack_threads'.format(self.storagerouter.machine_id)
        amount_threads = Configuration.get(key=threads_key, default=2)
        if not isinstance(amount_threads, int):
            self.error_messages.append('Amount of threads to spawn must be an integer for StorageRouter with ID {0}'.format(self.storagerouter.machine_id))
            return

        amount_threads = max(amount_threads, 1)  # Make sure amount_threads is at least 1
        amount_threads = min(min(self.queue.qsize(), amount_threads), 20)  # Make sure amount threads is max 20
        self._logger.info('{0} - Spawning {1} threads for proxy services {2}'.format(self._log, amount_threads, ', '.join(self.alba_proxy_services)))
        for index in range(amount_threads):
            # Name the threads for unit testing purposes
            thread = Thread(name='execute_scrub_{0}_{1}_{2}'.format(self.vpool.guid, self.partition_guid, index),
                            target=self._execute_scrub,
                            args=(index,))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

        # Delete the proxy again
        self._remove_proxies()

    def _execute_scrub(self, thread_number):
        """
        Executes the actual scrubbing
        - Ask the Volumedriver what scrub work is to be done
        - Ask to Volumedriver to do scrub work
        - Ask the Volumedriver to apply this work (when not applied, scrubbing leaves garbage on the backend)
        :param thread_number: Number of the Thread that is executing the scrubbing
        :type thread_number: int
        :return: None
        :rtype: NoneType
        """
        def _verify_mds_config(current_vdisk):
            current_vdisk.invalidate_dynamics('info')
            vdisk_configs = current_vdisk.info['metadata_backend_config']
            if len(vdisk_configs) == 0:
                raise RuntimeError('Could not load MDS configuration')
            return vdisk_configs
        log = '{0} - Scrubbing thread {1}'.format(self._log, thread_number)
        try:
            # Empty the queue with vDisks to scrub
            with remote(self.storagerouter.ip, [VDisk]) as rem:
                while True:
                    vdisk_guid = self.queue.get(False)  # Raises Empty Exception when queue is empty, so breaking the while True loop
                    volatile_key = 'ovs_scrubbing_vdisk_{0}'.format(vdisk_guid)
                    try:
                        # Check MDS master is local. Trigger MDS handover if necessary
                        vdisk = rem.VDisk(vdisk_guid)
                        vdisk_log = '{0} - vDisk {1} with guid {2} and volume id {3}'.format(log, vdisk.name, vdisk.guid, vdisk.volume_id)
                        self._logger.info('{0} - Started scrubbing at location {1}'.format(vdisk_log, self.scrub_directory))
                        configs = _verify_mds_config(current_vdisk=vdisk)
                        storagedriver = StorageDriverList.get_by_storagedriver_id(vdisk.storagedriver_id)
                        if configs[0].get('ip') != storagedriver.storagerouter.ip:
                            self._logger.info('{0} - MDS master is not local, trigger handover'.format(vdisk_log, vdisk.name))
                            MDSServiceController.ensure_safety(vdisk_guid=vdisk_guid)  # Do not use a remote VDisk instance here
                            configs = _verify_mds_config(current_vdisk=vdisk)
                            if configs[0].get('ip') != storagedriver.storagerouter.ip:
                                self._logger.warning('{0} - Skipping because master MDS still not local'.format(vdisk_log, vdisk.name))
                                continue

                        # Check if vDisk is already being scrubbed
                        if self._volatile.add(key=volatile_key, value=volatile_key, time=24 * 60 * 60) is False:
                            self._logger.warning('{0} - Skipping because vDisk is already being scrubbed'.format(vdisk_log, vdisk.name))
                            continue

                        # Fetch the generic lease
                        lease_interval = Configuration.get('/ovs/volumedriver/intervals|locked_lease', default=5)
                        # Lease per vpool
                        lease_interval = Configuration.get('/ovs/vpools/{0}/scrub/tweaks|locked_lease_interval'.format(self.vpool.guid), default=lease_interval)

                        # Register that the disk is being scrubbed
                        log_path = LogHandler.get_sink_path('scrubber_{0}'.format(self.vpool.name), allow_override=True, forced_target_type='file')
                        location_data = {'scrub_directory': self.scrub_directory,
                                         'storagerouter_guid': self.storagerouter.guid,
                                         'log_path': log_path}
                        registrator = self.stack_work_handler.register_vdisk_for_scrub(vdisk_guid, location_data)
                        scrub_exception = None
                        try:
                            if 'post_vdisk_scrub_registration' in self._test_hooks:
                                self._test_hooks['post_vdisk_scrub_registration'](self, vdisk_guid)
                            # Do the actual scrubbing
                            with vdisk.storagedriver_client.make_locked_client(str(vdisk.volume_id), update_interval_secs=lease_interval) as locked_client:
                                self._logger.info('{0} - Retrieve and apply scrub work'.format(vdisk_log, vdisk.name))
                                work_units = locked_client.get_scrubbing_workunits()
                                for work_unit in work_units:
                                    res = locked_client.scrub(work_unit=work_unit,
                                                              scratch_dir=self.scrub_directory,
                                                              log_sinks=[log_path],
                                                              backend_config=Configuration.get_configuration_path(self.backend_config_key))
                                    locked_client.apply_scrubbing_result(scrubbing_work_result=res)
                                if work_units:
                                    self._logger.info('{0} - {1} work units successfully applied'.format(vdisk_log, len(work_units)))
                                else:
                                    self._logger.info('{0} - No scrubbing required'.format(vdisk_log, vdisk.name))
                        except Exception as ex:
                            scrub_exception = ex
                            raise
                        finally:
                            self.stack_work_handler.unregister_vdisk_for_scrub(vdisk_guid, registrator, scrub_exception)
                            if 'post_vdisk_scrub_unregistration' in self._test_hooks:
                                self._test_hooks['post_vdisk_scrub_unregistration'](self, vdisk_guid)
                    except Exception as ex:
                        if isinstance(ex, ObjectNotFoundException):
                            message = '{0} - Scrubbing failed vDisk with guid {1} because the vDisk is no longer available'.format(log, vdisk_guid)
                            self._logger.warning(message)
                        else:
                            vdisk = VDisk(vdisk_guid)
                            vdisk_log = '{0} - vDisk {1} with volume id {2}'.format(log, vdisk.name, vdisk.volume_id)
                            message = '{0} - Scrubbing failed'.format(vdisk_log, vdisk.name)
                            self.error_messages.append(message)
                            self._logger.exception(message)
                    finally:
                        # Remove vDisk from volatile memory and scrub work
                        self._volatile.delete(volatile_key)
                        self.stack_work_handler.unregister_vdisk(vdisk_guid)

        except Empty:  # Raised when all items have been fetched from the queue
            self._logger.info('{0} - Queue completely processed'.format(log))
        except Exception:
            message = '{0} - Scrubbing failed'.format(log)
            self.error_messages.append(message)
            self._logger.exception(message)

    def _get_registered_proxy_users(self):
        """
        Retrieves all stacks using a certain proxy
        - Discards obsolete data
        - Removes obsolete data keys
        :return: List of stacks using the proxy
        :rtype: list
        """
        # This will strip out the job_id, stack_id to check the relevancy of the item (keeping the worker context)
        return self._get_relevant_items(relevant_values=self.worker_contexts.values(),
                                        relevant_keys=self.worker_context.keys())

    def _register_proxy_usage(self):
        """
        Registers that this worker is using the proxy
        This registration is used to lock the usage of the proxy/scrub directory
        :return: Current list of items
        :rtype: list
        """
        # Keep it pure (for now)
        special = {'relevant_work_items': None}

        def _get_value_and_expected_value():
            relevant_work_items, fetched_work_items = self._get_registered_proxy_users()
            relevant_work_items.append(self.registering_data)
            special['relevant_work_items'] = relevant_work_items
            return relevant_work_items, fetched_work_items

        self._logger.info('{0} - Registering usage of {1}'.format(self._log, ', '.join(self.alba_proxy_services)))

        # Attempt to save with all fetched data during work generation, expect the current key to not have changed
        self._safely_store(self._key, get_value_and_expected_value=_get_value_and_expected_value, logging_start=self._log)
        self.registered_proxy = True
        return special['relevant_work_items']

    def _unregister_proxy_usage(self):
        """
        Unregisters that this worker is using this proxy
        :return: The remaining data entries
        :rtype: list
        """
        # Keep it pure (for now)
        special = {'relevant_work_items': None}

        def _get_value_and_expected_value():
            relevant_work_items, fetched_work_items = self._get_registered_proxy_users()
            try:
                relevant_work_items.remove(self.registering_data)
            except ValueError:
                message = 'The registering data ({0}) is not in the list. Something must have removed it!'.format(self.registering_data)
                self._logger.error(self._format_message(message))
                raise ValueError(message)
            special['relevant_work_items'] = relevant_work_items
            return relevant_work_items, fetched_work_items

        self._logger.info('{0} - Unregistering usage of {1}'.format(self._log, ', '.join(self.alba_proxy_services)))

        if self.registered_proxy is True:
            # Attempt to save with all fetched data during work generation, expect the current key to not have changed
            self._safely_store(self._key,
                               get_value_and_expected_value=_get_value_and_expected_value,
                               logging_start='{0} - Unregistering proxies {1}'.format(self._log, ', '.join(self.alba_proxy_services)),
                               max_retries=self.stacks_to_spawn)
        self._logger.info(self._format_message('Successfully unregistered proxies {0}'.format(', '.join(self.alba_proxy_services))))
        return special['relevant_work_items']

    def _long_poll_proxy_state(self, states, timeout=None):
        """
        Start long polling for the proxy state. This state is not fetched by the service manager but rather by the threads injecting a key in Arakoon
        This function will return once the requested state has been achieved
        :param states: States of the key
        :type states: Possible states are:
        - 'deploying': proxy is still being deployed
        - 'deployed': proxy has been deployed
        - 'removing': proxy is being removed
        - 'removed': proxy has been removed
        :param timeout: Amount of seconds to wait (Defaults to 5 minutes)
        :return: Current state or None (when the given key does not exist)
        :rtype: str
        """
        if timeout is None:
            timeout = self.lock_time
        if self._persistent.exists(self._state_key) is True:
            start = time.time()
            while True:
                current_state = self._persistent.get(self._state_key)
                if current_state in states:
                    return current_state
                if time.time() - start > timeout:
                    raise RuntimeError('Long polling for the state has timed out')
                self._logger.debug(self._format_message('Proxies {0{ their state does not match any in  \'{1}\'  (Current: {2})'
                                                        .format(', '.join(self.alba_proxy_services), states, current_state)))
                time.sleep(1)
        return None

    def _set_proxies_state(self, state):
        """
        Sets the state of the proxy
        :param state: State of key
        :return: The state that was set
        """
        self._persistent.set(self._state_key, state)

    def _deploy_proxies(self):
        """
        Deploy a scrubbing proxy
        - Validates if a proxy is already present
        - Sets up a proxy consistently
        :return: None
        :rtype: NoneType
        """
        if 'pre_proxy_deployment' in self._test_hooks:
            self._test_hooks['pre_proxy_deployment'](self)

        self._logger.info(self._format_message('Checking for ALBA proxies {0} deployment'.format(', '.join(self.alba_proxy_services))))
        registered_users = self._register_proxy_usage()
        self._logger.info(self._format_message('Current registered users: {0}'.format(registered_users)))
        try:
            if len(registered_users) == 1:
                # First to register, allowed to deploy the proxy
                self._logger.info(self._format_message('Deploying ALBA proxies {0}'.format(', '.join(self.alba_proxy_services))))
                # Check the proxy status - could be that it is removing
                self._set_proxies_state('deploying')
                scrub_proxy_configs = []
                with self._client_cluster_lock:
                    self._logger.info(self._format_message('Creating directory {0}'.format(self.scrub_directory)))
                    client = SSHClient(self.storagerouter, 'root', cached=False)  # Explicitly request a new connection here
                    client.dir_create(self.scrub_directory)
                    client.dir_chmod(self.scrub_directory, 0777)  # Celery task executed by 'ovs' user and should be able to write in it
                    machine_id = System.get_my_machine_id(client)
                    port_range = Configuration.get('/ovs/framework/hosts/{0}/ports|storagedriver'.format(machine_id))
                    ports = System.get_free_ports(selected_range=port_range, nr=len(self.alba_proxy_services), client=client)
                    scrub_config = Configuration.get('ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(self.vpool.guid)).copy()
                    for alba_proxy_index, alba_proxy_service in enumerate(self.alba_proxy_services):
                        port = ports[alba_proxy_index]
                        scrub_proxy_config_key = self.scrub_config_key.format(alba_proxy_index)
                        scrub_proxy_config = scrub_config.copy()
                        scrub_proxy_config['port'] = port
                        scrub_proxy_config['transport'] = 'tcp'
                        scrub_proxy_configs.append(scrub_proxy_config)
                        Configuration.set(scrub_proxy_config_key, json.dumps(scrub_proxy_config, indent=4), raw=True)
                        params = {'VPOOL_NAME': self.vpool.name,
                                  'LOG_SINK': LogHandler.get_sink_path(alba_proxy_service),
                                  'CONFIG_PATH': Configuration.get_configuration_path(scrub_proxy_config_key)}
                        self._service_manager.add_service(name='ovs-albaproxy', params=params, client=client, target_name=alba_proxy_service)
                        self._service_manager.start_service(name=alba_proxy_service, client=client)

                        if 'post_single_proxy_deployment' in self._test_hooks:
                            self._test_hooks['post_single_proxy_deployment'](self, alba_proxy_service)

                        self._logger.info(self._format_message('Deployed ALBA proxy {0} (Config: {1})'.format(alba_proxy_service, scrub_proxy_config)))
                # Backend config is tied to the proxy, so only need to register while the proxy has to be deployed
                self._logger.info(self._format_message('Setting up backend config'))
                backend_config = copy.deepcopy(self.backend_connection_manager_config)
                if backend_config.get('backend_type') != 'MULTI':
                    scrub_proxy_config = scrub_proxy_configs[0]  # Only 1 proxy would be deployed
                    backend_config['alba_connection_host'] = '127.0.0.1'
                    backend_config['alba_connection_port'] = scrub_proxy_config['port']
                else:
                    # Iterate all proxy configs from the VPool and adjust the socket
                    proxy_config_template = None
                    for key in backend_config.copy().iterkeys():
                        # Proxy keys are currently set to to a stringified integer value
                        if key.isdigit():
                            # Remove all proxy configurations of the VPool to replace it with the scrubber ones
                            proxy_config = backend_config.pop(key)
                            if proxy_config_template is None:
                                proxy_config_template = proxy_config
                    # Build up config for our purposes
                    for index, scrub_proxy_config in enumerate(scrub_proxy_configs):
                        proxy_config = proxy_config_template.copy()
                        proxy_config.update({'alba_connection_host': '127.0.0.1',
                                             'alba_connection_port': scrub_proxy_config['port']})
                        backend_config[str(index)] = proxy_config
                # Copy backend connection manager information in separate key
                Configuration.set(self.backend_config_key, json.dumps({"backend_connection_manager": backend_config}, indent=4), raw=True)
                self._logger.info(self._format_message('Backend config was set up'))

                if 'post_proxy_deployment' in self._test_hooks:
                    self._test_hooks['post_proxy_deployment'](self)

                self._set_proxies_state('deployed')
            else:
                # Long polling for the status
                self._logger.info(self._format_message('Re-using existing proxy services {0}'.format(', '.join(self.alba_proxy_services))))
                self._logger.info(self._format_message('Waiting for proxy services {0} to be deployed by another worker'.format(', '.join(self.alba_proxy_services))))
                proxy_state = self._long_poll_proxy_state(states=['deployed', 'removed'])
                if proxy_state == 'removed':  # A different worker has tried to remove and might or might not have succeeded
                        raise RuntimeError(self._format_message('Proxy services {0} deployment failed with another worker'.format(', '.join(self.alba_proxy_services))))
                self._logger.info(self._format_message('Proxy services {0} was deployed'.format(', '.join(self.alba_proxy_services))))
        except Exception:
            message = '{0} - An error occurred deploying ALBA proxies {1}'.format(self._log, ', '.join(self.alba_proxy_services))
            self.error_messages.append(message)
            self._logger.exception(message)
            self._remove_proxies(force=True)
            raise  # No proxy could be set so no scrubbing could happen for this worker

    def _remove_proxies(self, force=False):
        """
        Removes the proxy that was used
        :param force: Force removal. This will skip all the check of registered user.
        Should be set to True when deployment failed to re-deploy the proxy by a different worker
        :return: None
        :rtype: None
        """
        proxy_users = self._unregister_proxy_usage()  # Always unregister the usage
        if force is False:
            if len(proxy_users) > 0:
                self._logger.info('{0} - Cannot remove services {1} as it is still in use by others'.format(self._log, ', '.join(self.alba_proxy_services)))
                return
        removal_errors = []
        try:
            # No longer using the proxy as we want to remove it
            self._set_proxies_state('removing')
            with self._client_cluster_lock:
                self._logger.info(self._format_message('Removing directory {0}'.format(self.scrub_directory)))
                client = SSHClient(self.storagerouter, 'root', cached=False)  # Explicitly ask for a new connection here
                client.dir_delete(self.scrub_directory)
                self._logger.info(self._format_message('Removing services {0}'.format(', '.join(self.alba_proxy_services))))
                for alba_proxy_index, alba_proxy_service in enumerate(self.alba_proxy_services):
                    proxy_scrub_config_key = self.scrub_config_key.format(alba_proxy_index)
                    try:
                        if self._service_manager.has_service(alba_proxy_service, client=client) is True:
                            if self._service_manager.get_service_status(name=alba_proxy_service, client=client) == 'active':
                                self._service_manager.stop_service(alba_proxy_service, client=client)
                            self._service_manager.remove_service(alba_proxy_service, client=client)
                            self._logger.info('{0} - Removed service {1}'.format(self._log, alba_proxy_service))
                    except Exception:
                        message = '{0} - Removing service {1} failed'.format(self._log, alba_proxy_service)
                        removal_errors.append(message)
                        self._logger.exception(message)
                    try:
                        if Configuration.exists(proxy_scrub_config_key):
                            Configuration.delete(proxy_scrub_config_key)
                    except Exception:
                        message = '{0} - Removing the config of service {1} failed'.format(self._log, alba_proxy_service)
                        removal_errors.append(message)
                        self._logger.exception(message)
            if len(removal_errors) > 0:
                message = '{0} - Errors while removing services {1}: \n - {2}'.format(self._log, ', '.join(self.alba_proxy_services), '\n - '.join(removal_errors))
                self._logger.error(message)
                raise RuntimeError(message)
            self._logger.info('{0} - Removed services {1}'.format(self._log, ', '.join(self.alba_proxy_services)))
        except Exception:
            message = '{0} - Removing services {1} failed'.format(self._log, ', '.join(self.alba_proxy_services))
            removal_errors.append(message)
            self._logger.exception(message)
            raise
        finally:
            # Not sure if this is the right call. Proxy deployment/removal errors should be looked after
            # This will cause the next proxy setup to fail as the service already exists and that one will attempt to clean up again
            self._set_proxies_state('removed')


class Scrubber(ScrubShared):
    """
    This class represents a scrub job being handled
    It will:
    - Divide the scrub work among all StorageRouters with a SCRUB partition
    - Create a job entry into ovs/framework/jobs/scrub
    - Executes scrub work for a given vDisk queue and vPool, based on scrub_info
      - Will re-use already deployed proxies
      - Keep internal track of items to scrub
    - Cleanup stale job entries
    """

    _KEY_LIFETIME = 7 * 24 * 60 * 60  # All job keys are kept for 7 days and after that the next scrubbing job will remove the outdated ones

    def __init__(self, vpool_guids=None, vdisk_guids=None, storagerouter_guid=None, manual=False, task_id=None):
        """
        :param vpool_guids: Guids of the vPools that need to be scrubbed completely
        :type vpool_guids: list
        :param vdisk_guids: Guids of the vDisks that need to be scrubbed
        :type vdisk_guids: list
        :param storagerouter_guid: Guid of the StorageRouter to execute the scrub work on
        :type storagerouter_guid: str
        :param manual: Indicator whether the execute_scrub is called manually or as scheduled task (automatically)
        :type manual: bool
        :param task_id: An ID for the current scrub task (this can be the current celery job id or None for a generated one)
        """
        # Validation
        if vdisk_guids is None:
            vdisk_guids = []
        if vpool_guids is None:
            vpool_guids = []
        if not isinstance(vpool_guids, list):
            raise ValueError('vpool_guids should be a list')
        if not isinstance(vdisk_guids, list):
            raise ValueError('vdisk_guids should be a list')
        if storagerouter_guid is not None and not isinstance(storagerouter_guid, basestring):
            raise ValueError('storagerouter_guid should be a str')

        if manual is False and (len(vpool_guids) > 0 or len(vdisk_guids) > 0):
            raise ValueError('When specifying vDisks or vPools, "manual" must be True')

        super(Scrubber, self).__init__(task_id or str(uuid.uuid4()))

        if os.environ.get('RUNNING_UNITTESTS') == 'True' and not ScrubShared._unittest_data['setup']:
            self.setup_for_unittests()

        self.task_id = task_id  # Be able to differentiate between directly executed ones for debugging purposes
        self.vdisk_guids = vdisk_guids
        self.vpool_guids = vpool_guids
        self.storagerouter_guid = storagerouter_guid
        self.manual = manual

        self._log = 'Scrubber {0}'.format(self.job_id)

        self.time_start = None
        self.time_end = None
        self.clients = self.build_clients()
        self.vpool_vdisk_map = self.generate_vpool_vdisk_map(vpool_guids=vpool_guids, vdisk_guids=vdisk_guids, manual=manual)
        self.scrub_locations = self.get_scrub_locations(self.storagerouter_guid)
        self.worker_contexts = self.get_worker_contexts()  # Could give off an outdated view but that would be picked up by the next scrub job

        # Scrubbing stack
        self.error_messages = []  # Keep track of all messages that might occur
        self.max_stacks_per_vpool = None
        self.stack_workers = []  # Unit tests can hook into this variable to do some fiddling
        self.stack_threads = []

    @staticmethod
    def setup_for_unittests():
        """
        Current mocks do not yet a System.get_my_storagerouter or anything related worker services
        This function will inject a mock so unittests can actually test the logic of the scrubber
        """
        # Setup System
        storagerouter = StorageRouterList.get_storagerouters()[0]
        System._machine_id['none'] = System._machine_id[storagerouter.ip]

        # Setup the worker service for all storagerouters
        service_name = 'ovs-workers'
        service_manager = ServiceFactory.get_manager()
        for storagerouter in StorageRouterList.get_storagerouters():
            client = SSHClient(storagerouter, 'root')
            if not service_manager.has_service(service_name, client):
                service_manager.add_service(service_name, client)
                service_manager.start_service(service_name, client)
        ScrubShared._unittest_data['setup'] = True

    def build_clients(self):
        """
        Builds SSHClients towards all StorageRouters
        :return: SSHClient mapped by storagerouter
        :rtype: dict((storagerouter, sshclient))
        """
        clients = {}
        with self._client_lock:
            for storagerouter in StorageRouterList.get_storagerouters():
                client = None
                tries = 0
                max_tries = 5
                while client is None:
                    tries += 1
                    if tries > max_tries:
                        self._logger.error(self._format_message('Assuming StorageRouter {0} is dead. Not scrubbing there'.format(storagerouter.ip)))
                        break
                    try:
                        # Requesting new client to avoid races (if the same worker would build the clients again)
                        client = SSHClient(storagerouter, username='root', timeout=30, cached=False)
                    except Exception:
                        self._logger.exception(self._format_message('Unable to connect to StorageRouter {0} - Retrying {1} more times before assuming it is down'.format(storagerouter.ip, max_tries - tries)))
                if client is not None:
                    clients[storagerouter] = client
        return clients

    def get_worker_contexts(self):
        """
        Retrieves information about the all workers (where it is executed and under what PID)
        This information is later used to check which data can be discarded (because of interrupted workers)
        :return: Information about the current workers
        :rtype: dict
        """
        workers_context = {}
        for storagerouter, client in self.clients.iteritems():
            worker_pid = 0
            worker_start = None
            try:
                # Retrieve the current start time of the process (used to create a unique key)
                # Output of the command:
                #                  STARTED   PID
                # Mon Jan 22 11:49:04 2018 22287
                worker_pid = self._service_manager.get_service_pid(name='ovs-workers', client=client)
                if worker_pid == 0:
                    self._logger.warning('The workers are down on StorageRouter {0}'.format(storagerouter.guid))
                else:
                    worker_start = self._service_manager.get_service_start_time(name='ovs-workers', client=client)
            except Exception:
                self._logger.exception(self._format_message('Unable to retrieve information about the worker'))
            workers_context[storagerouter] = {'storagerouter_guid': storagerouter.guid,
                                              'worker_pid': worker_pid,
                                              'worker_start': worker_start}
        if System.get_my_storagerouter() not in workers_context:
            raise ValueError(self._format_message('The context about the workers on this machine should be known'))
        return workers_context

    def execute_scrubbing(self):
        """
        Execute the scrubbing work
        Every vpool will have its own set of stacks to scrub. These stacks deploy scrubbing threads internally
        The number of stacks for every vpool is calculated based on the number of vpools to scrub in total ( 6+ -> 1/vpool, 6>x>=3 -> 2/vpool, 3> -> 3/vpool)
        :return: None
        :rtype: NoneType
        """
        self._logger.info(self._format_message('Executing scrub'))
        number_of_vpools = len(self.vpool_vdisk_map)
        if number_of_vpools >= 6:
            self.max_stacks_per_vpool = 1
        elif number_of_vpools >= 3:
            self.max_stacks_per_vpool = 2
        else:
            self.max_stacks_per_vpool = 5

        self.time_start = time.time()
        self.set_main_job_info()
        counter = 0
        vp_work_map = {}
        for vp, vdisks in self.vpool_vdisk_map.iteritems():
            logging_start = self._format_message('vPool {0}'.format(vp.name))
            # Verify amount of vDisks on vPool
            self._logger.info('{0} - Checking scrub work'.format(logging_start))
            stack_work_handler = StackWorkHandler(vpool=vp, vdisks=vdisks, worker_contexts=self.worker_contexts, job_id=self.job_id)
            vpool_queue = stack_work_handler.generate_save_scrub_work()
            vp_work_map[vp] = (vpool_queue, stack_work_handler)
            if vpool_queue.qsize() == 0:
                self._logger.info('{0} - No scrub work'.format(logging_start))
                continue
            stacks_to_spawn = min(self.max_stacks_per_vpool, len(self.scrub_locations))
            self._logger.info('{0} - Spawning {1} stack{2}'.format(logging_start, stacks_to_spawn, '' if stacks_to_spawn == 1 else 's'))
            for stack_number in xrange(stacks_to_spawn):
                scrub_target = self.scrub_locations[counter % len(self.scrub_locations)]
                stack_worker = StackWorker(queue=vpool_queue,
                                           vpool=vp,
                                           scrub_info=scrub_target,
                                           error_messages=self.error_messages,
                                           worker_contexts=self.worker_contexts,
                                           stack_work_handler=stack_work_handler,
                                           job_id=self.job_id,
                                           stacks_to_spawn=stacks_to_spawn,
                                           stack_number=stack_number)
                self.stack_workers.append(stack_worker)
                stack = Thread(target=stack_worker.deploy_stack_and_scrub,
                               args=())
                stack.start()
                self.stack_threads.append(stack)
                counter += 1

        if 'post_stack_worker_deployment' in self._test_hooks:
            self._test_hooks['post_stack_worker_deployment'](self)

        for thread in self.stack_threads:
            thread.join()

        # Update the job info
        self.time_end = time.time()
        self.set_main_job_info()

        self._cleanup_job_entries()

        if len(self.error_messages) > 0:
            try:
                self._clean_up_leftover_items(vp_work_map)
            except Exception as ex:
                self.error_messages.append(self._format_message('Exception while clearing remaining entries: {0}'.format(str(ex))))
            raise Exception(self._format_message('Errors occurred while scrubbing:\n  - {0}'.format('\n  - '.join(self.error_messages))))

    def _clean_up_leftover_items(self, vpool_work_map):
        """
        Cleans up leftover work items when scrubbing would have failed
        Doing this in the scrubber as the workers do not know if other workers have failed or not
        :param vpool_work_map: A mapping with which vpool did what work and the stackwork handler
        :type vpool_work_map: dict((ovs.dal.hybrids.vpool.VPool, tuple(queue.Queue, StackWorkHandler)
        :return: None
        :rtype: NoneType
        """
        for vpool, work_tools in vpool_work_map.iteritems():
            queue, stack_work_handler = work_tools
            if queue.qsize() > 0:
                self._logger.warning(self._format_message('Clearing remaining items for vpool {0}. The following items will be unregistered: \n - {1}'
                                                          .format(vpool.name, '\n - '.join(queue.queue))))
                while queue.qsize() > 0:
                    vdisk_guid = queue.get()
                    stack_work_handler.unregister_vdisk(vdisk_guid)

    def set_main_job_info(self):
        """
        Registers the current scrubbing job within Configuration
        This allows for better debugging / linking jobs
        :return: None
        :rtype: NoneType
        """
        # Validation
        if any(item is None for item in [self.max_stacks_per_vpool, self.time_start]):
            raise ValueError('Scrubbing has not been executed yet. Not registering the current job')

        job_key = '{0}/{1}/job_info'.format(self._SCRUB_KEY, self.job_id)
        job_info = {'scrub_locations': [self._covert_data_objects(x) for x in self.scrub_locations],
                    'task_id': self.task_id,
                    'max_stacks_per_vpool': self.max_stacks_per_vpool,
                    'vpool_vdisk_map': self._covert_data_objects(self.vpool_vdisk_map),
                    'time_start': self.time_start,
                    'time_end': self.time_end,
                    'worker_contexts': self._covert_data_objects(self.worker_contexts)}
        Configuration.set(job_key, json.dumps(job_info, indent=4), raw=True)

    @classmethod
    def generate_vpool_vdisk_map(cls, vpool_guids=None, vdisk_guids=None, manual=False):
        """
        Generates a mapping between the provided vpools and vdisks
        :param vpool_guids: Guids of the vPools
        :type vpool_guids: list
        :param vdisk_guids: Guids of the vdisks
        :type vdisk_guids: list
        :param manual: Indicator whether the execute_scrub is called manually or as scheduled task (automatically)
        :type manual: bool
        :return: The mapping
        :rtype: dict
        """
        if vdisk_guids is None:
            vdisk_guids = []
        if vpool_guids is None:
            vpool_guids = []
        if manual is True:
            vpool_vdisk_map = {}
            for vpool_guid in set(vpool_guids):
                vpool = VPool(vpool_guid)
                vpool_vdisk_map[vpool] = list(vpool.vdisks)
            for vdisk_guid in set(vdisk_guids):
                try:
                    vdisk = VDisk(vdisk_guid)
                    if vdisk.vpool not in vpool_vdisk_map:
                        vpool_vdisk_map[vdisk.vpool] = []
                    if vdisk not in vpool_vdisk_map[vdisk.vpool]:
                        vpool_vdisk_map[vdisk.vpool].append(vdisk)
                except ObjectNotFoundException:
                    cls._logger.warning('vDisk with guid {0} is no longer available. Skipping'.format(vdisk_guid))
        else:
            vpool_vdisk_map = dict((vpool, list(vpool.vdisks)) for vpool in VPoolList.get_vpools())
        return vpool_vdisk_map

    def get_scrub_locations(self, storagerouter_guid=None):
        """
        Retrieve all scrub locations
        :param storagerouter_guid: Guid of the StorageRouter to execute the scrub work on
        :type storagerouter_guid: str
        :raises: ValueError when no scrub locations could be found
        :return: Scrubbing location info (Example: [{'scrub_path': FOLDER, 'partition_guid': GUID, 'storagerouter': StorageRouter object}]
        :rtype: list[dict]
        """
        scrub_locations = []
        storagerouters = StorageRouterList.get_storagerouters() if storagerouter_guid is None else [StorageRouter(storagerouter_guid)]
        for storagerouter in storagerouters:
            if storagerouter not in self.clients:
                # StorageRouter is assumed to be dead so not using it as a scrubbing location
                continue
            scrub_partitions = storagerouter.partition_config.get(DiskPartition.ROLES.SCRUB, [])
            if len(scrub_partitions) == 0:
                continue
            try:
                for partition_guid in scrub_partitions:
                    partition = DiskPartition(partition_guid)
                    self._logger.info(self._format_message('Storage Router {0} has {1} partition at {2}'.format(storagerouter.ip, DiskPartition.ROLES.SCRUB, partition.folder)))
                    scrub_locations.append({'scrub_path': str(partition.folder),
                                            'partition_guid': partition.guid,
                                            'storagerouter': storagerouter})
            except UnableToConnectException:
                self._logger.warning(self._format_message('Storage Router {0} is not reachable'.format(storagerouter.ip)))
            except Exception:
                self._logger.exception(self._format_message('Could not retrieve worker information of Storage Router {0}'.format(storagerouter.ip)))

        if len(scrub_locations) == 0:
            raise ValueError('No scrub locations found, cannot scrub')
        return scrub_locations

    def _cleanup_job_entries(self):
        """
        Clean up job entries which have been stored longer than the _KEY_LIFETIME number of seconds
        :return: List of removed keys
        :rtype: list
        """
        removed_keys = []
        try:
            with volatile_mutex('scrubber_clean_entries', wait=30):
                for key in Configuration.list(self._SCRUB_KEY):
                    full_key = os.path.join(self._SCRUB_KEY, key)
                    job_info = Configuration.get('{0}/job_info'.format(full_key))
                    time_start = job_info.get('time_start')
                    time_end = job_info.get('time_end')
                    if time_start is None or (time_end is not None and time_end - time_start >= self._KEY_LIFETIME):
                        Configuration.delete(full_key)
                        removed_keys.append(full_key)
                if len(removed_keys) > 0:
                    self._logger.info(self._format_message('Cleaned up the following outdated scrub keys: {0}'.format('\n - '.join(removed_keys))))
        except NoLockAvailableException:
            self._logger.warning(self._format_message('Could not get the lock to clean entries'))
        except Exception:
            self._logger.exception(self._format_message('Unable to clear entries'))
        return removed_keys
