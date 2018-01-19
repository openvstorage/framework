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
import json
import time
import uuid
from Queue import Empty, Queue
from random import randint
from threading import Thread
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.generic.volatilemutex import volatile_mutex, NoLockAvailableException
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs_extensions.storage.exceptions import AssertException
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.lib.mdsservice import MDSServiceController
from ovs.log.log_handler import LogHandler


class ScrubShared(object):
    """
    Class which has functions to ensure consistency
    """
    _logger = LogHandler.get('lib', name='generic tasks scrub')

    _SCRUB_KEY = 'ovs/framework/jobs/scrub'  # Parent key for all scrub related jobs
    _SCRUB_NAMESPACE = 'ovs_jobs_scrub'

    def __init__(self):
        self._persistent = PersistentFactory.get_client()

    def _safely_store(self, key, value, expected_value, logging_start, key_not_exist=False, max_retries=5, hooks=None):
        """
        Safely store a key/value pair within the persistent storage
        :param key: Key to store
        :type key: str
        :param value: Value to store
        :type value: any
        :param expected_value: Current value on the storage side
        :type expected_value: any
        :param logging_start: Start of the logging line
        :type logging_start: str
        :param max_retries: Number of retries to attempt
        :type max_retries: int
        :param key_not_exist: Only store if the key does not exist
        :type key_not_exist: bool
        :param hooks: Hooks to execute on certain conditions:
        - assert_fail_before: function to be called before other assert_fail hooks would be called
        - assert_fail_value: function to be called after the assert fail which changes the value to be saved
        - assert_fail_expected: function to be called after the assert fail which changes the 'expect_value' to be asserted
        :return: Stored value or the current value if key_not_exists is True and the key is already present
        :rtype: any
        :raises: AssertException:
        - When the save could not happen
        """
        def _execute_hook(hook, args=None, kwargs=None):
            if kwargs is None:
                kwargs = {}
            if args is None:
                args = []
            func = hooks.get(hook)
            if func is not None and callable(func):
                hook(*args, **kwargs)

        if hooks is None:
            hooks = {}
        transaction = self._persistent.begin_transaction()
        tries = 0
        success = False
        last_exception = None
        return_value = value
        if key_not_exist is True and self._persistent.exists(key) is True:
            return_value = self._persistent.get(key)
            success = True
            self._logger.debug('{0} - key {1} is already present and key_not_exist given. Not saving and returning current value ({2})'
                               .format(logging_start, key, return_value))
        while success is False:
            if key_not_exist is True and self._persistent.exists(key) is True:
                # No need to save a new one ourselves
                success = True
                return_value = self._persistent.get(key)
                continue
            tries += 0
            if tries > max_retries:
                raise last_exception
            self._persistent.assert_value(key, expected_value, transaction=transaction)
            self._persistent.set(key, value, transaction=transaction)
            try:
                self._logger.debug('{0} - Registering key ({1}:{2})'.format(logging_start, key, value))
                self._persistent.apply_transaction(transaction)
                success = True
            except AssertException as ex:
                self._logger.debug('{0} - Asserting failed. Retrying {1} more times'.format(logging_start, max_retries - tries))
                last_exception = ex
                time.sleep(randint(0, 25) / 100.0)
                _execute_hook('assert_fail_before')
                _execute_hook('assert_fail_expected')
                _execute_hook('assert_fail_value')
        return return_value


class StackWorkGenerator(ScrubShared):
    """
    Handles generation and saving of vpool stack work
    """
    def __init__(self, vpool, vdisks):
        """
        Initialize
        :param vpool: vPool to generate work for
        :param vdisks: vDisks to include in the work
        """
        super(StackWorkGenerator, self).__init__()

        self.vpool = vpool
        self.vdisks = vdisks
        self.work_queue = Queue()

        self._registered_work_items = None
        self._key = '{0}_vpool_{1}'.format(Scrubber._SCRUB_NAMESPACE, vpool.guid)  # Key to store stack work under
        self._log = 'Scrubber - vPool {0}'.format(self.vpool.name)

    def generate_save_scrub_work(self):
        """
        Generates applicable scrub work and saves the scrub work consistently
        Generates scrub work to be done
        :return: Queue of work items
        """
        work_queue = self._generate_scrub_work()
        if work_queue.qsize() > 0:
            work_queue = self._save_work()
        return work_queue

    def _generate_scrub_work(self):
        """
        Generates scrub work to be done
        :return: Queue of work items
        """
        # Save the key if it does not exists, else return the value
        self._registered_work_items = self._safely_store(self._key, [], expected_value=None, key_not_exist=True, logging_start=self._log)
        # Clear current queue
        with self.work_queue.mutex:
            self.work_queue.queue.clear()
        for vd in self.vdisks:
            logging_start_vd = '{0} - vDisk {1} {2}'.format(self._log, vd.guid, vd.name)
            if vd.guid in self._registered_work_items:
                self._logger.info('{0} - has already been registered to get scrubbed, not queueing again'.format(logging_start_vd))
                continue
            if vd.is_vtemplate is True:
                self._logger.info('{0} - Is a template, not scrubbing'.format(logging_start_vd))
                continue
            vd.invalidate_dynamics('storagedriver_id')
            if not vd.storagedriver_id:
                self._logger.warning('{0} - No StorageDriver ID found'.format(logging_start_vd))
                continue
            self.work_queue.put(vd.guid)
        return self.work_queue

    def _save_work(self):
        """
        Consistently save the generated scrub work
        :return: Current work to be done
        :rtype: Queue
        """
        # Register some hooks to combat race conditions: Scrubbing vdisks might have changed (some removed, some added)
        # To combat this, regenerate our work and apply
        hooks = {'assert_fail_before': self._generate_scrub_work,
                 'assert_fail_value': self._get_total_work_items,
                 # Total items will be changed as registered items are fetched again and work items generated again
                 'assert_fail_expected': lambda *args, **kwargs: self._registered_work_items}  # Registered items will be fetched again by the generator

        # Attempt to save with all fetched data during work generation, expect the current key to not have changed
        self._safely_store(self._key, self._get_total_work_items(),
                           expected_value=self._registered_work_items,
                           logging_start=self._log,
                           hooks=hooks)
        # The queue might be different of when the function got called due to the hooking in the save
        return self.work_queue

    def _get_total_work_items(self, *args, **kwargs):
        """
        Computes the total work to be done for scrubbing by adding up the current work with the existing work
        :return: list of total work
        :rtype: list
        """
        _ = args, kwargs
        return self._registered_work_items + list(self.work_queue.queue)


class StackWorker(ScrubShared):
    """
    This class represents a worker of the scrubbing stack
    """
    def __init__(self, queue, vpool, scrub_info, error_messages):
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
        """
        super(StackWorker, self).__init__()
        self.queue = queue
        self.vpool = vpool
        self.error_messages = error_messages

        self.storagerouter = scrub_info['storagerouter']
        self.partition_guid = scrub_info['partition_guid']
        self.alba_proxy_service = 'ovs-albaproxy_{0}_{1}_{2}_scrub'.format(self.vpool.name, self.storagerouter.name, self.partition_guid)
        self.scrub_directory = '{0}/scrub_work_{1}'.format(scrub_info['scrub_path'], uuid.uuid4())
        self.scrub_config_key = 'ovs/vpools/{0}/proxies/scrub/scrub_config_{1}'.format(vpool.guid, self.partition_guid)
        self.backend_config_key = 'ovs/vpools/{0}/proxies/scrub/backend_config_{1}'.format(vpool.guid, self.partition_guid)

        self.lock_time = 5 * 60
        self.lock_key = 'ovs_albaproxy_scrub_{0}'.format(self.alba_proxy_service)
        self.service_manager = ServiceFactory.get_manager()
        self.client = None
        self.scrub_config = None

        self._log = 'Scrubber - vPool {0} - StorageRouter {1}'.format(self.vpool.name, self.storagerouter.name)

    def deploy_stack_and_scrub(self):
        """
        Executes scrub work for a given vDisk queue and vPool, based on scrub_info
        - Sets up a scrubbing proxy (if the proxy is not present yet)
        - Launches scrubbing threads
        - Cleans up the scrubbing proxy (if the proxy is no longer used)
        :return: None
        :rtype: NoneType
        """
        if len(self.vpool.storagedrivers) == 0 or not self.vpool.storagedrivers[0].storagedriver_id:
            self.error_messages.append('vPool {0} does not have any valid StorageDrivers configured'.format(self.vpool.name))
            return

        service_manager = ServiceFactory.get_manager()
        client = None
        random_uuid = uuid.uuid4()
        storagerouter = self.scrub_info['storagerouter']
        partition_guid = self.scrub_info['partition_guid']
        alba_proxy_service = 'ovs-albaproxy_{0}_{1}_{2}_scrub'.format(self.vpool.name, storagerouter.name, partition_guid)
        scrub_directory = '{0}/scrub_work_{1}'.format(self.scrub_info['scrub_path'], random_uuid)
        scrub_config_key = 'ovs/vpools/{0}/proxies/scrub/scrub_config_{1}'.format(vpool.guid, partition_guid)
        backend_config_key = 'ovs/vpools/{0}/proxies/scrub/backend_config_{1}'.format(vpool.guid, partition_guid)
        lock_time = 5 * 60
        lock_key = 'ovs_albaproxy_scrub_{0}'.format(alba_proxy_service)

        # Deploy a proxy
        scrub_config = None
        self._deploy_proxy()

        # Execute the actual scrubbing
        threads = []
        threads_key = '/ovs/framework/hosts/{0}/config|scrub_stack_threads'.format(storagerouter.machine_id)
        amount_threads = Configuration.get(key=threads_key) if Configuration.exists(key=threads_key) else 2
        if not isinstance(amount_threads, int):
            self.error_messages.append('Amount of threads to spawn must be an integer for StorageRouter with ID {0}'.format(storagerouter.machine_id))
            return

        amount_threads = max(amount_threads, 1)  # Make sure amount_threads is at least 1
        amount_threads = min(min(self.queue.qsize(), amount_threads), 20)  # Make sure amount threads is max 20
        self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Spawning {2} threads for proxy service {3}'.format(self.vpool.name, storagerouter.name, amount_threads, alba_proxy_service))
        for index in range(amount_threads):
            thread = Thread(name='execute_scrub_{0}_{1}_{2}'.format(self.vpool.guid, partition_guid, index),
                            target=self._execute_scrub,
                            args=(queue, vpool, scrub_info, scrub_directory, self.error_messages))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

        # Delete the proxy again
        try:
            with volatile_mutex(name=lock_key, wait=lock_time):
                self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Removing service {2}'.format(vpool.name, storagerouter.name, alba_proxy_service))
                client = SSHClient(storagerouter, 'root')
                client.dir_delete(scrub_directory)
                if service_manager.has_service(alba_proxy_service, client=client):
                    service_manager.stop_service(alba_proxy_service, client=client)
                    service_manager.remove_service(alba_proxy_service, client=client)
                if Configuration.exists(scrub_config_key):
                    Configuration.delete(scrub_config_key)
                self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Removed service {2}'.format(vpool.name, storagerouter.name, alba_proxy_service))
        except Exception:
            message = 'Scrubber - vPool {0} - StorageRouter {1} - Removing service {2} failed'.format(vpool.name, storagerouter.name, alba_proxy_service)
            error_messages.append(message)
            self._logger.exception(message)

    def _deploy_proxy(self):
        """
        Deploy a scrubbing proxy
        - Validates if a proxy is already present
        - Sets up a proxy consistently
        :return: The config to use for scrubbing
        :rtype: dict
        """
        scrub_config = None
        try:
            # @todo keep track of what proxy is doing what (figure out an Arakoon key which can be shared across multiple jobs and how to detect if one is busy >>
            # Locking with volatile as different workers need to lock the remote detection/deployment of the proxy
            with volatile_mutex(name=self.lock_key, wait=self.lock_time):
                self._logger.info('{0} - Deploying ALBA proxy {1}'.format(self._log, self.alba_proxy_service))
                self.client = SSHClient(self.storagerouter, 'root')
                self.client.dir_create(self.scrub_directory)
                self.client.dir_chmod(self.scrub_directory, 0777)  # Celery task executed by 'ovs' user and should be able to write in it
                if self.service_manager.has_service(name=self.alba_proxy_service, client=self.client) is True and self.service_manager.get_service_status(name=self.alba_proxy_service, client=client) == 'active':
                    self._logger.info('{0} - Re-using existing proxy service {1}'.format(self._log, self.alba_proxy_service))
                    self.scrub_config = Configuration.get(self.scrub_config_key)
                else:
                    machine_id = System.get_my_machine_id(self.client)
                    port_range = Configuration.get('/ovs/framework/hosts/{0}/ports|storagedriver'.format(machine_id))
                    with volatile_mutex('deploy_proxy_for_scrub_{0}'.format(self.storagerouter.guid), wait=30):
                        port = System.get_free_ports(selected_range=port_range, nr=1, client=self.client)[0]
                    scrub_config = Configuration.get('ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(vpool.guid))
                    scrub_config['port'] = port
                    scrub_config['transport'] = 'tcp'
                    self.scrub_config = scrub_config
                    Configuration.set(self.scrub_config_key, json.dumps(scrub_config, indent=4), raw=True)

                    params = {'VPOOL_NAME': self.vpool.name,
                              'LOG_SINK': LogHandler.get_sink_path(self.alba_proxy_service),
                              'CONFIG_PATH': Configuration.get_configuration_path(self.scrub_config_key)}
                    self.service_manager.add_service(name='ovs-albaproxy', params=params, client=self.client, target_name=self.alba_proxy_service)
                    self.service_manager.start_service(name=self.alba_proxy_service, client=self.client)
                    self._logger.info('{0} - Deployed ALBA proxy {1} (Config: {2})'.format(self._log, self.alba_proxy_service, scrub_config))
                    # @Todo better check if editing needs to happen (other threads also call this)
                backend_config = Configuration.get('ovs/vpools/{0}/hosts/{1}/config'.format(self.vpool.guid, self.vpool.storagedrivers[0].storagedriver_id))['backend_connection_manager']
                if backend_config.get('backend_type') != 'MULTI':
                    backend_config['alba_connection_host'] = '127.0.0.1'
                    backend_config['alba_connection_port'] = scrub_config['port']
                else:
                    for value in backend_config.itervalues():
                        if isinstance(value, dict):
                            value['alba_connection_host'] = '127.0.0.1'
                            value['alba_connection_port'] = scrub_config['port']
                # Copy backend connection manager information in separate key
                Configuration.set(self.backend_config_key, json.dumps({"backend_connection_manager": backend_config}, indent=4), raw=True)
                # Todo register usage of the proxy consistently
        except Exception:
            message = '{0} - An error occurred deploying ALBA proxy {1}'.format(self._log, self.alba_proxy_service)
            if scrub_config is not None:
                message = '{0} (Config: {1})'.format(message, scrub_config)
            self.error_messages.append(message)
            self._logger.exception(message)
        return scrub_config

    def _remove_proxy(self):
        """
        Removes the proxy that was used
        :return: None
        """
        # @todo check if other workers are using the proxy
        if self.client is not None and self.service_manager.has_service(name=self.alba_proxy_service, client=self.client) is True:
            if self.service_manager.get_service_status(name=self.alba_proxy_service, client=self.client) == 'active':
                self.service_manager.stop_service(name=self.alba_proxy_service, client=self.client)
            self.service_manager.remove_service(name=self.alba_proxy_service, client=self.client)
        if Configuration.exists(self.scrub_config_key):
            Configuration.delete(self.scrub_config_key)


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
    # @todo set key lifetime back to 7 days
    # _KEY_LIFETIME = 7 * 24 * 60 * 60  # All job keys are kept for 7 days and after that the next scrubbing job will remove the outdated ones
    _KEY_LIFETIME = 1

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

        super(Scrubber, self).__init__()

        self.scrub_id = task_id or str(uuid.uuid4())
        self.task_id = task_id  # Be able to differentiate between directly executed ones for debugging purposes
        self.vdisk_guids = vdisk_guids
        self.vpool_guids = vpool_guids
        self.storagerouter_guid = storagerouter_guid
        self.manual = manual

        self.time_start = None
        self.time_end = None

        self.vpool_vdisk_map = self.generate_vpool_vdisk_map(vpool_guids=vpool_guids, vdisk_guids=vdisk_guids, manual=manual)
        self.scrub_locations = self.get_scrub_locations(storagerouter_guid)

        # Scrubbing stack
        self.error_messages = []  # Keep track of all messages that might occur
        self.max_stacks_per_vpool = None
        self.stacks = {}
        self.stack_threads = []

    def execute_scrubbing(self):
        """
        Execute the scrubbing work
        Every vpool will have its own set of stacks to scrub. These stacks deploy scrubbing threads internally
        The number of stacks for every vpool is calculated based on the number of vpools to scrub in total ( 6+ -> 1/vpool, 6>x>=3 -> 2/vpool, 3> -> 3/vpool)
        :return: None
        :rtype: NoneType
        """
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
        for vp, vdisks in self.vpool_vdisk_map.iteritems():
            logging_start = 'Scrubber - vPool {0}'.format(vp.name)
            # Verify amount of vDisks on vPool
            self._logger.info('{0} - Checking scrub work'.format(logging_start))
            stack_work_generator = StackWorkGenerator(vpool=vp, vdisks=vdisks)
            vpool_queue = stack_work_generator.generate_save_scrub_work()
            if vpool_queue.qsize() == 0:
                self._logger.info('{0} - No scrub work'.format(logging_start))
                continue
            stacks_to_spawn = min(self.max_stacks_per_vpool, len(self.scrub_locations))
            self._logger.info('{0} - Spawning {1} stack{2}'.format(logging_start, stacks_to_spawn, '' if stacks_to_spawn == 1 else 's'))
            for _ in xrange(stacks_to_spawn):
                scrub_target = self.scrub_locations[counter % len(self.scrub_locations)]
                stack = Thread(target=self._deploy_stack_and_scrub,
                               args=(vpool_queue, vp, scrub_target, self.error_messages))
                stack.start()
                self.stack_threads.append(stack)
                counter += 1

        for thread in self.stack_threads:
            thread.join()

        # Update the job info
        self.time_end = time.time()
        self.set_main_job_info()

        self._cleanup_job_entries()

        if len(self.error_messages) > 0:
            raise Exception('Errors occurred while scrubbing:\n  - {0}'.format('\n  - '.join(self.error_messages)))

    def _deploy_stack_and_scrub(self, queue, vpool, scrub_info, error_messages):
        """
        Executes scrub work for a given vDisk queue and vPool, based on scrub_info
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
        :return: None
        :rtype: NoneType
        """
        if len(vpool.storagedrivers) == 0 or not vpool.storagedrivers[0].storagedriver_id:
            error_messages.append('vPool {0} does not have any valid StorageDrivers configured'.format(vpool.name))
            return

        service_manager = ServiceFactory.get_manager()
        client = None
        random_uuid = uuid.uuid4()
        storagerouter = scrub_info['storagerouter']
        partition_guid = scrub_info['partition_guid']
        alba_proxy_service = 'ovs-albaproxy_{0}_{1}_{2}_scrub'.format(vpool.name, storagerouter.name, partition_guid)
        scrub_directory = '{0}/scrub_work_{1}'.format(scrub_info['scrub_path'], random_uuid)
        scrub_config_key = 'ovs/vpools/{0}/proxies/scrub/scrub_config_{1}'.format(vpool.guid, partition_guid)
        backend_config_key = 'ovs/vpools/{0}/proxies/scrub/backend_config_{1}'.format(vpool.guid, partition_guid)
        lock_time = 5 * 60
        lock_key = 'ovs_albaproxy_scrub_{0}'.format(alba_proxy_service)

        # Deploy a proxy
        scrub_config = None
        try:
            # @todo keep track of what proxy is doing what (figure out an Arakoon key which can be shared across multiple jobs and how to detect if one is busy >>
            # Locking with volatile as different workers need to lock the remote detection/deployment of the proxy
            with volatile_mutex(name=lock_key, wait=lock_time):
                self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Deploying ALBA proxy {2}'.format(vpool.name, storagerouter.name, alba_proxy_service))
                client = SSHClient(storagerouter, 'root')
                client.dir_create(scrub_directory)
                client.dir_chmod(scrub_directory, 0777)  # Celery task executed by 'ovs' user and should be able to write in it
                if service_manager.has_service(name=alba_proxy_service, client=client) is True and service_manager.get_service_status(name=alba_proxy_service, client=client) == 'active':
                    self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Re-using existing proxy service {2}'.format(vpool.name, storagerouter.name, alba_proxy_service))
                    scrub_config = Configuration.get(scrub_config_key)
                else:
                    machine_id = System.get_my_machine_id(client)
                    port_range = Configuration.get('/ovs/framework/hosts/{0}/ports|storagedriver'.format(machine_id))
                    with volatile_mutex('deploy_proxy_for_scrub_{0}'.format(storagerouter.guid), wait=30):
                        port = System.get_free_ports(selected_range=port_range, nr=1, client=client)[0]
                    scrub_config = Configuration.get('ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(vpool.guid))
                    scrub_config['port'] = port
                    scrub_config['transport'] = 'tcp'
                    Configuration.set(scrub_config_key, json.dumps(scrub_config, indent=4), raw=True)

                    params = {'VPOOL_NAME': vpool.name,
                              'LOG_SINK': LogHandler.get_sink_path(alba_proxy_service),
                              'CONFIG_PATH': Configuration.get_configuration_path(scrub_config_key)}
                    service_manager.add_service(name='ovs-albaproxy', params=params, client=client, target_name=alba_proxy_service)
                    service_manager.start_service(name=alba_proxy_service, client=client)
                    self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Deployed ALBA proxy {2} (Config: {3})'.format(vpool.name, storagerouter.name, alba_proxy_service, scrub_config))

                # @Todo better check if editing needs to happen (other threads also call this)
                backend_config = Configuration.get('ovs/vpools/{0}/hosts/{1}/config'.format(vpool.guid, vpool.storagedrivers[0].storagedriver_id))['backend_connection_manager']
                if backend_config.get('backend_type') != 'MULTI':
                    backend_config['alba_connection_host'] = '127.0.0.1'
                    backend_config['alba_connection_port'] = scrub_config['port']
                else:
                    for value in backend_config.itervalues():
                        if isinstance(value, dict):
                            value['alba_connection_host'] = '127.0.0.1'
                            value['alba_connection_port'] = scrub_config['port']
                # Copy backend connection manager information in separate key
                Configuration.set(backend_config_key, json.dumps({"backend_connection_manager": backend_config}, indent=4), raw=True)
        except Exception:
            message = 'Scrubber - vPool {0} - StorageRouter {1} - An error occurred deploying ALBA proxy {2}'.format(vpool.name, storagerouter.name, alba_proxy_service)
            if scrub_config is not None:
                message = '{0} (Config: {1})'.format(message, scrub_config)
            error_messages.append(message)
            self._logger.exception(message)
            if client is not None and service_manager.has_service(name=alba_proxy_service, client=client) is True:
                if service_manager.get_service_status(name=alba_proxy_service, client=client) == 'active':
                    service_manager.stop_service(name=alba_proxy_service, client=client)
                service_manager.remove_service(name=alba_proxy_service, client=client)
            if Configuration.exists(scrub_config_key):
                Configuration.delete(scrub_config_key)

        # Execute the actual scrubbing
        threads = []
        threads_key = '/ovs/framework/hosts/{0}/config|scrub_stack_threads'.format(storagerouter.machine_id)
        amount_threads = Configuration.get(key=threads_key) if Configuration.exists(key=threads_key) else 2
        if not isinstance(amount_threads, int):
            error_messages.append('Amount of threads to spawn must be an integer for StorageRouter with ID {0}'.format(storagerouter.machine_id))
            return

        amount_threads = max(amount_threads, 1)  # Make sure amount_threads is at least 1
        amount_threads = min(min(queue.qsize(), amount_threads), 20)  # Make sure amount threads is max 20
        self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Spawning {2} threads for proxy service {3}'.format(vpool.name, storagerouter.name, amount_threads, alba_proxy_service))
        for index in range(amount_threads):
            thread = Thread(name='execute_scrub_{0}_{1}_{2}'.format(vpool.guid, partition_guid, index),
                            target=self._execute_scrub,
                            args=(queue, vpool, scrub_info, scrub_directory, error_messages))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

        # Delete the proxy again
        try:
            with volatile_mutex(name=lock_key, wait=lock_time):
                self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Removing service {2}'.format(vpool.name, storagerouter.name, alba_proxy_service))
                client = SSHClient(storagerouter, 'root')
                client.dir_delete(scrub_directory)
                if service_manager.has_service(alba_proxy_service, client=client):
                    service_manager.stop_service(alba_proxy_service, client=client)
                    service_manager.remove_service(alba_proxy_service, client=client)
                if Configuration.exists(scrub_config_key):
                    Configuration.delete(scrub_config_key)
                self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Removed service {2}'.format(vpool.name, storagerouter.name, alba_proxy_service))
        except Exception:
            message = 'Scrubber - vPool {0} - StorageRouter {1} - Removing service {2} failed'.format(vpool.name, storagerouter.name, alba_proxy_service)
            error_messages.append(message)
            self._logger.exception(message)

    def _execute_scrub(self, queue, vpool, scrub_info, scrub_dir, error_messages):
        def _verify_mds_config(current_vdisk):
            current_vdisk.invalidate_dynamics('info')
            vdisk_configs = current_vdisk.info['metadata_backend_config']
            if len(vdisk_configs) == 0:
                raise RuntimeError('Could not load MDS configuration')
            return vdisk_configs

        storagerouter = scrub_info['storagerouter']
        partition_guid = scrub_info['partition_guid']
        volatile_client = VolatileFactory.get_client()
        backend_config_key = 'ovs/vpools/{0}/proxies/scrub/backend_config_{1}'.format(vpool.guid, partition_guid)
        try:
            # Empty the queue with vDisks to scrub
            with remote(storagerouter.ip, [VDisk]) as rem:
                while True:
                    vdisk = None
                    vdisk_guid = queue.get(
                        False)  # Raises Empty Exception when queue is empty, so breaking the while True loop
                    volatile_key = 'ovs_scrubbing_vdisk_{0}'.format(vdisk_guid)
                    try:
                        # Check MDS master is local. Trigger MDS handover if necessary
                        vdisk = rem.VDisk(vdisk_guid)
                        self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - Started scrubbing at location {3}'.format(vpool.name, storagerouter.name, vdisk.name, scrub_dir))
                        configs = _verify_mds_config(current_vdisk=vdisk)
                        storagedriver = StorageDriverList.get_by_storagedriver_id(vdisk.storagedriver_id)
                        if configs[0].get('ip') != storagedriver.storagerouter.ip:
                            self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - MDS master is not local, trigger handover'.format(vpool.name, storagerouter.name, vdisk.name))
                            MDSServiceController.ensure_safety(vdisk_guid=vdisk_guid)  # Do not use a remote VDisk instance here
                            configs = _verify_mds_config(current_vdisk=vdisk)
                            if configs[0].get('ip') != storagedriver.storagerouter.ip:
                                self._logger.warning('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - Skipping because master MDS still not local'.format(vpool.name, storagerouter.name, vdisk.name))
                                continue

                        # Check if vDisk is already being scrubbed
                        if volatile_client.add(key=volatile_key, value=volatile_key, time=24 * 60 * 60) is False:
                            self._logger.warning('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - Skipping because vDisk is already being scrubbed'.format(vpool.name, storagerouter.name, vdisk.name))
                            continue

                        # Do the actual scrubbing
                        with vdisk.storagedriver_client.make_locked_client(str(vdisk.volume_id)) as locked_client:
                            self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - Retrieve and apply scrub work'.format(
                                    vpool.name, storagerouter.name, vdisk.name))
                            work_units = locked_client.get_scrubbing_workunits()
                            for work_unit in work_units:
                                res = locked_client.scrub(work_unit=work_unit,
                                                          scratch_dir=scrub_dir,
                                                          log_sinks=[LogHandler.get_sink_path(
                                                              'scrubber_{0}'.format(vpool.name), allow_override=True,
                                                              forced_target_type='file')],
                                                          backend_config=Configuration.get_configuration_path(
                                                              backend_config_key))
                                locked_client.apply_scrubbing_result(scrubbing_work_result=res)
                            if work_units:
                                self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - {3} work units successfully applied'.format(
                                        vpool.name, storagerouter.name, vdisk.name, len(work_units)))
                            else:
                                self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - No scrubbing required'.format(vpool.name, storagerouter.name, vdisk.name))
                    except Exception:
                        if vdisk is None:
                            message = 'Scrubber - vPool {0} - StorageRouter {1} - vDisk with guid {2} could not be found'.format(
                                vpool.name, storagerouter.name, vdisk_guid)
                        else:
                            message = 'Scrubber - vPool {0} - StorageRouter {1} - vDisk {2} - Scrubbing failed'.format(
                                vpool.name, storagerouter.name, vdisk.name)
                        error_messages.append(message)
                        self._logger.exception(message)
                    finally:
                        # Remove vDisk from volatile memory
                        volatile_client.delete(volatile_key)

        except Empty:  # Raised when all items have been fetched from the queue
            self._logger.info('Scrubber - vPool {0} - StorageRouter {1} - Queue completely processed'.format(vpool.name, storagerouter.name))
        except Exception:
            message = 'Scrubber - vPool {0} - StorageRouter {1} - Scrubbing failed'.format(vpool.name, storagerouter.name)
            error_messages.append(message)
            self._logger.exception(message)

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

        job_key = '{0}/{1}/job_info'.format(self._SCRUB_KEY, self.scrub_id)
        job_info = {'scrub_locations': [self._covert_data_objects(x) for x in self.scrub_locations],
                    'task_id': self.task_id,
                    'max_stacks_per_vpool': self.max_stacks_per_vpool,
                    'vpool_vdisk_map': self._covert_data_objects(self.vpool_vdisk_map),
                    'time_start': self.time_start,
                    'time_end': self.time_end}
        Configuration.set(job_key, json.dumps(job_info, indent=4), raw=True)

    @staticmethod
    def generate_vpool_vdisk_map(vpool_guids=None, vdisk_guids=None, manual=False):
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
                vdisk = VDisk(vdisk_guid)
                if vdisk.vpool not in vpool_vdisk_map:
                    vpool_vdisk_map[vdisk.vpool] = []
                if vdisk not in vpool_vdisk_map[vdisk.vpool]:
                    vpool_vdisk_map[vdisk.vpool].append(vdisk)
        else:
            vpool_vdisk_map = dict((vpool, list(vpool.vdisks)) for vpool in VPoolList.get_vpools())
        return vpool_vdisk_map

    @classmethod
    def get_scrub_locations(cls, storagerouter_guid=None):
        """
        Retrieve all scrub locations
        :param storagerouter_guid: Guid of the StorageRouter to execute the scrub work on
        :type storagerouter_guid: str
        :raises: ValueError when no scrub locations could be found
        :return: Scrubbing location info (Example: [{'scrub_path': FOLDER, 'partition_guid': GUID, 'storagerouter': StorageRouter object}]
        :rtype: list[dict]
        """
        scrub_locations = []
        storagerouters = StorageRouterList.get_storagerouters() if storagerouter_guid is None else [
            StorageRouter(storagerouter_guid)]
        for storagerouter in storagerouters:
            scrub_partitions = storagerouter.partition_config.get(DiskPartition.ROLES.SCRUB, [])
            if len(scrub_partitions) == 0:
                continue
            try:
                SSHClient(endpoint=storagerouter, username='root')
                for partition_guid in scrub_partitions:
                    partition = DiskPartition(partition_guid)
                    cls._logger.info('Scrubber - Storage Router {0} has {1} partition at {2}'.format(storagerouter.ip, DiskPartition.ROLES.SCRUB, partition.folder))
                    scrub_locations.append({'scrub_path': str(partition.folder),
                                            'partition_guid': partition.guid,
                                            'storagerouter': storagerouter})
            except UnableToConnectException:
                cls._logger.warning('Scrubber - Storage Router {0} is not reachable'.format(storagerouter.ip))

        if len(scrub_locations) == 0:
            raise ValueError('No scrub locations found, cannot scrub')
        return scrub_locations

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

    @classmethod
    def _cleanup_job_entries(cls):
        """
        Clean up job entries which have been stored longer than the _KEY_LIFETIME number of seconds
        :return: List of removed keys
        :rtype: list
        """
        removed_keys = []
        try:
            with volatile_mutex('scrubber_clean_entries', wait=30):
                for key in Configuration.list(cls._SCRUB_KEY):
                    full_key = '{0}/{1}'.format(cls._SCRUB_KEY, key)
                    job_info = Configuration.get('{0}/job_info'.format(full_key))
                    time_start = job_info.get('time_start')
                    time_end = job_info.get('time_end')
                    if time_start is None or (time_end is not None and time_end - time_start >= cls._KEY_LIFETIME):
                        Configuration.delete(full_key)
                        removed_keys.append(full_key)
                if len(removed_keys) > 0:
                    cls._logger.info('Cleaned up the following outdated scrub keys: {0}'.format('\n - '.join(removed_keys)))
        except NoLockAvailableException:
            cls._logger.warning('Could not get the lock to clean entries')
        return removed_keys
