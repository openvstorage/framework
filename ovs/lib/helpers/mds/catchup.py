# Copyright (C) 2018 iNuron NV
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
MDS Catchup module
"""

import time
import uuid
import collections
from random import randint
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.service import Service
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs_extensions.storage.exceptions import AssertException
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storageserver.storagedriver import MDSMetaDataBackendConfig, MDSNodeConfig, MetadataServerClient
from ovs_extensions.testing.exceptions import WorkerLossException
from ovs.lib.helpers.mds.shared import MDSShared
from ovs.log.log_handler import LogHandler


class MDSCatchUp(MDSShared):
    """
    Class responsible for catching up MDSes asynchronously
    - Registers metadata in Arakoon to ensure that only one catchup happens
    - Offloads the catchup to a new thread: if the worker process would get killed:
      the catchup would still happen by the MDSClient so a re-locking will be happening and it will wait for the original
      catchup to finish
    """
    # Extra caching
    _volumedriver_contexts_cache = {}
    _worker_contexts_cache = {}

    _logger = LogHandler.get('lib', 'mds catchup')

    _CATCH_UP_NAME_SPACE = 'ovs_jobs_catchup'
    _CATCH_UP_VDISK_KEY = '{0}_{{0}}'.format(_CATCH_UP_NAME_SPACE)  # Second format should be the vdisk guid

    def __init__(self, vdisk_guid):
        # type: (str) -> None
        """
        Initializes a new MDSCatchUp
        :param vdisk_guid: Guid of the vDisk to catch up for
        :type vdisk_guid: str
        """
        self.id = str(uuid.uuid4())
        self.vdisk = VDisk(vdisk_guid)
        self.mds_key = self._CATCH_UP_VDISK_KEY.format(self.vdisk.guid)
        self.tlog_threshold = Configuration.get('ovs/volumedriver/mds|tlogs_behind', default=100)
        self.volumedriver_service_name = 'ovs-volumedriver_{0}'.format(self.vdisk.vpool.name)
        self.mds_client_timeout = Configuration.get('ovs/vpools/{0}/mds_config|mds_client_connection_timeout'.format(self.vdisk.vpool_guid), default=120)
        self.mds_clients = {}
        self.dry_run = False

        self._service_manager = ServiceFactory.get_manager()
        self._persistent = PersistentFactory.get_client()
        self._log = 'MDS catchup {0} - vDisk {1} (volume id: {2})'.format(self.id, self.vdisk.guid, self.vdisk.volume_id)

        self._clients = self.build_clients()
        self._volumedriver_contexts = self.get_volumedriver_contexts()
        self._worker_contexts = self.get_worker_contexts()
        self._worker_context = self._worker_contexts[System.get_my_storagerouter()]
        self._relevant_contexts = self._get_all_relevant_contexts()  # All possible contexts (by mixing volumedriver ones with workers)

    def get_volumedriver_contexts(self):
        # type: () -> Dict[Service, Dict[str, str]]
        """
        Return all possible contexts that can be handled in
        :return: Information about the associated volumedrivers
        :rtype: dict
        """
        contexts = {}
        if self.vdisk.vpool not in self._volumedriver_contexts_cache:
            self._volumedriver_contexts_cache[self.vdisk.vpool] = {}
        for service in self.map_mds_services_by_socket_for_vdisk(self.vdisk).itervalues():
            if service.storagerouter not in self._volumedriver_contexts_cache[self.vdisk.vpool] or 'volumedriver_pid' not in self._volumedriver_contexts_cache[self.vdisk.vpool][service.storagerouter]:
                try:
                    if service.storagerouter not in self._clients:
                        client = self.build_ssh_client(service.storagerouter)
                        if client is None:
                            continue
                        self._clients[service.storagerouter] = client
                    client = self._clients[service.storagerouter]
                    volumedriver_pid = self._service_manager.get_service_pid(name=self.volumedriver_service_name, client=client)
                    if volumedriver_pid == 0:
                        self._logger.warning(self._format_message('Volumedriver {0} is down on StorageRouter {1}. Won\'t be able to catchup service {2}'
                                             .format(self.volumedriver_service_name, service.storagerouter.ip, service.name)))
                        continue
                    volumedriver_start = self._service_manager.get_service_start_time(name=self.volumedriver_service_name, client=client)
                    context = {'volumedriver_pid': volumedriver_pid, 'volumedriver_start': volumedriver_start}
                    self._volumedriver_contexts_cache[self.vdisk.vpool][service.storagerouter] = context
                except:
                    self._logger.exception(self._format_message('Exception while retrieving context for service {0}'.format(service.name)))
                    continue
            contexts[service] = self._volumedriver_contexts_cache[self.vdisk.vpool][service.storagerouter]
        return contexts

    def build_clients(self):
        # type: () -> Dict[StorageRouter, SSHClient]
        """
        Builds SSHClients towards all StorageRouters
        :return: SSHClient mapped by storagerouter
        :rtype: dict((storagerouter, sshclient))
        """
        clients = {}
        for storagerouter in StorageRouterList.get_storagerouters():
            client = self.build_ssh_client(storagerouter)
            if client is not None:
                clients[storagerouter] = client
        return clients

    def get_worker_contexts(self):
        # type: () -> dict
        """
        Retrieves information about the all workers (where it is executed and under what PID)
        This information is later used to check which data can be discarded (because of interrupted workers)
        :return: Information about the current workers
        :rtype: dict
        """
        workers_context = {}
        for storagerouter, client in self._clients.iteritems():
            if storagerouter not in self._worker_contexts_cache:
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
                self._worker_contexts_cache[storagerouter] = {'storagerouter_guid': storagerouter.guid,
                                                              'worker_pid': worker_pid,
                                                              'worker_start': worker_start}
            workers_context[storagerouter] = self._worker_contexts_cache[storagerouter]
        if System.get_my_storagerouter() not in workers_context:
            raise ValueError(self._format_message('The context about the workers on this machine should be known'))
        return workers_context

    def build_ssh_client(self, storagerouter, max_retries=5):
        # type: (StorageRouter, int) -> SSHClient
        """
        Build an sshclient with retries for a certain endpoint
        :param storagerouter: Point to connect too
        :type storagerouter: StorageRouter
        :param max_retries: Maximum amount of time to retry
        :return: The built sshclient
        :rtype: SSHClient
        """
        client = None
        tries = 0
        while client is None:
            tries += 1
            if tries > max_retries:
                self._logger.error(self._format_message('Assuming StorageRouter {0} is dead. Unable to checkup there'.format(storagerouter.ip)))
                break
            try:
                # Requesting new client to avoid races (if the same worker would build the clients again)
                client = SSHClient(storagerouter, username='root', timeout=30, cached=False)
            except Exception:
                self._logger.exception(self._format_message('Unable to connect to StorageRouter {0} - Retrying {1} more times before assuming it is down'.format(storagerouter.ip, max_retries - tries)))
        if client is not None:
            return client

    def _catch_up(self, mds_client, service):
        # type: (MDSClient, Service) -> None
        """
        Perform a catchup for the service
        :param mds_client: MDSClient
        :type mds_client: volumedriver.storagerouter.storagerouterclient.MDSClient
        :param service: Associated service
        :type service: Service
        :return:
        """
        registered_catchup = self.register_catch_up(service)
        do_finally = True
        reset_volumedriver_cache = False
        log_identifier = 'MDS Service {0} at {1}:{2}'.format(service.name, service.storagerouter.ip, service.ports[0])
        try:
            self._logger.info(self._format_message('{0} catch up registrations: {1}'.format(log_identifier, registered_catchup)))
            if len(registered_catchup) > 1:
                self._logger.info(self._format_message('{0} is already being caught up'.format(log_identifier)))
                return
            mds_client.catch_up(str(self.vdisk.volume_id), dry_run=self.dry_run)
        except WorkerLossException:  # Thrown during unittests to simulate a worker getting killed at this stage
            do_finally = False
            raise
        except Exception:
            self._logger.exception('Exception occurred while going to/doing catch up')
            # The volumedriver might have been killed. Invalidate the cache for this instance
            reset_volumedriver_cache = True
            raise
        finally:
            if do_finally:
                try:
                    self.unregister_catch_up(service)
                except Exception:
                    self._logger(self._format_message('{0} - Failed to unregister catchup'.format(log_identifier)))
                finally:
                    if reset_volumedriver_cache:
                        self.reset_volumedriver_cache_for_service(service)

    def catch_up(self):
        # type: () -> List[Tuple[Service, int, bool]]
        """
        Catch up all MDS services
        :return: List with information which mdses were behind and how much
        :rtype: list
        """
        behind = []
        for service in self._volumedriver_contexts.iterkeys():
            caught_up = False
            service_identifier = '{0} ({1}:{2})'.format(service.name, service.storagerouter.ip, service.ports[0])
            client = MetadataServerClient.load(service=service, timeout=self.mds_client_timeout)
            if client is None:
                self._logger.error(self._format_message('Cannot establish a MDS client connection for service {0}'.format(service_identifier)))
                continue
            try:
                # Verify how much the Service is behind (No catchup action is invoked)
                tlogs_behind_master = client.catch_up(str(self.vdisk.volume_id), dry_run=True)
            except RuntimeError:
                self._logger.exception(self._format_message('Unable to fetch the tlogs behind master for service {0}'.format(service_identifier)))
                continue
            if tlogs_behind_master >= self.tlog_threshold:
                self._logger.warning(self._format_message('Service {0} is {1} tlogs behind master. Catching up because threshold was reached ({1}/{2})'
                                     .format(service_identifier, tlogs_behind_master, self.tlog_threshold)))
                # @todo offload to a thread
                self._catch_up(client, service)
                caught_up = True
            else:
                self._logger.info(self._format_message('Service {0} does not need catching up ({1}/{2})'
                                                       .format(service_identifier, tlogs_behind_master, self.tlog_threshold)))
            behind.append((Service, tlogs_behind_master, caught_up))
        return behind

    def _get_mds_catch_ups(self, context):
        # type: (Dict[str, str]) -> List[Dict[str, str]]
        """
        Returns all relevant mds catch ups happening
        - When the volumedriver is no longer the same as for when it got registered:
        the entry will be cleared and catch up can happen again
        - When the workers died: the catchup might still have complete on the volumedriver level but the entry was not cleared:
        this entry will be cleared and the lock release
        :param context: Context object
        :type context: dict
        :return: List of relevant work items
        :rtype: list
        """
        return self._get_relevant_items(self.mds_key,
                                        relevant_values=self._relevant_contexts,
                                        relevant_keys=context.keys())

    def _get_all_relevant_contexts(self):
        # type: () -> List[Dict[str, str]]
        """
        Retrieve all possible contexts
        Combines worker information together with volumedriver information
        If the worker information would get out of date -> catch up should happen again with a refreshed lock but the
        mds client will wait for the already pending catch up to wait
        if the volumedriver would get out of date -> that worker is no longer relevant (should have received an exception)
        :return: All relevant contexts
        :rtype: list[dict]
        """
        relevant_contexts = []
        for worker_context in self._worker_contexts.itervalues():
            for context in self._volumedriver_contexts.itervalues():
                worker_context_copy = worker_context.copy()
                worker_context_copy.update(context)
                relevant_contexts.append(worker_context_copy)
        return relevant_contexts

    def _get_relevant_context(self, service):
        # type: (Service) -> Dict[str, str]
        """
        Get a fully relevant context for a MDS service
        :param service: Service object
        :type service: Service
        :return: Relevant context item
        :rtype: dict
        """
        worker_context_copy = self._worker_context.copy()
        worker_context_copy.update(self._volumedriver_contexts[service])
        return worker_context_copy

    def register_catch_up(self, service):
        # type: (Service) -> List[Dict[str, str]]
        """
        Register that catch up is happening for this vdisk
        :param service: Service object of the MDSService
        :type service: Service
        :return: List of relevant work items
        :rtype: list
        """
        special = {'relevant_work_items': None}
        registering_data = self._get_relevant_context(service)

        def _get_value_and_expected_value():
            relevant_work_items, fetched_work_items = self._get_mds_catch_ups(registering_data)
            relevant_work_items.append(registering_data)
            special['relevant_work_items'] = relevant_work_items
            return relevant_work_items, fetched_work_items

        log_start = self._format_message('{0} - Registering catch up of {1} ({2}:{3})'.format(self._log, service.name, service.storagerouter.ip, service.ports[0]))
        self._logger.info(log_start)

        # Attempt to save with all fetched data during work generation, expect the current key to not have changed
        self._safely_store(self.mds_key, get_value_and_expected_value=_get_value_and_expected_value, logging_start=log_start)
        return special['relevant_work_items']

    def unregister_catch_up(self, service):
        special = {'relevant_work_items': None}
        registering_data = self._get_relevant_context(service)

        def _get_value_and_expected_value():
            relevant_work_items, fetched_work_items = self._get_mds_catch_ups(registering_data)
            relevant_work_items.remove(registering_data)
            special['relevant_work_items'] = relevant_work_items
            return relevant_work_items, fetched_work_items

        log_start = self._format_message('{0} - Unregistering catch up of {1} ({2}:{3})'.format(self._log, service.name, service.storagerouter.ip, service.ports[0]))
        self._logger.info(log_start)

        # Attempt to save with all fetched data during work generation, expect the current key to not have changed
        self._safely_store(self.mds_key, get_value_and_expected_value=_get_value_and_expected_value, logging_start=log_start)
        return special['relevant_work_items']

    @staticmethod
    def map_mds_services_by_socket_for_vdisk(vdisk):
        # type: (VDisk) -> Dict[str, Service]
        """
        Maps the mds services related to the vpool by their socket
        :param vdisk: VDisk object to
        :return: A dict wth sockets as key, service as value
        :rtype: Dict[str, ovs.dal.hybrids.j_mdsservice.MDSService
        """
        # Sorted was added merely for unittests, because they rely on specific order of services and their ports
        # Default sorting behavior for relations used to be based on order in which relations were added
        # Now sorting is based on guid (DAL speedup changes)
        service_per_key = collections.OrderedDict()  # OrderedDict to keep the ordering in the dict
        for service in sorted([j_mds.mds_service.service for j_mds in vdisk.mds_services], key=lambda k: k.ports):
            service_per_key['{0}:{1}'.format(service.storagerouter.ip, service.ports[0])] = service
        return service_per_key

    def _safely_store(self, key, get_value_and_expected_value, logging_start, max_retries=20):
        # type: (str, callable, str, int) -> any
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
        :return: Stored value or the current value if key_not_exists is True and the key is already present
        :rtype: any
        :raises: AssertException:
        - When the save could not happen
        """
        # @todo move this to the persistent client instead
        tries = 0
        success = False
        last_exception = None
        # Call the passed function
        value, expected_value = get_value_and_expected_value()
        return_value = value
        while success is False:
            transaction = self._persistent.begin_transaction()
            return_value = value  # Value might change because of hooking
            tries += 1
            if tries > max_retries:
                raise last_exception
            self._persistent.assert_value(key, expected_value, transaction=transaction)
            self._persistent.set(key, value, transaction=transaction)
            try:
                self._persistent.apply_transaction(transaction)
                success = True
            except AssertException as ex:
                self._logger.warning('{0} - Asserting failed for key {1}. Retrying {2} more times'.format(logging_start, key, max_retries - tries))
                last_exception = ex
                time.sleep(randint(0, 25) / 100.0)
                self._logger.info('{0} - Executing the passed function again'.format(logging_start))
                value, expected_value = get_value_and_expected_value()
        return return_value

    def _get_relevant_items(self, key, relevant_values, relevant_keys):
        # type: (str, List[Dict], List[any]) -> Tuple[List, List]
        """
        Retrieves all scrub work currently being done based on relevant values and the relevant format
        - Filters out own data
        - Filters out relevant data
        - Removes obsolete data
        :param key: Key to fetch
        :type key: str
        :param relevant_values: The values to check relevancy on (Only supporting dict types)
        :type relevant_values: list[dict]
        :param relevant_keys: The keys that are relevant for checking relevancy
        (found items will strip keys to match to this format) (this format will be used to check in relevant values)
        :type relevant_keys: list
        :return: All relevant items and all fetched items
        :rtype: tuple(list, list)
        :raises: ValueError: When an irregular item has been detected
        """
        if any(not isinstance(v, dict)for v in relevant_values):
            raise ValueError('Not all relevant values are a dict')
        if not isinstance(relevant_keys, list):
            raise ValueError('The relevant keys should be a list of keys that are relevant')
        if any(set(v.keys()) != set(relevant_keys) for v in relevant_values):
            raise ValueError('The relevant values do not match the relevant format')
        fetched_items = self._fetch_registered_items(key)
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
                    self._logger.info(self._format_message('Will be removing {0} on the next save as it is no longer relevant'.format(item)))
        except KeyError:
            raise ValueError(self._format_message('Someone is registering keys to this namespace'))
        return relevant_work_items, fetched_items

    def _fetch_registered_items(self, key):
        # type: (str) -> any
        """
        Fetches all items currently registered on the key
        Saves them under _fetched_work_items for caching purposes. When None is returned, an empty list is set
        :param key: Key to fetch
        :type key: str
        :return: All current items (None if the key has not yet been registered)
        """
        if key is None:
            raise ValueError('key has no value. Nothing to fetch')
        if self._persistent.exists(key) is True:
            items = self._persistent.get(key)
        else:
            items = None
        return items

    def _format_message(self, message):
        # type: (str) -> str
        if self._log is None:
            raise ValueError('_log property has no value. Nothing to format with')
        return '{0} - {1}'.format(self._log, message)

    @classmethod
    def reset_cache(cls):
        # type: () -> None
        """
        Reset all caches
        """
        cls._volumedriver_contexts_cache = {}
        cls._worker_contexts_cache = {}

    def reset_volumedriver_cache_for_service(self, service):
        # type: (Service) -> None
        """
        Reset the cache for a particular service
        """
        if self.vdisk.vpool not in self._volumedriver_contexts_cache:
            return
        if service.storagerouter not in self._volumedriver_contexts_cache[self.vdisk.vpool] or 'volumedriver_pid' not in self._volumedriver_contexts_cache[self.vdisk.vpool][service.storagerouter]:
            return
        self._volumedriver_contexts_cache[self.vdisk.vpool][service.storagerouter].pop('volumedriver_pid')
        self._volumedriver_contexts_cache[self.vdisk.vpool][service.storagerouter].pop('volumedriver_start')
