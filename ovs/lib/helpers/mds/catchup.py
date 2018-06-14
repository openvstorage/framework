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
import collections
from random import randint
from ovs.dal.hybrids.service import Service
from ovs.dal.hybrids.vdisk import VDisk
from ovs_extensions.storage.exceptions import AssertException
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.generic.configuration import Configuration
from ovs.lib.helpers.mds.shared import MDSShared
from ovs.log.log_handler import LogHandler
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storageserver.storagedriver import MDSMetaDataBackendConfig, MDSNodeConfig, MetadataServerClient


class MDSCatchUp(MDSShared):
    """
    Class responsible for catching up MDSes asynchronously
    - Registers metadata in Arakoon to ensure that only one catchup happens
    - Offloads the catchup to a new thread: if the worker process would get killed:
      the catchup would still happen by the MDSClient so a re-locking will be happening and it will wait for the original
      catchup to finish
    """
    # Extra caching
    _contexts_cache = {}
    _clients = {}
    _sockets_service_map = {}

    _logger = LogHandler.get('lib', 'mds catchup')

    _CATCH_UP_NAME_SPACE = 'ovs_jobs_catchup'
    _CATCH_UP_VDISK_KEY = '{0}_{{0}}'.format(_CATCH_UP_NAME_SPACE)  # Second format should be the vdisk guid

    def __init__(self, vdisk_guid):
        """
        Initializes a new MDSCatchUp
        :param vdisk_guid: Guid of the vDisk to catch up for
        :type vdisk_guid: str
        """
        self.vdisk = VDisk(vdisk_guid)
        self.mds_key = self._CATCH_UP_VDISK_KEY.format(self.vdisk.guid)
        self.tlog_threshold = Configuration.get('ovs/volumedriver/mds|tlogs_behind', default=100)
        self.volumedriver_service_name = 'ovs-volumedriver_{0}'.format(self.vdisk.vpool.name)
        self.mds_client_timeout = Configuration.get('ovs/vpools/{0}/mds_config|mds_client_connection_timeout'.format(self.vdisk.vpool_guid), default=120)
        self.mds_clients = {}
        self.dry_run = True

        self._service_manager = ServiceFactory.get_manager()
        self._persistent = PersistentFactory.get_client()
        self._log = 'MDS catchup for vDisk {0} (volume id: {1})'.format(self.vdisk.guid, self.vdisk.volume_id)

        self.contexts = self.get_contexts()

    def get_context(self, service):
        """
        Return the context to handle in
        """
        if service not in self.contexts:
            return None
        return self.contexts[service]

    def get_contexts(self):
        """
        Return all possible contexts that can be handled in
        """
        contexts = {}
        if self.vdisk.vpool not in self._contexts_cache:
            self._contexts_cache[self.vdisk.vpool] = {}
        for service in self.map_mds_services_by_socket_for_vdisk(self.vdisk).itervalues():
            if service.storagerouter not in self._contexts_cache[self.vdisk.vpool]:
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
                    volumedriver_start = self._service_manager.get_service_start_time(
                        name=self.volumedriver_service_name, client=client)
                    context = {'volumedriver_pid': volumedriver_pid, 'volumedriver_start': volumedriver_start}
                    self._contexts_cache[self.vdisk.vpool][service.storagerouter] = context
                except:
                    self._logger.exception(self._format_message('Exception while retrieving context for service {0}'.format(service.name)))
                    continue
            contexts[service] = self._contexts_cache[self.vdisk.vpool][service.storagerouter]
        return contexts

    def build_ssh_client(self, storagerouter, max_retries=5):
        """
        Build an sshclient with retries for a certain endpoint
        :param storagerouter: Point to connect too
        :param max_retries: Maximum amount of time to retry
        :return:
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
        registered_catchup = self.register_catch_up(service)
        try:
            log_identifier = 'MDS Service {0} at {1}:{2}'.format(service.name, service.storagerouter.ip, service.ports[0])
            self._logger.info(self._format_message('{0} catch up registrations: {1}'.format(log_identifier, registered_catchup)))
            if len(registered_catchup) > 1:
                self._logger.info(self._format_message('{0} is already being caught up'.format(log_identifier)))
                return
            mds_client.catch_up(str(self.vdisk.volume_id), dry_run=self.dry_run)
        finally:
            self.unregister_catch_up(service)

    def catch_up(self):
        # type: () -> None
        """
        Catch up all MDS services
        """
        for service in self.contexts.iterkeys():
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
            else:
                self._logger.info(self._format_message('Service {0} does not need catching up ({1}/{2})'
                                                       .format(service_identifier, tlogs_behind_master, self.tlog_threshold)))

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
        registering_data = self.contexts[service]

        def _get_value_and_expected_value():
            relevant_work_items, fetched_work_items = self._get_relevant_items(self.mds_key,
                                                                               relevant_values=self.contexts.values(),
                                                                               relevant_keys=registering_data.keys())
            relevant_work_items.append(registering_data)
            special['relevant_work_items'] = relevant_work_items
            return relevant_work_items, fetched_work_items

        log_start = self._format_message('{0} - Registering catch up of {1} ({2}:{3})'.format(self._log, service.name, service.storagerouter.ip, service.ports[0]))
        self._logger.info(log_start)

        # Attempt to save with all fetched data during work generation, expect the current key to not have changed
        self._safely_store(self.mds_key, get_value_and_expected_value=_get_value_and_expected_value, logging_start=log_start)
        return special['relevant_work_items']

    # def register_catch_up_false_data(self, service):
    #     # type: (Service) -> List[Dict[str, str]]
    #     """
    #     Register that catch up is happening for this vdisk
    #     :param service: Service object of the MDSService
    #     :type service: Service
    #     :return: List of relevant work items
    #     :rtype: list
    #     """
    #     special = {'relevant_work_items': None}
    #     registering_data = {'volumedriver_pid': 100*1000, 'volumedriver_start': 'Not a real time'}
    #
    #     def _get_value_and_expected_value():
    #         relevant_work_items, fetched_work_items = self._get_relevant_items(self.mds_key,
    #                                                                            relevant_values=self.contexts.values(),
    #                                                                            relevant_keys=registering_data.keys())
    #         relevant_work_items.append(registering_data)
    #         special['relevant_work_items'] = relevant_work_items
    #         return relevant_work_items, fetched_work_items
    #
    #     log_start = '{0} - Registering catch up of {1} ({2}:{3})'.format(self._log, service.name, service.storagerouter.ip, service.ports[0])
    #     self._logger.info(log_start)
    #
    #     # Attempt to save with all fetched data during work generation, expect the current key to not have changed
    #     self._safely_store(self.mds_key, get_value_and_expected_value=_get_value_and_expected_value, logging_start=log_start)
    #     return special['relevant_work_items']

    def unregister_catch_up(self, service):
        special = {'relevant_work_items': None}
        registering_data = self.contexts[service]

        def _get_value_and_expected_value():
            relevant_work_items, fetched_work_items = self._get_relevant_items(self.mds_key,
                                                                               relevant_values=self.contexts.values(),
                                                                               relevant_keys=registering_data.keys())
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
        if self._log is None:
            raise ValueError('_log property has no value. Nothing to format with')
        return '{0} - {1}'.format(self._log, message)
