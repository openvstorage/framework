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
MDS Safety module
"""

import math
import time
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.service import Service
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.storageserver.storagedriver import MDSMetaDataBackendConfig, MDSNodeConfig, MetadataServerClient, SRCObjectNotFoundException
from ovs.lib.helpers.mds.shared import MDSShared


class SafetyEnsurer(MDSShared):
    """
    Class responsible to ensure the MDS Safety of a volume
    """
    _logger = Logger('lib')

    def __init__(self, vdisk_guid, excluded_storagerouter_guids=None):
        """

        :param vdisk_guid: vDisk GUID to calculate a new safety for
        :type vdisk_guid: str
        :param excluded_storagerouter_guids: GUIDs of StorageRouters to leave out of calculation (Eg: When 1 is down or unavailable)
        :type excluded_storagerouter_guids: list[str]
        """
        if excluded_storagerouter_guids is None:
            excluded_storagerouter_guids = []

        self.vdisk = VDisk(vdisk_guid)
        self.excluded_storagerouters = [StorageRouter(sr_guid) for sr_guid in excluded_storagerouter_guids]

        self.sr_client_timeout = Configuration.get('ovs/vpools/{0}/mds_config|sr_client_connection_timeout'.format(self.vdisk.vpool_guid), default=300)
        self.mds_client_timeout = Configuration.get('ovs/vpools/{0}/mds_config|mds_client_connection_timeout'.format(self.vdisk.vpool_guid), default=120)
        self.tlogs, self.safety, self.max_load = self.get_mds_config()
        # Filled in by functions
        self.metadata_backend_config_start = {}
        # Layout related
        self.mds_layout = {'primary': {'used': [],
                                       'loads': {},
                                       'available': []},
                           'secondary': {'used': [],
                                         'loads': {},
                                         'available': []}}
        self.services_load = {}
        self.recommended_primary = None
        self.recommended_secondary = None
        self.master_service = None
        self.slave_services = []
        self.mds_client_cache = {}

    def validate_vdisk(self):
        """
        Validates if the vDisk is ready for ensuring the MDS safety
        :raises SRCObjectNotFoundException: If the vDisk is no associated with a StorageRouter
        :raises RuntimeError: if
        - Current host is in the excluded storagerouters
        - vDisk is in a different state than running
        :return: None
        :rtype: NoneType
        """
        self.vdisk.invalidate_dynamics(['info', 'storagerouter_guid'])

        if self.vdisk.storagerouter_guid is None:
            raise SRCObjectNotFoundException(
                'Cannot ensure MDS safety for vDisk {0} with guid {1} because vDisk is not attached to any StorageRouter'.format(self.vdisk.name, self.vdisk.guid))

        vdisk_storagerouter = StorageRouter(self.vdisk.storagerouter_guid)
        if vdisk_storagerouter in self.excluded_storagerouters:
            raise RuntimeError('Current host ({0}) of vDisk {1} is in the list of excluded StorageRouters'.format(vdisk_storagerouter.ip, self.vdisk.guid))

        if self.vdisk.info['live_status'] != VDisk.STATUSES.RUNNING:
            raise RuntimeError('vDisk {0} is not {1}, cannot update MDS configuration'.format(self.vdisk.guid, VDisk.STATUSES.RUNNING))

        self.metadata_backend_config_start = self.vdisk.info['metadata_backend_config']
        if self.vdisk.info['metadata_backend_config'] == {}:
            raise RuntimeError('Configured MDS layout for vDisk {0} could not be retrieved}, cannot update MDS configuration'.format(self.vdisk.guid))

    def map_mds_services_by_socket(self):
        """
        Maps the mds services related to the vpool by their socket
        :return: A dict wth sockets as key, service as value
        :rtype: Dict[str, ovs.dal.hybrids.j_mdsservice.MDSService
        """
        return super(SafetyEnsurer, self).map_mds_services_by_socket(self.vdisk)

    def get_primary_and_secondary_storagerouters(self):
        # type: () -> Tuple[List[StorageRouter], List[StorageRouter]]
        """
        Retrieve the primary and secondary storagerouters for MDS deployment
        :return: Both primary and secondary storagerouters
        :rtype: Tuple[List[StorageRouter], List[StorageRouter]]
        """
        # Create a pool of StorageRouters being a part of the primary and secondary domains of this StorageRouter
        vdisk = self.vdisk

        vdisk_storagerouter = StorageRouter(vdisk.storagerouter_guid)
        primary_domains = [junction.domain for junction in vdisk_storagerouter.domains if junction.backup is False]
        secondary_domains = [junction.domain for junction in vdisk_storagerouter.domains if junction.backup is True]
        primary_storagerouters = set()
        secondary_storagerouters = set()
        for domain in primary_domains:
            primary_storagerouters.update(StorageRouterList.get_primary_storagerouters_for_domain(domain))
        for domain in secondary_domains:
            secondary_storagerouters.update(StorageRouterList.get_primary_storagerouters_for_domain(domain))

        # In case no domains have been configured
        if len(primary_storagerouters) == 0:
            primary_storagerouters = set(StorageRouterList.get_storagerouters())

        # Remove all excluded StorageRouters from primary StorageRouters
        primary_storagerouters = primary_storagerouters.difference(self.excluded_storagerouters)

        # Remove all StorageRouters from secondary which are present in primary, all excluded
        secondary_storagerouters = secondary_storagerouters.difference(primary_storagerouters)
        secondary_storagerouters = secondary_storagerouters.difference(self.excluded_storagerouters)

        # Make sure to only use the StorageRouters related to the current vDisk's vPool
        related_storagerouters = [sd.storagerouter for sd in vdisk.vpool.storagedrivers if sd.storagerouter is not None]
        primary_storagerouters = list(primary_storagerouters.intersection(related_storagerouters))
        secondary_storagerouters = list(secondary_storagerouters.intersection(related_storagerouters))

        if vdisk_storagerouter not in primary_storagerouters:
            raise RuntimeError('Host of vDisk {0} ({1}) should be part of the primary domains'.format(vdisk.name, vdisk_storagerouter.name))

        primary_storagerouters.sort(key=lambda sr: ExtensionsToolbox.advanced_sort(element=sr.ip, separator='.'))
        secondary_storagerouters.sort(key=lambda sr: ExtensionsToolbox.advanced_sort(element=sr.ip, separator='.'))
        for primary_storagerouter in primary_storagerouters:
            self._logger.debug('vDisk {0} - Primary StorageRouter {1} with IP {2}'.format(vdisk.guid, primary_storagerouter.name, primary_storagerouter.ip))
        for secondary_storagerouter in secondary_storagerouters:
            self._logger.debug('vDisk {0} - Secondary StorageRouter {1} with IP {2}'.format(vdisk.guid, secondary_storagerouter.name, secondary_storagerouter.ip))
        for excluded_storagerouter in self.excluded_storagerouters:
            self._logger.debug('vDisk {0} - Excluded StorageRouter {1} with IP {2}'.format(vdisk.guid, excluded_storagerouter.name, excluded_storagerouter.ip))

        return primary_storagerouters, secondary_storagerouters

    def get_mds_config(self):
        # type: () -> Tuple[int, int, int]
        """
        Get the MDS Config parameters
        :return: tlogs, safety and maxload
        :rtype: int, int, int
        """
        mds_config = Configuration.get('/ovs/vpools/{0}/mds_config'.format(self.vdisk.vpool_guid))
        return mds_config['mds_tlogs'], mds_config['mds_safety'], mds_config['mds_maxload']

    def get_reconfiguration_reasons(self):
        # type: () -> List[str]
        """
        Check if reconfiguration is required
        Fill in the state of all MDSes while checking the reasons
        :return: All reconfiguration reasons
        :rtype: List[str]
        """
        services_by_socket = self.map_mds_services_by_socket()
        primary_storagerouters, secondary_storagerouters = self.get_primary_and_secondary_storagerouters()
        vdisk_storagerouter = StorageRouter(self.vdisk.storagerouter_guid)

        current_service_ips = []
        reconfigure_reasons = set()
        for index, config in enumerate(self.metadata_backend_config_start):  # Ordered MASTER, SLAVE(S)
            config_key = '{0}:{1}'.format(config['ip'], config['port'])
            service = services_by_socket.get(config_key)
            if not service:
                self._logger.critical('vDisk {0} - Storage leak detected. Namespace {1} for service {2} will never be deleted automatically because service does no longer exist in model'.format(self.vdisk.guid, self.vdisk.volume_id, config_key))
                reconfigure_reasons.add('{0} {1} cannot be used anymore'.format('Master' if index == 0 else 'Slave', config_key))
            else:
                if service.storagerouter.ip in current_service_ips:
                    reconfigure_reasons.add('Multiple MDS services on the same node with IP {0}'.format(service.storagerouter.ip))
                else:
                    current_service_ips.append(service.storagerouter.ip)
                if index == 0:
                    self.master_service = service
                else:
                    self.slave_services.append(service)

        nodes = set()
        for service in services_by_socket.itervalues():
            importance = None
            if service.storagerouter in primary_storagerouters:
                importance = 'primary'
            elif service.storagerouter in secondary_storagerouters:
                importance = 'secondary'

            # If MDS already in use, take current load, else take next load
            loads = self.get_mds_load(mds_service=service.mds_service)
            if service == self.master_service or service in self.slave_services:  # Service is still in use
                load = loads[0]
                if importance:
                    self.mds_layout[importance]['used'].append(service)
                else:
                    reconfigure_reasons.add('Service {0} cannot be used anymore because StorageRouter with IP {1} is not part of the domains'.format(service.name, service.storagerouter.ip))
            else:  # Service is not in use, but available
                load = loads[1]
            self.services_load[service] = load

            if importance:
                nodes.add(service.storagerouter.ip)
                self.mds_layout[importance]['available'].append(service)
                if load <= self.max_load:
                    self._logger.debug('vDisk {0} - Service {1}:{2} has capacity - Load: {3}%'.format(self.vdisk.guid, service.storagerouter.ip, service.ports[0], load))
                    if load not in self.mds_layout[importance]['loads']:
                        self.mds_layout[importance]['loads'][load] = []
                    self.mds_layout[importance]['loads'][load].append(service)
                else:
                    self._logger.debug('vDisk {0} - Service {1}:{2} is overloaded - Load: {3}%'.format(self.vdisk.guid, service.storagerouter.ip, service.ports[0], load))

        if len(current_service_ips) > self.safety:
            reconfigure_reasons.add('Too much safety - Current: {0} - Expected: {1}'.format(len(current_service_ips), self.safety))
        if len(current_service_ips) < self.safety and len(current_service_ips) < len(nodes):
            reconfigure_reasons.add('Not enough safety - Current: {0} - Expected: {1}'.format(len(current_service_ips), self.safety))
        if self.master_service:
            if self.services_load[self.master_service] > self.max_load:
                reconfigure_reasons.add('Master overloaded - Current load: {0}% - Max load: {1}%'.format(self.services_load[self.master_service], self.max_load))
            if self.master_service.storagerouter_guid != self.vdisk.storagerouter_guid:
                reconfigure_reasons.add('Master {0}:{1} is not local - Current location: {0} - Expected location: {2}'.format(self.master_service.storagerouter.ip, self.master_service.ports[0], vdisk_storagerouter.ip))
            if self.master_service not in self.mds_layout['primary']['used']:
                reconfigure_reasons.add('Master service {0}:{1} not in primary domain'.format(self.master_service.storagerouter.ip, self.master_service.ports[0]))
        for slave_service in self.slave_services:
            if self.services_load[slave_service] > self.max_load:
                reconfigure_reasons.add('Slave {0}:{1} overloaded - Current load: {2}% - Max load: {3}%'.format(slave_service.storagerouter.ip, slave_service.ports[0], self.services_load[slave_service], self.max_load))

        # Check reconfigure required based upon domains
        self.recommended_primary = int(math.ceil(self.safety / 2.0)) if len(secondary_storagerouters) > 0 else self.safety
        self.recommended_secondary = self.safety - self.recommended_primary

        primary_services_used = len(self.mds_layout['primary']['used'])
        primary_services_available = len(self.mds_layout['primary']['available'])
        if primary_services_used < self.recommended_primary and primary_services_used < primary_services_available:
            reconfigure_reasons.add('Not enough services in use in primary domain - Current: {0} - Expected: {1}'.format(primary_services_used, self.recommended_primary))
        if primary_services_used > self.recommended_primary:
            reconfigure_reasons.add('Too many services in use in primary domain - Current: {0} - Expected: {1}'.format(primary_services_used, self.recommended_primary))

        # More services can be used in secondary domain
        secondary_services_used = len(self.mds_layout['secondary']['used'])
        secondary_services_available = len(self.mds_layout['secondary']['available'])
        if secondary_services_used < self.recommended_secondary and secondary_services_used < secondary_services_available:
            reconfigure_reasons.add('Not enough services in use in secondary domain - Current: {0} - Expected: {1}'.format(secondary_services_used, self.recommended_secondary))
        if secondary_services_used > self.recommended_secondary:
            # Too many services in secondary domain
            reconfigure_reasons.add('Too many services in use in secondary domain - Current: {0} - Expected: {1}'.format(secondary_services_used, self.recommended_secondary))

        # If secondary domain present, check order in which the slave services are configured
        secondary = False
        for slave_service in self.slave_services:
            if secondary and slave_service in self.mds_layout['primary']['used']:
                reconfigure_reasons.add('A slave in secondary domain has priority over a slave in primary domain')
                break
            if slave_service in self.mds_layout['secondary']['used']:
                secondary = True

        self._logger.info('vDisk {0} - Current configuration: {1}'.format(self.vdisk.guid, self.metadata_backend_config_start))

        return reconfigure_reasons

    def create_new_master(self):
        # type: () -> Tuple[List[Service], Service]
        """
        Check and create a new MDS master if necessary
        Master configured according to StorageDriver must be modelled
        Master must be local
        Master cannot be overloaded
        Master must be in primary domain (if no domains available, this check is irrelevant because all StorageRouters will match)
        :return: The newly created services and the previous master (if a master switch happened)
        :rtype: Tuple[List[Service], Service]
        """
        new_services = []
        previous_master = None
        log_start = 'vDisk {0}'.format(self.vdisk.guid)

        if self.master_service \
                and self.master_service.storagerouter_guid == self.vdisk.storagerouter_guid \
                and self.services_load[self.master_service] <= self.max_load \
                and self.master_service in self.mds_layout['primary']['used']:
            # Master is OK, so add as 1st element to new configuration. Reconfiguration is now based purely on slave misconfiguration
            new_services.append(self.master_service)
            self._logger.debug('{0} - Master is still OK, re-calculating slaves'.format(log_start))
        else:
            # Master is not OK --> try to find the best non-overloaded LOCAL MDS slave in the primary domain to make master
            self._logger.debug('{0} - Master is not OK, re-calculating master'.format(log_start))
            current_load = 0
            new_local_master_service = None
            re_used_local_slave_service = None
            for service in self.mds_layout['primary']['available']:
                if service == self.master_service:
                    # Make sure the current master_service is not re-used as master for whatever reason
                    continue
                # This load indicates the load it would become if a vDisk would be moved to this Service
                next_load = self.services_load[service]
                if next_load <= self.max_load and service.storagerouter_guid == self.vdisk.storagerouter_guid:
                    if current_load > next_load or (not re_used_local_slave_service and not new_local_master_service):
                        current_load = next_load  # Load for least loaded service
                        new_local_master_service = service  # If no local slave is found to re-use, this new_local_master_service is used
                        if service in self.slave_services:
                            self._logger.debug('{0} - Slave service {1}:{2} will be recycled'.format(log_start, service.storagerouter.ip, service.ports[0]))
                            re_used_local_slave_service = service  # A slave service is found to re-use as new master
                            self.slave_services.remove(service)

            if not re_used_local_slave_service:
                # There's no non-overloaded local slave found. Keep the current master (if available) and add a local MDS (if available) as slave.
                # Next iteration, the newly added slave will be checked if it has caught up already
                # If amount of tlogs to catchup is < configured amount of tlogs --> we wait for catchup, so master can be removed and slave can be promoted
                if self.master_service:
                    self._logger.debug('{0} - Keeping current master service'.format(log_start))
                    new_services.append(self.master_service)
                if new_local_master_service:
                    self._logger.debug('{0} - Adding new slave service {1}:{2} to catch up'.format(log_start, new_local_master_service.storagerouter.ip, new_local_master_service.ports[0]))
                    new_services.append(new_local_master_service)
            else:
                # A non-overloaded local slave was found
                # We verify how many tlogs the slave is behind and do 1 of the following:
                #     1. tlogs_behind_master < tlogs configured --> Invoke the catchup action and wait for it
                #     2. tlogs_behind_master >= tlogs configured --> Add current master service as 1st in list, append non-overloaded local slave as 2nd in list and let StorageDriver do the catchup (next iteration we check again)
                # noinspection PyTypeChecker
                client = MetadataServerClient.load(service=re_used_local_slave_service, timeout=self.mds_client_timeout)
                if client is None:
                    raise RuntimeError('Cannot establish a MDS client connection for service {0}:{1}'.format(re_used_local_slave_service.storagerouter.ip, re_used_local_slave_service.ports[0]))
                self.mds_client_cache[re_used_local_slave_service] = client
                try:
                    tlogs_behind_master = client.catch_up(str(self.vdisk.volume_id), dry_run=True)  # Verify how much tlogs local slave Service is behind (No catchup action is invoked)
                except RuntimeError as ex:
                    if 'Namespace does not exist' in ex.message:
                        client.create_namespace(str(self.vdisk.volume_id))
                        tlogs_behind_master = client.catch_up(str(self.vdisk.volume_id), dry_run=True)
                    else:
                        raise

                self._logger.debug('{0} - Recycled slave is {1} tlogs behind'.format(log_start, tlogs_behind_master))
                if tlogs_behind_master < self.tlogs:
                    start = time.time()
                    try:
                        client.catch_up(str(self.vdisk.volume_id), dry_run=False)
                        self._logger.debug('{0} - Catchup took {1}s'.format(log_start, round(time.time() - start, 2)))
                    except Exception:
                        self._logger.exception('{0} - Catching up failed'.format(log_start))
                        raise  # Catchup failed, so we don't know whether the new slave can be promoted to master yet

                    # It's up to date, so add it as a new master
                    new_services.append(re_used_local_slave_service)
                    if self.master_service is not None:
                        # The current master (if available) is now candidate to become one of the slaves (Determined below during slave calculation)
                        # The current master can potentially be on a different node, thus might become slave
                        self.slave_services.insert(0, self.master_service)
                        previous_master = self.master_service
                else:
                    # It's not up to date, keep the previous master (if available) and give the local slave some more time to catch up
                    # @todo this needs to trigger a new job the new local master is there
                    if self.master_service is not None:
                        new_services.append(self.master_service)
                    new_services.append(re_used_local_slave_service)

        service_string = ', '.join(["{{'ip': '{0}', 'port': {1}}}".format(service.storagerouter.ip, service.ports[0]) for service in new_services])
        self._logger.debug('vDisk {0} - Configuration after MASTER calculation: [{1}]'.format(self.vdisk.guid, service_string))

        return new_services, previous_master

    def create_new_slaves(self, new_services):
        # type: (List[str]) -> Tuple[List[Service], List[Service]]
        """
        Check and create a new MDS slaves if necessary
        :param new_services: Services used for MDS master
        :type new_services: List[str]
        :return: New slave services for the primary domain, New slave services for the secondary domain
        :rtype: Tuple[List[Service], List[Service]]
        """
        def _add_suitable_nodes(local_importance, local_safety, services_to_recycle=None):
            if services_to_recycle is None:
                services_to_recycle = []
            if local_importance == 'primary':
                local_services = new_primary_services
            else:
                local_services = new_secondary_services

            if len(new_node_ips) < local_safety:
                for local_load in sorted(self.mds_layout[local_importance]['loads']):
                    possible_services = self.mds_layout[local_importance]['loads'][local_load]
                    if len(services_to_recycle) > 0:
                        possible_services = [serv for serv in services_to_recycle if
                                             serv in possible_services]  # Maintain order of services_to_recycle

                    for local_service in possible_services:
                        if len(new_node_ips) >= local_safety:
                            return

                        if local_service.storagerouter.ip not in new_node_ips:
                            if local_service.storagerouter not in storagerouter_cache:
                                try:
                                    SSHClient(local_service.storagerouter)
                                    storagerouter_cache[local_service.storagerouter] = True
                                except UnableToConnectException:
                                    storagerouter_cache[local_service.storagerouter] = False

                            if storagerouter_cache[local_service.storagerouter] is True:
                                local_services.append(local_service)
                                new_node_ips.add(local_service.storagerouter.ip)
                            else:
                                self._logger.debug('vDisk {0} - Skipping StorageRouter with IP {1} as it is unreachable'.format(self.vdisk.guid, local_service.storagerouter.ip))

        new_node_ips = {new_services[0].storagerouter.ip} if len(new_services) > 0 else set()  # Currently we can only have the local IP in the list of new_services
        storagerouter_cache = {}
        new_primary_services = []
        new_secondary_services = []

        # Try to re-use slaves from primary domain until recommended_primary safety reached
        _add_suitable_nodes(local_importance='primary', local_safety=self.recommended_primary, services_to_recycle=self.slave_services)

        # Add new slaves until primary safety reached
        _add_suitable_nodes(local_importance='primary', local_safety=self.recommended_primary)

        # Try to re-use slaves from secondary domain until safety reached
        _add_suitable_nodes(local_importance='secondary', local_safety=self.safety, services_to_recycle=self.slave_services)

        # Add new slaves until safety reached
        _add_suitable_nodes(local_importance='secondary', local_safety=self.safety)

        # In case safety has not been reached yet, we try to add nodes from primary domain until safety has been reached
        _add_suitable_nodes(local_importance='primary', local_safety=self.safety)

        # Extend the new services with the newly added primary and secondary services
        return new_primary_services, new_secondary_services

    def apply_reconfigurations(self, new_services, previous_master_service):
        # type: (List[Service], Service) -> None
        """
        Applies all calculated reconfigurations
        - Deploys the services
        - Notifies the Storagerouter
        :param new_services: List of new services to be used in the reconfiguration (Master and slaves)
        Note the order matters here! First the master, then slaves in primary domain, then slaves in secondary domain
        :type new_services: List[Service]
        :param previous_master_service: Previous master service in case the master should be switched around (None if no previous master)
        :type previous_master_service: Service
        :return: None
        :rtype: NoneType
        """
        # Verify an MDSClient can be created for all relevant services
        services_to_check = new_services + self.slave_services
        if self.master_service is not None:
            services_to_check.append(self.master_service)
        for service in services_to_check:
            if service not in self.mds_client_cache:
                client = MetadataServerClient.load(service=service, timeout=self.mds_client_timeout)
                if client is None:
                    raise RuntimeError('Cannot establish a MDS client connection for service {0}:{1}'.format(service.storagerouter.ip, service.ports[0]))
                self.mds_client_cache[service] = client

        configs_all = []
        new_namespace_services = []
        configs_without_replaced_master = []
        log_start = 'vDisk {0}'.format(self.vdisk.guid)
        for service in new_services:
            client = self.mds_client_cache[service]
            try:
                if str(self.vdisk.volume_id) not in client.list_namespaces():
                    client.create_namespace(str(self.vdisk.volume_id))  # StorageDriver does not throw error if already existing or does not create a duplicate namespace
                    new_namespace_services.append(service)
            except Exception:
                self._logger.exception('{0} - Creating new namespace {1} failed for Service {2}:{3}'.format(log_start, self.vdisk.volume_id, service.storagerouter.ip, service.ports[0]))
                # Clean up newly created namespaces
                for new_namespace_service in new_namespace_services:
                    client = self.mds_client_cache[new_namespace_service]
                    try:
                        self._logger.warning('{0}: Deleting newly created namespace {1} for service {2}:{3}'.format(log_start, self.vdisk.volume_id, new_namespace_service.storagerouter.ip, new_namespace_service.ports[0]))
                        client.remove_namespace(str(self.vdisk.volume_id))
                    except RuntimeError:
                        pass  # If somehow the namespace would not exist, we don't care.
                raise  # Currently nothing has been changed on StorageDriver level, so we can completely abort

            # noinspection PyArgumentList
            config = MDSNodeConfig(address=str(service.storagerouter.ip), port=service.ports[0])
            if previous_master_service != service:  # This only occurs when a slave has caught up with master and old master gets replaced with new master
                configs_without_replaced_master.append(config)
            configs_all.append(config)

        start = time.time()
        update_failure = False
        try:
            self._logger.debug('{0} - Updating MDS configuration'.format(log_start))
            if len(configs_without_replaced_master) != len(configs_all):  # First update without previous master to avoid race conditions (required by voldrv)
                self._logger.debug('{0} - Without previous master: {1}:{2}'.format(log_start, previous_master_service.storagerouter.ip, previous_master_service.ports[0]))
                self.vdisk.storagedriver_client.update_metadata_backend_config(volume_id=str(self.vdisk.volume_id),
                                                                               metadata_backend_config=MDSMetaDataBackendConfig(configs_without_replaced_master),
                                                                               req_timeout_secs=self.sr_client_timeout)
                self._logger.debug('{0} - Updating MDS configuration without previous master took {1}s'.format(log_start, time.time() - start))
            self.vdisk.storagedriver_client.update_metadata_backend_config(volume_id=str(self.vdisk.volume_id),
                                                                           metadata_backend_config=MDSMetaDataBackendConfig(configs_all),
                                                                           req_timeout_secs=self.sr_client_timeout)
            # Verify the configuration - chosen by the framework - passed to the StorageDriver is effectively the correct configuration
            self.vdisk.invalidate_dynamics('info')
            self._logger.debug('{0} - Configuration after update: {1}'.format(self.vdisk.guid, self.vdisk.info['metadata_backend_config']))

            duration = time.time() - start
            if duration > 5:
                self._logger.critical('{0} - Updating MDS configuration took {1}s'.format(log_start, duration))
        except RuntimeError:
            # @TODO: Timeout throws RuntimeError for now. Replace this once https://github.com/openvstorage/volumedriver/issues/349 is fixed
            if time.time() - start >= self.sr_client_timeout:  # Timeout reached, clean up must be done manually once server side finished
                self._logger.critical('{0} - Updating MDS configuration timed out'.format(log_start))
                for service in [svc for svc in services_to_check if svc not in new_services]:
                    self._logger.critical('{0} - Manual remove namespace action required for MDS {1}:{2} and namespace {3}'.format(log_start, service.storagerouter.ip, service.ports[0], self.vdisk.volume_id))
                for service in new_services[1:]:
                    self._logger.critical('{0} - Manual set SLAVE role action required for MDS {1}:{2} and namespace {3}'.format(log_start, service.storagerouter.ip, service.ports[0], self.vdisk.volume_id))
                self._logger.critical('{0} - Sync vDisk to reality action required'.format(log_start))
            else:
                self._logger.exception('{0}: Failed to update the metadata backend configuration'.format(log_start))
                update_failure = True  # No need to clean new namespaces if time out would have occurred
            # Always raise
            #     * In case of a timeout, the manual actions are logged and user knows the ensure_safety has failed
            #     * In any other case, the newly created namespaces are deleted
            raise
        except Exception:
            self._logger.exception('{0}: Failed to update the metadata backend configuration'.format(log_start))
            update_failure = True
            raise
        finally:
            if update_failure:
                # Remove newly created namespaces when updating would go wrong to avoid storage leaks
                for new_namespace_service in new_namespace_services:
                    client = self.mds_client_cache[new_namespace_service]
                    try:
                        self._logger.warning('{0}: Deleting newly created namespace {1} for service {2}:{3}'.format(log_start, self.vdisk.volume_id, new_namespace_service.storagerouter.ip, new_namespace_service.ports[0]))
                        client.remove_namespace(str(self.vdisk.volume_id))
                    except RuntimeError:
                        pass  # If somehow the namespace would not exist, we don't care.

        self._sync_vdisk_to_reality(self.vdisk)
        for service in services_to_check:
            if service not in new_services:
                self._logger.debug('{0} - Deleting namespace for vDisk on service {1}:{2}'.format(log_start, service.storagerouter.ip, service.ports[0]))
                client = self.mds_client_cache[service]
                try:
                    client.remove_namespace(str(self.vdisk.volume_id))
                except RuntimeError:
                    pass  # If somehow the namespace would not exist, we don't care.

        for service in new_services[1:]:
            client = self.mds_client_cache[service]
            try:
                if client.get_role(nspace=str(self.vdisk.volume_id)) != MetadataServerClient.MDS_ROLE.SLAVE:
                    self._logger.debug('{0} - Demoting service {1}:{2} to SLAVE'.format(log_start, service.storagerouter.ip, service.ports[0]))
                    start = time.time()
                    client.set_role(nspace=str(self.vdisk.volume_id), role=MetadataServerClient.MDS_ROLE.SLAVE)
                    duration = time.time() - start
                    if duration > 5:
                        self._logger.critical('{0} - Demoting service {1}:{2} to SLAVE took {3}s'.format(log_start, service.storagerouter.ip, service.ports[0], duration))
            except Exception:
                self._logger.critical('{0} - Failed to demote service {1}:{2} to SLAVE'.format(log_start, service.storagerouter.ip, service.ports[0]))
                raise

    def catchup_mds_slaves(self):
        # type: () -> None
        """
        Performs a catchup for MDS slaves if their tlogs behind reach a certain threshold
        """

    def ensure_safety(self):
        # type: () -> None
        """
        Ensures (or tries to ensure) the safety of a given vDisk.
        Assumptions:
            * A local overloaded master is better than a non-local non-overloaded master
            * Prefer master/slaves to be on different hosts, a subsequent slave on the same node doesn't add safety
            * Don't actively overload services (e.g. configure an MDS as slave causing it to get overloaded)
            * Too much safety is not wanted (it adds loads to nodes while not required)
            * Order of slaves is:
                * All slaves on StorageRouters in primary Domain of vDisk host
                * All slaves on StorageRouters in secondary Domain of vDisk host
                * Eg: Safety of 2 (1 master + 1 slave)
                    mds config = [local master in primary, slave in secondary]
                * Eg: Safety of 3 (1 master + 2 slaves)
                    mds config = [local master in primary, slave in primary, slave in secondary]
                * Eg: Safety of 4 (1 master + 3 slaves)
                    mds config = [local master in primary, slave in primary, slave in secondary, slave in secondary]
        :raises RuntimeError: If host of vDisk is part of the excluded StorageRouters
                              If host of vDisk is not part of the StorageRouters in the primary domain
                              If catchup command fails for a slave
                              If MDS client cannot be created for any of the current or new MDS services
                              If updateMetadataBackendConfig would fail for whatever reason
        :raises SRCObjectNotFoundException: If vDisk does not have a StorageRouter GUID
        :return: None
        :rtype: NoneType
        """
        self._logger.info('vDisk {0} - Start checkup for vDisk {1}'.format(self.vdisk.guid, self.vdisk.name))
        self.validate_vdisk()

        self._logger.debug('vDisk {0} - Safety: {1}, Max load: {2}%, Tlogs: {3}'.format(self.vdisk.guid, self.safety, self.max_load, self.tlogs))

        self.vdisk.reload_client('storagedriver')
        self.vdisk.reload_client('objectregistry')

        reconfigure_reasons = self.get_reconfiguration_reasons()
        if not reconfigure_reasons:
            self._logger.info('vDisk {0} - No reconfiguration required'.format(self.vdisk.guid))
            self._sync_vdisk_to_reality(self.vdisk)
            return
        self._logger.info('vDisk {0} - Reconfiguration required. Reasons:'.format(self.vdisk.guid))
        for reason in reconfigure_reasons:
            self._logger.info('vDisk {0} -    * {1}'.format(self.vdisk.guid, reason))

        new_services = []

        new_master_services, previous_master = self.create_new_master()
        new_services.extend(new_master_services)
        # At this point we can have:
        #     Local master which is OK
        #     Local master + catching up new local master (because 1st is overloaded)
        #     Local master + catching up slave (because 1st was overloaded)
        #     Local slave which has caught up and been added as 1st in list of new_services
        #     Nothing at all --> Can only occur when the current master service (according to StorageDriver) has been deleted in the model and no other local MDS is available (Very unlikely scenario to occur, if possible at all)
        # Now the slaves will be added according to the rules described in the docstring
        # When local master + catching up service is present, this counts as safety of 1, because eventually the current master will be removed

        new_primary_services, new_secondary_services = self.create_new_slaves(new_services)
        new_services.extend(new_primary_services)
        new_services.extend(new_secondary_services)

        service_string = ', '.join(["{{'ip': '{0}', 'port': {1}}}".format(service.storagerouter.ip, service.ports[0]) for service in new_services])
        self._logger.debug('vDisk {0} - Configuration after SLAVE calculation: [{1}]'.format(self.vdisk.guid, service_string))
        if new_services == [self.master_service] + self.slave_services and len(new_services) == len(self.metadata_backend_config_start):
            self._logger.info('vDisk {0} - Could not calculate a better MDS layout. Nothing to update'.format(self.vdisk.guid))
            self._sync_vdisk_to_reality(self.vdisk)
            return

        self.apply_reconfigurations(new_services, previous_master)
        self._logger.info('vDisk {0}: Completed'.format(self.vdisk.guid))
