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
MDSService module
"""

import math
import time
import random
import datetime
import collections
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.j_mdsservice import MDSService
from ovs.dal.hybrids.j_mdsservicevdisk import MDSServiceVDisk
from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
from ovs.dal.hybrids.service import Service
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.lists.servicelist import ServiceList
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.generic.configuration import Configuration, NotFoundException
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.storageserver.storagedriver import LOG_LEVEL_MAPPING, MDSMetaDataBackendConfig, MDSNodeConfig, MetadataServerClient, SRCObjectNotFoundException, StorageDriverConfiguration
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.helpers.toolbox import Schedule
from volumedriver.storagerouter import storagerouterclient


class MDSServiceController(object):
    """
    Contains all BLL related to MDSServices
    """
    _logger = Logger('lib')
    _log_level = LOG_LEVEL_MAPPING[_logger.getEffectiveLevel()]

    # noinspection PyCallByClass,PyTypeChecker
    storagerouterclient.Logger.setupLogging(Logger.load_path('storagerouterclient'), _log_level)
    # noinspection PyArgumentList
    storagerouterclient.Logger.enableLogging()

    @staticmethod
    def prepare_mds_service(storagerouter, vpool):
        """
        Prepares an MDS service:
            * Creates the required configuration
            * Sets up the service files
        Assumes the StorageRouter and vPool are already configured with a StorageDriver and that all model-wise configurations regarding both have been completed.

        :param storagerouter: StorageRouter on which the MDS service will be created
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :param vpool: The vPool for which the MDS service will be created
        :type vpool: ovs.dal.hybrids.vpool.VPool
        :raises RuntimeError: vPool is not extended on StorageRouter
                              No ServiceType found for 'MetadataServer'
                              No free port is found for the new MDSService
                              No partition found on StorageRouter with DB role
        :return: Newly created junction service
        :rtype: ovs.dal.hybrids.j_mdsservice.MDSService
        """
        from ovs.lib.storagedriver import StorageDriverController  # Import here to prevent from circular imports

        MDSServiceController._logger.info('StorageRouter {0} - vPool {1}: Preparing MDS junction service'.format(storagerouter.name, vpool.name))

        mds_service = MDSService()
        with volatile_mutex(name='prepare_mds_{0}'.format(storagerouter.guid), wait=30):
            # VALIDATIONS
            # Verify passed StorageRouter is part of the vPool
            storagerouter.invalidate_dynamics(['vpools_guids'])
            if vpool.guid not in storagerouter.vpools_guids:
                raise RuntimeError('StorageRouter {0} is not part of vPool {1}'.format(storagerouter.name, vpool.name))

            # Verify ServiceType existence
            mds_service_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.MD_SERVER)
            if mds_service_type is None:
                raise RuntimeError('No ServiceType found with name {0}'.format(ServiceType.SERVICE_TYPES.MD_SERVER))

            # Retrieve occupied ports for current StorageRouter and max MDSService number for current vPool/StorageRouter combo
            service_number = -1
            occupied_ports = []
            for service in mds_service_type.services:
                if service.storagerouter_guid == storagerouter.guid:
                    occupied_ports.extend(service.ports)
                    if service.mds_service.vpool_guid == vpool.guid:
                        service_number = max(service.mds_service.number, service_number)

            client = SSHClient(endpoint=storagerouter)
            mds_port_range = Configuration.get('/ovs/framework/hosts/{0}/ports|mds'.format(System.get_my_machine_id(client)))
            free_ports = System.get_free_ports(selected_range=mds_port_range,
                                               exclude=occupied_ports,
                                               nr=1,
                                               client=client)
            if len(free_ports) != 1:
                raise RuntimeError('Failed to find an available port on StorageRouter {0} within range {1}'.format(storagerouter.name, mds_port_range))

            # Partition check
            db_partition = None
            for disk in storagerouter.disks:
                for partition in disk.partitions:
                    if DiskPartition.ROLES.DB in partition.roles:
                        db_partition = partition
                        break
            if db_partition is None:
                raise RuntimeError('Could not find DB partition on StorageRouter {0}'.format(storagerouter.name))

            # Verify StorageDriver configured
            storagedrivers = [sd for sd in vpool.storagedrivers if sd.storagerouter_guid == storagerouter.guid]
            if len(storagedrivers) != 1:
                raise RuntimeError('Expected to find a configured StorageDriver for vPool {0} on StorageRouter {1}'.format(vpool.name, storagerouter.name))

            # MODEL UPDATES
            # Service and MDS service
            service_number += 1
            MDSServiceController._logger.info('StorageRouter {0} - vPool {1}: Adding junction service with number {2}'.format(storagerouter.name, vpool.name, service_number))

            service = Service()
            service.name = 'metadataserver_{0}_{1}'.format(vpool.name, service_number)
            service.type = mds_service_type
            service.ports = free_ports
            service.storagerouter = storagerouter
            service.save()
            mds_service.vpool = vpool
            mds_service.number = service_number
            mds_service.service = service
            mds_service.save()

            # StorageDriver partitions
            MDSServiceController._logger.info('StorageRouter {0} - vPool {1}: Adding StorageDriverPartition on partition with mount point {2}'.format(storagerouter.name, vpool.name, db_partition.mountpoint))
            storagedriver = storagedrivers[0]
            sdp = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                     'role': DiskPartition.ROLES.DB,
                                                                                     'sub_role': StorageDriverPartition.SUBROLE.MDS,
                                                                                     'partition': db_partition,
                                                                                     'mds_service': mds_service})

            # CONFIGURATIONS
            # Volumedriver
            mds_nodes = []
            for sd_partition in storagedriver.partitions:
                if sd_partition.role == DiskPartition.ROLES.DB and sd_partition.sub_role == StorageDriverPartition.SUBROLE.MDS and sd_partition.mds_service is not None:
                    service = sd_partition.mds_service.service
                    mds_nodes.append({'host': service.storagerouter.ip,
                                      'port': service.ports[0],
                                      'db_directory': '{0}/db'.format(sd_partition.path),
                                      'scratch_directory': '{0}/scratch'.format(sd_partition.path)})

            MDSServiceController._logger.info('StorageRouter {0} - vPool {1}: Configuring StorageDriver with MDS nodes: {2}'.format(storagerouter.name, vpool.name, mds_nodes))
            # Generate the correct section in the StorageDriver's configuration
            try:
                storagedriver_config = StorageDriverConfiguration(vpool.guid, storagedriver.storagedriver_id)
                storagedriver_config.configure_metadata_server(mds_nodes=mds_nodes)
                storagedriver_config.save(client)
            except Exception:
                MDSServiceController._logger.exception('StorageRouter {0} - vPool {1}: Configuring StorageDriver failed. Reverting model changes'.format(storagerouter.name, vpool.name))
                # Clean up model changes if error occurs
                sdp.delete()
                mds_service.delete()  # Must be removed before the service
                service.delete()
        return mds_service

    @staticmethod
    def remove_mds_service(mds_service, reconfigure, allow_offline=False):
        """
        Removes an MDS service
        :param mds_service: The MDS service to remove
        :type mds_service: ovs.dal.hybrids.j_mdsservice.MDSService
        :param reconfigure: Indicates whether reconfiguration is required
        :type reconfigure: bool
        :param allow_offline: Indicates whether it's OK that the node for which mds services are cleaned is offline
        :type allow_offline: bool
        :raises RuntimeError: When vDisks present on the MDSService to be removed
                              No StorageDriver is linked to the MDSService to be removed
        :raises UnableToConnectException: When StorageRouter on which the MDSService resides is unreachable and allow_offline flag is False
        :return: None
        :rtype: NoneType
        """
        if len(mds_service.vdisks_guids) > 0 and allow_offline is False:
            raise RuntimeError('Cannot remove MDSService that is still serving disks')

        if len(mds_service.storagedriver_partitions) == 0 or mds_service.storagedriver_partitions[0].storagedriver is None:
            raise RuntimeError('Failed to retrieve the linked StorageDriver to this MDS Service {0}'.format(mds_service.service.name))

        vpool = mds_service.vpool
        root_client = None
        storagerouter = mds_service.service.storagerouter
        storagedriver = mds_service.storagedriver_partitions[0].storagedriver
        MDSServiceController._logger.info('StorageRouter {0} - vPool {1}: Removing MDS junction service for port {2}'.format(storagerouter.name, vpool.name, mds_service.service.ports[0]))
        try:
            root_client = SSHClient(endpoint=storagerouter, username='root')
            MDSServiceController._logger.debug('StorageRouter {0} - vPool {1}: Established SSH connection'.format(storagerouter.name, vpool.name))
        except UnableToConnectException:
            if allow_offline is True:
                MDSServiceController._logger.warning('StorageRouter {0} - vPool {1}: Allowed offline node during MDS service removal'.format(storagerouter.name, vpool.name))
            else:
                MDSServiceController._logger.exception('StorageRouter {0} - vPool {1}: Failed to connect to StorageRouter'.format(storagerouter.name, vpool.name))
                raise

        # Reconfigure StorageDriver
        if reconfigure is True and root_client is not None:
            mds_nodes = []
            for sd_partition in storagedriver.partitions:
                if sd_partition.role == DiskPartition.ROLES.DB and sd_partition.sub_role == StorageDriverPartition.SUBROLE.MDS and sd_partition.mds_service != mds_service:
                    service = sd_partition.mds_service.service
                    mds_nodes.append({'host': service.storagerouter.ip,
                                      'port': service.ports[0],
                                      'db_directory': '{0}/db'.format(sd_partition.path),
                                      'scratch_directory': '{0}/scratch'.format(sd_partition.path)})

            # Generate the correct section in the StorageDriver's configuration
            MDSServiceController._logger.info('StorageRouter {0} - vPool {1}: Configuring StorageDriver with MDS nodes: {2}'.format(storagerouter.name, vpool.name, mds_nodes))
            storagedriver_config = StorageDriverConfiguration(vpool.guid, storagedriver.storagedriver_id)
            storagedriver_config.configure_metadata_server(mds_nodes=mds_nodes)
            storagedriver_config.save(root_client)

        # Clean up model
        MDSServiceController._logger.info('StorageRouter {0} - vPool {1}: Cleaning model'.format(storagerouter.name, vpool.name))
        directories_to_clean = []
        for sd_partition in mds_service.storagedriver_partitions:
            directories_to_clean.append(sd_partition.path)
            sd_partition.delete()

        if allow_offline is True:  # Certain vDisks might still be attached to this offline MDS service --> Delete relations
            for junction in mds_service.vdisks:
                junction.delete()

        mds_service.delete()
        mds_service.service.delete()

        # Clean up file system
        if root_client is not None:
            MDSServiceController._logger.info('StorageRouter {0} - vPool {1}: Deleting directories from file system: {2}'.format(storagerouter.name, vpool.name, directories_to_clean))
            tries = 5
            while tries > 0:
                try:
                    root_client.dir_delete(directories=directories_to_clean, follow_symlinks=True)
                    for dir_name in directories_to_clean:
                        MDSServiceController._logger.debug('StorageRouter {0} - vPool {1}: Recursively removed directory: {2}'.format(storagerouter.name, vpool.name, dir_name))
                    break
                except Exception:
                    MDSServiceController._logger.warning('StorageRouter {0} - vPool {1}: Waiting for the MDS service to go down...'.format(storagerouter.name, vpool.name))
                    time.sleep(5)
                    tries -= 1
                    if tries == 0:
                        MDSServiceController._logger.exception('StorageRouter {0} - vPool {1}: Deleting directories failed'.format(storagerouter.name, vpool.name))
                        raise

    @staticmethod
    @ovs_task(name='ovs.mds.mds_checkup', schedule=Schedule(minute='30', hour='0,4,8,12,16,20'), ensure_single_info={'mode': 'DEFAULT'})
    def mds_checkup():
        """
        Validates the current MDS setup/configuration and takes actions where required
        Actions:
            * Verify which StorageRouters are available
            * Make mapping between vPools and its StorageRouters
            * For each vPool make sure every StorageRouter has at least 1 MDS service with capacity available
            * For each vPool retrieve the optimal configuration and store it for each StorageDriver
            * For each vPool run an ensure safety for all vDisks
        :raises RuntimeError: When ensure safety fails for any vDisk
        :return: None
        :rtype: NoneType
        """
        MDSServiceController._logger.info('Started')

        # Verify StorageRouter availability
        root_client_cache = {}
        storagerouters = StorageRouterList.get_storagerouters()
        storagerouters.sort(key=lambda _sr: ExtensionsToolbox.advanced_sort(element=_sr.ip, separator='.'))
        offline_nodes = []
        for storagerouter in storagerouters:
            try:
                root_client = SSHClient(endpoint=storagerouter, username='root')
                MDSServiceController._logger.debug('StorageRouter {0} - ONLINE'.format(storagerouter.name))
            except UnableToConnectException:
                root_client = None
                offline_nodes.append(storagerouter)
                MDSServiceController._logger.error('StorageRouter {0} - OFFLINE'.format(storagerouter.name))
            root_client_cache[storagerouter] = root_client

        # Create mapping per vPool and its StorageRouters
        mds_dict = collections.OrderedDict()
        for vpool in sorted(VPoolList.get_vpools(), key=lambda k: k.name):
            MDSServiceController._logger.info('vPool {0}'.format(vpool.name))
            mds_dict[vpool] = {}

            # Loop all StorageDrivers and add StorageDriver to mapping
            for storagedriver in vpool.storagedrivers:
                storagerouter = storagedriver.storagerouter
                if storagerouter not in mds_dict[vpool]:
                    mds_dict[vpool][storagerouter] = {'client': root_client_cache.get(storagerouter),
                                                      'services': [],
                                                      'storagedriver': storagedriver}

            # Loop all MDS Services and append services to appropriate vPool / StorageRouter combo
            mds_services = vpool.mds_services
            mds_services.sort(key=lambda _mds_service: ExtensionsToolbox.advanced_sort(element=_mds_service.service.storagerouter.ip, separator='.'))
            for mds_service in mds_services:
                service = mds_service.service
                storagerouter = service.storagerouter
                if storagerouter not in mds_dict[vpool]:
                    mds_dict[vpool][storagerouter] = {'client': root_client_cache.get(storagerouter),
                                                      'services': [],
                                                      'storagedriver': None}
                MDSServiceController._logger.debug('vPool {0} - StorageRouter {1} - Service on port {2}'.format(vpool.name, storagerouter.name, service.ports[0]))
                mds_dict[vpool][storagerouter]['services'].append(mds_service)

        failures = []
        for vpool, storagerouter_info in mds_dict.iteritems():
            # Make sure there's at least 1 MDS on every StorageRouter that's not overloaded
            # Remove all MDS Services which have been manually marked for removal (by setting its capacity to 0)
            max_load = Configuration.get('/ovs/vpools/{0}/mds_config|mds_maxload'.format(vpool.guid))
            for storagerouter in sorted(storagerouter_info, key=lambda k: k.ip):
                root_client = mds_dict[vpool][storagerouter]['client']
                mds_services = mds_dict[vpool][storagerouter]['services']

                has_room = False
                for mds_service in list(sorted(mds_services, key=lambda k: k.number)):
                    port = mds_service.service.ports[0]
                    number = mds_service.number
                    # Manual intervention required here in order for the MDS to be cleaned up
                    # @TODO: Remove this and make a dynamic calculation to check which MDSes to remove
                    if mds_service.capacity == 0 and len(mds_service.vdisks_guids) == 0:
                        MDSServiceController._logger.warning('vPool {0} - StorageRouter {1} - MDS Service {2} on port {3}: Removing'.format(vpool.name, storagerouter.name, number, port))
                        try:
                            MDSServiceController.remove_mds_service(mds_service=mds_service, reconfigure=True, allow_offline=root_client is None)
                        except Exception:
                            MDSServiceController._logger.exception('vPool {0} - StorageRouter {1} - MDS Service {2} on port {3}: Failed to remove'.format(vpool.name, storagerouter.name, number, port))
                        mds_services.remove(mds_service)
                    else:
                        _, load = MDSServiceController._get_mds_load(mds_service=mds_service)
                        if load < max_load:
                            MDSServiceController._logger.debug('vPool {0} - StorageRouter {1} - MDS Service {2} on port {3}: Capacity available'.format(vpool.name, storagerouter.name, number, port))
                            has_room = True
                        else:
                            MDSServiceController._logger.debug('vPool {0} - StorageRouter {1} - MDS Service {2} on port {3}: No capacity available'.format(vpool.name, storagerouter.name, number, port))

                if has_room is False:
                    MDSServiceController._logger.info('vPool {0} - StorageRouter {1} - Adding new MDS Service'.format(vpool.name, storagerouter.name))
                    try:
                        mds_services.append(MDSServiceController.prepare_mds_service(storagerouter=storagerouter, vpool=vpool))
                    except Exception:
                        MDSServiceController._logger.exception('vPool {0} - StorageRouter {1} - Failed to create new MDS Service'.format(vpool.name, storagerouter.name))

            # After potentially having added new MDSes, retrieve the optimal configuration
            try:
                mds_config_set = MDSServiceController.get_mds_storagedriver_config_set(vpool=vpool, offline_nodes=offline_nodes)
                MDSServiceController._logger.debug('vPool {0} - Optimal configuration {1}'.format(vpool.name, mds_config_set))
            except (NotFoundException, RuntimeError):
                MDSServiceController._logger.exception('vPool {0} - Failed to retrieve the optimal configuration'.format(vpool.name))

            # Apply the optimal MDS configuration per StorageDriver
            for storagerouter in sorted(storagerouter_info, key=lambda k: k.ip):
                root_client = mds_dict[vpool][storagerouter]['client']
                storagedriver = mds_dict[vpool][storagerouter]['storagedriver']

                if storagedriver is None:
                    MDSServiceController._logger.critical('vPool {0} - StorageRouter {1} - No matching StorageDriver found'.format(vpool.name, storagerouter.name))
                    continue
                if storagerouter.guid not in mds_config_set:
                    MDSServiceController._logger.critical('vPool {0} - StorageRouter {1} - Not marked as offline, but could not retrieve an optimal MDS config'.format(vpool.name, storagerouter.name))
                    continue
                if root_client is None:
                    MDSServiceController._logger.debug('vPool {0} - StorageRouter {1} - Marked as offline, not setting optimal MDS configuration'.format(vpool.name, storagerouter.name))
                    continue

                storagedriver_config = StorageDriverConfiguration(vpool_guid=vpool.guid, storagedriver_id=storagedriver.storagedriver_id)
                if storagedriver_config.config_missing is False:
                    optimal_mds_config = mds_config_set[storagerouter.guid]
                    MDSServiceController._logger.debug('vPool {0} - StorageRouter {1} - Storing optimal MDS configuration: {2}'.format(vpool.name, storagerouter.name, optimal_mds_config))
                    storagedriver_config.configure_filesystem(fs_metadata_backend_mds_nodes=optimal_mds_config)
                    storagedriver_config.save(root_client)

            # Execute a safety check, making sure the master/slave configuration is optimal.
            MDSServiceController._logger.info('vPool {0} - Ensuring safety for all vDisks'.format(vpool.name))
            for vdisk in vpool.vdisks:
                try:
                    MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)
                except Exception:
                    message = 'Ensure safety for vDisk {0} with guid {1} failed'.format(vdisk.name, vdisk.guid)
                    MDSServiceController._logger.exception(message)
                    failures.append(message)
        if len(failures) > 0:
            raise RuntimeError('\n - ' + '\n - '.join(failures))
        MDSServiceController._logger.info('Finished')

    # noinspection PyUnresolvedReferences
    @staticmethod
    @ovs_task(name='ovs.mds.ensure_safety', ensure_single_info={'mode': 'DEDUPED'})
    def ensure_safety(vdisk_guid, excluded_storagerouter_guids=list()):
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
        :param vdisk_guid: vDisk GUID to calculate a new safety for
        :type vdisk_guid: str
        :param excluded_storagerouter_guids: GUIDs of StorageRouters to leave out of calculation (Eg: When 1 is down or unavailable)
        :type excluded_storagerouter_guids: list[str]
        :return: None
        :rtype: NoneType
        """
        # noinspection PyUnresolvedReferences
        def _add_suitable_nodes(local_importance, local_safety, services_to_recycle=list()):
            if local_importance == 'primary':
                local_services = new_primary_services
            else:
                local_services = new_secondary_services

            if len(new_node_ips) < local_safety:
                for local_load in sorted(all_info_dict[local_importance]['loads']):
                    possible_services = all_info_dict[local_importance]['loads'][local_load]
                    if len(services_to_recycle) > 0:
                        possible_services = [serv for serv in possible_services if serv in services_to_recycle]

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
                                MDSServiceController._logger.debug('vDisk {0} - Skipping StorageRouter with IP {1} as it is unreachable'.format(vdisk.guid, local_service.storagerouter.ip))


        vdisk = VDisk(vdisk_guid)
        excluded_storagerouters = [StorageRouter(sr_guid) for sr_guid in excluded_storagerouter_guids]
        MDSServiceController._logger.info('vDisk {0} - Start checkup for virtual disk {1}'.format(vdisk.guid, vdisk.name))
        mds_config = Configuration.get('/ovs/vpools/{0}/mds_config'.format(vdisk.vpool_guid))
        tlogs = mds_config['mds_tlogs']
        safety = mds_config['mds_safety']
        max_load = mds_config['mds_maxload']
        MDSServiceController._logger.debug('vDisk {0} - Safety: {1}, Max load: {2}%, Tlogs: {3}'.format(vdisk.guid, safety, max_load, tlogs))

        ######################
        # GATHER INFORMATION #
        ######################
        vdisk.reload_client('storagedriver')
        vdisk.reload_client('objectregistry')

        vdisk.invalidate_dynamics('storagerouter_guid')
        if vdisk.storagerouter_guid is None:
            raise SRCObjectNotFoundException('Cannot ensure MDS safety for vDisk {0} with guid {1} because vDisk is not attached to any StorageRouter'.format(vdisk.name, vdisk.guid))

        # Sorted was added merely for unittests, because they rely on specific order of services and their ports
        # Default sorting behavior for relations used to be based on order in which relations were added
        # Now sorting is based on guid (DAL speedup changes)
        service_per_key = collections.OrderedDict()  # OrderedDict to keep the ordering in the dict
        for service in sorted([mds.service for mds in vdisk.vpool.mds_services if mds.service.storagerouter not in excluded_storagerouters], key=lambda k: k.ports):
            service_per_key['{0}:{1}'.format(service.storagerouter.ip, service.ports[0])] = service

        # Create a pool of StorageRouters being a part of the primary and secondary domains of this StorageRouter
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

        if vdisk_storagerouter not in primary_storagerouters or vdisk_storagerouter in secondary_storagerouters:
            raise ValueError('StorageRouter {0} for vDisk {1} should be part of the primary domains and NOT be part of the secondary domains'.format(vdisk_storagerouter.name, vdisk.name))

        # Remove all excluded StorageRouters from primary StorageRouter
        primary_storagerouters = primary_storagerouters.difference(excluded_storagerouters)

        # Remove all StorageRouters from secondary which are present in primary and all excluded
        secondary_storagerouters = secondary_storagerouters.difference(primary_storagerouters)
        secondary_storagerouters = secondary_storagerouters.difference(excluded_storagerouters)

        ###################################
        # VERIFY RECONFIGURATION REQUIRED #
        ###################################
        vdisk.invalidate_dynamics('info')
        master_service = None
        slave_services = []
        current_service_ips = []
        reconfigure_reasons = []
        for index, config in enumerate(vdisk.info['metadata_backend_config']):  # Ordered MASTER, SLAVE(S)
            config_key = '{0}:{1}'.format(config['ip'], config['port'])
            service = service_per_key.get(config_key)
            if service is None:
                reconfigure_reasons.append('{0} {1} cannot be used anymore'.format('Master' if index == 0 else 'Slave', config_key))
            else:
                if service.storagerouter.ip in current_service_ips:
                    reconfigure_reasons.append('Multiple MDS services on the same node with IP {0}'.format(service.storagerouter.ip))
                else:
                    current_service_ips.append(service.storagerouter.ip)

                if index == 0:
                    master_service = service
                else:
                    slave_services.append(service)

        services_load = {}
        all_info_dict = {'primary': {'used': [],
                                     'loads': {},
                                     'available': []},
                         'secondary': {'used': [],
                                       'loads': {},
                                       'available': []}}

        nodes = set()
        for service in service_per_key.itervalues():
            importance = None
            if service.storagerouter in primary_storagerouters:
                importance = 'primary'
            elif service.storagerouter in secondary_storagerouters:
                importance = 'secondary'

            # If MDS already in use, take current load, else take next load
            loads = MDSServiceController._get_mds_load(mds_service=service.mds_service)
            if service == master_service or service in slave_services:  # Service is still in use
                load = loads[0]
                if importance is not None:
                    all_info_dict[importance]['used'].append(service)
                else:
                    reconfigure_reasons.append('Service {0} cannot be used anymore because StorageRouter with IP {1} is not part of the domains'.format(service.name, service.storagerouter.ip))
            else:  # Service is not in use, but available
                load = loads[1]
            services_load[service] = load

            if importance is not None:
                nodes.add(service.storagerouter.ip)
                all_info_dict[importance]['available'].append(service)
                if load <= max_load:
                    if load not in all_info_dict[importance]['loads']:
                        all_info_dict[importance]['loads'][load] = []
                    all_info_dict[importance]['loads'][load].append(service)

        if len(current_service_ips) > safety:
            reconfigure_reasons.append('Too much safety')
        if len(current_service_ips) < safety and len(current_service_ips) < len(nodes):
            reconfigure_reasons.append('Not enough safety')
        if master_service is not None and services_load[master_service] > max_load:
            reconfigure_reasons.append('Master overloaded')
        if master_service is not None and master_service.storagerouter_guid != vdisk.storagerouter_guid:
            reconfigure_reasons.append('Master is not local')
        if any(service for service in slave_services if services_load[service] > max_load):
            reconfigure_reasons.append('One or more slaves overloaded')

        # Check reconfigure required based upon domains
        recommended_primary = math.ceil(safety / 2.0) if len(secondary_storagerouters) > 0 else safety
        recommended_secondary = safety - recommended_primary

        if master_service is not None and master_service not in all_info_dict['primary']['used']:
            reconfigure_reasons.append('Master service not in primary domain')

        primary_services_used = len(all_info_dict['primary']['used'])
        primary_services_available = len(all_info_dict['primary']['available'])
        if primary_services_used < recommended_primary and primary_services_used < primary_services_available:
            reconfigure_reasons.append('Not enough services in use in primary domain')
        if primary_services_used > recommended_primary:
            reconfigure_reasons.append('Too many services in use in primary domain')

        # More services can be used in secondary domain
        secondary_services_used = len(all_info_dict['secondary']['used'])
        secondary_services_available = len(all_info_dict['secondary']['available'])
        if secondary_services_used < recommended_secondary and secondary_services_used < secondary_services_available:
            reconfigure_reasons.append('Not enough services in use in secondary domain')
        if secondary_services_used > recommended_secondary:
            # Too many services in secondary domain
            reconfigure_reasons.append('Too many services in use in secondary domain')

        # If secondary domain present, check order in which the slave services are configured
        secondary = False
        for slave_service in slave_services:
            if secondary is True and slave_service in all_info_dict['primary']['used']:
                reconfigure_reasons.append('A slave in secondary domain has priority over a slave in primary domain')
                break
            if slave_service in all_info_dict['secondary']['used']:
                secondary = True

        if not reconfigure_reasons:
            current_config_string = ', '.join(['{0}:{1}'.format(service.storagerouter.ip, service.ports[0]) for service in [master_service] + slave_services])
            MDSServiceController._logger.info('vDisk {0} - No reconfiguration required'.format(vdisk.guid))
            MDSServiceController._logger.debug('vDisk {0} - Current configuration: {1}'.format(vdisk.guid, current_config_string))
            MDSServiceController._sync_vdisk_to_reality(vdisk)
            return

        MDSServiceController._logger.info('vDisk {0} - Reconfiguration required. Reasons:'.format(vdisk.guid))
        for reason in reconfigure_reasons:
            MDSServiceController._logger.info('vDisk {0} -    * {1}'.format(vdisk.guid, reason))

        ##########################################
        # CREATE NEW CONFIGURATION (MASTER PART) #
        ##########################################

        # Master configured according to StorageDriver must be modelled
        # Master must be local
        # Master cannot be overloaded
        # Master must be in primary domain (if no domains available, this check is irrelevant because all StorageRouters will match)
        new_services = []
        previous_master = None
        if master_service is not None and \
            master_service.storagerouter_guid == vdisk.storagerouter_guid and \
            services_load[master_service] <= max_load and \
            master_service in all_info_dict['primary']['used']:
                new_services.append(master_service)  # Master is OK, so add as 1st element to new configuration. Reconfiguration is now based purely on slave misconfiguration
        else:
            # Master is not OK --> try to find the best non-overloaded LOCAL MDS slave in the primary domain to make master
            current_load = 0
            new_local_master_service = None
            re_used_local_slave_service = None
            for service in all_info_dict['primary']['available']:
                if service == master_service:
                    # Make sure the current master_service is not re-used for whatever reason
                    continue
                next_load = services_load[service]  # This load indicates the load it would become if a vDisk would be moved to this Service
                if next_load <= max_load and service.storagerouter_guid == vdisk.storagerouter_guid:
                    if current_load > next_load or (re_used_local_slave_service is None and new_local_master_service is None):
                        current_load = next_load  # Load for least loaded service
                        new_local_master_service = service  # If no local slave is found to re-use, this new_local_master_service is used
                        if service in slave_services:
                            re_used_local_slave_service = service  # A slave service is found to re-use as new master
                            slave_services.remove(service)

            if re_used_local_slave_service is None:
                # There's no non-overloaded local slave found. Keep the current master (if available) and add a local MDS (if available) as slave.
                # Next iteration, the newly added slave will be checked if it has caught up already
                # If amount of tlogs to catchup is < configured amount of tlogs --> we wait for catchup, so master can be removed and slave can be promoted
                if master_service is not None:
                    new_services.append(master_service)
                if new_local_master_service is not None:
                    new_services.append(new_local_master_service)
            else:
                # A non-overloaded local slave was found
                # We verify how many tlogs the slave is behind and do 1 of the following:
                #     1. tlogs_behind_master < tlogs configured --> Invoke the catchup action and wait for it
                #     2. tlogs_behind_master >= tlogs configured --> Add current master service as 1st in list, append non-overloaded local slave as 2nd in list and let StorageDriver do the catchup (next iteration we check again)
                client = MetadataServerClient.load(re_used_local_slave_service)
                try:
                    tlogs_behind_master = client.catch_up(str(vdisk.volume_id), dry_run=True)  # Verify how much tlogs local slave Service is behind (No catchup action is invoked)
                except RuntimeError as ex:
                    if 'Namespace does not exist' in ex.message:
                        client.create_namespace(str(vdisk.volume_id))
                        tlogs_behind_master = client.catch_up(str(vdisk.volume_id), dry_run=True)
                    else:
                        raise

                if tlogs_behind_master < tlogs:
                    start = time.time()
                    try:
                        client.catch_up(str(vdisk.volume_id), dry_run=False)
                        MDSServiceController._logger.debug('vDisk {0} - Catchup took {1}s'.format(vdisk.guid, round(time.time() - start, 2)))
                    except Exception:
                        MDSServiceController._logger.exception('vDisk {0} - Catching up failed'.format(vdisk.guid))

                    # It's up to date, so add it as a new master
                    new_services.append(re_used_local_slave_service)
                    if master_service is not None:
                        # The current master (if available) is now candidate to become one of the slaves (Determined below during slave calculation)
                        # The current master can potentially be on a different node, thus might become slave
                        slave_services.append(master_service)
                        previous_master = master_service
                else:
                    # It's not up to date, keep the previous master (if available) and give the local slave some more time to catch up
                    if master_service is not None:
                        new_services.append(master_service)
                    new_services.append(re_used_local_slave_service)

        # At this point we can have:
        #     Local master which is OK
        #     Local master + catching up new local master (because 1st is overloaded)
        #     Local master + catching up slave (because 1st was overloaded)
        #     Local slave which has caught up and been added as 1st in list of new_services
        #     Nothing at all --> Can only occur when the current master service (according to StorageDriver) has been deleted in the model and no other local MDS is available (Very unlikely scenario to occur, if possible at all)
        # Now the slaves will be added according to the rules described in the docstring
        # When local master + catching up service is present, this counts as safety of 1, because eventually the current master will be removed

        #########################################
        # CREATE NEW CONFIGURATION (SLAVE PART) #
        #########################################
        new_node_ips = {new_services[0].storagerouter.ip} if len(new_services) > 0 else set()  # Currently we can only have the local IP in the list of new_services
        storagerouter_cache = {}
        new_primary_services = []
        new_secondary_services = []

        # Try to re-use slaves from primary domain until recommended_primary safety reached
        _add_suitable_nodes(local_importance='primary', local_safety=recommended_primary, services_to_recycle=slave_services)

        # Add new slaves until primary safety reached
        _add_suitable_nodes(local_importance='primary', local_safety=recommended_primary)

        # Try to re-use slaves from secondary domain until recommend_secondary safety reached
        _add_suitable_nodes(local_importance='secondary', local_safety=safety, services_to_recycle=slave_services)

        # Add new slaves until secondary safety reached
        _add_suitable_nodes(local_importance='secondary', local_safety=safety)

        # In case safety has not been reached yet, we try to add nodes from primary domain until safety has been reached
        _add_suitable_nodes(local_importance='primary', local_safety=safety)

        # Correct the order of the services in case secondary slaves come before primary slaves
        new_services.extend(new_primary_services)
        new_services.extend(new_secondary_services)

        # Build the new configuration and update the vDisk
        configs_all = []
        configs_without_caught_up_master = []
        for service in new_services:
            client = MetadataServerClient.load(service)
            client.create_namespace(str(vdisk.volume_id))
            # noinspection PyArgumentList
            config = MDSNodeConfig(address=str(service.storagerouter.ip), port=service.ports[0])
            if previous_master != service:
                configs_without_caught_up_master.append(config)
            configs_all.append(config)
        try:
            if len(configs_without_caught_up_master) != len(configs_all):  # First update without previous master to avoid race conditions (required by voldrv)
                vdisk.storagedriver_client.update_metadata_backend_config(volume_id=str(vdisk.volume_id),
                                                                          metadata_backend_config=MDSMetaDataBackendConfig(configs_without_caught_up_master))
            vdisk.storagedriver_client.update_metadata_backend_config(volume_id=str(vdisk.volume_id),
                                                                      metadata_backend_config=MDSMetaDataBackendConfig(configs_all))
        except Exception:
            MDSServiceController._logger.exception('vDisk {0}: Failed to update the metadata backend configuration'.format(vdisk.guid))
            raise Exception('MDS configuration for volume {0} with guid {1} could not be changed'.format(vdisk.name, vdisk.guid))

        for service in new_services[1:]:
            client = MetadataServerClient.load(service)
            client.set_role(str(vdisk.volume_id), MetadataServerClient.MDS_ROLE.SLAVE)

        for service in list(all_info_dict['primary']['used']) + list(all_info_dict['secondary']['used']):
            if service not in new_services:
                client = MetadataServerClient.load(service)
                try:
                    client.remove_namespace(str(vdisk.volume_id))
                except RuntimeError:
                    pass  # If somehow the namespace would not exist, we don't care.

        MDSServiceController._sync_vdisk_to_reality(vdisk)
        MDSServiceController._logger.info('vDisk {0}: Completed'.format(vdisk.guid))

    @staticmethod
    def get_preferred_mds(storagerouter, vpool):
        """
        Gets the MDS on this StorageRouter/vPool pair which is preferred to achieve optimal balancing
        :param storagerouter: StorageRouter to retrieve the best MDS service for
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :param vpool: vPool to retrieve the best MDS service for
        :type vpool: ovs.dal.hybrids.vpool.VPool
        :return: Preferred MDS service (least loaded), current load on that MDS service
        :rtype: tuple(ovs.dal.hybrids.j_mdsservice.MDSService, float)
        """
        mds_info = (None, float('inf'))
        for mds_service in vpool.mds_services:
            if mds_service.service.storagerouter_guid == storagerouter.guid:
                load = MDSServiceController._get_mds_load(mds_service=mds_service)[0]
                if mds_info[0] is None or load < mds_info[1]:
                    mds_info = (mds_service, load)
        return mds_info

    @staticmethod
    def get_mds_storagedriver_config_set(vpool, offline_nodes=list()):
        """
        Builds a configuration for all StorageRouters from a given vPool with following goals:
            * Primary MDS is the local one
            * All slaves are on different hosts
            * Maximum `mds_safety` nodes are returned
        The configuration returned is the default configuration used by the volumedriver of which in normal use-cases
        only the 1st entry is used, because at volume creation time, the volumedriver needs to create 1 master MDS
        During ensure_safety, we actually create/set the MDS slaves for each volume

        :param vpool: vPool to get StorageDriver configuration for
        :type vpool: ovs.dal.hybrids.vpool.VPool
        :param offline_nodes: Nodes which are currently unreachable via the SSHClient functionality
        :type offline_nodes: list
        :raises RuntimeError: When no MDS Service can be found for a specific vPool/StorageRouter combo
        :raises NotFoundException: When configuration management is unavailable
        :return: MDS configuration for a vPool
        :rtype: dict[list]
        """
        mds_per_storagerouter = {}
        mds_per_load = {}
        for storagedriver in vpool.storagedrivers:
            storagerouter = storagedriver.storagerouter
            if storagerouter in offline_nodes:
                continue
            mds_service, load = MDSServiceController.get_preferred_mds(storagerouter, vpool)
            if mds_service is None:
                raise RuntimeError('Could not find an MDS service')
            mds_per_storagerouter[storagerouter] = {'host': storagerouter.ip, 'port': mds_service.service.ports[0]}
            if load not in mds_per_load:
                mds_per_load[load] = []
            mds_per_load[load].append(storagerouter)

        safety = Configuration.get('/ovs/vpools/{0}/mds_config|mds_safety'.format(vpool.guid))
        config_set = {}
        for storagerouter, ip_info in mds_per_storagerouter.iteritems():
            config_set[storagerouter.guid] = [ip_info]
            for importance in ['primary', 'secondary']:
                domains = [junction.domain for junction in storagerouter.domains if junction.backup is (importance == 'secondary')]
                possible_storagerouters = set()
                for domain in domains:
                    possible_storagerouters.update(StorageRouterList.get_primary_storagerouters_for_domain(domain))

                for load in sorted(mds_per_load):
                    if len(config_set[storagerouter.guid]) >= safety:
                        break
                    other_storagerouters = mds_per_load[load]
                    random.shuffle(other_storagerouters)
                    for other_storagerouter in other_storagerouters:
                        if len(config_set[storagerouter.guid]) >= safety:
                            break
                        if other_storagerouter != storagerouter and other_storagerouter in possible_storagerouters:
                            config_set[storagerouter.guid].append(mds_per_storagerouter[other_storagerouter])
        return config_set

    @staticmethod
    def monitor_mds_layout():
        """
        Prints the current MDS layout
        :return: None
        :rtype: NoneType
        """
        try:
            while True:
                output = ['',
                          'Open vStorage - MDS debug information',
                          '=====================================',
                          'timestamp: {0}'.format(datetime.datetime.now()),
                          '']
                vpools_deployed = False
                for storagerouter in sorted(StorageRouterList.get_storagerouters(), key=lambda k: k.name):
                    vpools = set(sd.vpool for sd in storagerouter.storagedrivers)
                    if len(vpools) > 0:
                        vpools_deployed = True
                        output.append('+ {0} ({1})'.format(storagerouter.name, storagerouter.ip))
                    for vpool in sorted(vpools, key=lambda k: k.name):
                        output.append('  + {0}'.format(vpool.name))
                        for mds_service in sorted(vpool.mds_services, key=lambda k: k.number):
                            if mds_service.service.storagerouter_guid == storagerouter.guid:
                                masters, slaves = 0, 0
                                for junction in mds_service.vdisks:
                                    if junction.is_master:
                                        masters += 1
                                    else:
                                        slaves += 1
                                capacity = mds_service.capacity
                                if capacity == -1:
                                    capacity = 'infinite'
                                load, _ = MDSServiceController._get_mds_load(mds_service)
                                if load == float('inf'):
                                    load = 'infinite'
                                else:
                                    load = '{0}%'.format(round(load, 2))
                                output.append('    + {0} - port {1} - {2} master(s), {3} slave(s) - capacity: {4}, load: {5}'.format(
                                    mds_service.number, mds_service.service.ports[0], masters, slaves, capacity, load
                                ))
                if vpools_deployed is False:
                    output.append('No vPools deployed')
                print '\x1b[2J\x1b[H' + '\n'.join(output)
                time.sleep(1)
        except KeyboardInterrupt:
            pass

    @staticmethod
    def _sync_vdisk_to_reality(vdisk):
        """
        Syncs the MDS junction services for a vDisk to the services configured in the StorageDriver
        :param vdisk: vDisk to synchronize
        :type vdisk: ovs.dal.hybrids.vdisk.VDisk
        :return: None
        :rtype: NoneType
        """
        MDSServiceController._logger.info('vDisk {0} - {1}: Syncing to reality'.format(vdisk.guid, vdisk.name))

        sd_master_ip = None  # IP of the master service according to StorageDriver
        sd_master_port = None  # Port of the master service according to StorageDriver
        sd_mds_config = collections.OrderedDict()  # MDS services according to StorageDriver
        model_mds_config = collections.OrderedDict()  # MDS services according to model

        # with volatile_mutex('sync_vdisk_to_reality_{0}'.format(vdisk.guid), wait=10):
        vdisk.reload_client('storagedriver')
        vdisk.invalidate_dynamics(['info', 'storagerouter_guid'])

        # Verify the StorageDriver services
        MDSServiceController._logger.debug('vDisk {0} - {1}: Current MDS Config: {2}'.format(vdisk.guid, vdisk.name, vdisk.info['metadata_backend_config']))
        for index, mds_entry in enumerate(vdisk.info['metadata_backend_config']):
            ip = mds_entry['ip']
            port = mds_entry['port']
            if index == 0:  # First entry is the master MDS service
                sd_master_ip = ip
                sd_master_port = port
            if ip not in sd_mds_config:
                sd_mds_config[ip] = []
            sd_mds_config[ip].append(port)

        # Verify the model junction services (Relations between the MDS Services and the vDisks)
        for junction in list(vdisk.mds_services):
            model_ip = junction.mds_service.service.storagerouter.ip
            model_port = junction.mds_service.service.ports[0]
            MDSServiceController._logger.debug('vDisk {0} - {1}: Validating junction service {2}:{3}'.format(vdisk.guid, vdisk.name, model_ip, model_port))

            # Remove duplicate junction services
            if model_ip in model_mds_config and model_port in model_mds_config[model_ip]:
                MDSServiceController._logger.warning('vDisk {0} - {1}: Deleting junction service {2}:{3} : Duplicate'.format(vdisk.guid, vdisk.name, model_ip, model_port))
                junction.delete()
                continue

            # Remove junction services not known by StorageDriver
            elif model_ip not in sd_mds_config or model_port not in sd_mds_config[model_ip]:
                MDSServiceController._logger.warning('vDisk {0} - {1}: Deleting junction service {2}:{3} : Unknown by StorageDriver'.format(vdisk.guid, vdisk.name, model_ip, model_port))
                junction.delete()
                continue

            junction.is_master = model_ip == sd_master_ip and model_port == sd_master_port
            junction.save()
            if model_ip not in model_mds_config:
                model_mds_config[model_ip] = []
            model_mds_config[model_ip].append(model_port)

        MDSServiceController._logger.debug('vDisk {0} - {1}: MDS services according to model: {2}'.format(vdisk.guid, vdisk.name, ', '.join(['{0}:{1}'.format(ip, port) for ip, ports in model_mds_config.iteritems() for port in ports])))
        MDSServiceController._logger.debug('vDisk {0} - {1}: MDS services according to StorageDriver: {2}'.format(vdisk.guid, vdisk.name, ', '.join(['{0}:{1}'.format(ip, port) for ip, ports in sd_mds_config.iteritems() for port in ports])))
        for ip, ports in sd_mds_config.iteritems():
            for port in ports:
                if ip not in model_mds_config or port not in model_mds_config[ip]:
                    MDSServiceController._logger.warning('vDisk {0} - {1}: Modeling junction service {2}:{3}'.format(vdisk.guid, vdisk.name, ip, port))
                    service = ServiceList.get_by_ip_ports(ip, [port])
                    if service is None and vdisk.storagerouter_guid is not None:
                        MDSServiceController._logger.critical('vDisk {0} - {1}: Failed to find an MDS Service for {2}:{3}. Creating a new MDS Service'.format(vdisk.guid, vdisk.name, ip, port))
                        storagerouter = StorageRouter(vdisk.storagerouter_guid)
                        try:
                            service = MDSServiceController.prepare_mds_service(storagerouter=storagerouter, vpool=vdisk.vpool).service
                        except Exception:
                            MDSServiceController._logger.exception('vDisk {0} - {1}: Creating MDS Service failed'.format(vdisk.guid, vdisk.name))

                    if service is not None:
                        mds_service_vdisk = MDSServiceVDisk()
                        mds_service_vdisk.vdisk = vdisk
                        mds_service_vdisk.mds_service = service.mds_service
                        mds_service_vdisk.is_master = sd_master_ip == service.storagerouter.ip and sd_master_port == service.ports[0]
                        mds_service_vdisk.save()
                        MDSServiceController._logger.debug('vDisk {0} - {1}: Modeled junction service {2}:{3}'.format(vdisk.guid, vdisk.name, ip, port))
        MDSServiceController._logger.info('vDisk {0} - {1}: Synced to reality'.format(vdisk.guid, vdisk.name))

    @staticmethod
    def _get_mds_load(mds_service):
        """
        Gets a 'load' for an MDS service based on its capacity and the amount of assigned vDisks
        :param mds_service: MDS service the get current load for
        :type mds_service: ovs.dal.hybrids.j_mdsservice.MDSService
        :return: Load of the MDS service
        :rtype: tuple(float, float)
        """
        service_capacity = float(mds_service.capacity)
        if service_capacity < 0:
            return 50.0, 50.0
        if service_capacity == 0:
            return float('inf'), float('inf')
        usage = len(mds_service.vdisks_guids)
        return round(usage / service_capacity * 100.0, 5), round((usage + 1) / service_capacity * 100.0, 5)
