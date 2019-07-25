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

import sys
import math
import time
import random
import Queue
import datetime
import collections
import logging
from threading import Thread
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.vpoollist import VPoolList
from ovs_extensions.constants.vpools import MDS_CONFIG_PATH
from ovs.extensions.generic.configuration import Configuration, NotFoundException
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.helpers.exceptions import EnsureSingleTimeoutReached
from ovs.lib.helpers.mds.catchup import MDSCatchUp
from ovs.lib.helpers.mds.safety import SafetyEnsurer
from ovs.lib.helpers.mds.shared import MDSShared
from ovs.lib.helpers.toolbox import Schedule


class MDSCheckupEnsureSafetyFailures(Exception):
    """
    Raised when errors occur during an mds checkup for a single VPool
    """


class MDSServiceController(MDSShared):
    """
    Contains all BLL related to MDSServices
    """

    _logger = logging.getLogger(__name__)

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
            storagedriver_config.configuration.mds_config.mds_nodes=mds_nodes
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
    def _get_mds_information(vpools=None):
        # type: (Optional[List[VPool]]) -> Tuple[collections.OrderedDict, List[StorageRouter]]
        """
        Retrieve a complete overview of all storagerouters and their mds layout
        :param vpools: VPools to get the overview for
        :type vpools: List[VPool]
        :return: - An overview with the vpool as keys, storagerouter - client, services and storagedriver map
                 - All storagerouters that were offline
        :rtype: Tuple[collection.OrderedDict, List[StorageRouter]]
        """
        # Verify StorageRouter availability
        if vpools is None:
            vpools = VPoolList.get_vpools()

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
        for vpool in sorted(vpools, key=lambda k: k.name):
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
        return mds_dict, offline_nodes

    @staticmethod
    @ovs_task(name='ovs.mds.mds_checkup_single', ensure_single_info={'mode': 'DEDUPED',
                                                                     'ignore_arguments': ['mds_dict', 'offline_nodes']})
    def mds_checkup_single(vpool_guid, mds_dict=None, offline_nodes=None):
        # type: (str, collections.OrderedDict, List[StorageRouter]) -> None
        """
        Validates the current MDS setup/configuration and takes actions where required
        Actions:
            * Verify which StorageRouters are available
            * Make mapping between vPools and its StorageRouters
            * For each vPool make sure every StorageRouter has at least 1 MDS service with capacity available
            * For each vPool retrieve the optimal configuration and store it for each StorageDriver
            * For each vPool run an ensure safety for all vDisks
        :param vpool_guid: Guid of the VPool to do the checkup for
        :type vpool_guid: str
        :param mds_dict: OrderedDict containing all mds related information
        :type mds_dict: collections.OrderedDict
        :param offline_nodes: Nodes that are marked as unreachable
        :type offline_nodes: List[StorageRouter]
        :raises RuntimeError: When ensure safety fails for any vDisk
        :return: None
        :rtype: NoneType
        :raises: MDSCheckupEnsureSafetyFailures when the ensure safety has failed for any vdisk
        """
        params_to_verify = [mds_dict, offline_nodes]
        vpool = VPool(vpool_guid)

        if any(p is not None for p in params_to_verify) and not all(p is not None for p in params_to_verify):
            raise ValueError('Both mds_dict and offline_nodes must be given instead of providing either one')
        if not mds_dict:
            mds_dict, offline_nodes = MDSServiceController._get_mds_information([vpool])

        ensure_safety_failures = []
        storagerouter_info = mds_dict[vpool]
        # Make sure there's at least 1 MDS on every StorageRouter that's not overloaded
        # Remove all MDS Services which have been manually marked for removal (by setting its capacity to 0)
        max_load = Configuration.get('{0}|mds_maxload'.format(MDS_CONFIG_PATH.format(vpool.guid)))
        for storagerouter in sorted(storagerouter_info, key=lambda k: k.ip):
            total_load = 0.0
            root_client = mds_dict[vpool][storagerouter]['client']
            mds_services = mds_dict[vpool][storagerouter]['services']

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
                    _, next_load = MDSServiceController.get_mds_load(mds_service=mds_service)
                    if next_load == float('inf'):
                        total_load = sys.maxint * -1  # Cast to lowest possible value if any MDS service capacity is set to infinity
                    else:
                        total_load += next_load

                    if next_load < max_load:
                        MDSServiceController._logger.debug('vPool {0} - StorageRouter {1} - MDS Service {2} on port {3}: Capacity available - Load at {4}%'.format(vpool.name, storagerouter.name, number, port, next_load))
                    else:
                        MDSServiceController._logger.debug('vPool {0} - StorageRouter {1} - MDS Service {2} on port {3}: No capacity available - Load at {4}%'.format(vpool.name, storagerouter.name, number, port, next_load))

            if total_load >= max_load * len(mds_services):
                mds_services_to_add = int(math.ceil((total_load - max_load * len(mds_services)) / max_load))
                MDSServiceController._logger.info('vPool {0} - StorageRouter {1} - Average load per service {2:.2f}% - Max load per service {3:.2f}% - {4} MDS service{5} will be added'.format(
                    vpool.name, storagerouter.name, total_load / len(mds_services), max_load, mds_services_to_add, '' if mds_services_to_add == 1 else 's'
                ))

                for _ in range(mds_services_to_add):
                    MDSServiceController._logger.info('vPool {0} - StorageRouter {1} - Adding new MDS Service'.format(vpool.name, storagerouter.name))
                    try:
                        mds_services.append(MDSServiceController.prepare_mds_service(storagerouter=storagerouter, vpool=vpool))
                    except Exception:
                        MDSServiceController._logger.exception('vPool {0} - StorageRouter {1} - Failed to create new MDS Service'.format(vpool.name, storagerouter.name))

        # After potentially having added new MDSes, retrieve the optimal configuration
        mds_config_set = {}
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
                # Filesystem section in StorageDriver configuration are all parameters used for vDisks created directly on the filesystem
                # So when a vDisk gets created on the filesystem, these MDSes will be assigned to them
                storagedriver_config.configuration.filesystem_config.fs_metadata_backend_mds_nodes=optimal_mds_config
                storagedriver_config.save(root_client)

        # Execute a safety check, making sure the master/slave configuration is optimal.
        MDSServiceController._logger.info('vPool {0} - Ensuring safety for all vDisks'.format(vpool.name))
        for vdisk in vpool.vdisks:
            try:
                MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)
            except Exception:
                message = 'Ensure safety for vDisk {0} with guid {1} failed'.format(vdisk.name, vdisk.guid)
                MDSServiceController._logger.exception(message)
                ensure_safety_failures.append(message)

        if ensure_safety_failures:
            raise MDSCheckupEnsureSafetyFailures('\n - ' + '\n - '.join(ensure_safety_failures))

    @staticmethod
    @ovs_task(name='ovs.mds.mds_checkup', schedule=Schedule(minute='30', hour='0,4,8,12,16,20'), ensure_single_info={'mode': 'CHAINED'})
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

        mds_dict, offline_nodes = MDSServiceController._get_mds_information(VPoolList.get_vpools())

        ensure_safety_failures = []
        for vpool, storagerouter_info in mds_dict.iteritems():
            try:
                MDSServiceController.mds_checkup_single(vpool.guid, mds_dict, offline_nodes, ensure_single_timeout=1)
            except MDSCheckupEnsureSafetyFailures as ex:
                ensure_safety_failures.append(ex)
            except EnsureSingleTimeoutReached:
                # This exception is raised by the callback. The mds checkup calls the single checkup inline which can
                # invoke the callback if the same instance is already running (because of extend/shrink)
                MDSServiceController._logger.info('MDS Checkup single already running for VPool {}'.format(vpool.guid))

        if ensure_safety_failures:
            raise RuntimeError(''.join((str(failure) for failure in ensure_safety_failures)))
        MDSServiceController._logger.info('Finished')

    # noinspection PyUnresolvedReferences
    @staticmethod
    @ovs_task(name='ovs.mds.ensure_safety_vpool', ensure_single_info={'mode': 'DEDUPED', 'ignore_arguments': ['vdisk_guid', 'excluded_storagerouter_guids']})
    def _ensure_safety_vpool(vpool_guid, vdisk_guid, excluded_storagerouter_guids=None):
        """
        Ensures safety for a single vdisk of a vpool
        Allows multiple ensure safeties to run at the same time for different vpool
        Used internally
        :param vpool_guid: Guid of the VPool associated with the vDisk
        :type vpool_guid: str
        :param vdisk_guid: Guid of the vDisk to the safety off
        :type vdisk_guid: str
        :param excluded_storagerouter_guids: GUIDs of StorageRouters to leave out of calculation (Eg: When 1 is down or unavailable)
        :type excluded_storagerouter_guids: list[str]
        :return: None
        :rtype: NoneType
        """
        _ = vpool_guid

        if excluded_storagerouter_guids is None:
            excluded_storagerouter_guids = []

        safety_ensurer = SafetyEnsurer(vdisk_guid, excluded_storagerouter_guids)
        safety_ensurer.ensure_safety()

    @staticmethod
    @ovs_task(name='ovs.mds.ensure_safety')
    def ensure_safety(vdisk_guid, excluded_storagerouter_guids=None, **kwargs):
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
        :raises RuntimeError: If host of vDisk is part of the excluded StorageRouters
                              If host of vDisk is not part of the StorageRouters in the primary domain
                              If catchup command fails for a slave
                              If MDS client cannot be created for any of the current or new MDS services
                              If updateMetadataBackendConfig would fail for whatever reason
        :raises SRCObjectNotFoundException: If vDisk does not have a StorageRouter GUID
        :return: None
        :rtype: NoneType
        """
        vdisk = VDisk(vdisk_guid)
        return MDSServiceController._ensure_safety_vpool(vdisk.vpool_guid, vdisk.guid, excluded_storagerouter_guids, **kwargs)

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
                load = MDSServiceController.get_mds_load(mds_service=mds_service)[0]
                if mds_info[0] is None or load < mds_info[1]:
                    mds_info = (mds_service, load)
        return mds_info

    @staticmethod
    def get_mds_storagedriver_config_set(vpool, offline_nodes=None):
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
        if offline_nodes is None:
            offline_nodes = []
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

        safety = Configuration.get('{0}|mds_safety'.format(MDS_CONFIG_PATH.format(vpool.guid)))
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
                                load, _ = MDSServiceController.get_mds_load(mds_service)
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
    @ovs_task(name='ovs.mds.mds_catchup', schedule=Schedule(minute='30', hour='*/2'), ensure_single_info={'mode': 'DEFAULT'})
    def mds_catchup():
        """
        Looks to catch up all MDS slaves which are too far behind
        Only one catch for every storagedriver is invoked
        """
        # Only for caching purposes
        def storagedriver_worker(queue, error_list):
            # type: (Queue.Queue, List[str]) -> None
            while not queue.empty():
                mds_catch_up = queue.get()  # type: MDSCatchUp
                try:
                    mds_catch_up.catch_up(async=False)
                except Exception as ex:
                    MDSServiceController._logger.exception('Exceptions while catching for vDisk {0}'.format(mds_catch_up.vdisk.guid))
                    error_list.append(str(ex))
                finally:
                    queue.task_done()

        storagedriver_queues = {}
        for vdisk in VDiskList.get_vdisks():
            if vdisk.storagedriver_id not in storagedriver_queues:
                storagedriver_queues[vdisk.storagedriver_id] = Queue.Queue()
            # Putting it in the Queue ensures that the reference is still there so the caching is used optimally
            catch_up = MDSCatchUp(vdisk.guid)
            storagedriver_queues[vdisk.storagedriver_id].put(catch_up)

        errors = []
        threads = []
        for storadriver_id, storagedriver_queue in storagedriver_queues.iteritems():
            thread = Thread(target=storagedriver_worker, args=(storagedriver_queue, errors,))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

        if len(errors) > 0:
            raise RuntimeError('Exception occurred while catching up: \n - {0}'.format('\n - '.join(errors)))
