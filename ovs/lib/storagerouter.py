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
StorageRouter module
"""

import os
import copy
import time
from subprocess import CalledProcessError
from ovs.dal.hybrids.disk import Disk
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.vpoollist import VPoolList
from ovs_extensions.api.client import OVSClient
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.disk import DiskTools
from ovs.extensions.generic.logger import Logger
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.os.osfactory import OSFactory
from ovs.extensions.packages.packagefactory import PackageFactory
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storageserver.storagedriver import ClusterNodeConfig, LocalStorageRouterClient, LOG_LEVEL_MAPPING, StorageDriverConfiguration, StorageDriverClient
from ovs.extensions.support.agent import SupportAgent
from ovs.lib.disk import DiskController
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.storagedriver import StorageDriverController
from ovs.lib.vdisk import VDiskController
from ovs.lib.vpool import VPoolController
from volumedriver.storagerouter import storagerouterclient


class StorageRouterController(object):
    """
    Contains all BLL related to StorageRouter
    """
    _logger = Logger('lib')
    _log_level = LOG_LEVEL_MAPPING[_logger.getEffectiveLevel()]
    # noinspection PyCallByClass,PyTypeChecker
    storagerouterclient.Logger.setupLogging(Logger.load_path('storagerouterclient'), _log_level)
    # noinspection PyArgumentList
    storagerouterclient.Logger.enableLogging()

    @staticmethod
    @ovs_task(name='ovs.storagerouter.ping')
    def ping(storagerouter_guid, timestamp):
        """
        Update a StorageRouter's celery heartbeat
        :param storagerouter_guid: Guid of the StorageRouter to update
        :type storagerouter_guid: str
        :param timestamp: Timestamp to compare to
        :type timestamp: float
        :return: None
        :rtype: NoneType
        """
        with volatile_mutex('storagerouter_heartbeat_{0}'.format(storagerouter_guid)):
            storagerouter = StorageRouter(storagerouter_guid)
            if timestamp > storagerouter.heartbeats.get('celery', 0):
                storagerouter.heartbeats['celery'] = timestamp
                storagerouter.save()

    @staticmethod
    @ovs_task(name='ovs.storagerouter.get_metadata')
    def get_metadata(storagerouter_guid):
        """
        Gets physical information about the specified storagerouter
        :param storagerouter_guid: StorageRouter guid to retrieve the metadata for
        :type storagerouter_guid: str
        :return: Metadata information about the StorageRouter
        :rtype: dict
        """
        return {'partitions': StorageRouterController.get_partition_info(storagerouter_guid),
                'ipaddresses': StorageRouterController.get_ip_addresses(storagerouter_guid),
                'scrub_available': StorageRouterController._check_scrub_partition_present()}

    @staticmethod
    def get_ip_addresses(storagerouter_guid):
        """
        Retrieves the ip addresses of a Storageroter
        :param storagerouter_guid: Guid of the Storagerouter
        :return: list of ip addresses
        :rtype: list
        """
        storagerouter = StorageRouter(storagerouter_guid)
        client = SSHClient(storagerouter)
        return OSFactory.get_manager().get_ip_addresses(client=client)

    @staticmethod
    def get_partition_info(storagerouter_guid):
        """
        Retrieves information about the partitions of a Storagerouter
        :param storagerouter_guid: Guid of the Storagerouter
        :type storagerouter_guid: str
        :return: dict with information about the partitions
        :rtype: dict
        """
        storagerouter = StorageRouter(storagerouter_guid)
        client = SSHClient(storagerouter)
        services_mds = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.MD_SERVER).services
        services_arakoon = [service for service in ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON).services
                            if service.name != 'arakoon-ovsdb' and service.is_internal is True]

        partitions = dict((role, []) for role in DiskPartition.ROLES)
        for disk in storagerouter.disks:
            for disk_partition in disk.partitions:
                claimed_space_by_fwk = 0
                used_space_by_system = 0
                available_space_by_system = 0
                for storagedriver_partition in disk_partition.storagedrivers:
                    claimed_space_by_fwk += storagedriver_partition.size if storagedriver_partition.size is not None else 0
                    if client.dir_exists(storagedriver_partition.path):
                        try:
                            used_space_by_system += int(client.run(['du', '-B', '1', '-d', '0', storagedriver_partition.path], timeout=5).split('\t')[0])
                        except Exception as ex:
                            StorageRouterController._logger.warning('Failed to get directory usage for {0}. {1}'.format(storagedriver_partition.path, ex))

                if disk_partition.mountpoint is not None:
                    for alias in disk_partition.aliases:
                        StorageRouterController._logger.info('Verifying disk partition usage by checking path {0}'.format(alias))
                        disk_partition_device = client.file_read_link(path=alias)
                        try:
                            available_space_by_system = int(client.run(['df', '-B', '1', '--output=avail', disk_partition_device], timeout=5).splitlines()[-1])
                            break
                        except Exception as ex:
                            StorageRouterController._logger.warning('Failed to get partition usage for {0}. {1}'.format(disk_partition.mountpoint, ex))

                for role in disk_partition.roles:
                    size = 0 if disk_partition.size is None else disk_partition.size
                    if available_space_by_system > 0:
                        # Take available space reported by df then add back used by roles so that the only used space reported is space not managed by us
                        available = available_space_by_system + used_space_by_system - claimed_space_by_fwk
                    else:
                        available = size - claimed_space_by_fwk  # Subtract size for roles which have already been claimed by other vpools (but not necessarily already been fully used)

                    in_use = any(junction for junction in disk_partition.storagedrivers if junction.role == role)
                    if role == DiskPartition.ROLES.DB:
                        for service in services_arakoon:
                            if service.storagerouter_guid == storagerouter_guid:
                                in_use = True
                                break
                        for service in services_mds:
                            if service.storagerouter_guid == storagerouter_guid:
                                in_use = True
                                break

                    partitions[role].append({'ssd': disk.is_ssd,
                                             'guid': disk_partition.guid,
                                             'size': size,
                                             'in_use': in_use,
                                             'usable': True,  # Sizes smaller than 1GiB and smaller than 5% of largest WRITE partition will be un-usable
                                             'available': available if available > 0 else 0,
                                             'mountpoint': disk_partition.folder,  # Equals to mount point unless mount point is root ('/'), then we pre-pend mount point with '/mnt/storage'
                                             'storagerouter_guid': storagerouter_guid})

        # Strip out WRITE caches which are smaller than 5% of largest write cache size and smaller than 1GiB
        writecache_sizes = []
        for partition_info in partitions[DiskPartition.ROLES.WRITE]:
            writecache_sizes.append(partition_info['available'])
        largest_write_cache = max(writecache_sizes) if len(writecache_sizes) > 0 else 0
        for index, size in enumerate(writecache_sizes):
            if size < largest_write_cache * 5 / 100 or size < 1024 ** 3:
                partitions[DiskPartition.ROLES.WRITE][index]['usable'] = False

        return partitions

    @staticmethod
    def supports_block_cache(storagerouter_guid):
        """
        Checks whether a Storagerouter support block cache
        :param storagerouter_guid: Guid of the Storagerouter to check
        :type storagerouter_guid: str
        :return: True or False
        :rtype: bool
        """
        storagerouter = StorageRouter(storagerouter_guid)
        storagerouter.invalidate_dynamics(['features'])
        features = storagerouter.features
        if features is None:
            raise RuntimeError('Could not load available features')
        return 'block-cache' in features['alba']['features']

    @staticmethod
    def verify_fragment_cache_size(storagerouter_guid, writecache_size_requested, amount_of_proxies, fragment_cache_settings,
                                   block_cache_settings, partition_info=None):
        """
        Verifies whether the fragment cache size is large enough
        :param storagerouter_guid: Guid of the Storagerouter to check
        :param writecache_size_requested: Requested size that should be checked if it is possible
        :param amount_of_proxies: Amount of proxies that would be deployed
        :param fragment_cache_settings: Information about the fragment cache
        :param block_cache_settings: Information about the block cache
        :param partition_info: Information about the partitions (Optional, won't query for the info if supplied)
        :return: dict with information about the mointpoint and possible errors
        :rtype: dict
        """
        error_messages = []
        # Calculate available write cache size
        usable_write_partitions = StorageRouterController.get_usable_partitions(storagerouter_guid, DiskPartition.ROLES.WRITE, partition_info)
        writecache_size_available = sum(part['available'] for part in usable_write_partitions)

        largest_ssd_write_partition = None
        largest_sata_write_partition = None
        largest_ssd = 0
        largest_sata = 0
        for info in usable_write_partitions:
            if info['ssd'] is True and info['available'] > largest_ssd:
                largest_ssd = info['available']
                largest_ssd_write_partition = info['guid']
            elif info['ssd'] is False and info['available'] > largest_sata:
                largest_sata = info['available']
                largest_sata_write_partition = info['guid']

        mountpoint_cache = None
        local_amount_of_proxies = 0
        largest_write_mountpoint = None
        if largest_ssd_write_partition is None and largest_sata_write_partition is None:
            error_messages.append('No WRITE partition found to put the local caches on')
        else:
            largest_write_mountpoint = DiskPartition(largest_ssd_write_partition or largest_sata_write_partition)
            if fragment_cache_settings['is_backend'] is False:
                if fragment_cache_settings['read'] is True or fragment_cache_settings['write'] is True:  # Local fragment caching
                    local_amount_of_proxies += amount_of_proxies
            if block_cache_settings['is_backend'] is False:
                if block_cache_settings['read'] is True or block_cache_settings['write'] is True:  # Local block caching
                    local_amount_of_proxies += amount_of_proxies
            if local_amount_of_proxies > 0:
                mountpoint_cache = largest_write_mountpoint
                one_gib = 1024 ** 3  # 1GiB
                proportion = float(largest_ssd or largest_sata) * 100.0 / writecache_size_available
                available = proportion * writecache_size_requested / 100 * 0.10  # Only 10% is used on the largest WRITE partition for fragment caching
                fragment_size = available / local_amount_of_proxies
                if fragment_size < one_gib:
                    maximum = local_amount_of_proxies
                    while True:
                        if maximum == 0 or available / maximum > one_gib:
                            break
                        maximum -= 2 if local_amount_of_proxies > amount_of_proxies else 1
                    error_messages.append(
                        'Cache location is too small to deploy {0} prox{1}. {2}1GiB is required per proxy and with an available size of {3:.2f}GiB, {4} prox{5} can be deployed'.format(
                            amount_of_proxies,
                            'y' if amount_of_proxies == 1 else 'ies',
                            '2x ' if local_amount_of_proxies > amount_of_proxies else '',
                            available / 1024.0 ** 3, maximum, 'y' if maximum == 1 else 'ies'))
        return {'errors': error_messages,
                'largest_write_mountpoint': largest_write_mountpoint,
                'mountpoint_cache': mountpoint_cache,
                'local_amount_of_proxies': local_amount_of_proxies}

    @staticmethod
    def verify_required_roles(storagerouter_guid, required_roles=list(), partition_info=None):
        """
        Verifies if a storagerouter has all the specified required roles
        :param storagerouter_guid: Guid of the Storagerouter
        :param required_roles: list of roles
        :param partition_info: Information about the partitions (Optional, won't query for the info if supplied)
        :return: list with errors
        :rtype: list
        """
        if partition_info is None:
            partition_info = StorageRouterController.get_partition_info(storagerouter_guid)
        error_messages = []
        if StorageRouterController._check_scrub_partition_present() is False:
            error_messages.append('At least 1 StorageRouter must have a partition with a {0} role'.format(DiskPartition.ROLES.SCRUB))
        for required_role in required_roles:
            if required_role not in partition_info:
                error_messages.append('Missing required partition with a {0} role'.format(required_role))
            elif len(partition_info[required_role]) == 0:
                error_messages.append('At least 1 partition with a {0} role is required per StorageRouter'.format(required_role))
            elif required_role in [DiskPartition.ROLES.DB, DiskPartition.ROLES.DTL]:
                if len(partition_info[required_role]) > 1:
                    error_messages.append('Only 1 partition with a {0} role is allowed per StorageRouter'.format(required_role))
            else:
                total_available = [part['available'] for part in partition_info[required_role]]
                if total_available == 0:
                    error_messages.append('Not enough available space for {0}'.format(required_role))

        return error_messages

    @staticmethod
    def get_usable_partitions(storagerouter_guid, role=None, partition_info=None):
        """
        Get the usable parttions for a specific role
        :param storagerouter_guid: Guid of the Storagerouter
        :param role: Role of the partition (Optional, defaults to all roles)
        :param partition_info: Information about the partitions (Optional, will be fetched if not present)
        :return: List with usable write partitions
        """
        usable_partitions = []
        if partition_info is None:
            partition_info = StorageRouterController.get_partition_info(storagerouter_guid)
        if role is None:
            for role in DiskPartition.ROLES:
                usable_partitions.extend([part for part in partition_info[role] if part['usable'] is True])
        else:
            usable_partitions.extend([part for part in partition_info[role] if part['usable'] is True])
        return usable_partitions

    @staticmethod
    def get_allocation_info(storagerouter_guid, requested_size, role, partition_info=None):
        """
        Check if there would be overallocation for a certain role and size
        Returns the available info and if there would be overallocation
        :param storagerouter_guid: Guid of the Storagerouter to check on
        :type storagerouter_guid: str
        :param requested_size: Size requested in bytes to use
        :type requested_size: int
        :param role: Role to check for
        :type role: from ovs.dal.hybrids.diskpartition.DiskPartition.ROLES
        :param partition_info: Information about the partitions (Optional, won't query for the info if supplied)
        :type partition_info: dict
        :return: Dict with information about the overallocation and sizes
        :rtype: dict
        """
        usable_partitions = StorageRouterController.get_usable_partitions(storagerouter_guid, role, partition_info)
        available_size_gib = sum(part['available'] for part in usable_partitions)
        requested_size_gib = requested_size * 1024 ** 3
        return {'over_allocated': requested_size_gib > available_size_gib, 'requested_size': requested_size, 'available_size': available_size_gib / 1024 ** 3}

    @staticmethod
    def verify_mounted_mointpoints(storagerouter_guid, roles=list(), partition_info=None):
        """
        Verify that the specified roles are actually mounted
        :param storagerouter_guid: Guid of the Storagerouter to check mounts on
        :param roles: list of roles to check if they are mounted
        :param partition_info: Information about the partitions (Optional, won't query for the info if supplied)
        :return: list of errors
        :rtype: list
        """
        if partition_info is None:
            partition_info = StorageRouterController.get_partition_info(storagerouter_guid)
        error_messages = []
        storagerouter = StorageRouter(storagerouter_guid)
        client = SSHClient(storagerouter)
        for role, part_info in partition_info.iteritems():
            if role not in roles:
                continue
            for part in part_info:
                if not client.is_mounted(part['mountpoint']) and part['mountpoint'] != DiskPartition.VIRTUAL_STORAGE_LOCATION:
                    error_messages.append('Mount point {0} is not mounted'.format(part['mountpoint']))
        return error_messages

    @staticmethod
    @ovs_task(name='ovs.storagerouter.add_vpool')
    def add_vpool(parameters):
        """
        Add a vPool to the machine this task is running on
        :param parameters: Parameters for vPool creation
        :type parameters: dict
        :return: None
        :rtype: NoneType
        """
        required_params = {'vpool_name': (str, Toolbox.regex_vpool),
                           'storage_ip': (str, Toolbox.regex_ip),
                           'storagerouter_ip': (str, Toolbox.regex_ip),
                           'writecache_size': (int, {'min': 1, 'max': 10240}),  # Global write buffer
                           'config_params': (dict, {'dtl_mode': (str, StorageDriverClient.VPOOL_DTL_MODE_MAP.keys()),
                                                    'sco_size': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.keys()),
                                                    'cluster_size': (int, StorageDriverClient.CLUSTER_SIZES),
                                                    'write_buffer': (int, {'min': 128, 'max': 10240}),  # Volume write buffer
                                                    'dtl_transport': (str, StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP.keys()),
                                                    'advanced': (dict, {'number_of_scos_in_tlog': (float, {'min': 4, 'max': 20}),
                                                                        'non_disposable_scos_factor': (float, {'min': 1.5, 'max': 20})})}),
                           'mds_config_params': (dict, {'mds_safety': (int, {'min': 1, 'max': 5}, False)}, False),
                           'fragment_cache_on_read': (bool, None),
                           'fragment_cache_on_write': (bool, None),
                           'block_cache_on_read': (bool, None),
                           'block_cache_on_write': (bool, None),
                           'backend_info': (dict, {'preset': (str, Toolbox.regex_preset),
                                                   'alba_backend_guid': (str, Toolbox.regex_guid)}),
                           'backend_info_fc': (dict, {'preset': (str, Toolbox.regex_preset),
                                                      'alba_backend_guid': (str, Toolbox.regex_guid)}, False),
                           'backend_info_bc': (dict, {'preset': (str, Toolbox.regex_preset),
                                                      'alba_backend_guid': (str, Toolbox.regex_guid)}, False),
                           'connection_info': (dict, {'host': (str, Toolbox.regex_ip),
                                                      'port': (int, {'min': 1, 'max': 65535}),
                                                      'client_id': (str, None),
                                                      'client_secret': (str, None),
                                                      'local': (bool, None, False)}),
                           'parallelism': (dict, {'proxies': (int, {'min': 1, 'max': 16}, False)}, False)}

        ########################
        # VALIDATIONS (PART 1) #
        ########################
        # Check parameters
        if not isinstance(parameters, dict):
            raise ValueError('Parameters should be of type "dict"')
        Toolbox.verify_required_params(required_params, parameters)

        client = SSHClient(parameters['storagerouter_ip'])

        sd_config_params = parameters['config_params']
        sco_size = sd_config_params['sco_size']
        write_buffer = sd_config_params['write_buffer']  # Volume write buffer
        if (sco_size == 128 and write_buffer < 256) or not (128 <= write_buffer <= 10240):
            raise ValueError('Incorrect storagedriver configuration settings specified')

        # Verify vPool status and additional parameters
        vpool_name = parameters['vpool_name']
        vpool = VPoolList.get_vpool_by_name(vpool_name)
        new_vpool = vpool is None
        if new_vpool is False:
            if vpool.status != VPool.STATUSES.RUNNING:
                raise ValueError('VPool should be in {0} status'.format(VPool.STATUSES.RUNNING))

        # Check storagerouter existence
        storagerouter = StorageRouterList.get_by_ip(client.ip)
        if storagerouter is None:
            raise RuntimeError('Could not find StorageRouter with given IP address {0}'.format(client.ip))

        # Check RDMA capabilities
        if sd_config_params['dtl_transport'] == StorageDriverClient.FRAMEWORK_DTL_TRANSPORT_RSOCKET and storagerouter.rdma_capable is False:
            raise RuntimeError('The DTL transport is not supported by the StorageRouter')

        # Check duplicate vPool StorageDriver
        all_storagerouters = [storagerouter]
        if new_vpool is False:
            required_params_sd_config = {'sco_size': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.keys()),
                                         'dtl_mode': (str, StorageDriverClient.VPOOL_DTL_MODE_MAP.keys()),
                                         'write_buffer': (float, None),
                                         'dtl_transport': (str, StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP.keys()),
                                         'tlog_multiplier': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.values())}
            Toolbox.verify_required_params(required_params=required_params_sd_config,
                                           actual_params=vpool.configuration)

            for vpool_storagedriver in vpool.storagedrivers:
                if vpool_storagedriver.storagerouter_guid == storagerouter.guid:
                    raise RuntimeError('A StorageDriver is already linked to this StorageRouter for this vPool: {0}'.format(vpool_name))
            all_storagerouters += [sd.storagerouter for sd in vpool.storagedrivers]

        # Check storagerouter connectivity
        ip_client_map = {}
        offline_nodes = []
        for sr in all_storagerouters:
            try:
                ip_client_map[sr.ip] = {'ovs': SSHClient(sr.ip, username='ovs'),
                                        'root': SSHClient(sr.ip, username='root')}
            except UnableToConnectException:
                if sr == storagerouter:
                    raise RuntimeError('Node on which the vpool is being {0} is not reachable'.format('created' if new_vpool is True else 'extended'))
                offline_nodes.append(sr)  # We currently want to allow offline nodes while setting up or extend a vpool

        block_cache_on_read = parameters['block_cache_on_read']
        block_cache_on_write = parameters['block_cache_on_write']

        # Validate features
        storagerouter.invalidate_dynamics(['features'])
        features = storagerouter.features
        if features is None:
            raise RuntimeError('Could not load available features')
        supports_block_cache = 'block-cache' in features['alba']['features']
        if supports_block_cache is False and (block_cache_on_read is True or block_cache_on_write is True):
            raise RuntimeError('Block cache is not a supported feature')

        ################
        # CREATE VPOOL #
        ################
        connection_info = parameters['connection_info']
        if new_vpool is True:
            vpool = VPool()
            vpool.name = vpool_name
            vpool.login = connection_info['client_id']
            vpool.password = connection_info['client_secret']
            vpool.metadata = {}
            vpool.connection = '{0}:{1}'.format(connection_info['host'], connection_info['port'])
            vpool.description = vpool_name
            vpool.rdma_enabled = sd_config_params['dtl_transport'] == StorageDriverClient.FRAMEWORK_DTL_TRANSPORT_RSOCKET
            vpool.status = VPool.STATUSES.INSTALLING
            vpool.metadata_store_bits = 5
            vpool.save()
            # Configure this asap because certain flows don't check the vPool status yet and thus rely on the availability of this key
            Configuration.set(key='/ovs/vpools/{0}/mds_config'.format(vpool.guid),
                              value={'mds_tlogs': 100,
                                     'mds_safety': parameters.get('mds_config_params', {}).get('mds_safety', 3),
                                     'mds_maxload': 75})
        else:
            vpool.status = VPool.STATUSES.EXTENDING
            vpool.save()

        ########################
        # VALIDATIONS (PART 2) #
        ########################
        # When 2 or more jobs simultaneously run on the same StorageRouter, we need to check and create the StorageDriver partitions in locked context
        created_dirs = []
        root_client = ip_client_map[storagerouter.ip]['root']
        storagedriver = None
        partitions_mutex = volatile_mutex('add_vpool_partitions_{0}'.format(storagerouter.guid))
        try:
            partitions_mutex.acquire(wait=60)
            error_messages = []
            # Check mount point
            metadata = StorageRouterController.get_metadata(storagerouter.guid)
            partition_info = metadata['partitions']

            if StorageRouterController.mountpoint_exists(name=vpool_name, storagerouter_guid=storagerouter.guid):
                error_messages.append('The mount point for vPool {0} already exists'.format(vpool_name))

            # Check mount points are mounted
            required_mounted_roles = [DiskPartition.ROLES.DB, DiskPartition.ROLES.DTL, DiskPartition.ROLES.WRITE, DiskPartition.ROLES.SCRUB]
            error_messages.extend(StorageRouterController.verify_mounted_mointpoints(storagerouter_guid=storagerouter.guid,
                                                                                     roles=required_mounted_roles,
                                                                                     partition_info=partition_info))

            # Check required roles
            required_roles = [DiskPartition.ROLES.DB, DiskPartition.ROLES.DTL, DiskPartition.ROLES.WRITE]
            if metadata['scrub_available'] is False:
                error_messages.append('At least 1 StorageRouter must have a partition with a {0} role'.format(DiskPartition.ROLES.SCRUB))
            error_messages.extend(StorageRouterController.verify_required_roles(storagerouter_guid=storagerouter.guid,
                                                                                required_roles=required_roles,
                                                                                partition_info=partition_info))

            # Check backend information and connection information
            backend_info = parameters['backend_info']
            backend_info_fc = parameters.get('backend_info_fc', {})
            backend_info_bc = parameters.get('backend_info_bc', {})
            alba_backend_guid = backend_info['alba_backend_guid']
            alba_backend_guid_fc = backend_info_fc.get('alba_backend_guid')
            alba_backend_guid_bc = backend_info_bc.get('alba_backend_guid')
            connection_info_fc = parameters.get('connection_info_fc', {})
            connection_info_bc = parameters.get('connection_info_bc', {})
            use_fragment_cache_backend = alba_backend_guid_fc is not None
            use_block_cache_backend = alba_backend_guid_bc is not None

            if alba_backend_guid == alba_backend_guid_fc:
                error_messages.append('Backend and Fragment cache backend cannot be the same')
            if alba_backend_guid == alba_backend_guid_bc:
                error_messages.append('Backend and Block cache backend cannot be the same')
            if new_vpool is False and alba_backend_guid != vpool.metadata['backend']['backend_info']['alba_backend_guid']:
                error_messages.append('Incorrect ALBA Backend guid specified')

            if use_fragment_cache_backend is True:
                if 'connection_info_fc' not in parameters:
                    error_messages.append('Missing the connection information for the Fragment Cache Backend')
                else:
                    try:
                        Toolbox.verify_required_params(actual_params=parameters,
                                                       required_params={'cache_quota_fc': (int, None, False),
                                                                        'connection_info_fc': (dict, {'host': (str, Toolbox.regex_ip),
                                                                                                      'port': (int, {'min': 1, 'max': 65535}),
                                                                                                      'client_id': (str, None),
                                                                                                      'client_secret': (str, None),
                                                                                                      'local': (bool, None, False)})})
                    except RuntimeError as rte:
                        error_messages.append(rte.message)
            if use_block_cache_backend is True:
                if 'connection_info_bc' not in parameters:
                    error_messages.append('Missing the connection information for the Block Cache Backend')
                else:
                    try:
                        Toolbox.verify_required_params(actual_params=parameters,
                                                       required_params={'cache_quota_bc': (int, None, False),
                                                                        'connection_info_bc': (dict, {'host': (str, Toolbox.regex_ip),
                                                                                                      'port': (int, {'min': 1, 'max': 65535}),
                                                                                                      'client_id': (str, None),
                                                                                                      'client_secret': (str, None),
                                                                                                      'local': (bool, None, False)})})
                    except RuntimeError as rte:
                        error_messages.append(rte.message)

            # Check over-allocation for write cache
            writecache_size_requested = parameters['writecache_size'] * 1024 ** 3
            allocation_info = StorageRouterController.get_allocation_info(storagerouter_guid=storagerouter.guid,
                                                                          requested_size=writecache_size_requested / 1024 ** 3,
                                                                          role=DiskPartition.ROLES.WRITE,
                                                                          partition_info=partition_info)
            if allocation_info['over_allocated'] is True:
                error_messages.append('Too much space requested for {0} cache. Available: {1:.2f} GiB, Requested: {2:.2f} GiB'
                    .format(DiskPartition.ROLES.WRITE, allocation_info['available_size'], allocation_info['requested_size']))

            # Check current vPool configuration
            if new_vpool is False:
                current_vpool_configuration = vpool.configuration
                for key in sd_config_params.keys():
                    if key == 'mds_safety':  # MDS safety for a vPool can be overruled when extending
                        continue
                    current_value = current_vpool_configuration.get(key)
                    specified_value = sd_config_params[key]
                    if specified_value != current_value:
                        error_messages.append('Specified StorageDriver config "{0}" with value {1} does not match the value {2}'.format(key, specified_value, current_value))

            # Verify fragment cache is large enough
            amount_of_proxies = parameters.get('parallelism', {}).get('proxies', 2)
            fragment_cache_on_read = parameters['fragment_cache_on_read']
            fragment_cache_on_write = parameters['fragment_cache_on_write']

            fragment_cache_settings = {'read': fragment_cache_on_read, 'write': fragment_cache_on_write, 'is_backend': use_fragment_cache_backend}
            block_cache_settings = {'read': block_cache_on_read, 'write': block_cache_on_write, 'is_backend': use_block_cache_backend}
            verify_output = StorageRouterController.verify_fragment_cache_size(storagerouter_guid=storagerouter.guid,
                                                                               writecache_size_requested=writecache_size_requested,
                                                                               amount_of_proxies=amount_of_proxies,
                                                                               fragment_cache_settings=fragment_cache_settings,
                                                                               block_cache_settings=block_cache_settings,
                                                                               partition_info=partition_info)
            error_messages.extend(verify_output['errors'])
            largest_write_mountpoint = verify_output['largest_write_mountpoint']
            mountpoint_cache = verify_output['mountpoint_cache']
            local_amount_of_proxies = verify_output['local_amount_of_proxies']

            if error_messages:
                raise ValueError('Errors validating the specified parameters:\n - {0}'.format('\n - '.join(set(error_messages))))

            ############
            # MODELING #
            ############

            # Renew vPool metadata
            StorageRouterController._logger.info('Add vPool {0} started'.format(vpool_name))
            if new_vpool is True:
                new_backend_info = backend_info
                new_backend_info['connection_info'] = connection_info
                updated_metadata = {'backend': {'backend_info': new_backend_info},
                                    'caching_info': {}}
            else:
                updated_metadata = copy.deepcopy(vpool.metadata)

            caching_info = {'block_cache': {'is_backend': False},
                            'fragment_cache': {'is_backend': False}}
            if use_fragment_cache_backend is True:
                fragment_cache_backend_info = backend_info_fc
                fragment_cache_backend_info['connection_info'] = connection_info_fc
                caching_info['fragment_cache'] = {'backend_info': fragment_cache_backend_info,
                                                  'is_backend': True}
            if use_block_cache_backend is True:
                block_cache_backend_info = backend_info_bc
                block_cache_backend_info['connection_info'] = connection_info_bc
                caching_info['block_cache'] = {'backend_info': block_cache_backend_info,
                                               'is_backend': True}
            updated_metadata['caching_info'][storagerouter.guid] = caching_info

            StorageRouterController._logger.info('Refreshing metadata for {0} has started'.format(vpool.name))
            # Load backend properties for metadata
            renewed_metadata = VPoolController.get_renewed_backend_metadata(vpool.guid, sco_size, updated_metadata, new_vpool)
            # Set caching
            renewed_metadata['caching_info'][storagerouter.guid]['fragment_cache']['read'] = fragment_cache_on_read
            renewed_metadata['caching_info'][storagerouter.guid]['fragment_cache']['write'] = fragment_cache_on_write
            renewed_metadata['caching_info'][storagerouter.guid]['fragment_cache']['quota'] = parameters.get('cache_quota_fc')
            renewed_metadata['caching_info'][storagerouter.guid]['block_cache']['read'] = block_cache_on_read
            renewed_metadata['caching_info'][storagerouter.guid]['block_cache']['write'] = block_cache_on_write
            renewed_metadata['caching_info'][storagerouter.guid]['block_cache']['quota'] = parameters.get('cache_quota_bc')
            vpool.metadata = renewed_metadata
            vpool.save()

            # StorageDriver
            storagedriver = StorageDriverController.create_new_storagedriver(vpool_guid=vpool.guid,
                                                                             storagerouter_guid=storagerouter.guid,
                                                                             storage_ip=parameters['storage_ip'],
                                                                             amount_of_proxies=amount_of_proxies)

            # Assign WRITE / Fragment cache
            mountpoint_settings = {'mountpoint_cache': mountpoint_cache,
                                   'writecache_size_requested': writecache_size_requested,
                                   'largest_write_mountpoint': largest_write_mountpoint}
            created_partitions_output = StorageDriverController.configure_storagedriver_partitions(storagedriver_guid=storagedriver.guid,
                                                                                                   fragment_cache_settings=fragment_cache_settings,
                                                                                                   block_cache_settings=block_cache_settings,
                                                                                                   mountpoint_settings=mountpoint_settings,
                                                                                                   amount_of_proxies=amount_of_proxies,
                                                                                                   partition_info=partition_info)
            created_dirs = created_partitions_output['created_dirs']
            cache_size = created_partitions_output['cache_size']
            storagedriver_partitions = created_partitions_output['storagedriver_partitions']
            gap_configuration = created_partitions_output['gap_configuration']
            write_caches = created_partitions_output['write_caches']
        except Exception:
            StorageRouterController._logger.exception('Something went wrong during the validation or modeling of vPool {0} on StorageRouter {1}'.format(vpool.name, storagerouter.name))
            StorageRouterController._revert_vpool_status(vpool=vpool, status=VPool.STATUSES.RUNNING, storagedriver=storagedriver, client=root_client, dirs_created=created_dirs)
            raise
        finally:
            partitions_mutex.release()

        #################
        # ARAKOON SETUP #
        #################
        counter = 0
        while counter < 300:
            try:
                if StorageDriverController.manual_voldrv_arakoon_checkup() is True:
                    break
            except Exception:
                StorageRouterController._logger.exception('Arakoon checkup for voldrv cluster failed')
                StorageRouterController._revert_vpool_status(vpool=vpool, status=VPool.STATUSES.RUNNING, storagedriver=storagedriver, client=root_client, dirs_created=created_dirs)
                raise
            counter += 1
            time.sleep(1)
            if counter == 300:
                StorageRouterController._logger.warning('Arakoon checkup for the StorageDriver cluster could not be started')
                StorageRouterController._revert_vpool_status(vpool=vpool, status=VPool.STATUSES.RUNNING, storagedriver=storagedriver, client=root_client, dirs_created=created_dirs)
                raise RuntimeError('Arakoon checkup for the StorageDriver cluster could not be started')

        ####################
        # CLUSTER REGISTRY #
        ####################
        node_configs = []
        existing_storagedrivers = []
        for sd in vpool.storagedrivers:
            if sd != storagedriver:
                existing_storagedrivers.append(sd)
            sd.invalidate_dynamics('cluster_node_config')
            node_configs.append(ClusterNodeConfig(**sd.cluster_node_config))

        try:
            vpool.clusterregistry_client.set_node_configs(node_configs)
            for sd in existing_storagedrivers:
                vpool.storagedriver_client.update_cluster_node_configs(str(sd.storagedriver_id), req_timeout_secs=10)
        except:
            StorageRouterController._logger.exception('Updating cluster node configurations failed')
            if new_vpool is True:
                StorageRouterController._revert_vpool_status(vpool=vpool, storagedriver=storagedriver, client=root_client, dirs_created=created_dirs)
            else:
                StorageRouterController._revert_vpool_status(vpool=vpool, status=VPool.STATUSES.FAILURE, storagedriver=storagedriver, client=root_client, dirs_created=created_dirs)
                node_configs = []
                for sd in vpool.storagedrivers:
                    if sd != storagedriver:
                        sd.invalidate_dynamics('cluster_node_config')
                        node_configs.append(ClusterNodeConfig(**sd.cluster_node_config))
                try:
                    vpool.clusterregistry_client.set_node_configs(node_configs)
                    for sd in existing_storagedrivers:
                        vpool.storagedriver_client.update_cluster_node_configs(str(sd.storagedriver_id), req_timeout_secs=10)
                except:
                    StorageRouterController._logger.exception('Restoring cluster node configurations failed')
                StorageRouterController._revert_vpool_status(vpool=vpool, status=VPool.STATUSES.RUNNING, storagedriver=storagedriver, client=root_client, dirs_created=created_dirs)
            raise

        ############################
        # CONFIGURATION MANAGEMENT #
        ############################
        # Configure regular proxies and scrub proxies
        StorageDriverController.setup_proxy_configs(vpool_guid=vpool.guid,
                                                    storagedriver_guid=storagedriver.guid,
                                                    cache_size=cache_size,
                                                    local_amount_of_proxies=local_amount_of_proxies,
                                                    storagedriver_partitions_caches=storagedriver_partitions['cache'])
        ###########################
        # CONFIGURE STORAGEDRIVER #
        ###########################
        storagedriver_settings = {'sco_size': sd_config_params['sco_size'],
                                  'dtl_mode': sd_config_params['dtl_mode'],
                                  'cluster_size': sd_config_params['cluster_size'],
                                  'dtl_transport': sd_config_params['dtl_transport'],
                                  'volume_write_buffer': write_buffer}
        requested_mds_safety = parameters.get('mds_config_params', {}).get('mds_safety')
        StorageDriverController.configure_storagedriver(storagedriver_guid=storagedriver.guid,
                                                        storagedriver_settings=storagedriver_settings,
                                                        write_caches=write_caches,
                                                        gap_configuration=gap_configuration)

        DiskController.sync_with_reality(storagerouter.guid)

        MDSServiceController.prepare_mds_service(storagerouter=storagerouter, vpool=vpool)

        # Update the MDS safety if changed via API (vpool.configuration will be available at this point also for the newly added StorageDriver)
        vpool.invalidate_dynamics('configuration')
        if requested_mds_safety is not None and vpool.configuration['mds_config']['mds_safety'] != requested_mds_safety:
            Configuration.set(key='/ovs/vpools/{0}/mds_config|mds_safety'.format(vpool.guid), value=requested_mds_safety)

        ##################
        # START SERVICES #
        ##################
        try:
            StorageDriverController.start_services(storagedriver.guid)
        except Exception:
            StorageRouterController._logger.exception('Error during the starting of the services')
            StorageRouterController._revert_vpool_status(vpool=vpool, status=VPool.STATUSES.FAILURE)
            raise

        ###############
        # POST CHECKS #
        ###############
        mds_config_set = MDSServiceController.get_mds_storagedriver_config_set(vpool=vpool, offline_nodes=offline_nodes)
        for sr in all_storagerouters:
            if sr.ip not in ip_client_map:
                continue
            node_client = ip_client_map[sr.ip]['ovs']
            for current_storagedriver in [sd for sd in sr.storagedrivers if sd.vpool_guid == vpool.guid]:
                storagedriver_config = StorageDriverConfiguration(vpool.guid, current_storagedriver.storagedriver_id)
                if storagedriver_config.config_missing is False:
                    # Filesystem section in StorageDriver configuration are all parameters used for vDisks created directly on the filesystem
                    # So when a vDisk gets created on the filesystem, these MDSes will be assigned to them
                    storagedriver_config.configure_filesystem(fs_metadata_backend_mds_nodes=mds_config_set[sr.guid])
                    storagedriver_config.save(node_client)

        # Everything's reconfigured, refresh new cluster configuration
        for current_storagedriver in vpool.storagedrivers:
            if current_storagedriver.storagerouter.ip not in ip_client_map:
                continue
            vpool.storagedriver_client.update_cluster_node_configs(str(current_storagedriver.storagedriver_id), req_timeout_secs=10)

        vpool.status = VPool.STATUSES.RUNNING
        vpool.save()
        vpool.invalidate_dynamics(['configuration'])

        # When a node is offline, we can run into errors, but also when 1 or more volumes are not running
        # Scheduled tasks below, so don't really care whether they succeed or not
        try:
            VDiskController.dtl_checkup(vpool_guid=vpool.guid, ensure_single_timeout=600)
        except:
            pass
        for vdisk in vpool.vdisks:
            try:
                MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)
            except:
                pass
        StorageRouterController._logger.info('Add vPool {0} ended successfully'.format(vpool_name))

    @staticmethod
    @ovs_task(name='ovs.storagerouter.remove_storagedriver')
    def remove_storagedriver(storagedriver_guid, offline_storage_router_guids=list()):
        """
        Removes a StorageDriver (if its the last StorageDriver for a vPool, the vPool is removed as well)
        :param storagedriver_guid: Guid of the StorageDriver to remove
        :type storagedriver_guid: str
        :param offline_storage_router_guids: Guids of StorageRouters which are offline and will be removed from cluster.
                                             WHETHER VPOOL WILL BE DELETED DEPENDS ON THIS
        :type offline_storage_router_guids: list
        :return: None
        :rtype: NoneType
        """
        storage_driver = StorageDriver(storagedriver_guid)
        StorageRouterController._logger.info('StorageDriver {0} - Deleting StorageDriver {1}'.format(storage_driver.guid, storage_driver.name))

        #############
        # Validations
        vpool = storage_driver.vpool
        if vpool.status != VPool.STATUSES.RUNNING:
            raise ValueError('VPool should be in {0} status'.format(VPool.STATUSES.RUNNING))

        # Sync with reality to have a clear vision of vDisks
        VDiskController.sync_with_reality(storage_driver.vpool_guid)
        storage_driver.invalidate_dynamics('vdisks_guids')
        if len(storage_driver.vdisks_guids) > 0:
            raise RuntimeError('There are still vDisks served from the given StorageDriver')

        storage_router = storage_driver.storagerouter
        mds_services_to_remove = [mds_service for mds_service in vpool.mds_services if mds_service.service.storagerouter_guid == storage_router.guid]
        for mds_service in mds_services_to_remove:
            if len(mds_service.storagedriver_partitions) == 0 or mds_service.storagedriver_partitions[0].storagedriver is None:
                raise RuntimeError('Failed to retrieve the linked StorageDriver to this MDS Service {0}'.format(mds_service.service.name))

        StorageRouterController._logger.info('StorageDriver {0} - Checking availability of related StorageRouters'.format(storage_driver.guid, storage_driver.name))
        client = None
        errors_found = False
        storage_drivers_left = False
        storage_router_online = True
        available_storage_drivers = []
        for sd in vpool.storagedrivers:
            sr = sd.storagerouter
            if sr != storage_router:
                storage_drivers_left = True
            try:
                temp_client = SSHClient(sr, username='root')
                if sr.guid in offline_storage_router_guids:
                    raise Exception('StorageRouter "{0}" passed as "offline StorageRouter" appears to be reachable'.format(sr.name))
                if sr == storage_router:
                    mtpt_pids = temp_client.run("lsof -t +D '/mnt/{0}' || true".format(vpool.name.replace(r"'", r"'\''")), allow_insecure=True).splitlines()
                    if len(mtpt_pids) > 0:
                        raise RuntimeError('vPool cannot be deleted. Following processes keep the vPool mount point occupied: {0}'.format(', '.join(mtpt_pids)))
                with remote(temp_client.ip, [LocalStorageRouterClient]) as rem:
                    sd_key = '/ovs/vpools/{0}/hosts/{1}/config'.format(vpool.guid, sd.storagedriver_id)
                    if Configuration.exists(sd_key) is True:
                        try:
                            path = Configuration.get_configuration_path(sd_key)
                            lsrc = rem.LocalStorageRouterClient(path)
                            lsrc.server_revision()  # 'Cheap' call to verify whether volumedriver is responsive
                            StorageRouterController._logger.info('StorageDriver {0} - Available StorageDriver for migration - {1}'.format(storage_driver.guid, sd.name))
                            available_storage_drivers.append(sd)
                        except Exception as ex:
                            if 'ClusterNotReachableException' not in str(ex):
                                raise
                client = temp_client
                StorageRouterController._logger.info('StorageDriver {0} - StorageRouter {1} with IP {2} is online'.format(storage_driver.guid, sr.name, sr.ip))
            except UnableToConnectException:
                if sr == storage_router or sr.guid in offline_storage_router_guids:
                    StorageRouterController._logger.warning('StorageDriver {0} - StorageRouter {1} with IP {2} is offline'.format(storage_driver.guid, sr.name, sr.ip))
                    if sr == storage_router:
                        storage_router_online = False
                else:
                    raise RuntimeError('Not all StorageRouters are reachable')

        if client is None:
            raise RuntimeError('Could not find any responsive node in the cluster')

        ###############
        # Start removal
        if storage_drivers_left is True:
            vpool.status = VPool.STATUSES.SHRINKING
        else:
            vpool.status = VPool.STATUSES.DELETING
        vpool.save()

        available_sr_names = [sd.storagerouter.name for sd in available_storage_drivers]
        unavailable_sr_names = [sd.storagerouter.name for sd in vpool.storagedrivers if sd not in available_storage_drivers]
        StorageRouterController._logger.info('StorageDriver {0} - StorageRouters on which an available StorageDriver runs: {1}'.format(storage_driver.guid, ', '.join(available_sr_names)))
        if unavailable_sr_names:
            StorageRouterController._logger.warning('StorageDriver {0} - StorageRouters on which a StorageDriver is unavailable: {1}'.format(storage_driver.guid, ', '.join(unavailable_sr_names)))

        # Remove stale vDisks
        voldrv_vdisks = [entry.object_id() for entry in vpool.objectregistry_client.get_all_registrations()]
        voldrv_vdisk_guids = VDiskList.get_in_volume_ids(voldrv_vdisks).guids
        for vdisk_guid in set(vpool.vdisks_guids).difference(set(voldrv_vdisk_guids)):
            StorageRouterController._logger.warning('vDisk with guid {0} does no longer exist on any StorageDriver linked to vPool {1}, deleting...'.format(vdisk_guid, vpool.name))
            VDiskController.clean_vdisk_from_model(vdisk=VDisk(vdisk_guid))

        # Un-configure or reconfigure the MDSes
        StorageRouterController._logger.info('StorageDriver {0} - Reconfiguring MDSes'.format(storage_driver.guid))
        vdisks = []
        for mds in mds_services_to_remove:
            for junction in mds.vdisks:
                vdisk = junction.vdisk
                if vdisk in vdisks:
                    continue
                vdisks.append(vdisk)
                vdisk.invalidate_dynamics(['info', 'storagedriver_id'])
                if vdisk.storagedriver_id:
                    try:
                        StorageRouterController._logger.debug('StorageDriver {0} - vDisk {1} {2} - Ensuring MDS safety'.format(storage_driver.guid, vdisk.guid, vdisk.name))
                        MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid,
                                                           excluded_storagerouter_guids=[storage_router.guid] + offline_storage_router_guids)
                    except Exception:
                        StorageRouterController._logger.exception('StorageDriver {0} - vDisk {1} {2} - Ensuring MDS safety failed'.format(storage_driver.guid, vdisk.guid, vdisk.name))

        # Validate that all MDSes on current StorageRouter have been moved away
        # Ensure safety does not always throw an error, that's why we perform this check here instead of in the Exception clause of above code
        vdisks = []
        for mds in mds_services_to_remove:
            for junction in mds.vdisks:
                vdisk = junction.vdisk
                if vdisk in vdisks:
                    continue
                vdisks.append(vdisk)
                StorageRouterController._logger.critical('StorageDriver {0} - vDisk {1} {2} - MDS Services have not been migrated away'.format(storage_driver.guid, vdisk.guid, vdisk.name))
        if len(vdisks) > 0:
            # Put back in RUNNING, so it can be used again. Errors keep on displaying in GUI now anyway
            vpool.status = VPool.STATUSES.RUNNING
            vpool.save()
            raise RuntimeError('Not all MDS Services have been successfully migrated away')

        # Disable and stop DTL, voldrv and albaproxy services
        service_manager = ServiceFactory.get_manager()
        if storage_router_online is True:
            dtl_service = 'dtl_{0}'.format(vpool.name)
            voldrv_service = 'volumedriver_{0}'.format(vpool.name)
            client = SSHClient(storage_router, username='root')

            for service in [voldrv_service, dtl_service]:
                try:
                    if service_manager.has_service(service, client=client):
                        StorageRouterController._logger.debug('StorageDriver {0} - Stopping service {1}'.format(storage_driver.guid, service))
                        service_manager.stop_service(service, client=client)
                        StorageRouterController._logger.debug('StorageDriver {0} - Removing service {1}'.format(storage_driver.guid, service))
                        service_manager.remove_service(service, client=client)
                except Exception:
                    StorageRouterController._logger.exception('StorageDriver {0} - Disabling/stopping service {1} failed'.format(storage_driver.guid, service))
                    errors_found = True

            sd_config_key = '/ovs/vpools/{0}/hosts/{1}/config'.format(vpool.guid, storage_driver.storagedriver_id)
            if storage_drivers_left is False and Configuration.exists(sd_config_key):
                try:
                    for proxy in storage_driver.alba_proxies:
                        if service_manager.has_service(proxy.service.name, client=client):
                            StorageRouterController._logger.debug('StorageDriver {0} - Starting proxy {1}'.format(storage_driver.guid, proxy.service.name))
                            service_manager.start_service(proxy.service.name, client=client)
                            tries = 10
                            running = False
                            port = proxy.service.ports[0]
                            while running is False and tries > 0:
                                StorageRouterController._logger.debug('StorageDriver {0} - Waiting for the proxy {1} to start up'.format(storage_driver.guid, proxy.service.name))
                                tries -= 1
                                time.sleep(10 - tries)
                                try:
                                    client.run(['alba', 'proxy-statistics', '--host', storage_driver.storage_ip, '--port', str(port)])
                                    running = True
                                except CalledProcessError as ex:
                                    StorageRouterController._logger.error('StorageDriver {0} - Fetching alba proxy-statistics failed with error (but ignoring): {1}'.format(storage_driver.guid, ex))
                            if running is False:
                                raise RuntimeError('Alba proxy {0} failed to start'.format(proxy.service.name))
                            StorageRouterController._logger.debug('StorageDriver {0} - Alba proxy {0} running'.format(storage_driver.guid, proxy.service.name))

                    StorageRouterController._logger.debug('StorageDriver {0} - Destroying filesystem and erasing node configs'.format(storage_driver.guid))
                    with remote(client.ip, [LocalStorageRouterClient], username='root') as rem:
                        path = Configuration.get_configuration_path(sd_config_key)
                        storagedriver_client = rem.LocalStorageRouterClient(path)
                        try:
                            storagedriver_client.destroy_filesystem()
                        except RuntimeError as rte:
                            # If backend has already been deleted, we cannot delete the filesystem anymore --> storage leak!!!
                            if 'MasterLookupResult.Error' not in rte.message:
                                raise

                    # noinspection PyArgumentList
                    vpool.clusterregistry_client.erase_node_configs()
                except RuntimeError:
                    StorageRouterController._logger.exception('StorageDriver {0} - Destroying filesystem and erasing node configs failed'.format(storage_driver.guid))
                    errors_found = True

            for proxy in storage_driver.alba_proxies:
                service_name = proxy.service.name
                try:
                    if service_manager.has_service(service_name, client=client):
                        StorageRouterController._logger.debug('StorageDriver {0} - Stopping service {1}'.format(storage_driver.guid, service_name))
                        service_manager.stop_service(service_name, client=client)
                        StorageRouterController._logger.debug('StorageDriver {0} - Removing service {1}'.format(storage_driver.guid, service_name))
                        service_manager.remove_service(service_name, client=client)
                except Exception:
                    StorageRouterController._logger.exception('StorageDriver {0} - Disabling/stopping service {1} failed'.format(storage_driver.guid, service_name))
                    errors_found = True

        # Reconfigure cluster node configs
        if storage_drivers_left is True:
            try:
                StorageRouterController._logger.info('StorageDriver {0} - Reconfiguring cluster node configs'.format(storage_driver.guid))
                node_configs = []
                for sd in vpool.storagedrivers:
                    if sd != storage_driver:
                        sd.invalidate_dynamics(['cluster_node_config'])
                        config = sd.cluster_node_config
                        if storage_driver.storagedriver_id in config['node_distance_map']:
                            del config['node_distance_map'][storage_driver.storagedriver_id]
                        node_configs.append(ClusterNodeConfig(**config))
                StorageRouterController._logger.debug('StorageDriver {0} - Node configs - \n{1}'.format(storage_driver.guid, '\n'.join([str(config) for config in node_configs])))
                vpool.clusterregistry_client.set_node_configs(node_configs)
                for sd in available_storage_drivers:
                    if sd != storage_driver:
                        StorageRouterController._logger.debug('StorageDriver {0} - StorageDriver {1} {2} - Updating cluster node configs'.format(storage_driver.guid, sd.guid, sd.name))
                        vpool.storagedriver_client.update_cluster_node_configs(str(sd.storagedriver_id), req_timeout_secs=10)
            except Exception:
                StorageRouterController._logger.exception('StorageDriver {0} - Reconfiguring cluster node configs failed'.format(storage_driver.guid))
                errors_found = True

        # Removing MDS services
        StorageRouterController._logger.info('StorageDriver {0} - Removing MDS services'.format(storage_driver.guid))
        for mds_service in mds_services_to_remove:
            # All MDSServiceVDisk object should have been deleted above
            try:
                StorageRouterController._logger.debug('StorageDriver {0} - Remove MDS service (number {1}) for StorageRouter with IP {2}'.format(storage_driver.guid, mds_service.number, storage_router.ip))
                MDSServiceController.remove_mds_service(mds_service=mds_service,
                                                        reconfigure=False,
                                                        allow_offline=not storage_router_online)
            except Exception:
                StorageRouterController._logger.exception('StorageDriver {0} - Removing MDS service failed'.format(storage_driver.guid))
                errors_found = True

        # Clean up directories and files
        dirs_to_remove = [storage_driver.mountpoint]
        for sd_partition in storage_driver.partitions[:]:
            dirs_to_remove.append(sd_partition.path)
            sd_partition.delete()

        for proxy in storage_driver.alba_proxies:
            config_tree = '/ovs/vpools/{0}/proxies/{1}'.format(vpool.guid, proxy.guid)
            Configuration.delete(config_tree)

        if storage_router_online is True:
            # Cleanup directories/files
            StorageRouterController._logger.info('StorageDriver {0} - Deleting vPool related directories and files'.format(storage_driver.guid))
            try:
                mountpoints = StorageRouterController._get_mountpoints(client)
                for dir_name in dirs_to_remove:
                    if dir_name and client.dir_exists(dir_name) and dir_name not in mountpoints and dir_name != '/':
                        client.dir_delete(dir_name)
            except Exception:
                StorageRouterController._logger.exception('StorageDriver {0} - Failed to retrieve mount point information or delete directories'.format(storage_driver.guid))
                StorageRouterController._logger.warning('StorageDriver {0} - Following directories should be checked why deletion was prevented: {1}'.format(storage_driver.guid, ', '.join(dirs_to_remove)))
                errors_found = True

            StorageRouterController._logger.debug('StorageDriver {0} - Synchronizing disks with reality'.format(storage_driver.guid))
            try:
                DiskController.sync_with_reality(storage_router.guid)
            except Exception:
                StorageRouterController._logger.exception('StorageDriver {0} - Synchronizing disks with reality failed'.format(storage_driver.guid))
                errors_found = True

        Configuration.delete('/ovs/vpools/{0}/hosts/{1}'.format(vpool.guid, storage_driver.storagedriver_id))

        # Model cleanup
        StorageRouterController._logger.info('StorageDriver {0} - Cleaning up model'.format(storage_driver.guid))
        for proxy in storage_driver.alba_proxies:
            StorageRouterController._logger.debug('StorageDriver {0} - Removing alba proxy service {1} from model'.format(storage_driver.guid, proxy.service.name))
            service = proxy.service
            proxy.delete()
            service.delete()

        sd_can_be_deleted = True
        if storage_drivers_left is False:
            for relation in ['mds_services', 'storagedrivers', 'vdisks']:
                expected_amount = 1 if relation == 'storagedrivers' else 0
                if len(getattr(vpool, relation)) > expected_amount:
                    sd_can_be_deleted = False
                    break
        else:
            metadata_key = 'backend_aa_{0}'.format(storage_router.guid)
            if metadata_key in vpool.metadata:
                vpool.metadata.pop(metadata_key)
                vpool.save()
            metadata_key = 'backend_bc_{0}'.format(storage_router.guid)
            if metadata_key in vpool.metadata:
                vpool.metadata.pop(metadata_key)
                vpool.save()
            StorageRouterController._logger.debug('StorageDriver {0} - Checking DTL for all vDisks in vPool {1} with guid {2}'.format(storage_driver.guid, vpool.name, vpool.guid))
            try:
                VDiskController.dtl_checkup(vpool_guid=vpool.guid, ensure_single_timeout=600)
            except Exception:
                StorageRouterController._logger.exception('StorageDriver {0} - DTL checkup failed for vPool {1} with guid {2}'.format(storage_driver.guid, vpool.name, vpool.guid))

        if sd_can_be_deleted is True:
            storage_driver.delete()
            if storage_drivers_left is False:
                StorageRouterController._logger.info('StorageDriver {0} - Removing vPool from model'.format(storage_driver.guid))
                vpool.delete()
                Configuration.delete('/ovs/vpools/{0}'.format(vpool.guid))
        else:
            try:
                vpool.delete()  # Try to delete the vPool to invoke a proper stacktrace to see why it can't be deleted
            except Exception:
                errors_found = True
                StorageRouterController._logger.exception('StorageDriver {0} - Cleaning up vpool from the model failed'.format(storage_driver.guid))

        StorageRouterController._logger.info('StorageDriver {0} - Running MDS checkup'.format(storage_driver.guid))
        try:
            MDSServiceController.mds_checkup()
        except Exception:
            StorageRouterController._logger.exception('StorageDriver {0} - MDS checkup failed'.format(storage_driver.guid))

        if errors_found is True:
            if storage_drivers_left is True:
                vpool.status = VPool.STATUSES.FAILURE
                vpool.save()
            raise RuntimeError('1 or more errors occurred while trying to remove the StorageDriver. Please check the logs for more information')
        if storage_drivers_left is True:
            vpool.status = VPool.STATUSES.RUNNING
            vpool.save()
        StorageRouterController._logger.info('StorageDriver {0} - Deleted StorageDriver {1}'.format(storage_driver.guid, storage_driver.name))

    @staticmethod
    @ovs_task(name='ovs.storagerouter.get_version_info')
    def get_version_info(storagerouter_guid):
        """
        Returns version information regarding a given StorageRouter
        :param storagerouter_guid: StorageRouter guid to get version information for
        :type storagerouter_guid: str
        :return: Version information
        :rtype: dict
        """
        package_manager = PackageFactory.get_manager()
        client = SSHClient(StorageRouter(storagerouter_guid))
        return {'storagerouter_guid': storagerouter_guid,
                'versions': dict((pkg_name, str(version)) for pkg_name, version in package_manager.get_installed_versions(client).iteritems())}

    @staticmethod
    @ovs_task(name='ovs.storagerouter.get_support_info')
    def get_support_info():
        """
        Returns support information for the entire cluster
        :return: Support information
        :rtype: dict
        """
        celery_scheduling = Configuration.get(key='/ovs/framework/scheduling/celery', default={})
        stats_monkey_disabled = 'ovs.stats_monkey.run_all' in celery_scheduling and celery_scheduling['ovs.stats_monkey.run_all'] is None
        stats_monkey_disabled &= 'alba.stats_monkey.run_all' in celery_scheduling and celery_scheduling['alba.stats_monkey.run_all'] is None
        return {'cluster_id': Configuration.get(key='/ovs/framework/cluster_id'),
                'stats_monkey': not stats_monkey_disabled,
                'support_agent': Configuration.get(key='/ovs/framework/support|support_agent'),
                'remote_access': Configuration.get(key='ovs/framework/support|remote_access'),
                'stats_monkey_config': Configuration.get(key='ovs/framework/monitoring/stats_monkey', default={})}

    @staticmethod
    @ovs_task(name='ovs.storagerouter.get_support_metadata')
    def get_support_metadata():
        """
        Returns support metadata for a given storagerouter. This should be a routed task!
        :return: Metadata of the StorageRouter
        :rtype: dict
        """
        return SupportAgent().get_heartbeat_data()

    @staticmethod
    @ovs_task(name='ovs.storagerouter.get_logfiles')
    def get_logfiles(local_storagerouter_guid):
        """
        Collects logs, moves them to a web-accessible location and returns log tgz's filename
        :param local_storagerouter_guid: StorageRouter guid to retrieve log files on
        :type local_storagerouter_guid: str
        :return: Name of tgz containing the logs
        :rtype: str
        """
        this_storagerouter = System.get_my_storagerouter()
        this_client = SSHClient(this_storagerouter, username='root')
        logfile = this_client.run(['ovs', 'collect', 'logs']).strip()
        logfilename = logfile.split('/')[-1]

        storagerouter = StorageRouter(local_storagerouter_guid)
        webpath = '/opt/OpenvStorage/webapps/frontend/downloads'
        client = SSHClient(storagerouter, username='root')
        client.dir_create(webpath)
        client.file_upload('{0}/{1}'.format(webpath, logfilename), logfile)
        client.run(['chmod', '666', '{0}/{1}'.format(webpath, logfilename)])
        return logfilename

    @staticmethod
    @ovs_task(name='ovs.storagerouter.get_proxy_config')
    def get_proxy_config(vpool_guid, storagerouter_guid):
        """
        Gets the ALBA proxy for a given StorageRouter and vPool
        :param storagerouter_guid: Guid of the StorageRouter on which the ALBA proxy is configured
        :type storagerouter_guid: str
        :param vpool_guid: Guid of the vPool for which the proxy is configured
        :type vpool_guid: str
        :return: The ALBA proxy configuration
        :rtype: dict
        """
        vpool = VPool(vpool_guid)
        storagerouter = StorageRouter(storagerouter_guid)
        for sd in vpool.storagedrivers:
            if sd.storagerouter_guid == storagerouter.guid:
                if len(sd.alba_proxies) == 0:
                    raise ValueError('No ALBA proxies configured for vPool {0} on StorageRouter {1}'.format(vpool.name, storagerouter.name))
                return Configuration.get('/ovs/vpools/{0}/proxies/{1}/config/main'.format(vpool.guid, sd.alba_proxies[0].guid))
        raise ValueError('vPool {0} has not been extended to StorageRouter {1}'.format(vpool.name, storagerouter.name))

    @staticmethod
    @ovs_task(name='ovs.storagerouter.configure_support')
    def configure_support(support_info):
        """
        Configures support on all StorageRouters
        :param support_info: Information about which components should be configured
            {'stats_monkey': True,  # Enable/disable the stats monkey scheduled task
             'support_agent': True,  # Responsible for enabling the ovs-support-agent service, which collects heart beat data
             'remote_access': False,  # Cannot be True when support agent is False. Is responsible for opening an OpenVPN tunnel to allow for remote access
             'stats_monkey_config': {}}  # Dict with information on how to configure the stats monkey (Only required when enabling the stats monkey
        :type support_info: dict
        :return: None
        :rtype: NoneType
        """
        Toolbox.verify_required_params(actual_params=support_info,
                                       required_params={'stats_monkey': (bool, None, False),
                                                        'remote_access': (bool, None, False),
                                                        'support_agent': (bool, None, False),
                                                        'stats_monkey_config': (dict, None, False)})
        # All settings are optional, so if nothing is specified, no need to change anything
        if len(support_info) == 0:
            StorageRouterController._logger.warning('Configure support called without any specific settings. Doing nothing')
            return

        # Collect information
        support_agent_key = '/ovs/framework/support|support_agent'
        support_agent_new = support_info.get('support_agent')
        support_agent_old = Configuration.get(key=support_agent_key)
        support_agent_change = support_agent_new is not None and support_agent_old != support_agent_new

        remote_access_key = '/ovs/framework/support|remote_access'
        remote_access_new = support_info.get('remote_access')
        remote_access_old = Configuration.get(key=remote_access_key)
        remote_access_change = remote_access_new is not None and remote_access_old != remote_access_new

        stats_monkey_celery_key = '/ovs/framework/scheduling/celery'
        stats_monkey_config_key = '/ovs/framework/monitoring/stats_monkey'
        stats_monkey_new_config = support_info.get('stats_monkey_config')
        stats_monkey_old_config = Configuration.get(key=stats_monkey_config_key, default={})
        stats_monkey_celery_config = Configuration.get(key=stats_monkey_celery_key, default={})
        stats_monkey_new = support_info.get('stats_monkey')
        stats_monkey_old = stats_monkey_celery_config.get('ovs.stats_monkey.run_all') is not None or stats_monkey_celery_config.get('alba.stats_monkey.run_all') is not None
        stats_monkey_change = stats_monkey_new is not None and (stats_monkey_old != stats_monkey_new or stats_monkey_new_config != stats_monkey_old_config)

        # Make sure support agent is enabled when trying to enable remote access
        if remote_access_new is True:
            if support_agent_new is False or (support_agent_new is None and support_agent_old is False):
                raise RuntimeError('Remote access cannot be enabled without the heart beat enabled')

        # Collect root_client information
        root_clients = {}
        for storagerouter in StorageRouterList.get_storagerouters():
            try:
                root_clients[storagerouter] = SSHClient(endpoint=storagerouter, username='root')
            except UnableToConnectException:
                raise RuntimeError('Not all StorageRouters are reachable')

        if stats_monkey_new is True:
            Toolbox.verify_required_params(actual_params=stats_monkey_new_config,
                                           required_params={'host': (str, Toolbox.regex_ip),
                                                            'port': (int, {'min': 1, 'max': 65535}),
                                                            'database': (str, None),
                                                            'interval': (int, {'min': 1, 'max': 86400}),
                                                            'password': (str, None),
                                                            'transport': (str, ['influxdb', 'redis']),
                                                            'environment': (str, None)})
            if stats_monkey_new_config['transport'] == 'influxdb':
                Toolbox.verify_required_params(actual_params=stats_monkey_new_config, required_params={'username': (str, None)})

        # Configure remote access
        service_manager = ServiceFactory.get_manager()
        if remote_access_change is True:
            Configuration.set(key=remote_access_key, value=remote_access_new)
            cid = Configuration.get('/ovs/framework/cluster_id').replace(r"'", r"'\''")
            for storagerouter, root_client in root_clients.iteritems():
                if remote_access_new is False:
                    StorageRouterController._logger.info('Un-configuring remote access on StorageRouter {0}'.format(root_client.ip))
                    nid = storagerouter.machine_id.replace(r"'", r"'\''")
                    service_name = 'openvpn@ovs_{0}-{1}'.format(cid, nid)
                    if service_manager.has_service(name=service_name, client=root_client):
                        service_manager.stop_service(name=service_name, client=root_client)
                    root_client.file_delete(filenames=['/etc/openvpn/ovs_*'])

        # Configure support agent
        if support_agent_change is True:
            service_name = 'support-agent'
            Configuration.set(key=support_agent_key, value=support_agent_new)
            for root_client in root_clients.itervalues():
                if support_agent_new is True:
                    StorageRouterController._logger.info('Configuring support agent on StorageRouter {0}'.format(root_client.ip))
                    if service_manager.has_service(name=service_name, client=root_client) is False:
                        service_manager.add_service(name=service_name, client=root_client)
                    service_manager.restart_service(name=service_name, client=root_client)
                else:
                    StorageRouterController._logger.info('Un-configuring support agent on StorageRouter {0}'.format(root_client.ip))
                    if service_manager.has_service(name=service_name, client=root_client):
                        service_manager.stop_service(name=service_name, client=root_client)
                        service_manager.remove_service(name=service_name, client=root_client)

        # Configure stats monkey
        if stats_monkey_change is True:
            # 2 keys matter here:
            #    - /ovs/framework/scheduling/celery --> used to check whether the stats monkey is disabled or not
            #    - /ovs/framework/monitoring/stats_monkey --> contains the actual configuration parameters when enabling the stats monkey, such as host, port, username, ...
            service_name = 'scheduled-tasks'
            if stats_monkey_new is True:  # Enable the scheduled task by removing the key
                StorageRouterController._logger.info('Configuring stats monkey')
                interval = stats_monkey_new_config['interval']
                # The scheduled task cannot be configured to run more than once a minute, so for intervals < 60, the stats monkey task handles this itself
                StorageRouterController._logger.debug('Requested interval to run at: {0}'.format(interval))
                Configuration.set(key=stats_monkey_config_key, value=stats_monkey_new_config)
                if interval > 60:
                    days, hours, minutes, _ = ExtensionsToolbox.convert_to_days_hours_minutes_seconds(seconds=interval)
                    if days == 1:  # Max interval is 24 * 60 * 60, so once every day at 3 AM
                        schedule = {'hour': '3'}
                    elif hours > 0:
                        schedule = {'hour': '*/{0}'.format(hours)}
                    else:
                        schedule = {'minute': '*/{0}'.format(minutes)}
                    stats_monkey_celery_config['ovs.stats_monkey.run_all'] = schedule
                    stats_monkey_celery_config['alba.stats_monkey.run_all'] = schedule
                    StorageRouterController._logger.debug('Configured schedule is: {0}'.format(schedule))
                else:
                    stats_monkey_celery_config.pop('ovs.stats_monkey.run_all', None)
                    stats_monkey_celery_config.pop('alba.stats_monkey.run_all', None)
            else:  # Disable the scheduled task by setting the values for the celery tasks to None
                StorageRouterController._logger.info('Un-configuring stats monkey')
                stats_monkey_celery_config['ovs.stats_monkey.run_all'] = None
                stats_monkey_celery_config['alba.stats_monkey.run_all'] = None

            Configuration.set(key=stats_monkey_celery_key, value=stats_monkey_celery_config)
            for root_client in root_clients.itervalues():
                StorageRouterController._logger.debug('Restarting ovs-scheduled-tasks service on node with IP {0}'.format(root_client.ip))
                service_manager.restart_service(name=service_name, client=root_client)

    @staticmethod
    @ovs_task(name='ovs.storagerouter.mountpoint_exists')
    def mountpoint_exists(name, storagerouter_guid):
        """
        Checks whether a given mount point for a vPool exists
        :param name: Name of the mount point to check
        :type name: str
        :param storagerouter_guid: Guid of the StorageRouter on which to check for mount point existence
        :type storagerouter_guid: str
        :return: True if mount point not in use else False
        :rtype: bool
        """
        client = SSHClient(StorageRouter(storagerouter_guid))
        return client.dir_exists(directory='/mnt/{0}'.format(name))

    @staticmethod
    @ovs_task(name='ovs.storagerouter.refresh_hardware')
    def refresh_hardware(storagerouter_guid):
        """
        Refreshes all hardware related information
        :param storagerouter_guid: Guid of the StorageRouter to refresh the hardware on
        :type storagerouter_guid: str
        :return: None
        :rtype: NoneType
        """
        StorageRouterController.set_rdma_capability(storagerouter_guid)
        DiskController.sync_with_reality(storagerouter_guid)

    @staticmethod
    def set_rdma_capability(storagerouter_guid):
        """
        Check if the StorageRouter has been reconfigured to be able to support RDMA
        :param storagerouter_guid: Guid of the StorageRouter to check and set
        :type storagerouter_guid: str
        :return: None
        :rtype: NoneType
        """
        storagerouter = StorageRouter(storagerouter_guid)
        client = SSHClient(storagerouter, username='root')
        rdma_capable = False
        with remote(client.ip, [os], username='root') as rem:
            for root, dirs, files in rem.os.walk('/sys/class/infiniband'):
                for directory in dirs:
                    ports_dir = '/'.join([root, directory, 'ports'])
                    if not rem.os.path.exists(ports_dir):
                        continue
                    for sub_root, sub_dirs, _ in rem.os.walk(ports_dir):
                        if sub_root != ports_dir:
                            continue
                        for sub_directory in sub_dirs:
                            state_file = '/'.join([sub_root, sub_directory, 'state'])
                            if rem.os.path.exists(state_file):
                                if 'ACTIVE' in client.run(['cat', state_file]):
                                    rdma_capable = True
        storagerouter.rdma_capable = rdma_capable
        storagerouter.save()

    @staticmethod
    @ovs_task(name='ovs.storagerouter.configure_disk', ensure_single_info={'mode': 'CHAINED', 'global_timeout': 1800})
    def configure_disk(storagerouter_guid, disk_guid, partition_guid, offset, size, roles):
        """
        Configures a partition
        :param storagerouter_guid: Guid of the StorageRouter to configure a disk on
        :type storagerouter_guid: str
        :param disk_guid: Guid of the disk to configure
        :type disk_guid: str
        :param partition_guid: Guid of the partition on the disk
        :type partition_guid: str
        :param offset: Offset for the partition
        :type offset: int
        :param size: Size of the partition
        :type size: int
        :param roles: Roles assigned to the partition
        :type roles: list
        :return: None
        :rtype: NoneType
        """
        # Validations
        storagerouter = StorageRouter(storagerouter_guid)
        for role in roles:
            if role not in DiskPartition.ROLES or role == DiskPartition.ROLES.BACKEND:
                raise RuntimeError('Invalid role specified: {0}'.format(role))
        disk = Disk(disk_guid)
        if disk.storagerouter_guid != storagerouter_guid:
            raise RuntimeError('The given Disk is not on the given StorageRouter')
        for partition in disk.partitions:
            if DiskPartition.ROLES.BACKEND in partition.roles:
                raise RuntimeError('The given Disk is in use by a Backend')

        # Create partition
        if partition_guid is None:
            StorageRouterController._logger.debug('Creating new partition - Offset: {0} bytes - Size: {1} bytes - Roles: {2}'.format(offset, size, roles))
            with remote(storagerouter.ip, [DiskTools], username='root') as rem:
                if len(disk.aliases) == 0:
                    raise ValueError('Disk {0} does not have any aliases'.format(disk.name))
                rem.DiskTools.create_partition(disk_alias=disk.aliases[0],
                                               disk_size=disk.size,
                                               partition_start=offset,
                                               partition_size=size)
            DiskController.sync_with_reality(storagerouter_guid)
            disk = Disk(disk_guid)
            end_point = offset + size
            partition = None
            for part in disk.partitions:
                if offset < part.offset + part.size and end_point > part.offset:
                    partition = part
                    break

            if partition is None:
                raise RuntimeError('No new partition detected on disk {0} after having created 1'.format(disk.name))
            StorageRouterController._logger.debug('Partition created')
        else:
            StorageRouterController._logger.debug('Using existing partition')
            partition = DiskPartition(partition_guid)
            if partition.disk_guid != disk_guid:
                raise RuntimeError('The given DiskPartition is not on the given Disk')
            if partition.filesystem in ['swap', 'linux_raid_member', 'LVM2_member']:
                raise RuntimeError("It is not allowed to assign roles on partitions of type: ['swap', 'linux_raid_member', 'LVM2_member']")
            metadata = StorageRouterController.get_metadata(storagerouter_guid)
            partition_info = metadata['partitions']
            removed_roles = set(partition.roles) - set(roles)
            used_roles = []
            for role in removed_roles:
                for info in partition_info[role]:
                    if info['in_use'] and info['guid'] == partition.guid:
                        used_roles.append(role)
            if len(used_roles) > 0:
                raise RuntimeError('Roles in use cannot be removed. Used roles: {0}'.format(', '.join(used_roles)))

        # Add filesystem
        if partition.filesystem is None or partition_guid is None:
            StorageRouterController._logger.debug('Creating filesystem')
            if len(partition.aliases) == 0:
                raise ValueError('Partition with offset {0} does not have any aliases'.format(partition.offset))
            with remote(storagerouter.ip, [DiskTools], username='root') as rem:
                rem.DiskTools.make_fs(partition_alias=partition.aliases[0])
            DiskController.sync_with_reality(storagerouter_guid)
            partition = DiskPartition(partition.guid)
            if partition.filesystem not in ['ext4', 'xfs']:
                raise RuntimeError('Unexpected filesystem')
            StorageRouterController._logger.debug('Filesystem created')

        # Mount the partition and add to FSTab
        if partition.mountpoint is None:
            StorageRouterController._logger.debug('Configuring mount point')
            with remote(storagerouter.ip, [DiskTools], username='root') as rem:
                counter = 1
                mountpoint = '/mnt/{0}{1}'.format('ssd' if disk.is_ssd else 'hdd', counter)
                while True:
                    if not rem.DiskTools.mountpoint_exists(mountpoint):
                        break
                    counter += 1
                    mountpoint = '/mnt/{0}{1}'.format('ssd' if disk.is_ssd else 'hdd', counter)
                StorageRouterController._logger.debug('Found mount point: {0}'.format(mountpoint))
                rem.DiskTools.add_fstab(partition_aliases=partition.aliases,
                                        mountpoint=mountpoint,
                                        filesystem=partition.filesystem)
                rem.DiskTools.mount(mountpoint)
            DiskController.sync_with_reality(storagerouter_guid)
            partition = DiskPartition(partition.guid)
            if partition.mountpoint != mountpoint:
                raise RuntimeError('Unexpected mount point')
            StorageRouterController._logger.debug('Mount point configured')
        partition.roles = roles
        partition.save()
        StorageRouterController._logger.debug('Partition configured')

    @staticmethod
    def _check_scrub_partition_present():
        """
        Checks whether at least 1 scrub partition is present on any StorageRouter
        :return: True if at least 1 SCRUB role present in the cluster else False
        :rtype: bool
        """
        for storage_router in StorageRouterList.get_storagerouters():
            for disk in storage_router.disks:
                for partition in disk.partitions:
                    if DiskPartition.ROLES.SCRUB in partition.roles:
                        return True
        return False

    @staticmethod
    def _get_mountpoints(client):
        """
        Retrieve the mount points
        :param client: SSHClient to retrieve the mount points on
        :return: List of mount points
        :rtype: list[str]
        """
        mountpoints = []
        for mountpoint in client.run(['mount', '-v']).strip().splitlines():
            mp = mountpoint.split(' ')[2] if len(mountpoint.split(' ')) > 2 else None
            if mp and not mp.startswith('/dev') and not mp.startswith('/proc') and not mp.startswith('/sys') and not mp.startswith('/run') and not mp.startswith('/mnt/alba-asd') and mp != '/':
                mountpoints.append(mp)
        return mountpoints

    @staticmethod
    def _retrieve_alba_arakoon_config(alba_backend_guid, ovs_client):
        """
        Retrieve the ALBA Arakoon configuration
        :param alba_backend_guid: Guid of the ALBA Backend
        :type alba_backend_guid: str
        :param ovs_client: OVS client object
        :type ovs_client: OVSClient
        :return: Arakoon configuration information
        :rtype: dict
        """
        task_id = ovs_client.get('/alba/backends/{0}/get_config_metadata'.format(alba_backend_guid))
        successful, arakoon_config = ovs_client.wait_for_task(task_id, timeout=300)
        if successful is False:
            raise RuntimeError('Could not load metadata from environment {0}'.format(ovs_client.ip))
        return arakoon_config

    @staticmethod
    def _revert_vpool_status(vpool, status=VPool.STATUSES.RUNNING, storagedriver=None, client=None, dirs_created=list()):
        """
        Remove the vPool being created or revert the vPool being extended
        :return: None
        :rtype: NoneType
        """
        vpool.status = status
        vpool.save()

        if status == VPool.STATUSES.RUNNING:
            if len(dirs_created) > 0:
                try:
                    client.dir_delete(directories=dirs_created)
                except Exception:
                    StorageRouterController._logger.warning('Failed to clean up following directories: {0}'.format(', '.join(dirs_created)))

            if storagedriver is not None:
                for sdp in storagedriver.partitions:
                    sdp.delete()
                for proxy in storagedriver.alba_proxies:
                    proxy.delete()
                storagedriver.delete()
            if len(vpool.storagedrivers) == 0:
                vpool.delete()
                if Configuration.dir_exists(key='/ovs/vpools/{0}'.format(vpool.guid)):
                    Configuration.delete(key='/ovs/vpools/{0}'.format(vpool.guid))
