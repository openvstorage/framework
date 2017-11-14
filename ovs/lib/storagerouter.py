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
import json
import time
from subprocess import CalledProcessError
from ovs.dal.hybrids.disk import Disk
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.j_albaproxy import AlbaProxy
from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
from ovs.dal.hybrids.service import Service as DalService
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.vpoollist import VPoolList
from ovs_extensions.api.client import OVSClient
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig, ArakoonInstaller
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
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.storagedriver import ClusterNodeConfig, LocalStorageRouterClient, LOG_LEVEL_MAPPING, StorageDriverConfiguration, StorageDriverClient
from ovs.extensions.support.agent import SupportAgent
from ovs.lib.disk import DiskController
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.storagedriver import StorageDriverController
from ovs.lib.vdisk import VDiskController
from volumedriver.storagerouter import storagerouterclient


class StorageRouterController(object):
    """
    Contains all BLL related to StorageRouter
    """
    _logger = Logger('lib')
    _log_level = LOG_LEVEL_MAPPING[_logger.getEffectiveLevel()]
    _service_manager = ServiceFactory.get_manager()

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
        Gets physical information about the machine this task is running on
        :param storagerouter_guid: StorageRouter guid to retrieve the metadata for
        :type storagerouter_guid: str
        :return: Metadata information about the StorageRouter
        :rtype: dict
        """
        storagerouter = StorageRouter(storagerouter_guid)
        client = SSHClient(storagerouter)
        services_mds = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.MD_SERVER).services
        services_arakoon = [service for service in ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON).services if service.name != 'arakoon-ovsdb' and service.is_internal is True]

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

        return {'partitions': partitions,
                'ipaddresses': OSFactory.get_manager().get_ip_addresses(client=client),
                'scrub_available': StorageRouterController._check_scrub_partition_present()}

    @classmethod
    @ovs_task(name='ovs.storagerouter.add_vpool')
    def add_vpool(cls, parameters):
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
                           'writecache_size': (int, {'min': 1, 'max': 10240}),
                           'config_params': (dict, {'dtl_mode': (str, StorageDriverClient.VPOOL_DTL_MODE_MAP.keys()),
                                                    'sco_size': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.keys()),
                                                    'cluster_size': (int, StorageDriverClient.CLUSTER_SIZES),
                                                    'write_buffer': (int, {'min': 128, 'max': 10240}),
                                                    'dtl_transport': (str, StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP.keys())}),
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
        unique_id = System.get_my_machine_id(client)

        sd_config_params = parameters['config_params']
        sco_size = sd_config_params['sco_size']
        write_buffer = sd_config_params['write_buffer']
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
        dirs2create = []
        root_client = ip_client_map[storagerouter.ip]['root']
        storagedriver = None
        partitions_mutex = volatile_mutex('add_vpool_partitions_{0}'.format(storagerouter.guid))
        try:
            partitions_mutex.acquire(wait=60)
            # Check mount point
            metadata = cls.get_metadata(storagerouter.guid)
            error_messages = []
            partition_info = metadata['partitions']
            if cls.mountpoint_exists(name=vpool_name, storagerouter_guid=storagerouter.guid):
                error_messages.append('The mount point for vPool {0} already exists'.format(vpool_name))

            # Check mount points are mounted
            for role, part_info in partition_info.iteritems():
                if role not in [DiskPartition.ROLES.DB, DiskPartition.ROLES.DTL, DiskPartition.ROLES.WRITE, DiskPartition.ROLES.SCRUB]:
                    continue
                for part in part_info:
                    if not client.is_mounted(part['mountpoint']) and part['mountpoint'] != DiskPartition.VIRTUAL_STORAGE_LOCATION:
                        error_messages.append('Mount point {0} is not mounted'.format(part['mountpoint']))

            # Check required roles
            if metadata['scrub_available'] is False:
                error_messages.append('At least 1 StorageRouter must have a partition with a {0} role'.format(DiskPartition.ROLES.SCRUB))

            required_roles = [DiskPartition.ROLES.DB, DiskPartition.ROLES.DTL, DiskPartition.ROLES.WRITE]
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
            usable_write_partitions = [part for part in partition_info[DiskPartition.ROLES.WRITE] if part['usable'] is True]
            writecache_size_available = sum(part['available'] for part in usable_write_partitions)
            writecache_size_requested = parameters['writecache_size'] * 1024 ** 3
            if writecache_size_requested > writecache_size_available:
                error_messages.append('Too much space requested for {0} cache. Available: {1:.2f} GiB, Requested: {2:.2f} GiB'.format(DiskPartition.ROLES.WRITE,
                                                                                                                                      writecache_size_available / 1024.0 ** 3,
                                                                                                                                      writecache_size_requested / 1024.0 ** 3))

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
            amount_of_proxies = parameters.get('parallelism', {}).get('proxies', 2)
            fragment_cache_on_read = parameters['fragment_cache_on_read']
            fragment_cache_on_write = parameters['fragment_cache_on_write']
            local_amount_of_proxies = 0
            largest_write_mountpoint = None
            if largest_ssd_write_partition is None and largest_sata_write_partition is None:
                error_messages.append('No WRITE partition found to put the local caches on')
            else:
                largest_write_mountpoint = DiskPartition(largest_ssd_write_partition or largest_sata_write_partition)
                if use_fragment_cache_backend is False:
                    if fragment_cache_on_read is True or fragment_cache_on_write is True:  # Local fragment caching
                        local_amount_of_proxies += amount_of_proxies
                if use_block_cache_backend is False:
                    if block_cache_on_read is True or block_cache_on_write is True:  # Local block caching
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
                        error_messages.append('Cache location is too small to deploy {0} prox{1}. {2}1GiB is required per proxy and with an available size of {3:.2f}GiB, {4} prox{5} can be deployed'.format(
                            amount_of_proxies,
                            'y' if amount_of_proxies == 1 else 'ies',
                            '2x ' if local_amount_of_proxies > amount_of_proxies else '',
                            available / 1024.0 ** 3,
                            maximum,
                            'y' if maximum == 1 else 'ies'
                        ))

            if error_messages:
                raise ValueError('Errors validating the specified parameters:\n - {0}'.format('\n - '.join(set(error_messages))))

            ############
            # MODELING #
            ############

            # Renew vPool metadata
            cls._logger.info('Add vPool {0} started'.format(vpool_name))
            if new_vpool is True:
                metadata_map = {'backend': {'backend_info': backend_info,
                                            'connection_info': connection_info}}
            else:
                metadata_map = copy.deepcopy(vpool.metadata)

            if use_fragment_cache_backend is True:
                metadata_map['backend_aa_{0}'.format(storagerouter.guid)] = {'backend_info': backend_info_fc,
                                                                             'connection_info': connection_info_fc}
            if use_block_cache_backend is True:
                metadata_map['backend_bc_{0}'.format(storagerouter.guid)] = {'backend_info': backend_info_bc,
                                                                             'connection_info': connection_info_bc}

            read_preferences = []
            for key, metadata in metadata_map.iteritems():
                ovs_client = OVSClient(ip=metadata['connection_info']['host'],
                                       port=metadata['connection_info']['port'],
                                       credentials=(metadata['connection_info']['client_id'], metadata['connection_info']['client_secret']),
                                       version=6,
                                       cache_store=VolatileFactory.get_client())
                preset_name = metadata['backend_info']['preset']
                alba_backend_guid = metadata['backend_info']['alba_backend_guid']
                arakoon_config = cls._retrieve_alba_arakoon_config(alba_backend_guid=alba_backend_guid, ovs_client=ovs_client)
                backend_dict = ovs_client.get('/alba/backends/{0}/'.format(alba_backend_guid), params={'contents': 'name,usages,presets,backend,remote_stack'})
                preset_info = dict((preset['name'], preset) for preset in backend_dict['presets'])
                if preset_name not in preset_info:
                    raise RuntimeError('Given preset {0} is not available in backend {1}'.format(preset_name, backend_dict['name']))

                # Calculate ALBA local read preference
                if backend_dict['scaling'] == 'GLOBAL' and metadata['connection_info']['local'] is True:
                    for node_id, value in backend_dict['remote_stack'].iteritems():
                        if value.get('domain') is not None and value['domain']['guid'] in storagerouter.regular_domains:
                            read_preferences.append(node_id)

                policies = []
                for policy_info in preset_info[preset_name]['policies']:
                    policy = json.loads('[{0}]'.format(policy_info.strip('()')))
                    policies.append([policy[0], policy[1]])

                if key in vpool.metadata:
                    vpool.metadata[key]['backend_info']['policies'] = policies
                    vpool.metadata[key]['arakoon_config'] = arakoon_config
                else:
                    vpool.metadata[key] = {'backend_info': {'name': backend_dict['name'],
                                                            'preset': preset_name,
                                                            'policies': policies,
                                                            'sco_size': sco_size * 1024.0 ** 2 if new_vpool is True else vpool.configuration['sco_size'] * 1024.0 ** 2,
                                                            'frag_size': float(preset_info[preset_name]['fragment_size']),
                                                            'total_size': float(backend_dict['usages']['size']),
                                                            'backend_guid': backend_dict['backend_guid'],
                                                            'alba_backend_guid': alba_backend_guid},
                                           'connection_info': metadata['connection_info'],
                                           'arakoon_config': arakoon_config}
            if 'caching_info' not in vpool.metadata['backend']:
                vpool.metadata['backend']['caching_info'] = {}
            vpool.metadata['backend']['caching_info'][storagerouter.guid] = {'fragment_cache_on_read': fragment_cache_on_read,
                                                                             'fragment_cache_on_write': fragment_cache_on_write,
                                                                             'block_cache_on_read': block_cache_on_read,
                                                                             'block_cache_on_write': block_cache_on_write}
            if 'cache_quota_fc' in parameters:
                vpool.metadata['backend']['caching_info'][storagerouter.guid]['quota_fc'] = parameters['cache_quota_fc']
            if 'cache_quota_bc' in parameters:
                vpool.metadata['backend']['caching_info'][storagerouter.guid]['quota_bc'] = parameters['cache_quota_bc']
            vpool.save()

            # StorageDriver
            machine_id = System.get_my_machine_id(client)
            port_range = Configuration.get('/ovs/framework/hosts/{0}/ports|storagedriver'.format(machine_id))
            with volatile_mutex('add_vpool_get_free_ports_{0}'.format(machine_id), wait=30):
                model_ports_in_use = []
                for sd in StorageDriverList.get_storagedrivers():
                    if sd.storagerouter_guid == storagerouter.guid:
                        model_ports_in_use += sd.ports.values()
                        for proxy in sd.alba_proxies:
                            model_ports_in_use.append(proxy.service.ports[0])

                ports = System.get_free_ports(port_range, model_ports_in_use, 4 + amount_of_proxies, client)

                vrouter_id = '{0}{1}'.format(vpool_name, unique_id)
                storagedriver = StorageDriver()
                storagedriver.name = vrouter_id.replace('_', ' ')
                storagedriver.ports = {'management': ports[0],
                                       'xmlrpc': ports[1],
                                       'dtl': ports[2],
                                       'edge': ports[3]}
                storagedriver.vpool = vpool
                storagedriver.cluster_ip = Configuration.get('/ovs/framework/hosts/{0}/ip'.format(unique_id))
                storagedriver.storage_ip = parameters['storage_ip']
                storagedriver.mountpoint = '/mnt/{0}'.format(vpool_name)
                storagedriver.description = storagedriver.name
                storagedriver.storagerouter = storagerouter
                storagedriver.storagedriver_id = vrouter_id
                storagedriver.save()

                # ALBA Proxies
                proxy_service_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ALBA_PROXY)
                for proxy_id in xrange(amount_of_proxies):
                    service = DalService()
                    service.storagerouter = storagerouter
                    service.ports = [ports[4 + proxy_id]]
                    service.name = 'albaproxy_{0}_{1}'.format(vpool_name, proxy_id)
                    service.type = proxy_service_type
                    service.save()
                    alba_proxy = AlbaProxy()
                    alba_proxy.service = service
                    alba_proxy.storagedriver = storagedriver
                    alba_proxy.save()

            # StorageDriver Partitions
            # * Information about backoff_gap and trigger_gap (Reason for 'smallest_write_partition' introduction)
            # * Once the free space on a mount point is < trigger_gap (default 1GiB), it will be cleaned up and the cleaner attempts to
            # * make sure that <backoff_gap> free space is available => backoff_gap must be <= size of the partition
            # * Both backoff_gap and trigger_gap apply to each mount point individually, but cannot be configured on a per mount point base

            # Assign WRITE / Fragment cache
            frag_size = None
            sdp_frags = []
            writecaches = []
            smallest_write_partition = None
            for writecache_info in usable_write_partitions:
                available = writecache_info['available']
                partition = DiskPartition(writecache_info['guid'])
                proportion = available * 100.0 / writecache_size_available
                size_to_be_used = proportion * writecache_size_requested / 100
                write_cache_percentage = 0.98
                if mountpoint_cache is not None and partition == mountpoint_cache:
                    if fragment_cache_on_read is True or fragment_cache_on_write is True or block_cache_on_read is True or block_cache_on_write is True:  # Only in this case we actually make use of the fragment caching
                        frag_size = int(size_to_be_used * 0.10)  # Bytes
                        write_cache_percentage = 0.88
                    for _ in xrange(amount_of_proxies):
                        sdp_frag = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                                      'role': DiskPartition.ROLES.WRITE,
                                                                                                      'sub_role': StorageDriverPartition.SUBROLE.FCACHE,
                                                                                                      'partition': partition})
                        dirs2create.append(sdp_frag.path)
                        for subfolder in ['fc', 'bc']:
                            dirs2create.append('{0}/{1}'.format(sdp_frag.path, subfolder))
                        sdp_frags.append(sdp_frag)

                w_size = int(size_to_be_used * write_cache_percentage / 1024 / 4096) * 4096
                # noinspection PyArgumentList
                sdp_write = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': long(size_to_be_used),
                                                                                               'role': DiskPartition.ROLES.WRITE,
                                                                                               'sub_role': StorageDriverPartition.SUBROLE.SCO,
                                                                                               'partition': partition})
                writecaches.append({'path': sdp_write.path,
                                    'size': '{0}KiB'.format(w_size)})
                dirs2create.append(sdp_write.path)
                if smallest_write_partition is None or (w_size * 1024) < smallest_write_partition:
                    smallest_write_partition = w_size * 1024

            sdp_fd = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                        'role': DiskPartition.ROLES.WRITE,
                                                                                        'sub_role': StorageDriverPartition.SUBROLE.FD,
                                                                                        'partition': largest_write_mountpoint})
            dirs2create.append(sdp_fd.path)

            # Assign DB
            db_info = partition_info[DiskPartition.ROLES.DB][0]
            sdp_tlogs = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                           'role': DiskPartition.ROLES.DB,
                                                                                           'sub_role': StorageDriverPartition.SUBROLE.TLOG,
                                                                                           'partition': DiskPartition(db_info['guid'])})
            sdp_metadata = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                              'role': DiskPartition.ROLES.DB,
                                                                                              'sub_role': StorageDriverPartition.SUBROLE.MD,
                                                                                              'partition': DiskPartition(db_info['guid'])})
            dirs2create.append(sdp_tlogs.path)
            dirs2create.append(sdp_metadata.path)

            # Assign DTL
            dtl_info = partition_info[DiskPartition.ROLES.DTL][0]
            sdp_dtl = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                         'role': DiskPartition.ROLES.DTL,
                                                                                         'partition': DiskPartition(dtl_info['guid'])})
            dirs2create.append(sdp_dtl.path)
            dirs2create.append(storagedriver.mountpoint)

            gap_configuration = StorageDriverController.generate_backoff_gap_settings(smallest_write_partition)

            if frag_size is None and ((use_fragment_cache_backend is False and (fragment_cache_on_read is True or fragment_cache_on_write is True))
                                      or (use_block_cache_backend is False and (block_cache_on_read is True or block_cache_on_write is True))):
                raise ValueError('Something went wrong trying to calculate the cache sizes')

            root_client.dir_create(dirs2create)
        except Exception:
            cls._logger.exception('Something went wrong during the validation or modeling of vPool {0} on StorageRouter {1}'.format(vpool.name, storagerouter.name))
            cls._revert_vpool_status(vpool=vpool, status=VPool.STATUSES.RUNNING, storagedriver=storagedriver, client=root_client, dirs_created=dirs2create)
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
                cls._logger.exception('Arakoon checkup for voldrv cluster failed')
                cls._revert_vpool_status(vpool=vpool, status=VPool.STATUSES.RUNNING, storagedriver=storagedriver, client=root_client, dirs_created=dirs2create)
                raise
            counter += 1
            time.sleep(1)
            if counter == 300:
                cls._logger.warning('Arakoon checkup for the StorageDriver cluster could not be started')
                cls._revert_vpool_status(vpool=vpool, status=VPool.STATUSES.RUNNING, storagedriver=storagedriver, client=root_client, dirs_created=dirs2create)
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
            cls._logger.exception('Updating cluster node configurations failed')
            if new_vpool is True:
                cls._revert_vpool_status(vpool=vpool, storagedriver=storagedriver, client=root_client, dirs_created=dirs2create)
            else:
                cls._revert_vpool_status(vpool=vpool, status=VPool.STATUSES.FAILURE, storagedriver=storagedriver, client=root_client, dirs_created=dirs2create)
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
                    cls._logger.exception('Restoring cluster node configurations failed')
                cls._revert_vpool_status(vpool=vpool, status=VPool.STATUSES.RUNNING, storagedriver=storagedriver, client=root_client, dirs_created=dirs2create)
            raise

        ############################
        # CONFIGURATION MANAGEMENT #
        ############################
        # Configure regular proxies and scrub proxies
        manifest_cache_size = 500 * 1024 * 1024
        for proxy_id, alba_proxy in enumerate(storagedriver.alba_proxies):
            config_tree = '/ovs/vpools/{0}/proxies/{1}/config/{{0}}'.format(vpool.guid, alba_proxy.guid)
            metadata_keys = {'backend': 'abm'}
            if use_fragment_cache_backend is True:
                metadata_keys['backend_aa_{0}'.format(storagerouter.guid)] = 'abm_aa'
            if use_block_cache_backend is True:
                metadata_keys['backend_bc_{0}'.format(storagerouter.guid)] = 'abm_bc'
            for metadata_key in metadata_keys:
                arakoon_config = vpool.metadata[metadata_key]['arakoon_config']
                arakoon_config = ArakoonClusterConfig.convert_config_to(config=arakoon_config, return_type='INI')
                Configuration.set(config_tree.format(metadata_keys[metadata_key]), arakoon_config, raw=True)

            fragment_cache_scrub_info = ['none']
            if fragment_cache_on_read is False and fragment_cache_on_write is False:
                fragment_cache_info = ['none']
            elif use_fragment_cache_backend is True:
                fragment_cache_info = ['alba', {'albamgr_cfg_url': Configuration.get_configuration_path(config_tree.format('abm_aa')),
                                                'bucket_strategy': ['1-to-1', {'prefix': vpool.guid,
                                                                               'preset': vpool.metadata['backend_aa_{0}'.format(storagerouter.guid)]['backend_info']['preset']}],
                                                'manifest_cache_size': manifest_cache_size,
                                                'cache_on_read': fragment_cache_on_read,
                                                'cache_on_write': fragment_cache_on_write}]
                if fragment_cache_on_write is True:
                    # The scrubbers want only cache-on-write.
                    fragment_cache_scrub_info = copy.deepcopy(fragment_cache_info)
                    fragment_cache_scrub_info[1]['cache_on_read'] = False
            else:
                fragment_cache_info = ['local', {'path': '{0}/fc'.format(sdp_frags[proxy_id].path),
                                                 'max_size': frag_size / local_amount_of_proxies,
                                                 'cache_on_read': fragment_cache_on_read,
                                                 'cache_on_write': fragment_cache_on_write}]

            block_cache_scrub_info = ['none']
            if block_cache_on_read is False and block_cache_on_write is False:
                block_cache_info = ['none']
            elif use_block_cache_backend is True:
                block_cache_info = ['alba', {'albamgr_cfg_url': Configuration.get_configuration_path(config_tree.format('abm_bc')),
                                             'bucket_strategy': ['1-to-1', {'prefix': '{0}_bc'.format(vpool.guid),
                                                                            'preset': vpool.metadata['backend_bc_{0}'.format(storagerouter.guid)]['backend_info']['preset']}],
                                             'manifest_cache_size': manifest_cache_size,
                                             'cache_on_read': block_cache_on_read,
                                             'cache_on_write': block_cache_on_write}]
                if block_cache_on_write is True:
                    # The scrubbers want only cache-on-write.
                    block_cache_scrub_info = copy.deepcopy(block_cache_info)
                    block_cache_scrub_info[1]['cache_on_read'] = False
            else:
                block_cache_info = ['local', {'path': '{0}/bc'.format(sdp_frags[proxy_id].path),
                                              'max_size': frag_size / local_amount_of_proxies,
                                              'cache_on_read': block_cache_on_read,
                                              'cache_on_write': block_cache_on_write}]

            main_proxy_config = {'log_level': 'info',
                                 'port': alba_proxy.service.ports[0],
                                 'ips': [storagedriver.storage_ip],
                                 'manifest_cache_size': manifest_cache_size,
                                 'fragment_cache': fragment_cache_info,
                                 'transport': 'tcp',
                                 'read_preference': read_preferences,
                                 'albamgr_cfg_url': Configuration.get_configuration_path(config_tree.format('abm'))}
            if supports_block_cache is True:
                main_proxy_config['block_cache'] = block_cache_info
            Configuration.set(key=config_tree.format('main'), value=main_proxy_config)
            scrub_proxy_config = {'log_level': 'info',
                                  'port': 0,  # Will be overruled by the scrubber scheduled task
                                  'ips': ['127.0.0.1'],
                                  'manifest_cache_size': manifest_cache_size,
                                  'fragment_cache': fragment_cache_scrub_info,
                                  'transport': 'tcp',
                                  'read_preference': read_preferences,
                                  'albamgr_cfg_url': Configuration.get_configuration_path(config_tree.format('abm'))}
            if supports_block_cache is True:
                scrub_proxy_config['block_cache'] = block_cache_scrub_info
            Configuration.set(key='/ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(vpool.guid), value=scrub_proxy_config)

        ###########################
        # CONFIGURE STORAGEDRIVER #
        ###########################
        sco_size = sd_config_params['sco_size']
        dtl_mode = sd_config_params['dtl_mode']
        cluster_size = sd_config_params['cluster_size']
        dtl_transport = sd_config_params['dtl_transport']
        tlog_multiplier = StorageDriverClient.TLOG_MULTIPLIER_MAP[sco_size]
        sco_factor = float(write_buffer) / tlog_multiplier / sco_size  # sco_factor = write buffer / tlog multiplier (default 20) / sco size (in MiB)

        filesystem_config = {'fs_dtl_host': '',
                             'fs_enable_shm_interface': 0,
                             'fs_enable_network_interface': 1,
                             'fs_metadata_backend_arakoon_cluster_nodes': [],
                             'fs_metadata_backend_mds_nodes': [],
                             'fs_metadata_backend_type': 'MDS',
                             'fs_virtual_disk_format': 'raw',
                             'fs_raw_disk_suffix': '.raw',
                             'fs_file_event_rules': [{'fs_file_event_rule_calls': ['Rename'],
                                                      'fs_file_event_rule_path_regex': '.*'}]}
        if dtl_mode == 'no_sync':
            filesystem_config['fs_dtl_config_mode'] = StorageDriverClient.VOLDRV_DTL_MANUAL_MODE
        else:
            filesystem_config['fs_dtl_mode'] = StorageDriverClient.VPOOL_DTL_MODE_MAP[dtl_mode]
            filesystem_config['fs_dtl_config_mode'] = StorageDriverClient.VOLDRV_DTL_AUTOMATIC_MODE

        volume_manager_config = {'tlog_path': sdp_tlogs.path,
                                 'metadata_path': sdp_metadata.path,
                                 'clean_interval': 1,
                                 'dtl_throttle_usecs': 4000,
                                 'default_cluster_size': cluster_size * 1024,
                                 'number_of_scos_in_tlog': tlog_multiplier,
                                 'non_disposable_scos_factor': sco_factor}

        queue_urls = []
        mq_protocol = Configuration.get('/ovs/framework/messagequeue|protocol')
        mq_user = Configuration.get('/ovs/framework/messagequeue|user')
        mq_password = Configuration.get('/ovs/framework/messagequeue|password')
        for current_storagerouter in StorageRouterList.get_masters():
            queue_urls.append({'amqp_uri': '{0}://{1}:{2}@{3}:5672'.format(mq_protocol, mq_user, mq_password, current_storagerouter.ip)})

        backend_connection_manager = {'backend_type': 'MULTI',
                                      'backend_interface_retries_on_error': 5,
                                      'backend_interface_retry_interval_secs': 1,
                                      'backend_interface_retry_backoff_multiplier': 2.0}
        for index, proxy in enumerate(sorted(storagedriver.alba_proxies, key=lambda k: k.service.ports[0])):
            backend_connection_manager[str(index)] = {'alba_connection_host': storagedriver.storage_ip,
                                                      'alba_connection_port': proxy.service.ports[0],
                                                      'alba_connection_preset': vpool.metadata['backend']['backend_info']['preset'],
                                                      'alba_connection_timeout': 30,
                                                      'alba_connection_use_rora': True,
                                                      'alba_connection_transport': 'TCP',
                                                      'alba_connection_rora_manifest_cache_capacity': 25000,
                                                      'alba_connection_asd_connection_pool_capacity': 10,
                                                      'alba_connection_rora_timeout_msecs': 50,
                                                      'backend_type': 'ALBA'}
        volume_router = {'vrouter_id': vrouter_id,
                         'vrouter_redirect_timeout_ms': '120000',
                         'vrouter_keepalive_time_secs': '15',
                         'vrouter_keepalive_interval_secs': '5',
                         'vrouter_keepalive_retries': '2',
                         'vrouter_routing_retries': 10,
                         'vrouter_volume_read_threshold': 0,
                         'vrouter_volume_write_threshold': 0,
                         'vrouter_file_read_threshold': 0,
                         'vrouter_file_write_threshold': 0,
                         'vrouter_min_workers': 4,
                         'vrouter_max_workers': 16,
                         'vrouter_sco_multiplier': sco_size * 1024 / cluster_size,  # sco multiplier = SCO size (in MiB) / cluster size (currently 4KiB),
                         'vrouter_backend_sync_timeout_ms': 60000,
                         'vrouter_migrate_timeout_ms': 60000,
                         'vrouter_use_fencing': True}

        arakoon_cluster_name = str(Configuration.get('/ovs/framework/arakoon_clusters|voldrv'))
        arakoon_nodes = [{'host': node.ip,
                          'port': node.client_port,
                          'node_id': node.name} for node in ArakoonClusterConfig(cluster_id=arakoon_cluster_name).nodes]

        # DTL path is not used, but a required parameter. The DTL transport should be the same as the one set in the DTL server.
        storagedriver_config = StorageDriverConfiguration(vpool.guid, storagedriver.storagedriver_id)
        storagedriver_config.configure_backend_connection_manager(**backend_connection_manager)
        storagedriver_config.configure_content_addressed_cache(serialize_read_cache=False,
                                                               read_cache_serialization_path=[])
        storagedriver_config.configure_scocache(scocache_mount_points=writecaches,
                                                trigger_gap=Toolbox.convert_to_human_readable(size=gap_configuration['trigger']),
                                                backoff_gap=Toolbox.convert_to_human_readable(size=gap_configuration['backoff']))
        storagedriver_config.configure_distributed_transaction_log(dtl_path=sdp_dtl.path,  # Not used, but required
                                                                   dtl_transport=StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP[dtl_transport])
        storagedriver_config.configure_filesystem(**filesystem_config)
        storagedriver_config.configure_volume_manager(**volume_manager_config)
        storagedriver_config.configure_volume_router(**volume_router)
        storagedriver_config.configure_volume_router_cluster(vrouter_cluster_id=vpool.guid)
        storagedriver_config.configure_volume_registry(vregistry_arakoon_cluster_id=arakoon_cluster_name,
                                                       vregistry_arakoon_cluster_nodes=arakoon_nodes)
        storagedriver_config.configure_distributed_lock_store(dls_type='Arakoon',
                                                              dls_arakoon_cluster_id=arakoon_cluster_name,
                                                              dls_arakoon_cluster_nodes=arakoon_nodes)
        storagedriver_config.configure_file_driver(fd_cache_path=sdp_fd.path,
                                                   fd_extent_cache_capacity='1024',
                                                   fd_namespace='fd-{0}-{1}'.format(vpool_name, vpool.guid))
        storagedriver_config.configure_event_publisher(events_amqp_routing_key=Configuration.get('/ovs/framework/messagequeue|queues.storagedriver'),
                                                       events_amqp_uris=queue_urls)
        storagedriver_config.configure_threadpool_component(num_threads=16)
        storagedriver_config.configure_network_interface(network_max_neighbour_distance=StorageDriver.DISTANCES.FAR - 1)
        storagedriver_config.save(client)

        DiskController.sync_with_reality(storagerouter.guid)

        MDSServiceController.prepare_mds_service(storagerouter=storagerouter, vpool=vpool)

        # Update the MDS safety if changed via API (vpool.configuration will be available at this point also for the newly added StorageDriver)
        if new_vpool is False:
            vpool.invalidate_dynamics('configuration')
            mds_safety = parameters.get('mds_config_params', {}).get('mds_safety')
            if mds_safety is not None and vpool.configuration['mds_config']['mds_safety'] != mds_safety:
                Configuration.set(key='/ovs/vpools/{0}/mds_config|mds_safety'.format(vpool.guid),
                                  value=mds_safety)

        ##################
        # START SERVICES #
        ##################
        sd_params = {'KILL_TIMEOUT': '30',
                     'VPOOL_NAME': vpool_name,
                     'VPOOL_MOUNTPOINT': storagedriver.mountpoint,
                     'CONFIG_PATH': storagedriver_config.remote_path,
                     'OVS_UID': client.run(['id', '-u', 'ovs']).strip(),
                     'OVS_GID': client.run(['id', '-g', 'ovs']).strip(),
                     'LOG_SINK': Logger.get_sink_path('storagedriver_{0}'.format(storagedriver.storagedriver_id)),
                     'METADATASTORE_BITS': 5}
        dtl_params = {'DTL_PATH': sdp_dtl.path,
                      'DTL_ADDRESS': storagedriver.storage_ip,
                      'DTL_PORT': str(storagedriver.ports['dtl']),
                      'DTL_TRANSPORT': StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP[dtl_transport],
                      'LOG_SINK': Logger.get_sink_path('storagedriver-dtl_{0}'.format(storagedriver.storagedriver_id))}

        sd_service = 'ovs-volumedriver_{0}'.format(vpool.name)
        dtl_service = 'ovs-dtl_{0}'.format(vpool.name)

        try:
            if not cls._service_manager.has_service(ServiceFactory.SERVICE_WATCHER_VOLDRV, client=root_client):
                cls._service_manager.add_service(ServiceFactory.SERVICE_WATCHER_VOLDRV, client=root_client)
                cls._service_manager.start_service(WATCHER_VOLDRV, client=root_client)

            cls._service_manager.add_service(name='ovs-dtl', params=dtl_params, client=root_client, target_name=dtl_service)
            cls._service_manager.start_service(dtl_service, client=root_client)

            for proxy in storagedriver.alba_proxies:
                alba_proxy_params = {'VPOOL_NAME': vpool_name,
                                     'LOG_SINK': Logger.get_sink_path(proxy.service.name),
                                     'CONFIG_PATH': Configuration.get_configuration_path('/ovs/vpools/{0}/proxies/{1}/config/main'.format(vpool.guid, proxy.guid))}
                alba_proxy_service = 'ovs-{0}'.format(proxy.service.name)
                cls._service_manager.add_service(name='ovs-albaproxy', params=alba_proxy_params, client=root_client, target_name=alba_proxy_service)
                cls._service_manager.start_service(alba_proxy_service, client=root_client)

            cls._service_manager.add_service(name='ovs-volumedriver', params=sd_params, client=root_client, target_name=sd_service)

            storagedriver = StorageDriver(storagedriver.guid)
            current_startup_counter = storagedriver.startup_counter
            cls._service_manager.start_service(sd_service, client=root_client)
        except Exception:
            cls._logger.exception('Failed to start the relevant services for vPool {0} on StorageRouter {1}'.format(vpool.name, storagerouter.name))
            cls._revert_vpool_status(vpool=vpool, status=VPool.STATUSES.FAILURE)
            raise

        tries = 60
        while storagedriver.startup_counter == current_startup_counter and tries > 0:
            cls._logger.debug('Waiting for the StorageDriver to start up for vPool {0} on StorageRouter {1} ...'.format(vpool.name, storagerouter.name))
            if cls._service_manager.get_service_status(sd_service, client=root_client) != 'active':
                cls._revert_vpool_status(vpool=vpool, status=VPool.STATUSES.FAILURE)
                raise RuntimeError('StorageDriver service failed to start (service not running)')
            tries -= 1
            time.sleep(60 - tries)
            storagedriver = StorageDriver(storagedriver.guid)
        if storagedriver.startup_counter == current_startup_counter:
            cls._revert_vpool_status(vpool=vpool, status=VPool.STATUSES.FAILURE)
            raise RuntimeError('StorageDriver service failed to start (got no event)')
        cls._logger.debug('StorageDriver running')

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
        cls._logger.info('Add vPool {0} ended successfully'.format(vpool_name))

    @classmethod
    @ovs_task(name='ovs.storagerouter.remove_storagedriver')
    def remove_storagedriver(cls, storagedriver_guid, offline_storage_router_guids=list()):
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
        cls._logger.info('StorageDriver {0} - Deleting StorageDriver {1}'.format(storage_driver.guid, storage_driver.name))

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

        cls._logger.info('StorageDriver {0} - Checking availability of related StorageRouters'.format(storage_driver.guid, storage_driver.name))
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
                            cls._logger.info('StorageDriver {0} - Available StorageDriver for migration - {1}'.format(storage_driver.guid, sd.name))
                            available_storage_drivers.append(sd)
                        except Exception as ex:
                            if 'ClusterNotReachableException' not in str(ex):
                                raise
                client = temp_client
                cls._logger.info('StorageDriver {0} - StorageRouter {1} with IP {2} is online'.format(storage_driver.guid, sr.name, sr.ip))
            except UnableToConnectException:
                if sr == storage_router or sr.guid in offline_storage_router_guids:
                    cls._logger.warning('StorageDriver {0} - StorageRouter {1} with IP {2} is offline'.format(storage_driver.guid, sr.name, sr.ip))
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
        cls._logger.info('StorageDriver {0} - StorageRouters on which an available StorageDriver runs: {1}'.format(storage_driver.guid, ', '.join(available_sr_names)))
        if unavailable_sr_names:
            cls._logger.warning('StorageDriver {0} - StorageRouters on which a StorageDriver is unavailable: {1}'.format(storage_driver.guid, ', '.join(unavailable_sr_names)))

        # Remove stale vDisks
        voldrv_vdisks = [entry.object_id() for entry in vpool.objectregistry_client.get_all_registrations()]
        voldrv_vdisk_guids = VDiskList.get_in_volume_ids(voldrv_vdisks).guids
        for vdisk_guid in set(vpool.vdisks_guids).difference(set(voldrv_vdisk_guids)):
            cls._logger.warning('vDisk with guid {0} does no longer exist on any StorageDriver linked to vPool {1}, deleting...'.format(vdisk_guid, vpool.name))
            VDiskController.clean_vdisk_from_model(vdisk=VDisk(vdisk_guid))

        # Un-configure or reconfigure the MDSes
        cls._logger.info('StorageDriver {0} - Reconfiguring MDSes'.format(storage_driver.guid))
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
                        cls._logger.debug('StorageDriver {0} - vDisk {1} {2} - Ensuring MDS safety'.format(storage_driver.guid, vdisk.guid, vdisk.name))
                        MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid,
                                                           excluded_storagerouter_guids=[storage_router.guid] + offline_storage_router_guids)
                    except Exception:
                        cls._logger.exception('StorageDriver {0} - vDisk {1} {2} - Ensuring MDS safety failed'.format(storage_driver.guid, vdisk.guid, vdisk.name))

        # Validate that all MDSes on current StorageRouter have been moved away
        # Ensure safety does not always throw an error, that's why we perform this check here instead of in the Exception clause of above code
        vdisks = []
        for mds in mds_services_to_remove:
            for junction in mds.vdisks:
                vdisk = junction.vdisk
                if vdisk in vdisks:
                    continue
                vdisks.append(vdisk)
                cls._logger.critical('StorageDriver {0} - vDisk {1} {2} - MDS Services have not been migrated away'.format(storage_driver.guid, vdisk.guid, vdisk.name))
        if len(vdisks) > 0:
            # Put back in RUNNING, so it can be used again. Errors keep on displaying in GUI now anyway
            vpool.status = VPool.STATUSES.RUNNING
            vpool.save()
            raise RuntimeError('Not all MDS Services have been successfully migrated away')

        # Disable and stop DTL, voldrv and albaproxy services
        if storage_router_online is True:
            dtl_service = 'dtl_{0}'.format(vpool.name)
            voldrv_service = 'volumedriver_{0}'.format(vpool.name)
            client = SSHClient(storage_router, username='root')

            for service in [voldrv_service, dtl_service]:
                try:
                    if cls._service_manager.has_service(service, client=client):
                        cls._logger.debug('StorageDriver {0} - Stopping service {1}'.format(storage_driver.guid, service))
                        cls._service_manager.stop_service(service, client=client)
                        cls._logger.debug('StorageDriver {0} - Removing service {1}'.format(storage_driver.guid, service))
                        cls._service_manager.remove_service(service, client=client)
                except Exception:
                    cls._logger.exception('StorageDriver {0} - Disabling/stopping service {1} failed'.format(storage_driver.guid, service))
                    errors_found = True

            sd_config_key = '/ovs/vpools/{0}/hosts/{1}/config'.format(vpool.guid, storage_driver.storagedriver_id)
            if storage_drivers_left is False and Configuration.exists(sd_config_key):
                try:
                    for proxy in storage_driver.alba_proxies:
                        if cls._service_manager.has_service(proxy.service.name, client=client):
                            cls._logger.debug('StorageDriver {0} - Starting proxy {1}'.format(storage_driver.guid, proxy.service.name))
                            cls._service_manager.start_service(proxy.service.name, client=client)
                            tries = 10
                            running = False
                            port = proxy.service.ports[0]
                            while running is False and tries > 0:
                                cls._logger.debug('StorageDriver {0} - Waiting for the proxy {1} to start up'.format(storage_driver.guid, proxy.service.name))
                                tries -= 1
                                time.sleep(10 - tries)
                                try:
                                    client.run(['alba', 'proxy-statistics', '--host', storage_driver.storage_ip, '--port', str(port)])
                                    running = True
                                except CalledProcessError as ex:
                                    cls._logger.error('StorageDriver {0} - Fetching alba proxy-statistics failed with error (but ignoring): {1}'.format(storage_driver.guid, ex))
                            if running is False:
                                raise RuntimeError('Alba proxy {0} failed to start'.format(proxy.service.name))
                            cls._logger.debug('StorageDriver {0} - Alba proxy {0} running'.format(storage_driver.guid, proxy.service.name))

                    cls._logger.debug('StorageDriver {0} - Destroying filesystem and erasing node configs'.format(storage_driver.guid))
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
                    cls._logger.exception('StorageDriver {0} - Destroying filesystem and erasing node configs failed'.format(storage_driver.guid))
                    errors_found = True

            for proxy in storage_driver.alba_proxies:
                service_name = proxy.service.name
                try:
                    if cls._service_manager.has_service(service_name, client=client):
                        cls._logger.debug('StorageDriver {0} - Stopping service {1}'.format(storage_driver.guid, service_name))
                        cls._service_manager.stop_service(service_name, client=client)
                        cls._logger.debug('StorageDriver {0} - Removing service {1}'.format(storage_driver.guid, service_name))
                        cls._service_manager.remove_service(service_name, client=client)
                except Exception:
                    cls._logger.exception('StorageDriver {0} - Disabling/stopping service {1} failed'.format(storage_driver.guid, service_name))
                    errors_found = True

        # Reconfigure cluster node configs
        if storage_drivers_left is True:
            try:
                cls._logger.info('StorageDriver {0} - Reconfiguring cluster node configs'.format(storage_driver.guid))
                node_configs = []
                for sd in vpool.storagedrivers:
                    if sd != storage_driver:
                        sd.invalidate_dynamics(['cluster_node_config'])
                        config = sd.cluster_node_config
                        if storage_driver.storagedriver_id in config['node_distance_map']:
                            del config['node_distance_map'][storage_driver.storagedriver_id]
                        node_configs.append(ClusterNodeConfig(**config))
                cls._logger.debug('StorageDriver {0} - Node configs - \n{1}'.format(storage_driver.guid, '\n'.join([str(config) for config in node_configs])))
                vpool.clusterregistry_client.set_node_configs(node_configs)
                for sd in available_storage_drivers:
                    if sd != storage_driver:
                        cls._logger.debug('StorageDriver {0} - StorageDriver {1} {2} - Updating cluster node configs'.format(storage_driver.guid, sd.guid, sd.name))
                        vpool.storagedriver_client.update_cluster_node_configs(str(sd.storagedriver_id), req_timeout_secs=10)
            except Exception:
                cls._logger.exception('StorageDriver {0} - Reconfiguring cluster node configs failed'.format(storage_driver.guid))
                errors_found = True

        # Removing MDS services
        cls._logger.info('StorageDriver {0} - Removing MDS services'.format(storage_driver.guid))
        for mds_service in mds_services_to_remove:
            # All MDSServiceVDisk object should have been deleted above
            try:
                cls._logger.debug('StorageDriver {0} - Remove MDS service (number {1}) for StorageRouter with IP {2}'.format(storage_driver.guid, mds_service.number, storage_router.ip))
                MDSServiceController.remove_mds_service(mds_service=mds_service,
                                                        reconfigure=False,
                                                        allow_offline=not storage_router_online)
            except Exception:
                cls._logger.exception('StorageDriver {0} - Removing MDS service failed'.format(storage_driver.guid))
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
            cls._logger.info('StorageDriver {0} - Deleting vPool related directories and files'.format(storage_driver.guid))
            try:
                mountpoints = cls._get_mountpoints(client)
                for dir_name in dirs_to_remove:
                    if dir_name and client.dir_exists(dir_name) and dir_name not in mountpoints and dir_name != '/':
                        client.dir_delete(dir_name)
            except Exception:
                cls._logger.exception('StorageDriver {0} - Failed to retrieve mount point information or delete directories'.format(storage_driver.guid))
                cls._logger.warning('StorageDriver {0} - Following directories should be checked why deletion was prevented: {1}'.format(storage_driver.guid, ', '.join(dirs_to_remove)))
                errors_found = True

            cls._logger.debug('StorageDriver {0} - Synchronizing disks with reality'.format(storage_driver.guid))
            try:
                DiskController.sync_with_reality(storage_router.guid)
            except Exception:
                cls._logger.exception('StorageDriver {0} - Synchronizing disks with reality failed'.format(storage_driver.guid))
                errors_found = True

        Configuration.delete('/ovs/vpools/{0}/hosts/{1}'.format(vpool.guid, storage_driver.storagedriver_id))

        # Model cleanup
        cls._logger.info('StorageDriver {0} - Cleaning up model'.format(storage_driver.guid))
        for proxy in storage_driver.alba_proxies:
            cls._logger.debug('StorageDriver {0} - Removing alba proxy service {1} from model'.format(storage_driver.guid, proxy.service.name))
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
            cls._logger.debug('StorageDriver {0} - Checking DTL for all vDisks in vPool {1} with guid {2}'.format(storage_driver.guid, vpool.name, vpool.guid))
            try:
                VDiskController.dtl_checkup(vpool_guid=vpool.guid, ensure_single_timeout=600)
            except Exception:
                cls._logger.exception('StorageDriver {0} - DTL checkup failed for vPool {1} with guid {2}'.format(storage_driver.guid, vpool.name, vpool.guid))

        if sd_can_be_deleted is True:
            storage_driver.delete()
            if storage_drivers_left is False:
                cls._logger.info('StorageDriver {0} - Removing vPool from model'.format(storage_driver.guid))
                vpool.delete()
                Configuration.delete('/ovs/vpools/{0}'.format(vpool.guid))
        else:
            try:
                vpool.delete()  # Try to delete the vPool to invoke a proper stacktrace to see why it can't be deleted
            except Exception:
                errors_found = True
                cls._logger.exception('StorageDriver {0} - Cleaning up vpool from the model failed'.format(storage_driver.guid))

        cls._logger.info('StorageDriver {0} - Running MDS checkup'.format(storage_driver.guid))
        try:
            MDSServiceController.mds_checkup()
        except Exception:
            cls._logger.exception('StorageDriver {0} - MDS checkup failed'.format(storage_driver.guid))

        if errors_found is True:
            if storage_drivers_left is True:
                vpool.status = VPool.STATUSES.FAILURE
                vpool.save()
            raise RuntimeError('1 or more errors occurred while trying to remove the StorageDriver. Please check the logs for more information')
        if storage_drivers_left is True:
            vpool.status = VPool.STATUSES.RUNNING
            vpool.save()
        cls._logger.info('StorageDriver {0} - Deleted StorageDriver {1}'.format(storage_driver.guid, storage_driver.name))
        if len(VPoolList.get_vpools()) == 0:
            cluster_name = ArakoonClusterConfig.get_cluster_name('voldrv')
            if ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)['internal'] is True:
                cls._logger.debug('StorageDriver {0} - Removing Arakoon cluster {1}'.format(storage_driver.guid, cluster_name))
                try:
                    installer = ArakoonInstaller(cluster_name=cluster_name)
                    installer.load()
                    installer.delete_cluster()
                except Exception:
                    cls._logger.exception('StorageDriver {0} - Delete voldrv Arakoon cluster failed'.format(storage_driver.guid))
        if len(storage_router.storagedrivers) == 0 and storage_router_online is True:  # ensure client is initialized for storagerouter
            try:
                if cls._service_manager.has_service(ServiceFactory.SERVICE_WATCHER_VOLDRV, client=client):
                    cls._service_manager.stop_service(ServiceFactory.SERVICE_WATCHER_VOLDRV, client=client)
                    cls._service_manager.remove_service(ServiceFactory.SERVICE_WATCHER_VOLDRV, client=client)
            except Exception:
                cls._logger.exception('StorageDriver {0} - {1} deletion failed'.format(storage_driver.guid, ServiceFactory.SERVICE_WATCHER_VOLDRV))

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
        if remote_access_change is True:
            Configuration.set(key=remote_access_key, value=remote_access_new)
            cid = Configuration.get('/ovs/framework/cluster_id').replace(r"'", r"'\''")
            for storagerouter, root_client in root_clients.iteritems():
                if remote_access_new is False:
                    StorageRouterController._logger.info('Un-configuring remote access on StorageRouter {0}'.format(root_client.ip))
                    nid = storagerouter.machine_id.replace(r"'", r"'\''")
                    service_name = 'openvpn@ovs_{0}-{1}'.format(cid, nid)
                    if StorageRouterController._service_manager.has_service(name=service_name, client=root_client):
                        StorageRouterController._service_manager.stop_service(name=service_name, client=root_client)
                    root_client.file_delete(filenames=['/etc/openvpn/ovs_*'])

        # Configure support agent
        if support_agent_change is True:
            service_name = 'support-agent'
            Configuration.set(key=support_agent_key, value=support_agent_new)
            for root_client in root_clients.itervalues():
                if support_agent_new is True:
                    StorageRouterController._logger.info('Configuring support agent on StorageRouter {0}'.format(root_client.ip))
                    if StorageRouterController._service_manager.has_service(name=service_name, client=root_client) is False:
                        StorageRouterController._service_manager.add_service(name=service_name, client=root_client)
                    StorageRouterController._service_manager.restart_service(name=service_name, client=root_client)
                else:
                    StorageRouterController._logger.info('Un-configuring support agent on StorageRouter {0}'.format(root_client.ip))
                    if StorageRouterController._service_manager.has_service(name=service_name, client=root_client):
                        StorageRouterController._service_manager.stop_service(name=service_name, client=root_client)
                        StorageRouterController._service_manager.remove_service(name=service_name, client=root_client)

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
                StorageRouterController._service_manager.restart_service(name=service_name, client=root_client)

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
    def _revert_vpool_status(vpool, status=VPool.STATUSES.RUNNING, storagedriver=None, client=None, dirs_created=None):
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
