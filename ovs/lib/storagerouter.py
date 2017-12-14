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
StorageRouterController class used to make changes to existing StorageRouters
StorageRouterInstaller class used to validate / configure / edit StorageRouter settings when setting up a vPool on it
"""

import os
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
from ovs.extensions.db.arakooninstaller import ArakoonInstaller
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
from ovs.extensions.storageserver.storagedriver import ClusterNodeConfig, LocalStorageRouterClient, LOG_LEVEL_MAPPING, StorageDriverConfiguration
from ovs.extensions.support.agent import SupportAgent
from ovs.lib.disk import DiskController
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.storagedriver import StorageDriverController, StorageDriverInstaller
from ovs.lib.vdisk import VDiskController
from ovs.lib.vpool import VPoolInstaller
from volumedriver.storagerouter import storagerouterclient


class StorageRouterInstaller(object):
    """
    Class used to add a StorageDriver on a StorageRouter
    This class will be responsible for
        - validate_global_write_buffer: Validate the requested amount of global write buffer size can be supplied
        - validate_local_cache_size: Validate if fragment or block cache is local, whether enough size is available for the caching
        - validate_vpool_extendable: Validate whether the StorageRouter is eligible to have a/another vPool on it
    """
    def __init__(self, root_client, storagerouter, vp_installer, sd_installer):
        """
        Initialize a StorageRouterInstaller class instance containing information about:
            - Which StorageRouter to make changes on
            - SSHClient to the StorageRouter
            - vPool information on which a new StorageDriver is going to be deployed, eg: global vPool configurations, vPool name, ...
            - StorageDriver configurations, eg: backend information, connection information, caching information, configuration information, ...
        """
        self.root_client = root_client
        self.sd_installer = sd_installer
        self.vp_installer = vp_installer
        self.storagerouter = storagerouter

        self.created_dirs = []  # Contains directories which are being created during vPool creation/extension
        self.requested_proxies = 0
        self.block_cache_supported = False
        self.requested_local_proxies = 0  # When using local caching for both fragment AND block cache, this value is used for local cache size calculation
        self.largest_write_partition = None  # Used for cache size calculation (When using local fragment or local block cache)
        self.smallest_write_partition_size = None  # Used for trigger gap and backoff gap calculation
        self.global_write_buffer_requested_size = None

        # Be aware that below information always needs to be the latest when making calculations for adding StorageDriver partitions
        self.partition_info = None
        self.write_partitions = []
        self.global_write_buffer_available_size = None

    def validate_global_write_buffer(self, requested_size):
        """
        Validate whether the requested write buffer size can be supplied using all the partitions with a WRITE role assigned to it
        :param requested_size: The requested size in GiB for global write buffer usage
        :type requested_size: int
        :return: None
        :rtype: NoneType
        """
        if self.partition_info is None:
            raise RuntimeError('Partition information has not been retrieved yet')

        if not 1 <= requested_size <= 10240:
            raise RuntimeError('The requested global WRITE buffer size should be between 1GiB and 10240GiB')

        usable_partitions = [part for part in self.partition_info.get(DiskPartition.ROLES.WRITE, []) if part['usable'] is True]
        available_size = sum(part['available'] for part in usable_partitions)
        requested_size *= 1024.0 ** 3

        if requested_size > available_size:
            requested_gib = requested_size / 1024.0 ** 3
            available_gib = available_size / 1024.0 ** 3
            raise RuntimeError('Too much space requested for {0} cache. Available: {1:.2f} GiB, Requested: {2:.2f} GiB'.format(DiskPartition.ROLES.WRITE, available_gib, requested_gib))

        self.write_partitions = usable_partitions
        self.global_write_buffer_available_size = available_size
        self.global_write_buffer_requested_size = requested_size

    def validate_local_cache_size(self, requested_proxies):
        """
        Validate whether the requested amount of proxies can be deployed on local StorageRouter partitions having the WRITE role IF either local fragment cache or local block cache is used
        :param requested_proxies: Amount of proxies that have been requested for deployment
        :type requested_proxies: int
        :return: None
        :rtype: NoneType
        """
        if not 1 <= requested_proxies <= 16:
            raise RuntimeError('The requested amount of proxies to deploy should be a value between 1 and 16')

        if self.sd_installer.block_cache_local is False and self.sd_installer.fragment_cache_local is False:
            # Only verify whether we have enough available space to deploy the requested amount of proxies when fragment or block caching is done locally
            return

        if len(self.write_partitions) == 0 or self.global_write_buffer_requested_size is None or self.global_write_buffer_available_size is None:
            raise RuntimeError('Global write buffer calculation has not been done yet')

        # Calculate available write cache size
        largest_ssd_size = 0
        largest_sata_size = 0
        largest_ssd_write_partition = None
        largest_sata_write_partition = None
        for info in self.write_partitions:
            if info['ssd'] is True and info['available'] > largest_ssd_size:
                largest_ssd_size = info['available']
                largest_ssd_write_partition = info['guid']
            elif info['ssd'] is False and info['available'] > largest_sata_size:
                largest_sata_size = info['available']
                largest_sata_write_partition = info['guid']

        if largest_ssd_write_partition is None and largest_sata_write_partition is None:
            raise RuntimeError('No {0} partition found to put the local caches on'.format(DiskPartition.ROLES.WRITE))

        self.requested_proxies = requested_proxies
        self.largest_write_partition = DiskPartition(largest_ssd_write_partition or largest_sata_write_partition)
        if self.sd_installer.block_cache_local is True:
            self.requested_local_proxies += requested_proxies
        if self.sd_installer.fragment_cache_local is True:
            self.requested_local_proxies += requested_proxies

        proportion = float(largest_ssd_size or largest_sata_size) / self.global_write_buffer_available_size
        available_size = proportion * self.global_write_buffer_requested_size * 0.10  # Only 10% is used on the largest WRITE partition for fragment caching
        available_size_gib = available_size / 1024.0 ** 3
        if available_size / self.requested_local_proxies < 1024 ** 3:
            raise RuntimeError('Not enough space available ({0}GiB) on largest local WRITE partition to deploy {1} prox{2}'.format(available_size_gib, requested_proxies, 'y' if requested_proxies == 1 else 'ies'))

    def validate_vpool_extendable(self):
        """
        Perform some validations on the specified StorageRouter to verify whether a vPool can be created or extended on it
        :return: None
        :rtype: NoneType
        """
        if self.partition_info is None:
            raise RuntimeError('Partition information has not been retrieved yet')

        # Validate RDMA capabilities
        if self.sd_installer.rdma_enabled is True and self.storagerouter.rdma_capable is False:
            raise RuntimeError('DTL transport over RDMA is not supported by StorageRouter with IP {0}'.format(self.storagerouter.ip))

        # Validate block cache is allowed to be used
        if self.storagerouter.features is None:
            raise RuntimeError('Could not load available features')
        self.block_cache_supported = 'block-cache' in self.storagerouter.features.get('alba', {}).get('features', [])
        if self.block_cache_supported is False and (self.sd_installer.block_cache_on_read is True or self.sd_installer.block_cache_on_write is True):
            raise RuntimeError('Block cache is not a supported feature')

        # Validate mount point for the vPool to be created does not exist yet
        if StorageRouterController.mountpoint_exists(name=self.vp_installer.name, storagerouter_guid=self.storagerouter.guid):
            raise RuntimeError('The mount point for vPool {0} already exists'.format(self.vp_installer.name))

        # Validate SCRUB role available on any StorageRouter
        if StorageRouterController.check_scrub_partition_present() is False:
            raise RuntimeError('At least 1 StorageRouter must have a partition with a {0} role'.format(DiskPartition.ROLES.SCRUB))

        # Validate required roles present
        for required_role in [DiskPartition.ROLES.DB, DiskPartition.ROLES.DTL, DiskPartition.ROLES.WRITE]:
            if required_role not in self.partition_info:
                raise RuntimeError('Missing required partition with a {0} role'.format(required_role))
            elif len(self.partition_info[required_role]) == 0:
                raise RuntimeError('At least 1 partition with a {0} role is required per StorageRouter'.format(required_role))
            elif required_role in [DiskPartition.ROLES.DB, DiskPartition.ROLES.DTL]:
                if len(self.partition_info[required_role]) > 1:
                    raise RuntimeError('Only 1 partition with a {0} role is allowed per StorageRouter'.format(required_role))
            else:
                total_available = [part['available'] for part in self.partition_info[required_role]]
                if total_available == 0:
                    raise RuntimeError('Not enough available space for {0}'.format(required_role))

        # Validate mount points are mounted
        for role, part_info in self.partition_info.iteritems():
            if role not in [DiskPartition.ROLES.DB, DiskPartition.ROLES.DTL, DiskPartition.ROLES.WRITE, DiskPartition.ROLES.SCRUB]:
                continue

            for part in part_info:
                mount_point = part['mountpoint']
                if mount_point == DiskPartition.VIRTUAL_STORAGE_LOCATION:
                    continue
                if self.root_client.is_mounted(path=mount_point) is False:
                    raise RuntimeError('Mount point {0} is not mounted'.format(mount_point))


class StorageRouterController(object):
    """
    Contains all BLL related to StorageRouter
    """
    _logger = Logger('lib')
    _log_level = LOG_LEVEL_MAPPING[_logger.getEffectiveLevel()]
    _os_manager = OSFactory.get_manager()
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
        Gets physical information about the specified StorageRouter
        :param storagerouter_guid: StorageRouter guid to retrieve the metadata for
        :type storagerouter_guid: str
        :return: Metadata information about the StorageRouter
        :rtype: dict
        """
        return {'partitions': StorageRouterController.get_partition_info(storagerouter_guid),
                'ipaddresses': StorageRouterController.get_ip_addresses(storagerouter_guid),
                'scrub_available': StorageRouterController.check_scrub_partition_present()}

    @staticmethod
    def get_ip_addresses(storagerouter_guid):
        """
        Retrieves the IP addresses of a StorageRouter
        :param storagerouter_guid: Guid of the StorageRouter
        :return: List of IP addresses
        :rtype: list
        """
        client = SSHClient(endpoint=StorageRouter(storagerouter_guid))
        return StorageRouterController._os_manager.get_ip_addresses(client=client)

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
        client = SSHClient(endpoint=storagerouter)
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
        # VALIDATIONS
        if not isinstance(parameters, dict):
            raise ValueError('Parameters passed to create a vPool should be of type dict')

        # Check StorageRouter existence
        storagerouter = StorageRouterList.get_by_ip(ip=parameters.get('storagerouter_ip'))
        if storagerouter is None:
            raise RuntimeError('Could not find StorageRouter')

        # Validate requested vPool configurations
        vp_installer = VPoolInstaller(name=parameters.get('vpool_name'))
        vp_installer.validate(storagerouter=storagerouter)

        # Validate requested StorageDriver configurations
        cls._logger.info('vPool {0}: Validating StorageDriver configurations'.format(vp_installer.name))
        sd_installer = StorageDriverInstaller(storage_ip=parameters.get('storage_ip'),
                                              vp_installer=vp_installer,
                                              caching_info=parameters.get('caching_info'),
                                              backend_info={'main': parameters.get('backend_info'),
                                                            StorageDriverConfiguration.CACHE_BLOCK: parameters.get('backend_info_bc'),
                                                            StorageDriverConfiguration.CACHE_FRAGMENT: parameters.get('backend_info_fc')},
                                              connection_info={'main': parameters.get('connection_info'),
                                                               StorageDriverConfiguration.CACHE_BLOCK: parameters.get('connection_info_bc'),
                                                               StorageDriverConfiguration.CACHE_FRAGMENT: parameters.get('connection_info_fc')},
                                              sd_configuration=parameters.get('config_params'))

        partitions_mutex = volatile_mutex('add_vpool_partitions_{0}'.format(storagerouter.guid))
        try:
            # VPOOL CREATION
            # Create the vPool as soon as possible in the process to be displayed in the GUI (INSTALLING/EXTENDING state)
            if vp_installer.is_new is True:
                vp_installer.create(rdma_enabled=sd_installer.rdma_enabled)
                vp_installer.configure_mds(config=parameters.get('mds_config_params'))
            else:
                vp_installer.update_status(status=VPool.STATUSES.EXTENDING)

            # ADDITIONAL VALIDATIONS
            # Check StorageRouter connectivity
            cls._logger.info('vPool {0}: Validating StorageRouter connectivity'.format(vp_installer.name))
            linked_storagerouters = [storagerouter]
            if vp_installer.is_new is False:
                linked_storagerouters += [sd.storagerouter for sd in vp_installer.vpool.storagedrivers]

            ip_client_map = {}
            offline_nodes = []
            for sr in linked_storagerouters:
                try:
                    ip_client_map[sr.ip] = {'ovs': SSHClient(endpoint=sr.ip, username='ovs'),
                                            'root': SSHClient(endpoint=sr.ip, username='root')}
                except UnableToConnectException:
                    if sr == storagerouter:
                        raise RuntimeError('Node on which the vPool is being {0} is not reachable'.format('created' if vp_installer.is_new is True else 'extended'))
                    offline_nodes.append(sr)  # We currently want to allow offline nodes while setting up or extend a vPool

            sr_installer = StorageRouterInstaller(root_client=ip_client_map[storagerouter.ip]['root'],
                                                  sd_installer=sd_installer,
                                                  vp_installer=vp_installer,
                                                  storagerouter=storagerouter)
            sd_installer.sr_installer = sr_installer
            vp_installer.sr_installer = sr_installer
            vp_installer.sd_installer = sd_installer

            # When 2 or more jobs simultaneously run on the same StorageRouter, we need to check and create the StorageDriver partitions in locked context
            partitions_mutex.acquire(wait=60)
            sr_installer.partition_info = cls.get_partition_info(storagerouter_guid=storagerouter.guid)
            sr_installer.validate_vpool_extendable()
            sr_installer.validate_global_write_buffer(requested_size=parameters.get('writecache_size', 0))
            sr_installer.validate_local_cache_size(requested_proxies=parameters.get('parallelism', {}).get('proxies', 2))

            # MODEL STORAGEDRIVER AND PARTITION JUNCTIONS
            sd_installer.create()
            sd_installer.create_partitions()
        except Exception:
            cls._logger.exception('Something went wrong during the validation or modeling of vPool {0} on StorageRouter {1}'.format(vp_installer.name, storagerouter.name))
            vp_installer.revert_vpool(status=VPool.STATUSES.RUNNING)
            raise
        finally:
            partitions_mutex.release()

        cls._logger.info('vPool {0}: Refreshing metadata'.format(vp_installer.name))
        try:
            vp_installer.refresh_metadata()
        except Exception:
            # At this point still nothing irreversible has changed, so revert to RUNNING
            cls._logger.exception('vPool {0}: Refreshing metadata failed'.format(vp_installer.name))
            vp_installer.revert_vpool(status=VPool.STATUSES.RUNNING)
            raise

        # Arakoon setup
        counter = 0
        while counter < 300:
            try:
                if StorageDriverController.manual_voldrv_arakoon_checkup() is True:
                    break
            except Exception:
                cls._logger.exception('Arakoon checkup for voldrv cluster failed')
                vp_installer.revert_vpool(status=VPool.STATUSES.RUNNING)
                raise
            counter += 1
            time.sleep(1)
            if counter == 300:
                vp_installer.revert_vpool(status=VPool.STATUSES.RUNNING)
                raise RuntimeError('Arakoon checkup for the StorageDriver cluster could not be started')

        # Cluster registry
        vp_installer.configure_cluster_registry()
        try:
            vp_installer.configure_cluster_registry()
        except:
            cls._logger.exception('vPool {0}: Cluster registry configuration failed'.format(vp_installer.name))
            if vp_installer.is_new is True:
                vp_installer.revert_vpool(status=VPool.STATUSES.RUNNING)
            else:
                vp_installer.revert_vpool(status=VPool.STATUSES.FAILURE)
            raise

        # Configurations
        try:
            # Configure regular proxies and scrub proxies
            sd_installer.setup_proxy_configs()

            # Configure the StorageDriver service
            sd_installer.configure_storagedriver_service()

            DiskController.sync_with_reality(storagerouter.guid)
            MDSServiceController.prepare_mds_service(storagerouter=storagerouter, vpool=vp_installer.vpool)

            # Update the MDS safety if changed via API (vpool.configuration will be available at this point also for the newly added StorageDriver)
            vp_installer.vpool.invalidate_dynamics('configuration')
            if vp_installer.mds_safety is not None and vp_installer.vpool.configuration['mds_config']['mds_safety'] != vp_installer.mds_safety:
                Configuration.set(key='/ovs/vpools/{0}/mds_config|mds_safety'.format(vp_installer.vpool.guid), value=vp_installer.mds_safety)
        except:
            # From here on out we don't want to revert the vPool anymore, since it might break stuff even more, instead we just put it in FAILURE
            cls._logger.exception('vPool {0}: Configuration failed'.format(vp_installer.name))
            vp_installer.update_status(status=VPool.STATUSES.FAILURE)
            raise

        # Create and start watcher volumedriver, DTL, proxies and StorageDriver services
        try:
            sd_installer.start_services()
        except Exception:
            cls._logger.exception('vPool {0}: Creating and starting all services failed'.format(vp_installer.name))
            vp_installer.update_status(status=VPool.STATUSES.FAILURE)
            raise

        # Post creation/extension checkups
        try:
            mds_config_set = MDSServiceController.get_mds_storagedriver_config_set(vpool=vp_installer.vpool, offline_nodes=offline_nodes)
            for sr, clients in ip_client_map.iteritems():
                for current_storagedriver in [sd for sd in sr.storagedrivers if sd.vpool_guid == vp_installer.vpool.guid]:
                    storagedriver_config = StorageDriverConfiguration(vpool_guid=vp_installer.vpool.guid, storagedriver_id=current_storagedriver.storagedriver_id)
                    if storagedriver_config.config_missing is False:
                        # Filesystem section in StorageDriver configuration are all parameters used for vDisks created directly on the filesystem
                        # So when a vDisk gets created on the filesystem, these MDSes will be assigned to them
                        storagedriver_config.configure_filesystem(fs_metadata_backend_mds_nodes=mds_config_set[sr.guid])
                        storagedriver_config.save(client=clients['ovs'])

            # Everything's reconfigured, refresh new cluster configuration
            for current_storagedriver in vp_installer.vpool.storagedrivers:
                if current_storagedriver.storagerouter.ip not in ip_client_map:
                    continue
                vp_installer.vpool.storagedriver_client.update_cluster_node_configs(str(current_storagedriver.storagedriver_id), req_timeout_secs=10)
        except Exception:
            cls._logger.exception('vPool {0}: Updating the MDS node configuration or cluster node config failed'.format(vp_installer.name))
            vp_installer.update_status(status=VPool.STATUSES.FAILURE)
            raise

        # When a node is offline, we can run into errors, but also when 1 or more volumes are not running
        # Scheduled tasks below, so don't really care whether they succeed or not
        try:
            VDiskController.dtl_checkup(vpool_guid=vp_installer.vpool.guid, ensure_single_timeout=600)
        except:
            pass
        for vdisk in vp_installer.vpool.vdisks:
            try:
                MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)
            except:
                pass
        vp_installer.update_status(status=VPool.STATUSES.RUNNING)
        cls._logger.info('Add vPool {0} ended successfully'.format(vp_installer.name))

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
                cls._logger.exception('StorageDriver {0} - Cleaning up vPool from the model failed'.format(storage_driver.guid))

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
            cluster_name = ArakoonInstaller.get_cluster_name('voldrv')
            if ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)['internal'] is True:
                cls._logger.debug('StorageDriver {0} - Removing Arakoon cluster {1}'.format(storage_driver.guid, cluster_name))
                try:
                    installer = ArakoonInstaller(cluster_name=cluster_name)
                    installer.load()
                    installer.delete_cluster()
                except Exception:
                    cls._logger.exception('StorageDriver {0} - Delete voldrv Arakoon cluster failed'.format(storage_driver.guid))
                service_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON)
                service_name = ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name)
                for service in list(service_type.services):
                    if service.name == service_name:
                        service.delete()

        if len(storage_router.storagedrivers) == 0 and storage_router_online is True:  # ensure client is initialized for StorageRouter
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
        Returns support metadata for a given StorageRouter. This should be a routed task!
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
        ExtensionsToolbox.verify_required_params(actual_params=support_info,
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
            ExtensionsToolbox.verify_required_params(actual_params=stats_monkey_new_config,
                                                     required_params={'host': (str, ExtensionsToolbox.regex_ip),
                                                                      'port': (int, {'min': 1, 'max': 65535}),
                                                                      'database': (str, None),
                                                                      'interval': (int, {'min': 1, 'max': 86400}),
                                                                      'password': (str, None),
                                                                      'transport': (str, ['influxdb', 'redis']),
                                                                      'environment': (str, None)})
            if stats_monkey_new_config['transport'] == 'influxdb':
                ExtensionsToolbox.verify_required_params(actual_params=stats_monkey_new_config, required_params={'username': (str, None)})

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
    def check_scrub_partition_present():
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
