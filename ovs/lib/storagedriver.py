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
StorageDriver module
"""
import time
import copy
import json
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.j_albaproxy import AlbaProxy
from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
from ovs.dal.hybrids.service import Service
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig, ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.generic.sshclient import NotAuthenticatedException, SSHClient, UnableToConnectException
from ovs.lib.helpers.toolbox import Toolbox
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storageserver.storagedriver import ClusterNodeConfig, LocalStorageRouterClient, StorageDriverClient, StorageDriverConfiguration
from ovs.extensions.generic.system import System
from ovs.lib.disk import DiskController
from ovs.lib.helpers.decorators import add_hooks, log, ovs_task
from ovs.lib.helpers.toolbox import Schedule
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.vpool import VPoolController
from volumedriver.storagerouter import VolumeDriverEvents_pb2


class StorageDriverController(object):
    """
    Contains all BLL related to Storage Drivers
    """
    _logger = Logger('lib')

    ################
    # CELERY TASKS #
    ################
    @staticmethod
    @ovs_task(name='ovs.storagedriver.mark_offline')
    def mark_offline(storagerouter_guid):
        """
        Marks all StorageDrivers on this StorageRouter offline
        :param storagerouter_guid: Guid of the Storage Router
        :type storagerouter_guid: str
        :return: None
        """
        for storagedriver in StorageRouter(storagerouter_guid).storagedrivers:
            vpool = storagedriver.vpool
            if len(vpool.storagedrivers) > 1:
                storagedriver_client = StorageDriverClient.load(vpool, excluded_storagedrivers=[storagedriver])
                storagedriver_client.mark_node_offline(str(storagedriver.storagedriver_id))

    @staticmethod
    @ovs_task(name='ovs.storagedriver.volumedriver_error')
    @log('VOLUMEDRIVER_TASK')
    def volumedriver_error(code, volume_id):
        """
        Handles error messages/events from the volumedriver
        :param code: Volumedriver error code
        :type code: int
        :param volume_id: Name of the volume throwing the error
        :type volume_id: str
        :return: None
        """
        if code == VolumeDriverEvents_pb2.MDSFailover:
            vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)
            if vdisk is not None:
                MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)

    @staticmethod
    @ovs_task(name='ovs.storagedriver.cluster_registry_checkup', schedule=Schedule(minute='0', hour='0'), ensure_single_info={'mode': 'CHAINED'})
    def cluster_registry_checkup():
        """
        Verify whether changes have occurred in the cluster registry for each vPool
        :return: Information whether changes occurred
        :rtype: dict
        """
        changed_vpools = {}
        for vpool in VPoolList.get_vpools():
            changed_vpools[vpool.guid] = {'changes': False,
                                          'success': True}
            try:
                StorageDriverController._logger.info('Validating cluster registry settings for Vpool {0}'.format(vpool.guid))

                current_configs = vpool.clusterregistry_client.get_node_configs()
                changes = len(current_configs) == 0
                node_configs = []
                for sd in vpool.storagedrivers:
                    sd.invalidate_dynamics(['cluster_node_config'])
                    new_config = sd.cluster_node_config
                    node_configs.append(ClusterNodeConfig(**new_config))
                    if changes is False:
                        current_node_configs = [config for config in current_configs if config.vrouter_id == sd.storagedriver_id]
                        if len(current_node_configs) == 1:
                            current_node_config = current_node_configs[0]
                            for key in new_config:
                                if getattr(current_node_config, key) != new_config[key]:
                                    changes = True
                                    break
                changed_vpools[vpool.guid]['changes'] = changes

                if changes is True:
                    StorageDriverController._logger.info('Cluster registry settings for Vpool {0} needs to be updated'.format(vpool.guid))
                    available_storagedrivers = []
                    for sd in vpool.storagedrivers:
                        storagerouter = sd.storagerouter
                        try:
                            SSHClient(storagerouter, username='root')
                        except UnableToConnectException:
                            StorageDriverController._logger.warning('StorageRouter {0} not available.'.format(storagerouter.name))
                            continue

                        with remote(storagerouter.ip, [LocalStorageRouterClient]) as rem:
                            sd_key = '/ovs/vpools/{0}/hosts/{1}/config'.format(vpool.guid, sd.storagedriver_id)
                            if Configuration.exists(sd_key) is True:
                                path = Configuration.get_configuration_path(sd_key)
                                try:
                                    lsrc = rem.LocalStorageRouterClient(path)
                                    lsrc.server_revision()  # 'Cheap' call to verify whether volumedriver is responsive
                                    available_storagedrivers.append(sd)
                                except Exception as ex:
                                    if 'ClusterNotReachableException' in str(ex):
                                        StorageDriverController._logger.warning('StorageDriver {0} on StorageRouter {1} not available.'.format(
                                            sd.guid, storagerouter.name
                                        ))
                                    else:
                                        StorageDriverController._logger.exception('Got exception when validating StorageDriver {0} on StorageRouter {1}.'.format(
                                            sd.guid, storagerouter.name
                                        ))

                    StorageDriverController._logger.info('Updating cluster node configs for VPool {0}'.format(vpool.guid))
                    vpool.clusterregistry_client.set_node_configs(node_configs)
                    for sd in available_storagedrivers:
                        StorageDriverController._logger.info('Trigger config reload for StorageDriver {0}'.format(sd.guid))
                        vpool.storagedriver_client.update_cluster_node_configs(str(sd.storagedriver_id), req_timeout_secs=10)
                    StorageDriverController._logger.info('Updating cluster node configs for Vpool {0} completed'.format(vpool.guid))
                else:
                    StorageDriverController._logger.info('Cluster registry settings for Vpool {0} is up to date'.format(vpool.guid))
            except Exception as ex:
                StorageDriverController._logger.exception('Got exception when validating cluster registry settings for Vpool {0}.'.format(vpool.name))
                changed_vpools[vpool.guid]['success'] = False
                changed_vpools[vpool.guid]['error'] = ex.message
        return changed_vpools

    @staticmethod
    @ovs_task(name='ovs.storagedriver.scheduled_voldrv_arakoon_checkup',
              schedule=Schedule(minute='15', hour='*'),
              ensure_single_info={'mode': 'DEFAULT', 'extra_task_names': ['ovs.storagedriver.manual_voldrv_arakoon_checkup']})
    def scheduled_voldrv_arakoon_checkup():
        """
        Makes sure the volumedriver arakoon is on all available master nodes
        :return: None
        """
        StorageDriverController._voldrv_arakoon_checkup(False)

    @staticmethod
    @ovs_task(name='ovs.storagedriver.manual_voldrv_arakoon_checkup',
              ensure_single_info={'mode': 'DEFAULT', 'extra_task_names': ['ovs.storagedriver.scheduled_voldrv_arakoon_checkup']})
    def manual_voldrv_arakoon_checkup():
        """
        Creates a new Arakoon Cluster if required and extends cluster if possible on all available master nodes
        :return: True if task completed, None if task was discarded (by decorator)
        :rtype: bool|None
        """
        StorageDriverController._voldrv_arakoon_checkup(True)
        return True

    @staticmethod
    @ovs_task(name='ovs.storagedriver.refresh_configuration')
    def refresh_configuration(storagedriver_guid):
        """
        Refresh the StorageDriver's configuration (Configuration must have been updated manually)
        :param storagedriver_guid: Guid of the StorageDriver
        :type storagedriver_guid: str
        :return: Amount of changes the volumedriver detected
        :rtype: int
        """
        storagedriver = StorageDriver(storagedriver_guid)
        try:
            client = SSHClient(endpoint=storagedriver.storagerouter)
        except UnableToConnectException:
            raise Exception('StorageRouter with IP {0} is not reachable. Cannot refresh the configuration'.format(storagedriver.storagerouter.ip))

        storagedriver_config = StorageDriverConfiguration(vpool_guid=storagedriver.vpool_guid, storagedriver_id=storagedriver.storagedriver_id)
        return len(storagedriver_config.save(client=client, force_reload=True))

    #########
    # HOOKS #
    #########
    @staticmethod
    @add_hooks('nodetype', 'demote')
    def _on_demote(cluster_ip, master_ip, offline_node_ips=None):
        """
        Handles the demote for the StorageDrivers
        :param cluster_ip: IP of the node to demote
        :type cluster_ip: str
        :param master_ip: IP of the master node
        :type master_ip: str
        :param offline_node_ips: IPs of nodes which are offline
        :type offline_node_ips: list
        :return: None
        """
        _ = master_ip
        if offline_node_ips is None:
            offline_node_ips = []
        servicetype = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON)
        current_service = None
        remaining_ips = []
        for service in servicetype.services:
            if service.name == 'arakoon-voldrv' and service.is_internal is True:  # Externally managed arakoon cluster services do not have StorageRouters
                if service.storagerouter.ip == cluster_ip:
                    current_service = service
                elif service.storagerouter.ip not in offline_node_ips:
                    remaining_ips.append(service.storagerouter.ip)
        if current_service is not None:
            if len(remaining_ips) == 0:
                raise RuntimeError('Could not find any remaining arakoon nodes for the voldrv cluster')
            StorageDriverController._logger.debug('* Shrink StorageDriver cluster')
            cluster_name = str(Configuration.get('/ovs/framework/arakoon_clusters|voldrv'))
            arakoon_installer = ArakoonInstaller(cluster_name=cluster_name)
            arakoon_installer.load()
            arakoon_installer.shrink_cluster(removal_ip=cluster_ip,
                                             offline_nodes=offline_node_ips)
            arakoon_installer.restart_cluster_after_shrinking()
            current_service.delete()
            StorageDriverController._configure_arakoon_to_volumedriver(cluster_name=cluster_name)

    @staticmethod
    @add_hooks('noderemoval', 'remove')
    def _on_remove(cluster_ip, complete_removal):
        """
        Handles the StorageDriver removal part of a node
        :param cluster_ip: IP of the node which is being removed from the cluster
        :type cluster_ip: str
        :param complete_removal: Unused for StorageDriver, used for AlbaController
        :type complete_removal: bool
        :return: None
        """
        _ = complete_removal

        service_manager = ServiceFactory.get_manager()
        service_name = 'watcher-volumedriver'
        try:
            client = SSHClient(endpoint=cluster_ip, username='root')
            if service_manager.has_service(name=service_name, client=client):
                service_manager.stop_service(name=service_name, client=client)
                service_manager.remove_service(name=service_name, client=client)
        except (UnableToConnectException, NotAuthenticatedException):
            pass

    ####################
    # PUBLIC FUNCTIONS #
    ####################
    @staticmethod
    def add_storagedriverpartition(storagedriver, partition_info):
        """
        Stores new storagedriver partition object with correct number
        :param storagedriver: Storagedriver to create the partition for
        :type storagedriver: StorageDriver
        :param partition_info: Partition information containing, role, size, sub_role, disk partition, MDS service
        :type partition_info: dict
        :return: Newly created storage driver partition
        :rtype: StorageDriverPartition
        """
        role = partition_info['role']
        size = partition_info.get('size')
        sub_role = partition_info.get('sub_role')
        partition = partition_info['partition']
        mds_service = partition_info.get('mds_service')
        highest_number = 0
        for existing_sdp in storagedriver.partitions:
            if existing_sdp.partition_guid == partition.guid and existing_sdp.role == role and existing_sdp.sub_role == sub_role:
                highest_number = max(existing_sdp.number, highest_number)
        sdp = StorageDriverPartition()
        sdp.role = role
        sdp.size = size
        sdp.number = highest_number + 1
        sdp.sub_role = sub_role
        sdp.partition = partition
        sdp.mds_service = mds_service
        sdp.storagedriver = storagedriver
        sdp.save()
        return sdp

    #####################
    # PRIVATE FUNCTIONS #
    #####################
    @staticmethod
    def _voldrv_arakoon_checkup(create_cluster):
        def _add_service(service_storagerouter, arakoon_ports, service_name):
            """ Add a service to the storage router """
            new_service = Service()
            new_service.name = service_name
            new_service.type = service_type
            new_service.ports = arakoon_ports
            new_service.storagerouter = service_storagerouter
            new_service.save()
            return new_service

        current_ips = []
        current_services = []
        service_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON)
        cluster_name = Configuration.get('/ovs/framework/arakoon_clusters').get('voldrv')
        if cluster_name is not None:
            arakoon_service_name = ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name)
            for service in service_type.services:
                if service.name == arakoon_service_name:
                    current_services.append(service)
                    if service.is_internal is True:
                        current_ips.append(service.storagerouter.ip)

        all_sr_ips = [storagerouter.ip for storagerouter in StorageRouterList.get_slaves()]
        available_storagerouters = {}
        for storagerouter in StorageRouterList.get_masters():
            storagerouter.invalidate_dynamics(['partition_config'])
            if len(storagerouter.partition_config[DiskPartition.ROLES.DB]) > 0:
                available_storagerouters[storagerouter] = DiskPartition(storagerouter.partition_config[DiskPartition.ROLES.DB][0])
            all_sr_ips.append(storagerouter.ip)

        if create_cluster is True and len(current_services) == 0:  # Create new cluster
            metadata = ArakoonInstaller.get_unused_arakoon_metadata_and_claim(cluster_type=ServiceType.ARAKOON_CLUSTER_TYPES.SD)
            if metadata is None:  # No externally managed cluster found, we create 1 ourselves
                if not available_storagerouters:
                    raise RuntimeError('Could not find any Storage Router with a DB role')

                storagerouter, partition = available_storagerouters.items()[0]
                arakoon_voldrv_cluster = 'voldrv'
                arakoon_installer = ArakoonInstaller(cluster_name=arakoon_voldrv_cluster)
                arakoon_installer.create_cluster(cluster_type=ServiceType.ARAKOON_CLUSTER_TYPES.SD,
                                                 ip=storagerouter.ip,
                                                 base_dir=partition.folder)
                arakoon_installer.start_cluster()
                ports = arakoon_installer.ports[storagerouter.ip]
                metadata = arakoon_installer.metadata
                current_ips.append(storagerouter.ip)
            else:
                ports = []
                storagerouter = None

            cluster_name = metadata['cluster_name']
            Configuration.set('/ovs/framework/arakoon_clusters|voldrv', cluster_name)
            StorageDriverController._logger.info('Claiming {0} managed arakoon cluster: {1}'.format('externally' if storagerouter is None else 'internally', cluster_name))
            StorageDriverController._configure_arakoon_to_volumedriver(cluster_name=cluster_name)
            current_services.append(_add_service(service_storagerouter=storagerouter,
                                                 arakoon_ports=ports,
                                                 service_name=ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name)))

        cluster_name = Configuration.get('/ovs/framework/arakoon_clusters').get('voldrv')
        if cluster_name is None:
            return
        metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)
        if 0 < len(current_services) < len(available_storagerouters) and metadata['internal'] is True:
            for storagerouter, partition in available_storagerouters.iteritems():
                if storagerouter.ip in current_ips:
                    continue
                arakoon_installer = ArakoonInstaller(cluster_name=cluster_name)
                arakoon_installer.load()
                arakoon_installer.extend_cluster(new_ip=storagerouter.ip,
                                                 base_dir=partition.folder)
                _add_service(service_storagerouter=storagerouter,
                             arakoon_ports=arakoon_installer.ports[storagerouter.ip],
                             service_name=ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name))
                current_ips.append(storagerouter.ip)
                arakoon_installer.restart_cluster_after_extending(new_ip=storagerouter.ip)
            StorageDriverController._configure_arakoon_to_volumedriver(cluster_name=cluster_name)

    @staticmethod
    def _configure_arakoon_to_volumedriver(cluster_name):
        StorageDriverController._logger.info('Update existing vPools')
        config = ArakoonClusterConfig(cluster_id=cluster_name)
        arakoon_nodes = []
        for node in config.nodes:
            arakoon_nodes.append({'host': node.ip,
                                  'port': node.client_port,
                                  'node_id': node.name})
        if Configuration.dir_exists('/ovs/vpools'):
            for vpool_guid in Configuration.list('/ovs/vpools'):
                for storagedriver_id in Configuration.list('/ovs/vpools/{0}/hosts'.format(vpool_guid)):
                    storagedriver_config = StorageDriverConfiguration(vpool_guid, storagedriver_id)
                    storagedriver_config.configure_volume_registry(vregistry_arakoon_cluster_id=cluster_name,
                                                                   vregistry_arakoon_cluster_nodes=arakoon_nodes)
                    storagedriver_config.configure_distributed_lock_store(dls_type='Arakoon',
                                                                          dls_arakoon_cluster_id=cluster_name,
                                                                          dls_arakoon_cluster_nodes=arakoon_nodes)
                    storagedriver_config.save()

    @staticmethod
    def create_new_storagedriver(vpool_guid, storagerouter_guid, storage_ip, amount_of_proxies):
        """
        Prepares a new Storagedriver for a given vPool and Storagerouter
        :param vpool_guid: Guid of the vPool
        :param storagerouter_guid: Guid of the Storagerouter
        :param storage_ip: IP for the Storagedriver
        :param amount_of_proxies: Amount of the proxies to configure for this Storagedriver
        :return: The new Storagedriver
        :rtype: ovs.dal.hybrids.storagedriver.StorageDriver
        """
        vpool = VPool(vpool_guid)
        storagerouter = StorageRouter(storagerouter_guid)

        client = SSHClient(storagerouter)
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

            vrouter_id = '{0}{1}'.format(vpool.name, machine_id)
            storagedriver = StorageDriver()
            storagedriver.name = vrouter_id.replace('_', ' ')
            storagedriver.ports = {'management': ports[0],
                                   'xmlrpc': ports[1],
                                   'dtl': ports[2],
                                   'edge': ports[3]}
            storagedriver.vpool = vpool
            storagedriver.cluster_ip = Configuration.get('/ovs/framework/hosts/{0}/ip'.format(machine_id))
            storagedriver.storage_ip = storage_ip
            storagedriver.mountpoint = '/mnt/{0}'.format(vpool.name)
            storagedriver.description = storagedriver.name
            storagedriver.storagerouter = storagerouter
            storagedriver.storagedriver_id = vrouter_id
            storagedriver.save()

            # ALBA Proxies
            proxy_service_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ALBA_PROXY)
            for proxy_id in xrange(amount_of_proxies):
                service = Service()
                service.storagerouter = storagerouter
                service.ports = [ports[4 + proxy_id]]
                service.name = 'albaproxy_{0}_{1}'.format(vpool.name, proxy_id)
                service.type = proxy_service_type
                service.save()
                alba_proxy = AlbaProxy()
                alba_proxy.service = service
                alba_proxy.storagedriver = storagedriver
                alba_proxy.save()

        return storagedriver

    @staticmethod
    def configure_storagedriver_partitions(storagedriver_guid, fragment_cache_settings, block_cache_settings, mountpoint_settings,
                                           amount_of_proxies, partition_info=None):
        """
        Configure all partitions for a storagedriver
        Attempts to clean up in case of errors
        :param storagedriver_guid: Guid of the Storagedriver
        :type storagedriver_guid: str
        :param fragment_cache_settings: Settings of the fragment cache ({is_backend: bool, read: bool, write: bool})
        :type fragment_cache_settings: dict
        :param block_cache_settings: Settings of the block cache ({is_backend: bool, read: bool, write: bool})
        :type block_cache_settings: dict
        :param mountpoint_settings: Settings about the cache ({mountpoint_cache: , writecache_size_requested: , largest_write_mountpoint}
        :type mountpoint_settings: dict
        :param amount_of_proxies: Amount of proxies to deploy
        :type amount_of_proxies: int
        :param partition_info: Information about the partitions (Optional, won't query for the info if supplied)
        :type partition_info: dict
        :raises: ValueError: - When calculating the cache sizes went wrong
        :return: Dict with information about the created items
        :rtype: dict
        """
        # * Information about backoff_gap and trigger_gap (Reason for 'smallest_write_partition' introduction)
        # * Once the free space on a mount point is < trigger_gap (default 1GiB), it will be cleaned up and the cleaner attempts to
        # * make sure that <backoff_gap> free space is available => backoff_gap must be <= size of the partition
        # * Both backoff_gap and trigger_gap apply to each mount point individually, but cannot be configured on a per mount point base
        # Assign WRITE / Fragment cache
        from ovs.lib.storagerouter import StorageRouterController  # Avoid circular reference
        storagedriver = StorageDriver(storagedriver_guid)
        storagerouter = storagedriver.storagerouter
        if partition_info is None:
            partition_info = StorageRouterController.get_partition_info(storagerouter.guid)

        usable_write_partitions = StorageRouterController.get_usable_partitions(storagedriver.storagerouter_guid, DiskPartition.ROLES.WRITE, partition_info)
        writecache_size_available = sum(part['available'] for part in usable_write_partitions)
        root_client = SSHClient(storagerouter, username='root')

        cache_size = None
        storagedriver_partition_caches = []
        write_caches = []
        smallest_write_partition = None
        dirs_to_create = []
        try:
            for writecache_info in usable_write_partitions:
                available = writecache_info['available']
                partition = DiskPartition(writecache_info['guid'])
                proportion = available * 100.0 / writecache_size_available
                size_to_be_used = proportion * mountpoint_settings['writecache_size_requested'] / 100
                write_cache_percentage = 0.98
                if mountpoint_settings['mountpoint_cache'] is not None and partition == mountpoint_settings['mountpoint_cache']:
                    if fragment_cache_settings['read'] is True or fragment_cache_settings['write'] is True or block_cache_settings['read'] is True or block_cache_settings['write'] is True:  # Only in this case we actually make use of the fragment caching
                        cache_size = int(size_to_be_used * 0.10)  # Bytes
                        write_cache_percentage = 0.88
                    for _ in xrange(amount_of_proxies):
                        storagedriver_partition_cache = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                                                           'role': DiskPartition.ROLES.WRITE,
                                                                                                                           'sub_role': StorageDriverPartition.SUBROLE.FCACHE,
                                                                                                                           'partition': partition})
                        dirs_to_create.append(storagedriver_partition_cache.path)
                        for subfolder in ['fc', 'bc']:
                            dirs_to_create.append('{0}/{1}'.format(storagedriver_partition_cache.path, subfolder))
                        storagedriver_partition_caches.append(storagedriver_partition_cache)

                w_size = int(size_to_be_used * write_cache_percentage / 1024 / 4096) * 4096
                # noinspection PyArgumentList
                sdp_write = StorageDriverController.add_storagedriverpartition(storagedriver,
                                                                               {'size': long(size_to_be_used),
                                                                                'role': DiskPartition.ROLES.WRITE,
                                                                                'sub_role': StorageDriverPartition.SUBROLE.SCO,
                                                                                'partition': partition})
                write_caches.append({'path': sdp_write.path,
                                    'size': '{0}KiB'.format(w_size)})
                dirs_to_create.append(sdp_write.path)
                if smallest_write_partition is None or (w_size * 1024) < smallest_write_partition:
                    smallest_write_partition = w_size * 1024

            storagedriver_partition_file_driver = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                                                     'role': DiskPartition.ROLES.WRITE,
                                                                                                                     'sub_role': StorageDriverPartition.SUBROLE.FD,
                                                                                                                     'partition': mountpoint_settings['largest_write_mountpoint']})
            dirs_to_create.append(storagedriver_partition_file_driver.path)

            # Assign DB
            db_info = partition_info[DiskPartition.ROLES.DB][0]
            storagedriver_partition_tlogs = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                                               'role': DiskPartition.ROLES.DB,
                                                                                                               'sub_role': StorageDriverPartition.SUBROLE.TLOG,
                                                                                                               'partition': DiskPartition(db_info['guid'])})
            storagedriver_partition_metadata = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                                                  'role': DiskPartition.ROLES.DB,
                                                                                                                  'sub_role': StorageDriverPartition.SUBROLE.MD,
                                                                                                                  'partition': DiskPartition(db_info['guid'])})
            dirs_to_create.append(storagedriver_partition_tlogs.path)
            dirs_to_create.append(storagedriver_partition_metadata.path)

            # Assign DTL
            dtl_info = partition_info[DiskPartition.ROLES.DTL][0]
            storagedriver_partition_dtl = StorageDriverController.add_storagedriverpartition(storagedriver, {'size': None,
                                                                                                             'role': DiskPartition.ROLES.DTL,
                                                                                                             'partition': DiskPartition(dtl_info['guid'])})
            dirs_to_create.append(storagedriver_partition_dtl.path)
            dirs_to_create.append(storagedriver.mountpoint)

            gap_configuration = StorageDriverController.generate_backoff_gap_settings(smallest_write_partition)

            if cache_size is None and \
                    ((fragment_cache_settings['is_backend'] is False and (fragment_cache_settings['read'] is True or fragment_cache_settings['write'] is True))
                     or (block_cache_settings['is_backend'] is False and (block_cache_settings['read'] is True or block_cache_settings['write'] is True))):
                raise ValueError('Something went wrong trying to calculate the cache sizes')

            root_client.dir_create(dirs_to_create)
        except Exception:
            StorageDriverController._logger.exception('Something went wrong during the creation of directories for vPool {0} on StorageRouter {1}'
                                                      .format(storagedriver.vpool.name, storagerouter.name))
            failed_deletes = []
            # Delete directories
            for dir_to_create in dirs_to_create:
                try:
                    root_client.dir_delete(directories=dir_to_create)
                except Exception:
                    failed_deletes.append(dirs_to_create)
            # Delete relations
            for sdp in storagedriver.partitions:
                sdp.delete()
            if len(failed_deletes) > 0:
                StorageDriverController._logger.warning('Failed to clean up following directories: {0}'.format(', '.join(failed_deletes)))
            raise
        storagedriver_partitions = {'cache': storagedriver_partition_caches,
                                    'dtl': storagedriver_partition_dtl,
                                    'tlogs': storagedriver_partition_tlogs}
        return {'gap_configuration': gap_configuration,
                'created_dirs': dirs_to_create,
                'storagedriver_partitions': storagedriver_partitions,
                'cache_size': cache_size,
                'write_caches': write_caches}

    @staticmethod
    def generate_backoff_gap_settings(cache_size):
        """
        Generates decent gap sizes for a given cache size
        :param cache_size: Size of the cache on which the gap sizes should be based
        :type cache_size: int
        :return: Dictionary with keys 'trigger' and 'backoff', containing their sizes in bytes
        :rtype: dict
        """
        if cache_size is None:
            StorageDriverController._logger.warning('Got request to calculate gaps for None as cache size. Returned default (2/1GiB)'.format(cache_size))
            return {'backoff': 2 * 1024 ** 3,
                    'trigger': 1 * 1024 ** 3}
        gap_configuration = {}
        # Below "settings" = [factor of smallest partition size, maximum size in GiB, minimum size in bytes]
        for gap, gap_settings in {'backoff': [0.1, 50, 2],
                                  'trigger': [0.08, 40, 1]}.iteritems():
            current_config = int(cache_size * gap_settings[0])
            current_config = min(current_config, gap_settings[1] * 1024 ** 3)
            current_config = max(current_config, gap_settings[2])
            gap_configuration[gap] = current_config
        return gap_configuration

    @staticmethod
    def configure_storagedriver(storagedriver_guid, storagedriver_settings, write_caches, gap_configuration):
        """
        Configures the volumedriver with requested settings
        :param storagedriver_guid: Guid of the Storagedriver
        :type storagedriver_guid: str
        :param storagedriver_settings: Dict with information about the storagedriver (Eg: {sco_size: dtl_mode: cluster_size: dtl_transport: volume_write_buffer: })
        :type storagedriver_settings: dict
        :param write_caches: Write caches that were prepared
        :type: list
        :param gap_configuration: Trigger and backoff gap
        :type gap_configuration: dict
        :return: None
        :rtype: NoneType
        """
        storagedriver = StorageDriver(storagedriver_guid)
        storagerouter = storagedriver.storagerouter
        vpool = storagedriver.vpool

        client = SSHClient(storagerouter)
        machine_id = System.get_my_machine_id(client)
        vrouter_id = '{0}{1}'.format(vpool.name, machine_id)

        storagedriver_partition_file_driver = next(StorageDriverController.get_partitions_by_role(storagedriver_guid, DiskPartition.ROLES.WRITE, StorageDriverPartition.SUBROLE.FD))
        storagedriver_partition_dtl = next(StorageDriverController.get_partitions_by_role(storagedriver_guid, DiskPartition.ROLES.DTL))
        storagedriver_partition_tlogs = next(StorageDriverController.get_partitions_by_role(storagedriver_guid, DiskPartition.ROLES.DB, StorageDriverPartition.SUBROLE.TLOG))
        storagedriver_partition_metadata = next(StorageDriverController.get_partitions_by_role(storagedriver_guid, DiskPartition.ROLES.DB, StorageDriverPartition.SUBROLE.MD))

        sco_size = storagedriver_settings['sco_size']
        dtl_mode = storagedriver_settings['dtl_mode']
        cluster_size = storagedriver_settings['cluster_size']
        dtl_transport = storagedriver_settings['dtl_transport']
        tlog_multiplier = StorageDriverClient.TLOG_MULTIPLIER_MAP[sco_size]
        # sco_factor = volume write buffer / tlog multiplier (default 20) / sco size (in MiB)
        sco_factor = float(storagedriver_settings['volume_write_buffer']) / tlog_multiplier / sco_size

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

        volume_manager_config = {'tlog_path': storagedriver_partition_tlogs.path,
                                 'metadata_path': storagedriver_partition_metadata.path,
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
            queue_urls.append({'amqp_uri': '{0}://{1}:{2}@{3}:5672'.format(mq_protocol, mq_user, mq_password,
                                                                           current_storagerouter.ip)})

        backend_connection_manager = {'backend_type': 'MULTI',
                                      'backend_interface_retries_on_error': 5,
                                      'backend_interface_retry_interval_secs': 1,
                                      'backend_interface_retry_backoff_multiplier': 2.0}
        for index, proxy in enumerate(sorted(storagedriver.alba_proxies, key=lambda k: k.service.ports[0])):
            backend_connection_manager[str(index)] = {'alba_connection_host': storagedriver.storage_ip,
                                                      'alba_connection_port': proxy.service.ports[0],
                                                      'alba_connection_preset': vpool.metadata['backend']['backend_info']['preset'],
                                                      'alba_connection_timeout': 15,
                                                      'alba_connection_use_rora': True,
                                                      'alba_connection_transport': 'TCP',
                                                      'alba_connection_rora_manifest_cache_capacity': 5000,
                                                      'alba_connection_asd_connection_pool_capacity': 20,
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
                         'vrouter_sco_multiplier': sco_size * 1024 / cluster_size,
                         # sco multiplier = SCO size (in MiB) / cluster size (currently 4KiB),
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
        storagedriver_config.configure_scocache(scocache_mount_points=write_caches,
                                                trigger_gap=Toolbox.convert_to_human_readable(size=gap_configuration['trigger']),
                                                backoff_gap=Toolbox.convert_to_human_readable(size=gap_configuration['backoff']))
        storagedriver_config.configure_distributed_transaction_log(dtl_path=storagedriver_partition_dtl.path,  # Not used, but required
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
        storagedriver_config.configure_file_driver(fd_cache_path=storagedriver_partition_file_driver.path,
                                                   fd_extent_cache_capacity='1024',
                                                   fd_namespace='fd-{0}-{1}'.format(vpool.name, vpool.guid))
        storagedriver_config.configure_event_publisher(events_amqp_routing_key=Configuration.get('/ovs/framework/messagequeue|queues.storagedriver'),
                                                       events_amqp_uris=queue_urls)
        storagedriver_config.configure_threadpool_component(num_threads=16)
        storagedriver_config.configure_network_interface(network_max_neighbour_distance=StorageDriver.DISTANCES.FAR - 1)
        storagedriver_config.save(client)

    @staticmethod
    def calculate_update_impact(storagedriver_guid, requested_config):
        """
        Calculate the impact of the update for a storagedriver with a given config
        :param storagedriver_guid: Guid of the storage driver
        :type storagedriver_guid: str
        :param requested_config: requested configuration
        :type requested_config: dict
        :return: dict indicating what will be needed to be done
        :rtype: dict
        """
        storagedriver = StorageDriver(storagedriver_guid)
        # Get a difference between the current config and the requested one

        return {}

    @staticmethod
    def get_partitions_by_role(storagedriver_guid, role, sub_role=None):
        """
        Fetches all partitions for a Storagedriver that has a specific role (and optionally a sub role)
        :param storagedriver_guid: Guid of the StorageDriver
        :param role: Role of the partition
        :type role: str (ovs.dal.hybrids.DiskPartition.ROLES)
        :param sub_role: Sub role of the partition
        :type sub_role: str (ovs.dal.hybrids.j_storagedriverpartition.StorageDriverPartition.SUBROLE)
        :return: Generator object which yields StorageDriverPartition objects
        :rtype: Generator
        """
        storagedriver = StorageDriver(storagedriver_guid)
        for storagedriver_partition in storagedriver.partitions:
            if storagedriver_partition.role == role and (sub_role is None or storagedriver_partition.sub_role == sub_role):
                yield storagedriver_partition

    @staticmethod
    def setup_proxy_configs(vpool_guid, storagedriver_guid, cache_size, local_amount_of_proxies, storagedriver_partitions_caches):
        """
        Sets up the proxies their configuration data in the configuration management
        :param vpool_guid: Guid of the vPool to deploy proxies for
        :param storagedriver_guid: Guid of the Storagedriver to deploy proxies for
        :param cache_size: Size of the cache to use for caching (Fragment and/or block cache)
        :param local_amount_of_proxies: Amount of proxies to deploy
        :param storagedriver_partitions_caches: list of StorageDriverPartitions which were created for caching purposes
        :return: None
        :rtype: NoneType
        """
        # @Todo figure out a way to no longer depend on sdp_caches. Currently depending on it as the order of the last matters
        from ovs.lib.storagerouter import StorageRouterController  # Avoid circular reference
        vpool = VPool(vpool_guid)
        storagedriver = StorageDriver(storagedriver_guid)
        # Metadata is always saved before configuring so the read preference can be calculated based on the vpool metadata
        read_preferences = VPoolController.calculate_read_preferences(vpool.guid, storagedriver.storagerouter_guid)

        block_cache_settings = vpool.metadata['caching_info'][storagedriver.storagerouter_guid]['block_cache']
        fragment_cache_settings = vpool.metadata['caching_info'][storagedriver.storagerouter_guid]['fragment_cache']

        # Validate features
        supports_block_cache = StorageRouterController.supports_block_cache(storagedriver.storagerouter_guid)
        if supports_block_cache is False and (block_cache_settings['read'] is True or block_cache_settings['write'] is True):
            raise RuntimeError('Block cache is not a supported feature')

        # Configure regular proxies and scrub proxies
        manifest_cache_size = 16 * 1024 ** 3
        arakoon_data = {'abm': vpool.metadata['backend']['backend_info']['arakoon_config']}
        if fragment_cache_settings['is_backend'] is True:
            arakoon_data['abm_aa'] = fragment_cache_settings['backend_info']['arakoon_config']
        if block_cache_settings['is_backend'] is True:
            arakoon_data['abm_bc'] = block_cache_settings['backend_info']['arakoon_config']

        for proxy_id, alba_proxy in enumerate(storagedriver.alba_proxies):
            config_tree = '/ovs/vpools/{0}/proxies/{1}/config/{{0}}'.format(vpool.guid, alba_proxy.guid)
            for arakoon_entry, arakoon_config in arakoon_data.iteritems():
                arakoon_config = ArakoonClusterConfig.convert_config_to(config=arakoon_config, return_type='INI')
                Configuration.set(config_tree.format(arakoon_entry), arakoon_config, raw=True)

            fragment_cache_scrub_info = ['none']
            if fragment_cache_settings['read'] is False and fragment_cache_settings['write'] is False:
                fragment_cache_info = ['none']
            elif fragment_cache_settings['is_backend'] is True:
                fragment_cache_info = ['alba', {
                    'albamgr_cfg_url': Configuration.get_configuration_path(config_tree.format('abm_aa')),
                    'bucket_strategy': ['1-to-1', {'prefix': vpool.guid,
                                                   'preset': fragment_cache_settings['backend_info']['preset']}],
                    'manifest_cache_size': manifest_cache_size,
                    'cache_on_read': fragment_cache_settings['read'],
                    'cache_on_write': fragment_cache_settings['write']}]
                if fragment_cache_settings['write'] is True:
                    # The scrubbers want only cache-on-write.
                    fragment_cache_scrub_info = copy.deepcopy(fragment_cache_info)
                    fragment_cache_scrub_info[1]['cache_on_read'] = False
            else:
                fragment_cache_info = ['local', {'path': '{0}/fc'.format(storagedriver_partitions_caches[proxy_id].path),
                                                 'max_size': cache_size / local_amount_of_proxies,
                                                 'cache_on_read': fragment_cache_settings['read'],
                                                 'cache_on_write': fragment_cache_settings['write']}]

            block_cache_scrub_info = ['none']
            if block_cache_settings['read'] is False and block_cache_settings['write'] is False:
                block_cache_info = ['none']
            elif block_cache_settings['is_backend'] is True:
                block_cache_info = ['alba', {
                    'albamgr_cfg_url': Configuration.get_configuration_path(config_tree.format('abm_bc')),
                    'bucket_strategy': ['1-to-1', {'prefix': '{0}_bc'.format(vpool.guid),
                                                   'preset': block_cache_settings['backend_info']['preset']}],
                    'manifest_cache_size': manifest_cache_size,
                    'cache_on_read': block_cache_settings['read'],
                    'cache_on_write': block_cache_settings['write']}]
                if block_cache_settings['write'] is True:
                    # The scrubbers want only cache-on-write.
                    block_cache_scrub_info = copy.deepcopy(block_cache_info)
                    block_cache_scrub_info[1]['cache_on_read'] = False
            else:
                block_cache_info = ['local', {'path': '{0}/bc'.format(storagedriver_partitions_caches[proxy_id].path),
                                              'max_size': cache_size / local_amount_of_proxies,
                                              'cache_on_read': block_cache_settings['read'],
                                              'cache_on_write': block_cache_settings['write']}]

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
            Configuration.set(config_tree.format('main'), json.dumps(main_proxy_config, indent=4), raw=True)
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
            Configuration.set('/ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(vpool.guid), json.dumps(scrub_proxy_config, indent=4), raw=True)

    @staticmethod
    def start_services(storagedriver_guid):
        """
        Starts all services related to the Storagedriver
        :param storagedriver_guid: Guid of the Storagedriver
        :type storagedriver_guid: str
        :return: None
        :rtype: NoneType
        """
        storagedriver = StorageDriver(storagedriver_guid)
        storagerouter = storagedriver.storagerouter
        vpool = storagedriver.vpool

        root_client = SSHClient(storagerouter, username='root')
        client = SSHClient(storagerouter)

        storagedriver_config = StorageDriverConfiguration(vpool.guid, storagedriver.storagedriver_id)
        storagedriver_partition_dtl = next(StorageDriverController.get_partitions_by_role(storagedriver_guid, DiskPartition.ROLES.DTL))
        # Configurations are already in place at this point
        vpool.invalidate_dynamics('configuration')
        dtl_transport = vpool.configuration['dtl_transport']

        sd_params = {'KILL_TIMEOUT': '30',
                     'VPOOL_NAME': vpool.name,
                     'VPOOL_MOUNTPOINT': storagedriver.mountpoint,
                     'CONFIG_PATH': storagedriver_config.remote_path,
                     'OVS_UID': client.run(['id', '-u', 'ovs']).strip(),
                     'OVS_GID': client.run(['id', '-g', 'ovs']).strip(),
                     'LOG_SINK': Logger.get_sink_path('storagedriver_{0}'.format(storagedriver.storagedriver_id)),
                     'METADATASTORE_BITS': 5}
        dtl_params = {'DTL_PATH': storagedriver_partition_dtl.path,
                      'DTL_ADDRESS': storagedriver.storage_ip,
                      'DTL_PORT': str(storagedriver.ports['dtl']),
                      'DTL_TRANSPORT': StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP[dtl_transport],
                      'LOG_SINK': Logger.get_sink_path('storagedriver-dtl_{0}'.format(storagedriver.storagedriver_id))}

        sd_service = 'ovs-volumedriver_{0}'.format(vpool.name)
        dtl_service = 'ovs-dtl_{0}'.format(vpool.name)

        service_manager = ServiceFactory.get_manager()
        watcher_volumedriver_service = 'watcher-volumedriver'
        try:
            if not service_manager.has_service(watcher_volumedriver_service, client=root_client):
                service_manager.add_service(watcher_volumedriver_service, client=root_client)
                service_manager.start_service(watcher_volumedriver_service, client=root_client)

            service_manager.add_service(name='ovs-dtl', params=dtl_params, client=root_client, target_name=dtl_service)
            service_manager.start_service(dtl_service, client=root_client)

            for proxy in storagedriver.alba_proxies:
                alba_proxy_params = {'VPOOL_NAME': vpool.name,
                                     'LOG_SINK': Logger.get_sink_path(proxy.service.name),
                                     'CONFIG_PATH': Configuration.get_configuration_path('/ovs/vpools/{0}/proxies/{1}/config/main'.format(vpool.guid, proxy.guid))}
                alba_proxy_service = 'ovs-{0}'.format(proxy.service.name)
                service_manager.add_service(name='ovs-albaproxy', params=alba_proxy_params, client=root_client,
                                            target_name=alba_proxy_service)
                service_manager.start_service(alba_proxy_service, client=root_client)

            service_manager.add_service(name='ovs-volumedriver', params=sd_params, client=root_client,
                                        target_name=sd_service)

            storagedriver = StorageDriver(storagedriver.guid)
            current_startup_counter = storagedriver.startup_counter
            service_manager.start_service(sd_service, client=root_client)
        except Exception:
            StorageDriverController._logger.exception('Failed to start the relevant services for vPool {0} on StorageRouter {1}'.format(vpool.name, storagerouter.name))
            raise

        tries = 60
        while storagedriver.startup_counter == current_startup_counter and tries > 0:
            StorageDriverController._logger.debug( 'Waiting for the StorageDriver to start up for vPool {0} on StorageRouter {1} ...'.format(vpool.name, storagerouter.name))
            if service_manager.get_service_status(sd_service, client=root_client) != 'active':
                raise RuntimeError('StorageDriver service failed to start (service not running)')
            tries -= 1
            time.sleep(60 - tries)
            storagedriver = StorageDriver(storagedriver.guid)
        if storagedriver.startup_counter == current_startup_counter:
            raise RuntimeError('StorageDriver service failed to start (got no event)')
        StorageDriverController._logger.debug('StorageDriver running')

    @staticmethod
    def calculate_global_write_buffer(storagedriver_guid):
        """
        Calculate the global write buffer for a given Storagedriver
        :param storagedriver_guid: Guid of the Storagedriver
        :return: Calculated global write buffer
        :rtype: int
        """
        storagedriver = StorageDriver(storagedriver_guid)
        global_write_buffer = 0
        for partition in storagedriver.partitions:
            if partition.role == DiskPartition.ROLES.WRITE and partition.sub_role == StorageDriverPartition.SUBROLE.SCO:
                global_write_buffer += partition.size
        return global_write_buffer
