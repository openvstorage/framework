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

from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
from ovs.dal.hybrids.service import Service
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig, ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import NotAuthenticatedException, SSHClient, UnableToConnectException
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storageserver.storagedriver import ClusterNodeConfig, LocalStorageRouterClient, StorageDriverClient, StorageDriverConfiguration
from ovs.lib.helpers.decorators import add_hooks, log, ovs_task
from ovs.lib.helpers.toolbox import Schedule
from ovs.lib.mdsservice import MDSServiceController
from ovs.log.log_handler import LogHandler
from volumedriver.storagerouter import VolumeDriverEvents_pb2


class StorageDriverController(object):
    """
    Contains all BLL related to Storage Drivers
    """
    _logger = LogHandler.get('lib', name='storagedriver')

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
            disk = VDiskList.get_vdisk_by_volume_id(volume_id)
            if disk is not None:
                MDSServiceController.ensure_safety(disk)

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

        storagedriver_config = StorageDriverConfiguration(config_type='storagedriver',
                                                          vpool_guid=storagedriver.vpool_guid,
                                                          storagedriver_id=storagedriver.storagedriver_id)
        storagedriver_config.load()
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
                                                 base_dir=partition.folder,
                                                 log_sinks=LogHandler.get_sink_path('arakoon-server_{0}'.format(arakoon_voldrv_cluster)),
                                                 crash_log_sinks=LogHandler.get_sink_path('arakoon-server-crash_{0}'.format(arakoon_voldrv_cluster)))
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
                                                 base_dir=partition.folder,
                                                 log_sinks=LogHandler.get_sink_path('arakoon-server_{0}'.format(cluster_name)),
                                                 crash_log_sinks=LogHandler.get_sink_path('arakoon-server-crash_{0}'.format(cluster_name)))
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
                    storagedriver_config = StorageDriverConfiguration('storagedriver', vpool_guid, storagedriver_id)
                    storagedriver_config.load()
                    storagedriver_config.configure_volume_registry(vregistry_arakoon_cluster_id=cluster_name,
                                                                   vregistry_arakoon_cluster_nodes=arakoon_nodes)
                    storagedriver_config.configure_distributed_lock_store(dls_type='Arakoon',
                                                                          dls_arakoon_cluster_id=cluster_name,
                                                                          dls_arakoon_cluster_nodes=arakoon_nodes)
                    storagedriver_config.save()

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
        # Below "settings" = [factor of smallest parition size, maximum size in GiB, minimum size in bytes]
        for gap, gap_settings in {'backoff': [0.1, 50, 2],
                                  'trigger': [0.08, 40, 1]}.iteritems():
            current_config = int(cache_size * gap_settings[0])
            current_config = min(current_config, gap_settings[1] * 1024 ** 3)
            current_config = max(current_config, gap_settings[2])
            gap_configuration[gap] = current_config
        return gap_configuration
