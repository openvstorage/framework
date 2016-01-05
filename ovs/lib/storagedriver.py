# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
StorageDriver module
"""

import os
import volumedriver.storagerouter.VolumeDriverEvents_pb2 as VolumeDriverEvents
from ConfigParser import RawConfigParser
from celery.schedules import crontab
from ovs.celery_run import celery
from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.service import Service
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.lists.servicelist import ServiceList
from ovs.extensions.storageserver.storagedriver import StorageDriverClient, StorageDriverConfiguration
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonInstaller, ArakoonClusterConfig
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.remote import Remote
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.generic.configuration import Configuration
from ovs.lib.helpers.decorators import log
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.helpers.decorators import ensure_single, add_hooks
from ovs.log.logHandler import LogHandler
from volumedriver.storagerouter.storagerouterclient import LocalStorageRouterClient

logger = LogHandler.get('lib', name='storagedriver')


class StorageDriverController(object):
    """
    Contains all BLL related to Storage Drivers
    """

    @staticmethod
    @celery.task(name='ovs.storagedriver.move_away')
    def move_away(storagerouter_guid):
        """
        Moves away all vDisks from all Storage Drivers this Storage Router is serving
        :param storagerouter_guid: Guid of the Storage Router
        """
        storagedrivers = StorageRouter(storagerouter_guid).storagedrivers
        if len(storagedrivers) > 0:
            storagedriver_client = StorageDriverClient.load(storagedrivers[0].vpool)
            for storagedriver in storagedrivers:
                storagedriver_client.mark_node_offline(str(storagedriver.storagedriver_id))

    @staticmethod
    @celery.task(name='ovs.storagedriver.update_status')
    @log('VOLUMEDRIVER_TASK')
    def update_status(storagedriver_id):
        """
        Sets Storage Driver offline in case hypervisor management Center
        reports the hypervisor pmachine related to this Storage Driver
        as unavailable.
        :param storagedriver_id: ID of the storagedriver to update its status
        """
        pmachine = PMachineList.get_by_storagedriver_id(storagedriver_id)
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        storagerouter = storagedriver.storagerouter
        if pmachine.mgmtcenter:
            # Update status
            pmachine.invalidate_dynamics(['host_status'])
        else:
            # No management Center, cannot update status via api
            logger.info('Updating status of pmachine {0} using SSHClient'.format(pmachine.name))
            host_status = 'RUNNING'
            try:
                client = SSHClient(storagerouter, username='root')
                configuration_dir = client.config_read('ovs.core.cfgdir')
                logger.info('SSHClient connected successfully to {0} at {1}'.format(pmachine.name, client.ip))
                with Remote(client.ip, [LocalStorageRouterClient]) as remote:
                    lsrc = remote.LocalStorageRouterClient('{0}/storagedriver/storagedriver/{1}.json'.format(configuration_dir,
                                                                                                             storagedriver.vpool.name))
                    lsrc.server_revision()
                logger.info('LocalStorageRouterClient connected successfully to {0} at {1}'.format(pmachine.name, client.ip))
            except Exception as ex:
                logger.error('Connectivity check failed, assuming host {0} is halted. {1}'.format(pmachine.name, ex))
                host_status = 'HALTED'
            if host_status != 'RUNNING':
                # Host is stopped
                storagedriver_client = StorageDriverClient.load(storagedriver.vpool)
                storagedriver_client.mark_node_offline(str(storagedriver.storagedriver_id))

    @staticmethod
    @celery.task(name='ovs.storagedriver.volumedriver_error')
    @log('VOLUMEDRIVER_TASK')
    def volumedriver_error(code, volumename):
        """
        Handles error messages/events from the volumedriver
        :param code: Volumedriver error code
        :param volumename: Name of the volume throwing the error
        """
        if code == VolumeDriverEvents.MDSFailover:
            disk = VDiskList.get_vdisk_by_volume_id(volumename)
            if disk is not None:
                MDSServiceController.ensure_safety(disk)

    @staticmethod
    @add_hooks('setup', 'demote')
    def on_demote(cluster_ip, master_ip, offline_node_ips=None):
        """
        Handles the demote for the StorageDrivers
        :param cluster_ip: IP of the node to demote
        :param master_ip: IP of the master node
        :param offline_node_ips: IPs of nodes which are offline
        """
        if offline_node_ips is None:
            offline_node_ips = []
        client = SSHClient(cluster_ip, username='root') if cluster_ip not in offline_node_ips else None
        servicetype = ServiceTypeList.get_by_name('Arakoon')
        current_service = None
        remaining_ips = []
        for service in servicetype.services:
            if service.name == 'arakoon-voldrv':
                if service.storagerouter.ip == cluster_ip:
                    current_service = service
                elif service.storagerouter.ip not in offline_node_ips:
                    remaining_ips.append(service.storagerouter.ip)
        if current_service is not None:
            print '* Shrink StorageDriver cluster'
            ArakoonInstaller.shrink_cluster(master_ip, cluster_ip, 'voldrv', offline_node_ips)
            if client is not None and ServiceManager.has_service(current_service.name, client=client) is True:
                ServiceManager.stop_service(current_service.name, client=client)
                ServiceManager.remove_service(current_service.name, client=client)
            ArakoonInstaller.restart_cluster_remove('voldrv', remaining_ips)
            current_service.delete()
            for storagerouter in StorageRouterList.get_storagerouters():
                if storagerouter.ip not in offline_node_ips and storagerouter.ip != master_ip:
                    ArakoonInstaller.deploy_to_slave(master_ip, storagerouter.ip, 'voldrv')
            StorageDriverController._configure_arakoon_to_volumedriver(offline_node_ips)

    @staticmethod
    @add_hooks('setup', 'extranode')
    def on_extranode(cluster_ip, master_ip=None):
        """
        An extra node is added, make sure it has the voldrv arakoon client file if possible
        :param cluster_ip: IP of the extra node
        :param master_ip: IP of the master node
        """
        _ = master_ip  # The master_ip will be passed in by caller
        deployed = False
        client_list = []
        service_found = False
        servicetype = ServiceTypeList.get_by_name('Arakoon')
        for service in servicetype.services:
            if service.name == 'arakoon-voldrv':
                service_found = True
                if service.storagerouter not in client_list:
                    try:
                        SSHClient(service.storagerouter)
                        client_list.append(service.storagerouter)
                    except UnableToConnectException:
                        continue
                ArakoonInstaller.deploy_to_slave(service.storagerouter.ip, cluster_ip, 'voldrv')
                deployed = True
                break
        if service_found is True and deployed is False:
            raise RuntimeError('Failed to deploy arakoon config for voldrv cluster to slave with IP {0}'.format(cluster_ip))

    @staticmethod
    @celery.task(name='ovs.storagedriver.scheduled_voldrv_arakoon_checkup', schedule=crontab(minute='15', hour='*'))
    def scheduled_voldrv_arakoon_checkup():
        """
        Makes sure the volumedriver arakoon is on all available master nodes
        """
        StorageDriverController._voldrv_arakoon_checkup(False)

    @staticmethod
    @celery.task(name='ovs.storagedriver.manual_voldrv_arakoon_checkup')
    def manual_voldrv_arakoon_checkup():
        """
        Creates a new Arakoon Cluster if required and extends cluster if possible on all available master nodes
        """
        StorageDriverController._voldrv_arakoon_checkup(True)

    @staticmethod
    @ensure_single(task_name='ovs.storagedriver.voldrv_arakoon_checkup')
    def _voldrv_arakoon_checkup(create_cluster):
        def add_service(service_storagerouter, arakoon_result):
            """
            Add a service to the storage router
            :param service_storagerouter: Storage Router to add the service to
            :type service_storagerouter:  StorageRouter

            :param arakoon_result:        Port information
            :type arakoon_result:         Dictionary

            :return:                      The newly created and added service
            """
            new_service = Service()
            new_service.name = service_name
            new_service.type = service_type
            new_service.ports = [arakoon_result['client_port'], arakoon_result['messaging_port']]
            new_service.storagerouter = service_storagerouter
            new_service.save()
            return new_service

        cluster_name = 'voldrv'
        service_name = 'arakoon-voldrv'
        service_type = ServiceTypeList.get_by_name('Arakoon')
        current_services = []
        current_ips = []
        for service in service_type.services:
            if service.name == service_name:
                current_services.append(service)
                current_ips.append(service.storagerouter.ip)
        all_sr_ips = [storagerouter.ip for storagerouter in StorageRouterList.get_slaves()]
        available_storagerouters = {}
        for storagerouter in StorageRouterList.get_masters():
            storagerouter.invalidate_dynamics(['partition_config'])
            if len(storagerouter.partition_config[DiskPartition.ROLES.DB]) > 0:
                available_storagerouters[storagerouter] = DiskPartition(storagerouter.partition_config[DiskPartition.ROLES.DB][0])
            all_sr_ips.append(storagerouter.ip)
        if create_cluster is True and len(current_services) == 0 and len(available_storagerouters) > 0:
            storagerouter, partition = available_storagerouters.items()[0]
            result = ArakoonInstaller.create_cluster(cluster_name=cluster_name,
                                                     ip=storagerouter.ip,
                                                     base_dir=partition.folder)
            current_services.append(add_service(storagerouter, result))
            for sr_ip in all_sr_ips:
                if sr_ip not in current_ips:
                    ArakoonInstaller.deploy_to_slave(storagerouter.ip, sr_ip, cluster_name)
            ArakoonInstaller.restart_cluster_add(cluster_name, current_ips, storagerouter.ip)
            current_ips.append(storagerouter.ip)
            StorageDriverController._configure_arakoon_to_volumedriver()

        if 0 < len(current_services) < len(available_storagerouters):
            for storagerouter, partition in available_storagerouters.iteritems():
                if storagerouter.ip in current_ips:
                    continue
                result = ArakoonInstaller.extend_cluster(
                    current_services[0].storagerouter.ip,
                    storagerouter.ip,
                    cluster_name,
                    partition.folder
                )
                add_service(storagerouter, result)
                current_ips.append(storagerouter.ip)
                for sr_ip in all_sr_ips:
                    if sr_ip not in current_ips:
                        ArakoonInstaller.deploy_to_slave(current_services[0].storagerouter.ip, sr_ip, cluster_name)
                ArakoonInstaller.restart_cluster_add(cluster_name, current_ips, storagerouter.ip)
            StorageDriverController._configure_arakoon_to_volumedriver()

    @staticmethod
    def _configure_arakoon_to_volumedriver(offline_node_ips=None):
        print 'Update existing vPools'
        logger.info('Update existing vPools')
        if offline_node_ips is None:
            offline_node_ips = []
        for storagerouter in StorageRouterList.get_storagerouters():
            if storagerouter.ip in offline_node_ips:
                continue
            client = SSHClient(storagerouter.ip)
            config = ArakoonClusterConfig('voldrv')
            config.load_config(client)
            arakoon_nodes = []
            for node in config.nodes:
                arakoon_nodes.append({'host': node.ip,
                                      'port': node.client_port,
                                      'node_id': node.name})
            with Remote(storagerouter.ip, [os, RawConfigParser, Configuration, StorageDriverConfiguration], 'ovs') as remote:
                configuration_dir = '{0}/storagedriver/storagedriver'.format(
                    remote.Configuration.get('ovs.core.cfgdir'))
                if not remote.os.path.exists(configuration_dir):
                    remote.os.makedirs(configuration_dir)
                for json_file in remote.os.listdir(configuration_dir):
                    vpool_name = json_file.replace('.json', '')
                    if json_file.endswith('.json'):
                        if remote.os.path.exists('{0}/{1}.cfg'.format(configuration_dir, vpool_name)):
                            continue  # There's also a .cfg file, so this is an alba_proxy configuration file
                        storagedriver_config = remote.StorageDriverConfiguration('storagedriver', vpool_name)
                        storagedriver_config.load()
                        storagedriver_config.configure_volume_registry(vregistry_arakoon_cluster_id='voldrv',
                                                                       vregistry_arakoon_cluster_nodes=arakoon_nodes)
                        storagedriver_config.configure_distributed_lock_store(dls_type='Arakoon',
                                                                              dls_arakoon_cluster_id='voldrv',
                                                                              dls_arakoon_cluster_nodes=arakoon_nodes)
                        storagedriver_config.save(reload_config=True)

    @staticmethod
    def add_storagedriverpartition(storagedriver, partition_info):
        """
        Stores new storagedriver partition object with correct number
        :param storagedriver: Storagedriver to create the partition for
        :param partition_info: Partition information containing, role, size, sub_role, disk partition, MDS service
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
