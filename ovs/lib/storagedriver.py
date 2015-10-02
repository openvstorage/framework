# Copyright 2014 Open vStorage NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
StorageDriver module
"""

import volumedriver.storagerouter.VolumeDriverEvents_pb2 as VolumeDriverEvents
from ovs.celery_run import celery
from celery.schedules import crontab
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.service import Service
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.lists.servicelist import ServiceList
from ovs.extensions.storageserver.storagedriver import StorageDriverClient
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonInstaller
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.services.service import ServiceManager
from ovs.lib.helpers.decorators import log
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.helpers.decorators import ensure_single, add_hooks


class StorageDriverController(object):
    """
    Contains all BLL related to Storage Drivers
    """

    @staticmethod
    @celery.task(name='ovs.storagedriver.move_away')
    def move_away(storagerouter_guid):
        """
        Moves away all vDisks from all Storage Drivers this Storage Router is serving
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
        """
        pmachine = PMachineList.get_by_storagedriver_id(storagedriver_id)
        if pmachine.mgmtcenter:
            # Update status
            pmachine.invalidate_dynamics(['host_status'])
            host_status = pmachine.host_status
            if host_status != 'RUNNING':
                # Host is stopped
                storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
                storagedriver_client = StorageDriverClient.load(storagedriver.vpool)
                storagedriver_client.mark_node_offline(str(storagedriver.storagedriver_id))
        else:
            # No management Center, cannot update status via api
            # @TODO: should we try manually (ping, ssh)?
            pass

    @staticmethod
    @celery.task(name='ovs.storagedriver.volumedriver_error')
    @log('VOLUMEDRIVER_TASK')
    def volumedriver_error(code, volumename, storagedriver_id):
        """
        Handles error messages/events from the volumedriver
        """
        _ = storagedriver_id  # Required for the @log decorator
        if code == VolumeDriverEvents.MDSFailover:
            disk = VDiskList.get_vdisk_by_volume_id(volumename)
            if disk is not None:
                MDSServiceController.ensure_safety(disk)

    @staticmethod
    @add_hooks('setup', 'demote')
    def on_demote(cluster_ip, master_ip):
        """
        Handles the demote for the StorageDrivers
        """
        client = SSHClient(cluster_ip, username='root')
        servicetype = ServiceTypeList.get_by_name('Arakoon')
        current_service = None
        remaining_ips = []
        for service in servicetype.services:
            if service.name == 'arakoon-voldrv':
                if service.storagerouter.ip == cluster_ip:
                    current_service = service
                else:
                    remaining_ips.append(service.storagerouter.ip)
        if current_service is not None:
            print '* Shrink StorageDriver cluster'
            ArakoonInstaller.shrink_cluster(master_ip, cluster_ip, current_service.name)
            if ServiceManager.has_service(current_service.name, client=client) is True:
                ServiceManager.stop_service(current_service.name, client=client)
                ServiceManager.remove_service(current_service.name, client=client)
            ArakoonInstaller.restart_cluster_remove(current_service.name, remaining_ips)
            current_service.delete()

    @staticmethod
    @celery.task(name='ovs.storagedriver.scheduled_voldrv_arakoon_checkup', bind=True, schedule=crontab(minute='30', hour='*'))
    @ensure_single(['alba.nsm_checkup'])
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
    def _voldrv_arakoon_checkup(create_cluster):
        def add_service(service_storagerouter, service_partition, arakoon_result):
            new_service = Service()
            new_service.name = service_name
            new_service.type = service_type
            new_service.ports = [arakoon_result['client_port'], arakoon_result['messaging_port']]
            new_service.partition = service_partition
            new_service.storagerouter = service_storagerouter
            new_service.save()

        service_name = 'arakoon-voldrv'
        service_type = ServiceTypeList.get_by_name('Arakoon')
        current_services = []
        current_ips = []
        for service in service_type.services:
            if service.name == service_name:
                current_services.append(service)
                current_ips.append(service.storagerouter.ip)
        available_storagerouters = {}
        for storagerouter in StorageRouterList.get_masters():
            storagerouter.invalidate_dynamics(['partition_config'])
            if len(storagerouter.partition_config[DiskPartition.ROLES.DB]) > 0:
                available_storagerouters[storagerouter] = DiskPartition(storagerouter.partition_config[DiskPartition.ROLES.DB][0])
        if create_cluster is True and 0 < len(current_services) < len(available_storagerouters):
            storagerouter, partition = available_storagerouters.items()[0]
            result = ArakoonInstaller.create_cluster(cluster_name = service_name,
                                                     ip = storagerouter.ip,
                                                     exclude_ports = ServiceList.get_ports_for_ip(storagerouter.ip),
                                                     base_dir = partition.mountpoint,
                                                     plugins = None)
            add_service(storagerouter, partition, result)
            ArakoonInstaller.restart_cluster_add(service_name, current_ips, storagerouter.ip)
            current_ips.append(storagerouter.ip)

        if 0 < len(current_services) < len(available_storagerouters):
            for storagerouter, partition in available_storagerouters.iteritems():
                if storagerouter.ip in current_ips:
                    continue
                result = ArakoonInstaller.extend_cluster(
                    current_services[0].storagerouter.ip,
                    storagerouter.ip,
                    service_name,
                    ServiceList.get_ports_for_ip(storagerouter.ip),
                    partition.mountpoint
                )
                add_service(storagerouter, partition, result)
                ArakoonInstaller.restart_cluster_add(service_name, current_ips, storagerouter.ip)
                current_ips.append(storagerouter.ip)
