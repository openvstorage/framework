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
VPool module
"""

from ovs.celery_run import celery
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.extensions.fs.exportfs import Nfsexports
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.generic.sshclient import UnableToConnectException
from ovs.extensions.hypervisor.factory import Factory
from ovs.extensions.storageserver.storagedriver import StorageDriverClient
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
from ovs.lib.vmachine import VMachineController
from ovs.lib.helpers.decorators import log
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('lib', name='vpool')


class VPoolController(object):
    """
    Contains all BLL related to VPools
    """

    @staticmethod
    @celery.task(name='ovs.vpool.up_and_running')
    @log('VOLUMEDRIVER_TASK')
    def up_and_running(mountpoint, storagedriver_id):
        """
        Volumedriver informs us that the service is completely started. Post-start events can be executed
        :param mountpoint: Mountpoint to check
        :param storagedriver_id: ID of the storagedriver
        """
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        if storagedriver is None:
            raise RuntimeError('A Storage Driver with id {0} could not be found.'.format(storagedriver_id))
        storagedriver.startup_counter += 1
        storagedriver.save()
        if storagedriver.storagerouter.pmachine.hvtype == 'VMWARE':
            client = SSHClient(storagedriver.storagerouter)
            machine_id = System.get_my_machine_id(client)
            if EtcdConfiguration.get('/ovs/framework/hosts/{0}/storagedriver|vmware_mode'.format(machine_id)) == 'classic':
                nfs = Nfsexports()
                nfs.unexport(mountpoint)
                nfs.export(mountpoint)
                nfs.trigger_rpc_mountd()

    @staticmethod
    @celery.task(name='ovs.vpool.sync_with_hypervisor')
    def sync_with_hypervisor(vpool_guid):
        """
        Syncs all vMachines of a given vPool with the hypervisor
        :param vpool_guid: Guid of the vPool to synchronize
        """
        vpool = VPool(vpool_guid)
        if vpool.status != VPool.STATUSES.RUNNING:
            raise ValueError('Synchronizing with hypervisor is only allowed if your vPool is in {0} status'.format(VPool.STATUSES.RUNNING))
        for storagedriver in vpool.storagedrivers:
            pmachine = storagedriver.storagerouter.pmachine
            hypervisor = Factory.get(pmachine)
            for vm_object in hypervisor.get_vms_by_nfs_mountinfo(storagedriver.storage_ip, storagedriver.mountpoint):
                search_vpool = None if pmachine.hvtype == 'KVM' else vpool
                vmachine = VMachineList.get_by_devicename_and_vpool(devicename=vm_object['backing']['filename'],
                                                                    vpool=search_vpool)
                VMachineController.update_vmachine_config(vmachine, vm_object, pmachine)

    @staticmethod
    @celery.task(name='ovs.vpool.get_configuration')
    def get_configuration(vpool_guid):
        """
        Retrieve the running storagedriver configuration for the vPool
        :param vpool_guid: Guid of the vPool to retrieve running configuration for
        :return: Dictionary with configuration
        """
        vpool = VPool(vpool_guid)
        if not vpool.storagedrivers or not vpool.storagedrivers[0].storagerouter:
            return {}

        client = None
        for sd in vpool.storagedrivers:
            try:
                client = SSHClient(sd.storagerouter)
                client.run('pwd')
                break
            except UnableToConnectException:
                client = None
                pass
        if client is None:
            raise RuntimeError('Could not find an online storage router to retrieve vPool configuration from')
        storagedriver_config = StorageDriverConfiguration('storagedriver', vpool.guid, vpool.storagedrivers[0].storagedriver_id)
        storagedriver_config.load()

        dtl = storagedriver_config.configuration.get('distributed_transaction_log', {})
        file_system = storagedriver_config.configuration.get('filesystem', {})
        volume_router = storagedriver_config.configuration.get('volume_router', {})
        volume_manager = storagedriver_config.configuration.get('volume_manager', {})

        dtl_mode = file_system.get('fs_dtl_mode', StorageDriverClient.VOLDRV_DTL_ASYNC)
        dedupe_mode = volume_manager.get('read_cache_default_mode', StorageDriverClient.VOLDRV_CONTENT_BASED)
        cluster_size = volume_manager.get('default_cluster_size', 4096) / 1024
        dtl_transport = dtl.get('dtl_transport', StorageDriverClient.VOLDRV_DTL_TRANSPORT_TCP)
        cache_strategy = volume_manager.get('read_cache_default_behaviour', StorageDriverClient.VOLDRV_CACHE_ON_READ)
        sco_multiplier = volume_router.get('vrouter_sco_multiplier', 1024)
        dtl_config_mode = file_system.get('fs_dtl_config_mode', StorageDriverClient.VOLDRV_DTL_AUTOMATIC_MODE)
        tlog_multiplier = volume_manager.get('number_of_scos_in_tlog', 20)
        non_disposable_sco_factor = volume_manager.get('non_disposable_scos_factor', 12)

        sco_size = sco_multiplier * cluster_size / 1024  # SCO size is in MiB ==> SCO multiplier * cluster size (4 KiB by default)
        write_buffer = tlog_multiplier * sco_size * non_disposable_sco_factor

        dtl_mode = StorageDriverClient.REVERSE_DTL_MODE_MAP[dtl_mode]
        dtl_enabled = dtl_config_mode == StorageDriverClient.VOLDRV_DTL_AUTOMATIC_MODE
        if dtl_enabled is False:
            dtl_mode = StorageDriverClient.FRAMEWORK_DTL_NO_SYNC

        return {'sco_size': sco_size,
                'dtl_mode': dtl_mode,
                'dedupe_mode': StorageDriverClient.REVERSE_DEDUPE_MAP[dedupe_mode],
                'dtl_enabled': dtl_enabled,
                'cluster_size': cluster_size,
                'write_buffer': write_buffer,
                'dtl_transport': StorageDriverClient.REVERSE_DTL_TRANSPORT_MAP[dtl_transport],
                'cache_strategy': StorageDriverClient.REVERSE_CACHE_MAP[cache_strategy],
                'tlog_multiplier': tlog_multiplier}
