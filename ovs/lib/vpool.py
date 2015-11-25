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
        """
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        if storagedriver is None:
            raise RuntimeError('A Storage Driver with id {0} could not be found.'.format(storagedriver_id))
        storagedriver.startup_counter += 1
        storagedriver.save()
        if storagedriver.storagerouter.pmachine.hvtype == 'VMWARE':
            client = SSHClient(storagedriver.storagerouter)
            if client.config_read('ovs.storagedriver.vmware_mode') == 'classic':
                nfs = Nfsexports()
                nfs.unexport(mountpoint)
                nfs.export(mountpoint)
                nfs.trigger_rpc_mountd()

    @staticmethod
    @celery.task(name='ovs.vpool.sync_with_hypervisor')
    def sync_with_hypervisor(vpool_guid):
        """
        Syncs all vMachines of a given vPool with the hypervisor
        """
        vpool = VPool(vpool_guid)
        for storagedriver in vpool.storagedrivers:
            pmachine = storagedriver.storagerouter.pmachine
            hypervisor = Factory.get(pmachine)
            for vm_object in hypervisor.get_vms_by_nfs_mountinfo(storagedriver.storage_ip, storagedriver.mountpoint):
                search_vpool = None if pmachine.hvtype == 'KVM' else vpool
                vmachine = VMachineList.get_by_devicename_and_vpool(
                    devicename=vm_object['backing']['filename'],
                    vpool=search_vpool
                )
                VMachineController.update_vmachine_config(vmachine, vm_object, pmachine)

    @staticmethod
    @celery.task(name='ovs.vpool.get_configuration')
    def get_configuration(vpool_guid):
        vpool = VPool(vpool_guid)
        if not vpool.storagedrivers or not vpool.storagedrivers[0].storagerouter:
            return {}

        client = SSHClient(vpool.storagedrivers[0].storagerouter)
        storagedriver_config = StorageDriverConfiguration('storagedriver', vpool.name)
        storagedriver_config.load(client)

        dtl = storagedriver_config.configuration.get('failovercache', {})
        volume_router = storagedriver_config.configuration.get('volume_router', {})
        volume_manager = storagedriver_config.configuration.get('volume_manager', {})

        dedupe_mode = volume_manager.get('read_cache_default_mode', StorageDriverClient.VOLDRV_CONTENT_BASED)
        dtl_transport = dtl.get('failovercache_transport', StorageDriverClient.VOLDRV_DTL_TRANSPORT_TCP)
        cache_strategy = volume_manager.get('read_cache_default_behaviour', StorageDriverClient.VOLDRV_CACHE_ON_READ)
        sco_multiplier = volume_router.get('vrouter_sco_multiplier', 1024)
        tlog_multiplier = volume_manager.get('number_of_scos_in_tlog', 20)
        non_disposable_sco_factor = volume_manager.get('non_disposable_scos_factor', 12)

        dtl_mode = storagedriver_config.configuration.get('', {}).get('', None)
        sco_size = sco_multiplier * 4 / 1024  # SCO size is in MiB ==> SCO multiplier * cluster size (4 KiB by default)
        dtl_enabled = storagedriver_config.configuration.get('', {}).get('', False)
        dtl_location = storagedriver_config.configuration.get('', {}).get('', None)
        write_buffer = tlog_multiplier * sco_size * non_disposable_sco_factor

        return {'sco_size': sco_size,
                'dtl_mode': dtl_mode,
                'dtl_enabled': dtl_enabled,
                'dedupe_mode': StorageDriverClient.REVERSE_DEDUPE_MAP[dedupe_mode],
                'write_buffer': write_buffer,
                'dtl_location': dtl_location,
                'dtl_transport': StorageDriverClient.REVERSE_DTL_TRANSPORT_MAP[dtl_transport],
                'cache_strategy': StorageDriverClient.REVERSE_CACHE_MAP[cache_strategy],
                'tlog_multiplier': tlog_multiplier}
