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
VPool module
"""

from ovs.celery_run import celery
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.extensions.fs.exportfs import Nfsexports
from ovs.extensions.hypervisor.factory import Factory
from ovs.extensions.generic.sshclient import SSHClient
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
    def can_be_served_on(storagerouter_guid):
        """
        temporary check to avoid creating 2 ganesha nfs exported vpools
        as this is not yet supported on storage driver level
        """
        _ = storagerouter_guid
        return True

    @staticmethod
    @celery.task(name='ovs.vpool.set_config_params')
    def set_config_params(vpool_guid, config_params):
        """
        Sets configuration parameters to a given vpool/vdisk.
        """
        vpool = VPool(vpool_guid)
        resolved_configs = dict((vdisk.guid, vdisk.resolved_configuration) for vdisk in vpool.vdisks)
        vpool.configuration = config_params
        vpool.save()
        for vdisk in vpool.vdisks:
            vdisk.invalidate_dynamics(['resolved_configuration'])
            old_resolved_config = resolved_configs[vdisk.guid]
            new_resolved_config = vdisk.resolved_configuration
            for key, value in config_params.iteritems():
                if old_resolved_config.get(key) != new_resolved_config.get(key):
                    logger.info('Updating property {0} on vDisk {1} to {2}'.format(key, vdisk.guid, new_resolved_config.get(key)))
