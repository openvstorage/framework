# Copyright 2014 CloudFounders NV
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

from ovs.dal.hybrids.vpool import VPool
from ovs.extensions.hypervisor.factory import Factory
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.celery import celery
from ovs.lib.vmachine import VMachineController
from ovs.extensions.fs.exportfs import Nfsexports


class VPoolController(object):
    """
    Contains all BLL related to VPools
    """

    @staticmethod
    @celery.task(name='ovs.vpool.mountpoint_available_from_voldrv')
    def mountpoint_available_from_voldrv(mountpoint):
        """
        Hook for (re)exporting the NFS mountpoint
        """
        nfs = Nfsexports()
        nfs.unexport(mountpoint)
        nfs.export(mountpoint)

    @staticmethod
    @celery.task(name='ovs.vpool.sync_with_hypervisor')
    def sync_with_hypervisor(vpool_guid):
        """
        Syncs all vMachines of a given vPool with the hypervisor
        """
        vpool = VPool(vpool_guid)
        for vsr in vpool.vsrs:
            pmachine = vsr.serving_vmachine.pmachine
            hypervisor = Factory.get(pmachine)
            for vm_object in hypervisor.get_vms_by_nfs_mountinfo(vsr.storage_ip, vsr.mountpoint):
                vmachine = VMachineList.get_by_devicename_and_vpool(
                    devicename=vm_object['backing']['filename'],
                    vpool=vpool
                )
                VMachineController.update_vmachine_config(vmachine, vm_object, pmachine)
