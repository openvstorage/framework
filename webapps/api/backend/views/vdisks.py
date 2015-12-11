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
VDisk module
"""

from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import action, link
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.lib.vdisk import VDiskController
from backend.decorators import required_roles, load, return_list, return_object, return_task, log


class VDiskViewSet(viewsets.ViewSet):
    """
    Information about vDisks
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'vdisks'
    base_name = 'vdisks'

    @log()
    @required_roles(['read'])
    @return_list(VDisk)
    @load()
    def list(self, vmachineguid=None, vpoolguid=None):
        """
        Overview of all vDisks
        :param vmachineguid: Guid of the virtual machine to retrieve its disks
        :param vpoolguid: Guid of the vPool to retrieve its disks
        """
        if vmachineguid is not None:
            vmachine = VMachine(vmachineguid)
            return vmachine.vdisks
        elif vpoolguid is not None:
            vpool = VPool(vpoolguid)
            return vpool.vdisks
        return VDiskList.get_vdisks()

    @log()
    @required_roles(['read'])
    @return_object(VDisk)
    @load(VDisk)
    def retrieve(self, vdisk):
        """
        Load information about a given vDisk
        :param vdisk: Guid of the virtual disk to retrieve
        """
        return vdisk

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def rollback(self, vdisk, timestamp):
        """
        Rollbacks a vDisk to a given timestamp
        :param vdisk: Guid of the virtual disk
        :param timestamp: Timestamp of the snapshot to rollback to
        """
        return VDiskController.rollback.delay(diskguid=vdisk.guid,
                                              timestamp=timestamp)

    @action()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(VDisk)
    def set_config_params(self, vdisk, new_config_params):
        """
        Sets configuration parameters to a given vdisk.
        :param vdisk: Guid of the virtual disk to configure
        :param new_config_params: Configuration settings for the virtual disk
        """
        return VDiskController.set_config_params.delay(vdisk_guid=vdisk.guid, new_config_params=new_config_params)

    @link()
    @required_roles(['read'])
    @return_task()
    @load(VDisk)
    def get_config_params(self, vdisk):
        """
        Retrieve the configuration parameters for the given disk from the storagedriver.
        :param vdisk: Guid of the virtual disk to retrieve its running configuration
        """
        return VDiskController.get_config_params.delay(vdisk_guid=vdisk.guid)

    @action()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def clone(self, vdisk, name, storagerouter_guid, snapshot_id=None):
        """
        Clones a vDisk
        :param vdisk: Guid of the virtual disk to clone
        :param name: Name for the clone
        :param storagerouter_guid: Guid of the storagerouter hosting the virtual disk
        :param snapshot_id: ID of the snapshot to clone from
        """
        storagerouter = StorageRouter(storagerouter_guid)
        return VDiskController.clone.delay(diskguid=vdisk.guid,
                                           snapshotid=snapshot_id,
                                           devicename=name,
                                           pmachineguid=storagerouter.pmachine_guid,
                                           detached=True)

    @action()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def removesnapshot(self, vdisk, snapshot_id):
        """
        Remove a snapshot from a VDisk
        :param vdisk: Guid of the virtual disk whose snapshot is to be removed
        :param snapshot_id: ID of the snapshot to remove
        """
        return VDiskController.delete_snapshot.delay(diskguid=vdisk.guid,
                                                      snapshotid=snapshot_id)

    @action()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def set_as_template(self, vdisk):
        """
        Sets a vDisk as template
        :param vdisk: Guid of the virtual disk to set as template
        """
        return VDiskController.set_as_template.delay(diskguid=vdisk.guid)

    @action()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def create_snapshot(self, vdisk, name, timestamp=None, consistent=False, automatic=False, sticky=False, snapshot_id=None):
        """
        Creates a snapshot from the vDisk
        :param vdisk: Guid of the virtual disk to create snapshot from
        :param metadata: Metadata of the snapshot (dict)
        :param snapshot_id: (optional) id of the snapshot, default will be new uuid
        """
        metadata = {'label': name,
                    'timestamp': timestamp,
                    'is_consistent': True if consistent else False,
                    'is_sticky': True if sticky else False,
                    'is_automatic': True if automatic else False
        }
        return VDiskController.create_snapshot.delay(diskguid=vdisk.guid,
                                                     metadata=metadata,
                                                     snapshotid=snapshot_id)

    @action
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def set_as_template(self, vdisk):
        """
        Sets a vDisk as template
        :param vdisk: Guid of the virtual disk to set as template
        """
        return VDiskController.set_as_template.delay(diskguid=vdisk.guid)

    @action
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def create_snapshot(self, vdisk, metadata, snapshot_id=None):
        """
        Creates a snapshot from the vDisk
        :param vdisk: Guid of the virtual disk to create snapshot from
        :param metadata: Metadata of the snapshot (dict)
        :param snapshot_id: (optional) id of the snapshot, default will be new uuid
        """
        return VDiskController.create_snapshot.delay(diskguid=vdisk.guid,
                                                     metadata=metadata,
                                                     snapshotid=snapshot_id)
