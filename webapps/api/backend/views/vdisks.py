# Copyright 2016 iNuron NV
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
VDisk module
"""
from backend.decorators import required_roles, load, return_list, return_object, return_task, log
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.lib.vdisk import VDiskController
from rest_framework import viewsets
from rest_framework.decorators import action, link
from rest_framework.exceptions import NotAcceptable
from rest_framework.permissions import IsAuthenticated


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
    def set_config_params(self, vdisk, new_config_params, version):
        """
        Sets configuration parameters to a given vdisk.
        :param vdisk: Guid of the virtual disk to configure
        :param new_config_params: Configuration settings for the virtual disk
        :param version: API version
        """
        if version == 1 and 'dtl_target' in new_config_params:
            storage_router = StorageRouterList.get_by_ip(new_config_params['dtl_target'])
            if storage_router is None:
                raise NotAcceptable('API version 1 requires a Storage Router IP')
            new_config_params['dtl_target'] = storage_router.primary_failure_domain.guid
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
    @log()
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
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def remove_snapshot(self, vdisk, snapshot_id):
        """
        Remove a snapshot from a VDisk
        :param vdisk: Guid of the virtual disk whose snapshot is to be removed
        :param snapshot_id: ID of the snapshot to remove
        """
        return VDiskController.delete_snapshot.delay(diskguid=vdisk.guid,
                                                     snapshotid=snapshot_id)

    @action()
    @log()
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
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load()
    def create(self, devicename, size, vpool_guid, storagerouter_guid):
        """
        Create a new vdisk
        :param devicename: Name of the new vdisk
        :param size: size of  virtual disk
        :param vpool_guid: Guid of vPool to create new vdisk on
        :param storagerouter_guid: Guid of the storagerouter to assign disk to
        """
        storagerouter = StorageRouter(storagerouter_guid)
        for storagedriver in storagerouter.storagedrivers:
            if storagedriver.vpool.guid == vpool_guid:
                return VDiskController.create_new.delay(diskname=devicename,
                                                        size=size,
                                                        storagedriver_guid=storagedriver.guid)
        raise NotAcceptable('No storagedriver found for vPool: {0} and storageRouter: {1}'.format(vpool_guid,
                                                                                                  storagerouter_guid))

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def create_snapshot(self, vdisk, name, timestamp, consistent=False, automatic=False, sticky=False, snapshot_id=None):
        """
        Creates a snapshot from the vDisk
        :param vdisk: Guid of the virtual disk to create snapshot from
        :param name: Name of the snapshot (label)
        :param timestamp: Timestamp of the snapshot - integer
        :param consistent: Flag - is_consistent
        :param automatic: Flag - is_automatic
        :param sticky: Flag - is_sticky
        :param snapshot_id: (optional) id of the snapshot, default will be new uuid
        """
        metadata = {'label': name,
                    'timestamp': timestamp,
                    'is_consistent': True if consistent else False,
                    'is_sticky': True if sticky else False,
                    'is_automatic': True if automatic else False}
        return VDiskController.create_snapshot.delay(diskguid=vdisk.guid,
                                                     metadata=metadata,
                                                     snapshotid=snapshot_id)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def create_from_template(self, vdisk, devicename, pmachineguid, machineguid=None):
        """
        Create a new vdisk from a template vDisk
        :param vdisk: Guid of the template virtual disk
        :param devicename: Name of the new vdisk
        :param pmachineguid: Guid of pmachine to create new vdisk on
        :param machineguid: (optional) Guid of the machine to assign disk to
        """
        return VDiskController.create_from_template.delay(diskguid=vdisk.guid,
                                                          devicename=devicename,
                                                          pmachineguid=pmachineguid,
                                                          machineguid=machineguid)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def delete(self, vdisk):
        """
        Delete vdisk
        :param vdisk: Guid of the vdisk to delete
        """
        storagerouter = StorageRouter(vdisk.storagerouter_guid)
        return VDiskController.delete.s(diskguid=vdisk.guid).apply_async(routing_key="sr.{0}".format(storagerouter.machine_id))

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def schedule_backend_sync(self, vdisk):
        """
        Schedule a backend sync on a vdisk
        :param vdisk: vdisk to schedule a backend sync to
        :return: TLogName associated with the data sent off to the backend
        """
        storagerouter = StorageRouter(vdisk.storagerouter_guid)
        return VDiskController.schedule_backend_sync.s(vdisk_guid=vdisk.guid).apply_async(routing_key="sr.{0}".format(storagerouter.machine_id))

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def is_volume_synced_up_to_tlog(self, vdisk, tlog_name):
        """
        Verify if volume is synced to backend up to a specific tlog
        :param vdisk: vdisk to verify
        :param tlog_name: TLogName to verify
        """
        storagerouter = StorageRouter(vdisk.storagerouter_guid)
        return VDiskController.is_volume_synced_up_to_tlog.s(vdisk_guid=vdisk.guid, tlog_name=tlog_name).apply_async(routing_key="sr.{0}".format(storagerouter.machine_id))

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def is_volume_synced_up_to_snapshot(self, vdisk, snapshot_id):
        """
        Verify if volume is synced to backend up to a specific snapshot
        :param vdisk: vdisk to verify
        :param snapshot_id: Snapshot to verify
        """
        storagerouter = StorageRouter(vdisk.storagerouter_guid)
        return VDiskController.is_volume_synced_up_to_snapshot.s(vdisk_guid=vdisk.guid, snapshot_id=snapshot_id).apply_async(routing_key="sr.{0}".format(storagerouter.machine_id))
