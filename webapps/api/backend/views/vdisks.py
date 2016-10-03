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
VDisk module
"""
import time
from rest_framework import viewsets
from rest_framework.decorators import action, link
from rest_framework.permissions import IsAuthenticated
from backend.decorators import required_roles, load, return_list, return_object, return_task, log
from backend.exceptions import HttpNotAcceptableException
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.lib.vdisk import VDiskController


class VDiskViewSet(viewsets.ViewSet):
    """
    Information about vDisks
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'vdisks'
    base_name = 'vdisks'

    @log()
    @required_roles(['read', 'manage'])
    @return_list(VDisk)
    @load()
    def list(self, vpoolguid=None, storagerouterguid=None, query=None):
        """
        Overview of all vDisks
        :param vpoolguid: Guid of the vPool to retrieve its disks
        :param storagerouterguid: Guid of the StorageRouter to retrieve its disks
        :param query: A query to be executed if required
        """
        if vpoolguid is not None:
            vpool = VPool(vpoolguid)
            vdisks = vpool.vdisks
        elif storagerouterguid is not None:
            storagerouter = StorageRouter(storagerouterguid)
            vdisks = DataList(VDisk, {'type': DataList.where_operator.AND,
                                      'items': [('guid', DataList.operator.IN, storagerouter.vdisks_guids)]})
        else:
            vdisks = VDiskList.get_vdisks()
        if query is not None:
            query_vdisk_guids = DataList(VDisk, query).guids
            vdisks = [vdisk for vdisk in vdisks if vdisk.guid in query_vdisk_guids]
        return vdisks

    @log()
    @required_roles(['read', 'manage'])
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
        return VDiskController.rollback.delay(vdisk_guid=vdisk.guid,
                                              timestamp=str(timestamp))

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
                raise HttpNotAcceptableException(error_description='API version 1 requires a Storage Router IP',
                                                 error='invalid_version')
            new_config_params['dtl_target'] = [junction.domain_guid for junction in storage_router.domains]
        return VDiskController.set_config_params.delay(vdisk_guid=vdisk.guid, new_config_params=new_config_params)

    @link()
    @log()
    @required_roles(['read'])
    @return_list(VDisk)
    @load(VDisk)
    def get_children(self, vdisk, hints):
        """
        Returns a list of vDisk guid(s) of children of a given vDisk
        """
        children_vdisk_guids = []
        children_vdisks = []
        if vdisk.is_vtemplate is False:
            raise HttpNotAcceptableException(error_description='vDisk is not a vTemplate',
                                             error='impossible_request')
        for cdisk in vdisk.child_vdisks:
            if cdisk.guid not in children_vdisk_guids:
                children_vdisk_guids.append(cdisk.guid)
                if hints['full'] is True:
                    # Only load full object is required
                    children_vdisks.append(cdisk)
        return children_vdisks if hints['full'] is True else children_vdisk_guids

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
        :param name: Name for the clone (filename or user friendly name)
        :param storagerouter_guid: Guid of the storagerouter hosting the virtual disk
        :param snapshot_id: ID of the snapshot to clone from
        """
        return VDiskController.clone.delay(vdisk_guid=vdisk.guid,
                                           snapshot_id=snapshot_id,
                                           name=name,
                                           storagerouter_guid=storagerouter_guid)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def move(self, vdisk, target_storagerouter_guid):
        """
        Moves a vDisk
        :param vdisk: Guid of the virtual disk to move
        :type vdisk: VDisk
        :param target_storagerouter_guid: Guid of the StorageRouter to move the vDisk to
        :type target_storagerouter_guid: str
        :return: Celery async task
        """
        return VDiskController.move.delay(vdisk_guid=vdisk.guid,
                                          target_storagerouter_guid=target_storagerouter_guid)

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
        return VDiskController.delete_snapshot.delay(vdisk_guid=vdisk.guid,
                                                     snapshot_id=snapshot_id)

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
        return VDiskController.set_as_template.delay(vdisk_guid=vdisk.guid)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load()
    def create(self, name, size, vpool_guid, storagerouter_guid):
        """
        Create a new vdisk
        :param name: Name of the new vdisk
        :type name: str
        :param size: Size of  virtual disk in bytes
        :type size: int
        :param vpool_guid: Guid of vPool to create new vdisk on
        :type vpool_guid: str
        :param storagerouter_guid: Guid of the storagerouter to assign disk to
        :type storagerouter_guid: str
        :return: Celery task
        """
        storagerouter = StorageRouter(storagerouter_guid)
        for storagedriver in storagerouter.storagedrivers:
            if storagedriver.vpool_guid == vpool_guid:
                return VDiskController.create_new.delay(volume_name=name,
                                                        volume_size=size,
                                                        storagedriver_guid=storagedriver.guid)
        raise HttpNotAcceptableException(error_description='No storagedriver found for vPool: {0} and StorageRouter: {1}'.format(vpool_guid, storagerouter_guid),
                                         error='impossible_request')

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def create_snapshot(self, vdisk, name, version, timestamp=None, consistent=False, automatic=False, sticky=False):
        """
        Creates a snapshot from the vDisk
        :param vdisk: Guid of the virtual disk to create snapshot from
        :param name: Name of the snapshot (label)
        :param version: Client version
        :param timestamp: Timestamp of the snapshot - integer
        :param consistent: Flag - is_consistent
        :param automatic: Flag - is_automatic
        :param sticky: Flag - is_sticky
        """
        if version >= 3:
            timestamp = str(int(time.time()))
        metadata = {'label': name,
                    'timestamp': timestamp,
                    'is_consistent': True if consistent else False,
                    'is_sticky': True if sticky else False,
                    'is_automatic': True if automatic else False}
        return VDiskController.create_snapshot.delay(vdisk_guid=vdisk.guid,
                                                     metadata=metadata)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def create_from_template(self, vdisk, name, storagerouter_guid):
        """
        Create a new vdisk from a template vDisk
        :param vdisk: Guid of the template virtual disk
        :param name: Name of the new vdisk
        :param storagerouter_guid: Guid of StorageRouter to create new vDisk on
        """
        return VDiskController.create_from_template.delay(vdisk_guid=vdisk.guid,
                                                          name=name,
                                                          storagerouter_guid=storagerouter_guid)

    @link()
    @log()
    @required_roles(['read'])
    @return_list(StorageRouter)
    @load(VDisk)
    def get_target_storagerouters(self, vdisk, hints):
        """
        Gets all possible target Storage Routers for a given vDisk (e.g. when cloning, creating from template or moving)
        """
        _ = hints
        return [] if vdisk.vpool is None else [sd.storagerouter for sd in vdisk.vpool.storagedrivers]

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
        return VDiskController.delete.delay(vdisk_guid=vdisk.guid)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def delete_vtemplate(self, vdisk):
        """
        Deletes a vDisk (template)
        """
        if not vdisk.is_vtemplate:
            raise HttpNotAcceptableException(error_description='vDisk should be a vTemplate',
                                             error='impossible_request')
        return VDiskController.delete.delay(vdisk_guid=vdisk.guid)

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
        return VDiskController.schedule_backend_sync.delay(vdisk_guid=vdisk.guid)

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
        return VDiskController.is_volume_synced_up_to_tlog.delay(vdisk_guid=vdisk.guid, tlog_name=tlog_name)

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
        return VDiskController.is_volume_synced_up_to_snapshot.delay(vdisk_guid=vdisk.guid, snapshot_id=snapshot_id)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(VDisk)
    def extend(self, vdisk, new_size):
        """
        Extends a given vDisk to a new size
        :param vdisk: The vDisk to extend
        :type vdisk: VDisk
        :param new_size: The new size of the vDisk (in bytes)
        :type new_size: int
        """
        new_size = int(new_size)
        return VDiskController.extend.delay(vdisk_guid=vdisk.guid,
                                            volume_size=new_size)
