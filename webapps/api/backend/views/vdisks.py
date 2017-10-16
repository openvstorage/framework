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
from api.backend.decorators import load, log, required_roles, return_list, return_object, return_task
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs_extensions.api.exceptions import HttpNotAcceptableException
from ovs.lib.generic import GenericController
from ovs.lib.vdisk import VDiskController


class VDiskViewSet(viewsets.ViewSet):
    """
    Information about vDisks
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'vdisks'
    base_name = 'vdisks'
    return_exceptions = ['vdisks.create']

    @log()
    @required_roles(['read', 'manage'])
    @return_list(VDisk)
    @load()
    def list(self, vpoolguid=None, storagerouterguid=None, query=None):
        """
        Overview of all vDisks
        :param vpoolguid: Guid of the vPool to retrieve its disks
        :type vpoolguid: str
        :param storagerouterguid: Guid of the StorageRouter to retrieve its disks
        :type storagerouterguid: str
        :param query: A query to be executed if required
        :type query: DataQuery
        :return: List of vDisks matching the parameters specified
        :rtype: list[ovs.dal.hybrids.vdisk.VDisk]
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
        :type vdisk: VDisk
        :return: The vDisk based on the guid provided
        :rtype: ovs.dal.hybrids.vdisk.VDisk
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
        :type vdisk: VDisk
        :param timestamp: Timestamp of the snapshot to rollback to
        :type timestamp: int
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return VDiskController.rollback.delay(vdisk_guid=vdisk.guid,
                                              timestamp=str(timestamp))

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(VDisk)
    def restart(self, vdisk, force=False):
        """
        Restart a vDisk
        :param vdisk: Guid of the virtual disk
        :type vdisk: ovs.dal.hybrids.vdisk.VDisk
        :param force: Force a restart at a possible cost of data loss
        :type force: bool
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return VDiskController.restart.delay(vdisk_guid=vdisk.guid, force=force)

    @action()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(VDisk)
    def set_config_params(self, vdisk, new_config_params):
        """
        Sets configuration parameters to a given vdisk.
        :param vdisk: Guid of the virtual disk to configure
        :type vdisk: VDisk
        :param new_config_params: Configuration settings for the virtual disk
        :type new_config_params: dict
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        new_config_params.pop('dedupe_mode', None)
        new_config_params.pop('cache_strategy', None)
        new_config_params.pop('readcache_limit', None)
        return VDiskController.set_config_params.delay(vdisk_guid=vdisk.guid, new_config_params=new_config_params)

    @link()
    @log()
    @required_roles(['read'])
    @return_list(VDisk)
    @load(VDisk, max_version=7)
    def get_children(self, vdisk):
        """
        Returns a list of vDisk guid(s) of children of a given vDisk
        DEPRECATED API call - vDisk has new relation vDisk.child_vdisks
        :param vdisk: Vdisk to get the children from
        :type vdisk: VDisk
        :return: Guids of the child vDisks
        :rtype: list[str]
        """
        return vdisk.child_vdisks_guids

    @link()
    @required_roles(['read'])
    @return_task()
    @load(VDisk)
    def get_config_params(self, vdisk):
        """
        Retrieve the configuration parameters for the given disk from the storagedriver.
        :param vdisk: Guid of the virtual disk to retrieve its running configuration
        :type vdisk: VDisk
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return VDiskController.get_config_params.delay(vdisk_guid=vdisk.guid)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def clone(self, vdisk, name, storagerouter_guid, snapshot_id=None, pagecache_ratio=None, cache_quota=None):
        """
        Clones a vDisk
        :param vdisk: Guid of the virtual disk to clone
        :type vdisk: VDisk
        :param name: Name for the clone (filename or user friendly name)
        :type name: str
        :param storagerouter_guid: Guid of the storagerouter hosting the virtual disk
        :type storagerouter_guid: str
        :param snapshot_id: ID of the snapshot to clone from
        :type snapshot_id: str
        :param pagecache_ratio: Ratio (0 < x <= 1) of the pagecache size related to the size
        :type pagecache_ratio: float
        :param cache_quota: Maximum caching space(s) the new clone can consume (in Bytes) per cache type.
        :type cache_quota: dict
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return VDiskController.clone.delay(vdisk_guid=vdisk.guid,
                                           snapshot_id=snapshot_id,
                                           name=name,
                                           storagerouter_guid=storagerouter_guid,
                                           pagecache_ratio=pagecache_ratio,
                                           cache_quota=cache_quota)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def move(self, vdisk, target_storagerouter_guid, force=False):
        """
        Moves a vDisk
        :param vdisk: Guid of the virtual disk to move
        :type vdisk: VDisk
        :param target_storagerouter_guid: Guid of the StorageRouter to move the vDisk to
        :type target_storagerouter_guid: str
        :param force: Indicate whether to force the migration (forcing the migration might cause data loss)
        :type force: bool
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return VDiskController.move.delay(vdisk_guid=vdisk.guid,
                                          target_storagerouter_guid=target_storagerouter_guid,
                                          force=force)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def remove_snapshot(self, vdisk, snapshot_id):
        """
        Remove a snapshot from a VDisk
        :param vdisk: Guid of the virtual disk whose snapshot is to be removed
        :type vdisk: VDisk
        :param snapshot_id: ID of the snapshot to remove
        :type snapshot_id: str
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
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
        :type vdisk: VDisk
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        if len(vdisk.child_vdisks) > 0:
            raise HttpNotAcceptableException(error='impossible_request',
                                             error_description='vDisk has clones')
        return VDiskController.set_as_template.delay(vdisk_guid=vdisk.guid)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load()
    def create(self, name, size, vpool_guid, storagerouter_guid, pagecache_ratio=1.0, cache_quota=None):
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
        :param pagecache_ratio: Ratio (0 < x <= 1) of the pagecache size related to the size
        :type pagecache_ratio: float
        :param cache_quota: Maximum caching space(s) the new volume can consume (in Bytes) per cache type.
        :type cache_quota: dict
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        storagerouter = StorageRouter(storagerouter_guid)
        for storagedriver in storagerouter.storagedrivers:
            if storagedriver.vpool_guid == vpool_guid:
                return VDiskController.create_new.delay(volume_name=name,
                                                        volume_size=size,
                                                        storagedriver_guid=storagedriver.guid,
                                                        pagecache_ratio=pagecache_ratio,
                                                        cache_quota=cache_quota)
        raise HttpNotAcceptableException(error='impossible_request',
                                         error_description='No storagedriver found for vPool: {0} and StorageRouter: {1}'.format(vpool_guid, storagerouter_guid))

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def create_snapshot(self, vdisk, name, version, timestamp=None, consistent=False, automatic=False, sticky=False):
        """
        Creates a snapshot from the vDisk
        :param vdisk: Guid of the virtual disk to create snapshot from
        :type vdisk: VDisk
        :param name: Name of the snapshot (label)
        :type name: str
        :param version: Client version
        :type version: int
        :param timestamp: Timestamp of the snapshot
        :type timestamp: int
        :param consistent: Indicates whether the snapshot will be consistent
        :type consistent: bool
        :param automatic: Indicates whether the snapshot was triggered by an automatic or manual process
        :type automatic: bool
        :param sticky: Indicates whether the system should clean the snapshot automatically
        :type sticky: bool
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
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
    def create_from_template(self, vdisk, name, storagerouter_guid, pagecache_ratio=None, cache_quota=None):
        """
        Create a new vdisk from a template vDisk
        :param vdisk: Guid of the template virtual disk
        :type vdisk: VDisk
        :param name: Name of the new vdisk
        :type name: str
        :param storagerouter_guid: Guid of StorageRouter to create new vDisk on
        :type storagerouter_guid: str
        :param pagecache_ratio: Ratio (0 < x <= 1) of the pagecache size related to the size
        :type pagecache_ratio: float
        :param cache_quota: Maximum caching space(s) the new volume can consume (in Bytes) per cache type.
        :type cache_quota: dict
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return VDiskController.create_from_template.delay(vdisk_guid=vdisk.guid,
                                                          name=name,
                                                          storagerouter_guid=storagerouter_guid,
                                                          pagecache_ratio=pagecache_ratio,
                                                          cache_quota=cache_quota)

    @link()
    @log()
    @required_roles(['read'])
    @return_list(StorageRouter)
    @load(VDisk)
    def get_target_storagerouters(self, vdisk):
        """
        Gets all possible target Storage Routers for a given vDisk (e.g. when cloning, creating from template or moving)
        :param vdisk: The vDisk to get the targets for
        :type vdisk: VDisk
        :return: A list of StorageRouters on which the current vDisk is not attached, but the vDisk vPool is extended to
        :rtype: list[ovs.dal.hybrids.storagerouter.StorageRouter]
        """
        return [] if vdisk.vpool is None else [sd.storagerouter for sd in vdisk.vpool.storagedrivers]

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def delete(self, vdisk):
        """
        Delete a given vDisk
        :param vdisk: The vDisk to delete
        :type vdisk: VDisk
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        if len(vdisk.child_vdisks) > 0:
            raise HttpNotAcceptableException(error='impossible_request',
                                             error_description='vDisk has clones')
        return VDiskController.delete.delay(vdisk_guid=vdisk.guid)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def delete_vtemplate(self, vdisk):
        """
        Deletes a vDisk (template)
        :param vdisk: the vDisk (template) to delete
        :type vdisk: VDisk
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        if not vdisk.is_vtemplate:
            raise HttpNotAcceptableException(error='impossible_request',
                                             error_description='vDisk should be a vTemplate')
        if len(vdisk.child_vdisks) > 0:
            raise HttpNotAcceptableException(error='impossible_request',
                                             error_description='vTemplate has clones')
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
        :type vdisk: VDisk
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
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
        :type vdisk: VDisk
        :param tlog_name: TLog name to verify
        :type tlog_name: str
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
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
        :type vdisk: VDisk
        :param snapshot_id: Snapshot to verify
        :type snapshot_id: str
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
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
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        new_size = int(new_size)
        return VDiskController.extend.delay(vdisk_guid=vdisk.guid,
                                            volume_size=new_size)

    @link()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_list(StorageRouter)
    @load(VDisk)
    def get_scrub_storagerouters(self):
        """
        Loads a list of suitable StorageRouters for scrubbing the given vDisk
        :return: A list of StorageRouters which have the SCRUB role
        :rtype: list[ovs.dal.hybrids.storagerouter.StorageRouter]
        """
        storagerouters = []
        for storagerouter in StorageRouterList.get_storagerouters():
            scrub_partitions = storagerouter.partition_config.get(DiskPartition.ROLES.SCRUB, [])
            if len(scrub_partitions) == 0:
                continue
            storagerouters.append(storagerouter)
        return storagerouters

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(VDisk)
    def scrub(self, vdisk, storagerouter_guid=None):
        """
        Scrubs a given vDisk on a given StorageRouter
        :param vdisk: the vDisk to scrub
        :type vdisk: VDisk
        :param storagerouter_guid: The guid of the StorageRouter to do the scrubbing on
        :type storagerouter_guid: str
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return GenericController.execute_scrub.delay(vdisk_guids=[vdisk.guid], storagerouter_guid=storagerouter_guid, manual=True)
