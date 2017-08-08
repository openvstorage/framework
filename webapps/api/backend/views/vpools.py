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
VPool module
"""

import time
from rest_framework import viewsets
from rest_framework.decorators import link, action
from rest_framework.permissions import IsAuthenticated
from api.backend.decorators import required_roles, load, return_list, return_object, return_task, return_simple, log
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.vpoollist import VPoolList
from ovs_extensions.api.exceptions import HttpNotAcceptableException
from ovs.lib.generic import GenericController
from ovs.lib.storagerouter import StorageRouterController
from ovs.lib.vdisk import VDiskController


class VPoolViewSet(viewsets.ViewSet):
    """
    Information about vPools
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'vpools'
    base_name = 'vpools'

    @log()
    @required_roles(['read', 'manage'])
    @return_list(VPool, 'name')
    @load()
    def list(self):
        """
        Overview of all vPools
        """
        return VPoolList.get_vpools()

    @log()
    @required_roles(['read', 'manage'])
    @return_object(VPool)
    @load(VPool)
    def retrieve(self, vpool):
        """
        Load information about a given vPool
        :param vpool: vPool object to retrieve
        :type vpool: VPool
        """
        return vpool

    @link()
    @log()
    @required_roles(['read'])
    @return_list(StorageRouter)
    @load(VPool)
    def storagerouters(self, vpool):
        """
        Retrieves a list of StorageRouters, serving a given vPool
        :param vpool: vPool to retrieve the storagerouter information for
        :type vpool: VPool
        """
        return [storagedriver.storagerouter_guid for storagedriver in vpool.storagedrivers]

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(VPool)
    def shrink_vpool(self, vpool, storagerouter_guid):
        """
        Remove the storagedriver linking the specified vPool and storagerouter_guid
        :param vpool: vPool to shrink (or delete if its the last storagerouter linked to it)
        :type vpool: VPool
        :param storagerouter_guid: Guid of the Storage Router
        :type storagerouter_guid: str
        """
        sr = StorageRouter(storagerouter_guid)
        intersection = set(vpool.storagedrivers_guids).intersection(set(sr.storagedrivers_guids))
        if not intersection:
            raise HttpNotAcceptableException(error='impossible_request',
                                             error_description='Storage Router {0} is not a member of vPool {1}'.format(sr.name, vpool.name))
        return StorageRouterController.remove_storagedriver.delay(list(intersection)[0])

    @link()
    @log()
    @required_roles(['read'])
    @return_simple()
    @load(VPool)
    def devicename_exists(self, vpool, name=None, names=None):
        """
        Checks whether a given name can be created on the vpool
        :param vpool: vPool object
        :type vpool: VPool
        :param name: Candidate name
        :type name: str
        :param names: Candidate names
        :type names: list
        :return: Whether the devicename exists
        :rtype: bool
        """
        error_message = None
        if not (name is None) ^ (names is None):
            error_message = 'Either the name (string) or the names (list of strings) parameter must be passed'
        if name is not None and not isinstance(name, basestring):
            error_message = 'The name parameter must be a string'
        if names is not None and not isinstance(names, list):
            error_message = 'The names parameter must be a list of strings'
        if error_message is not None:
            raise HttpNotAcceptableException(error='impossible_request',
                                             error_description=error_message)

        if name is not None:
            devicename = VDiskController.clean_devicename(name)
            return VDiskList.get_by_devicename_and_vpool(devicename, vpool) is not None
        for name in names:
            devicename = VDiskController.clean_devicename(name)
            if VDiskList.get_by_devicename_and_vpool(devicename, vpool) is not None:
                return True
        return False

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VPool)
    def create_snapshots(self, vdisk_guids, name, timestamp=None, consistent=False, automatic=False, sticky=False):
        """
        Creates snapshots for a list of VDisks
        :param vdisk_guids: Guids of the virtual disks to create snapshot from
        :type vdisk_guids: list
        :param name: Name of the snapshot (label)
        :type name: str
        :param timestamp: Timestamp of the snapshot
        :type timestamp: int
        :param consistent: Indicates whether the snapshots will contain consistent data
        :type consistent: bool
        :param automatic: Indicate whether the snaphots are taken by an automatic process or manually
        :type automatic: bool
        :param sticky: Indicates whether the system should clean the snapshots
        :type sticky: bool
        """
        if timestamp is None:
            timestamp = str(int(time.time()))
        metadata = {'label': name,
                    'timestamp': timestamp,
                    'is_consistent': True if consistent else False,
                    'is_sticky': True if sticky else False,
                    'is_automatic': True if automatic else False}
        return VDiskController.create_snapshots.delay(vdisk_guids=vdisk_guids,
                                                      metadata=metadata)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VPool)
    def remove_snapshots(self, snapshot_mapping):
        """
        Remove a snapshot from a list of VDisks
        :param snapshot_mapping: Dict containing vDisk guid / Snapshot ID(s) pairs
        :type snapshot_mapping: dict
        """
        return VDiskController.delete_snapshots.delay(snapshot_mapping=snapshot_mapping)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(VPool)
    def sync_with_reality(self, vpool):
        """
        Syncs the model for given vPool with reality
        :param vpool: vPool to sync
        :type vpool: ovs.dal.hybrids.vpool.VPool
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return VDiskController.sync_with_reality.delay(vpool_guid=vpool.guid)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(VPool)
    def scrub_multiple_vdisks(self, vpool, vdisk_guids=None):
        """
        Scrubs the specified vDisks or all vDisks of the vPool is no guids are passed in
        :param vpool: The vPool to which the vDisks belong to scrub
        :type vpool: ovs.dal.hybrids.vpool.VPool
        :param vdisk_guids: The guids of the vDisks to scrub
        :type vdisk_guids: list
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        if vdisk_guids is None:
            vdisk_guids = []
        if set(vdisk_guids).difference(set(vpool.vdisks_guids)):
            raise HttpNotAcceptableException(error='invalid_data',
                                             error_description='Some of the vDisks specified do not belong to this vPool')
        return GenericController.execute_scrub.delay(vdisk_guids=vdisk_guids or vpool.vdisks_guids)
