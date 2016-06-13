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
PMachine module
"""

from rest_framework import status, viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import action
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.lib.vdisk import VDiskController
from backend.decorators import required_roles, load, return_list, return_object, return_plain, return_task, log


class StorageDriverViewSet(viewsets.ViewSet):
    """
    Information about StorageDrivers
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'storagedrivers'
    base_name = 'storagedrivers'

    @log()
    @required_roles(['read'])
    @return_list(StorageDriver)
    @load()
    def list(self):
        """
        Overview of all StorageDrivers
        """
        return StorageDriverList.get_storagedrivers()

    @log()
    @required_roles(['read'])
    @return_object(StorageDriver)
    @load(StorageDriver)
    def retrieve(self, storagedriver):
        """
        Load information about a given StorageDriver
        """
        return storagedriver

    @action()
    @log()
    @required_roles(['read'])
    @return_plain()
    @load(StorageDriver)
    def can_be_deleted(self, storagedriver):
        """
        Checks whether a Storage Driver can be deleted
        """
        result = True
        storagerouter = storagedriver.storagerouter
        storagedrivers_left = len([sd for sd in storagerouter.storagedrivers if sd.guid != storagedriver.guid])
        pmachine = storagerouter.pmachine
        vmachines = VMachineList.get_customer_vmachines()
        vpools_guids = [vmachine.vpool_guid for vmachine in vmachines if vmachine.vpool_guid is not None]
        pmachine_guids = [vmachine.pmachine_guid for vmachine in vmachines]
        vpool = storagedriver.vpool

        if storagedrivers_left is False and pmachine.guid in pmachine_guids and vpool.guid in vpools_guids:
            result = False
        if any(vdisk for vdisk in vpool.vdisks if vdisk.storagedriver_id == storagedriver.storagedriver_id):
            result = False
        return result

    @action()
    @required_roles(['read', 'write'])
    @return_task()
    @load(StorageDriver)
    def create_new_disk(self, storagedriver, diskname, size):
        """
        Create a new empty vdisk - including the volume in the backend
        :param storagedriver: Guid of the storagedriver holding the vdisk
        :param diskname: Name of the new vdisk
        :param size: Size in GB
        """
        return VDiskController.create_new.delay(diskname=diskname,
                                                size=size,
                                                storagedriver_guid=storagedriver.guid)
