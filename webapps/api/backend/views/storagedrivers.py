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
StorageDriver API module
"""

from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import action
from api.backend.decorators import load, log, required_roles, return_list, return_object, return_simple, return_task
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs_extensions.api.exceptions import HttpNotAcceptableException
from ovs.lib.storagedriver import StorageDriverController


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
    def list(self, vpool_guid=None):
        """
        Overview of all StorageDrivers
        :param vpool_guid: Guid of the vPool
        :type vpool_guid: str
        """
        if vpool_guid is not None:
            return VPool(vpool_guid).storagedrivers
        return StorageDriverList.get_storagedrivers()

    @log()
    @required_roles(['read'])
    @return_object(StorageDriver)
    @load(StorageDriver)
    def retrieve(self, storagedriver):
        """
        Load information about a given StorageDriver
        :param storagedriver: The StorageDriver to return
        :type storagedriver: StorageDriver
        """
        return storagedriver

    @action()
    @log()
    @required_roles(['read'])
    @return_simple()
    @load(StorageDriver)
    def can_be_deleted(self, storagedriver, version):
        """
        Checks whether a Storage Driver can be deleted
        :param storagedriver: StorageDriver to verify
        :type storagedriver: StorageDriver
        :param version: Client version
        :type version: int
        :return: Whether the StorageDriver can be deleted
        :rtype: bool
        """
        if version > 4:
            raise HttpNotAcceptableException(error_description='Only available in API versions 1 to 4',
                                             error='invalid_version')
        return not any(vdisk for vdisk in storagedriver.vpool.vdisks if vdisk.storagedriver_id == storagedriver.storagedriver_id)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageDriver)
    def refresh_configuration(self, storagedriver):
        """
        Refresh the configuration of the StorageDriver
        :param storagedriver: Guid of the Storage Driver
        :type storagedriver: StorageDriver
        """
        return StorageDriverController.refresh_configuration.delay(storagedriver_guid=storagedriver.guid)
