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
from api.backend.decorators import load, log, required_roles, return_list, return_object, return_task, return_simple
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.storagedriverlist import StorageDriverList
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

    @action()
    @log()
    @required_roles(['read'])
    @return_simple()
    @load(StorageDriver)
    def calculate_update_impact(self, storagedriver, vpool_updates, storagedriver_updates):
        """
        Calculates what impact the proposed update configuration would have
        :param storagedriver: Storagedriver linked with the call
        :type storagedriver: StorageDriver
        :param vpool_updates: Updates to be done to the vpool
        :type vpool_updates: dict
        :param storagedriver_updates: Updates to be done to the storagedriver
        :type storagedriver_updates: dict
        :return: Data on what actions would be taken
        :rtype: object
        """
        vpool_updates.update(storagedriver_updates)
        return vpool_updates