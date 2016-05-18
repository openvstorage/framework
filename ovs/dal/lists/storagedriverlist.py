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
VDiskList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.storagedriver import StorageDriver


class StorageDriverList(object):
    """
    This StorageDriverList class contains various lists regarding to the StorageDriver class
    """

    @staticmethod
    def get_storagedrivers():
        """
        Returns a list of all StorageDrivers
        """
        return DataList(StorageDriver, {'type': DataList.where_operator.AND,
                                        'items': []})

    @staticmethod
    def get_by_storagedriver_id(storagedriver_id):
        """
        Returns a list of all StorageDrivers based on a given storagedriver_id
        """
        storagedrivers = DataList(StorageDriver, {'type': DataList.where_operator.AND,
                                                  'items': [('storagedriver_id', DataList.operator.EQUALS, storagedriver_id)]})
        if len(storagedrivers) > 0:
            return storagedrivers[0]
        return None

    @staticmethod
    def get_storagedrivers_by_storagerouter(machineguid):
        """
        Returns a list of all StorageDrivers for Storage Router
        """
        return DataList(StorageDriver, {'type': DataList.where_operator.AND,
                                        'items': [('storagerouter_guid', DataList.operator.EQUALS, machineguid)]})
