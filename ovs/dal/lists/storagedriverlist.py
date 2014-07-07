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
VDiskList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
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
        storagedrivers = DataList({'object': StorageDriver,
                                   'data': DataList.select.DESCRIPTOR,
                                   'query': {'type': DataList.where_operator.AND,
                                             'items': []}}).data
        return DataObjectList(storagedrivers, StorageDriver)

    @staticmethod
    def get_by_storagedriver_id(storagedriver_id):
        """
        Returns a list of all StorageDrivers based on a given storagedriver_id
        """
        # pylint: disable=line-too-long
        storagedrivers = DataList({'object': StorageDriver,
                                   'data': DataList.select.DESCRIPTOR,
                                   'query': {'type': DataList.where_operator.AND,
                                             'items': [('storagedriver_id', DataList.operator.EQUALS, storagedriver_id)]}}).data
        # pylint: enable=line-too-long
        if storagedrivers:
            return DataObjectList(storagedrivers, StorageDriver)[0]
        return None

    @staticmethod
    def get_storagedrivers_by_storageappliance(machineguid):
        """
        Returns a list of all StorageDrivers for Storage Appliance
        """
        storagedrivers = DataList({'object': StorageDriver,
                                   'data': DataList.select.DESCRIPTOR,
                                   'query': {'type': DataList.where_operator.AND,
                                             'items': [('storageappliance_guid', DataList.operator.EQUALS, machineguid)]}}).data
        return DataObjectList(storagedrivers, StorageDriver)
