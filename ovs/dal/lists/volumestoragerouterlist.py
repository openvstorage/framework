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
from ovs.dal.hybrids.volumestoragerouter import VolumeStorageRouter


class VolumeStorageRouterList(object):
    """
    This VolumeStorageRouterList class contains various lists regarding to the VolumeStorageRouter class
    """

    @staticmethod
    def get_volumestoragerouters():
        """
        Returns a list of all VolumeStorageRouters
        """
        volumestoragerouters = DataList({'object': VolumeStorageRouter,
                                         'data': DataList.select.DESCRIPTOR,
                                         'query': {'type': DataList.where_operator.AND,
                                                   'items': []}}).data
        return DataObjectList(volumestoragerouters, VolumeStorageRouter)

    @staticmethod
    def get_by_vsrid(vsrid):
        """
        Returns a list of all VolumeStorageRouters based on a given vsrid
        """
        # pylint: disable=line-too-long
        volumestoragerouters = DataList({'object': VolumeStorageRouter,
                                         'data': DataList.select.DESCRIPTOR,
                                         'query': {'type': DataList.where_operator.AND,
                                                   'items': [('vsrid', DataList.operator.EQUALS, vsrid)]}}).data
        # pylint: enable=line-too-long
        if volumestoragerouters:
            return DataObjectList(volumestoragerouters, VolumeStorageRouter)[0]
        return None

    @staticmethod
    def get_volumestoragerouters_by_storageappliance(machineguid):
        """
        Returns a list of all VolumeStorageRouters for Storage Appliance
        """
        volumestoragerouters = DataList({'object': VolumeStorageRouter,
                                         'data': DataList.select.DESCRIPTOR,
                                         'query': {'type': DataList.where_operator.AND,
                                                   'items': [('storageappliance_guid', DataList.operator.EQUALS, machineguid)]}}).data
        return DataObjectList(volumestoragerouters, VolumeStorageRouter)
