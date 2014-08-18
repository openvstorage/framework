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
from ovs.dal.hybrids.vdisk import VDisk


class VDiskList(object):
    """
    This VDiskList class contains various lists regarding to the VDisk class
    """

    @staticmethod
    def get_vdisks():
        """
        Returns a list of all VDisks
        """
        vdisks = DataList({'object': VDisk,
                           'data': DataList.select.DESCRIPTOR,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': []}}).data
        return DataObjectList(vdisks, VDisk)

    @staticmethod
    def get_vdisk_by_volume_id(volume_id):
        """
        Returns a list of all VDisks based on a given volume id
        """
        # pylint: disable=line-too-long
        vdisks = DataList({'object': VDisk,
                           'data': DataList.select.DESCRIPTOR,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': [('volume_id', DataList.operator.EQUALS, volume_id)]}}).data
        # pylint: enable=line-too-long
        if vdisks:
            return DataObjectList(vdisks, VDisk)[0]
        return None

    @staticmethod
    def get_by_devicename_and_vpool(devicename, vpool):
        """
        Returns a list of all VDisks based on a given device name and vpool
        """
        # pylint: disable=line-too-long
        vds = DataList({'object': VDisk,
                        'data': DataList.select.DESCRIPTOR,
                        'query': {'type': DataList.where_operator.AND,
                                  'items': [('devicename', DataList.operator.EQUALS, devicename),
                                            ('vpool_guid', DataList.operator.EQUALS, vpool.guid)]}}).data  # noqa
        # pylint: enable=line-too-long
        if vds:
            if len(vds) != 1:
                raise RuntimeError('Invalid amount of vDisks found: {0}'.format(len(vds)))
            return DataObjectList(vds, VDisk)[0]
        return None

    @staticmethod
    def get_without_vmachine():
        """
        Gets all vDisks without a vMachine
        """
        # pylint: disable=line-too-long
        vdisks = DataList({'object': VDisk,
                           'data': DataList.select.DESCRIPTOR,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': [('vmachine_guid', DataList.operator.EQUALS, None)]}}).data
        # pylint: enable=line-too-long
        return DataObjectList(vdisks, VDisk)
