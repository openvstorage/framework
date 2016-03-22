# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
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
        return DataList(VDisk, {'type': DataList.where_operator.AND,
                                'items': []})

    @staticmethod
    def get_vdisk_by_volume_id(volume_id):
        """
        Returns a list of all VDisks based on a given volume id
        """
        vdisks = DataList(VDisk, {'type': DataList.where_operator.AND,
                                  'items': [('volume_id', DataList.operator.EQUALS, volume_id)]})
        if len(vdisks) > 0:
            return vdisks[0]
        return None

    @staticmethod
    def get_vdisk_by_name(vdiskname):
        """
        Returns all VDisks which have a given name
        """
        vdisks = DataList(VDisk, {'type': DataList.where_operator.AND,
                                  'items': [('name', DataList.operator.EQUALS, vdiskname)]})
        if len(vdisks) > 0:
            return vdisks
        return None

    @staticmethod
    def get_by_devicename_and_vpool(devicename, vpool):
        """
        Returns a list of all VDisks based on a given device name and vpool
        """
        vds = DataList(VDisk, {'type': DataList.where_operator.AND,
                               'items': [('devicename', DataList.operator.EQUALS, devicename),
                                         ('vpool_guid', DataList.operator.EQUALS, vpool.guid)]})
        if len(vds) > 0:
            if len(vds) != 1:
                raise RuntimeError('Invalid amount of vDisks found: {0}'.format(len(vds)))
            return vds[0]
        return None

    @staticmethod
    def get_without_vmachine():
        """
        Gets all vDisks without a vMachine
        """
        return DataList(VDisk, {'type': DataList.where_operator.AND,
                                'items': [('vmachine_guid', DataList.operator.EQUALS, None)]})

    @staticmethod
    def get_by_parentsnapshot(snapshotid):
        """
        Gets all vDisks whose parentsnapshot is snapshotid
        """
        return DataList(VDisk, {'type': DataList.where_operator.AND,
                                'items': [('parentsnapshot', DataList.operator.EQUALS, snapshotid)]})
