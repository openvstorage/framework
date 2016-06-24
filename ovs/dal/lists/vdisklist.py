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
    def get_in_volume_ids(volume_ids):
        """
        Returns all vDisks which volume_id is in the given list
        """
        return DataList(VDisk, {'type': DataList.where_operator.AND,
                                'items': [('volume_id', DataList.operator.IN, volume_ids)]})

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
    def get_by_parentsnapshot(snapshotid):
        """
        Gets all vDisks whose parentsnapshot is snapshotid
        """
        return DataList(VDisk, {'type': DataList.where_operator.AND,
                                'items': [('parentsnapshot', DataList.operator.EQUALS, snapshotid)]})
