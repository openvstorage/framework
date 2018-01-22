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
DiskList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.disk import Disk


class DiskList(object):
    """
    This DiskList class contains various lists regarding to the Disk class
    """

    @staticmethod
    def get_disks():
        """
        Returns a list of all Disks
        """
        return DataList(Disk, {'type': DataList.where_operator.AND,
                               'items': []})

    @staticmethod
    def get_disk_by_name_and_ip(name, ip):
        """
        Returns a Disk on given StorageRouter with given name
        :param name: name of the disk
        :type name: str
        :param ip: IP of the StorageRouter
        :type ip: str
        :return: Disk
        """
        disk = DataList(Disk, {'type': DataList.where_operator.AND,
                               'items':[('storagerouter.ip', DataList.operator.EQUALS, ip),
                                        ('name', DataList.operator.EQUALS, name)]})
        if len(disk) == 0:
            return None
        if len(disk) == 1:
            return disk[0]
        raise RuntimeError('Only one disk should be present with name {0} on storagerouter with ip {1}'.format(name, ip))
