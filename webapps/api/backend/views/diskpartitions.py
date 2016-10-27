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
DiskPartition module
"""

from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from ovs.dal.lists.diskpartitionlist import DiskPartitionList
from ovs.dal.hybrids.disk import Disk
from ovs.dal.hybrids.diskpartition import DiskPartition
from backend.decorators import required_roles, load, return_list, return_object, log


class DiskPartitionViewSet(viewsets.ViewSet):
    """
    Information about DiskPartitions
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'diskpartitions'
    base_name = 'diskpartitions'

    @log()
    @required_roles(['read'])
    @return_list(DiskPartition)
    @load()
    def list(self, diskguid=None):
        """
        Overview of all disks
        :param diskguid: Disk guid to get the partitions from
        :type diskguid: str
        """
        if diskguid is not None:
            disk = Disk(diskguid)
            return disk.partitions
        return DiskPartitionList.get_partitions()

    @log()
    @required_roles(['read'])
    @return_object(DiskPartition)
    @load(DiskPartition)
    def retrieve(self, diskpartition):
        """
        Load information about a given diskpartition
        :param diskpartition: The DiskPartition to return
        :type diskpartition: DiskPartition
        """
        return diskpartition
