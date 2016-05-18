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
DiskPartitionList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.diskpartition import DiskPartition


class DiskPartitionList(object):
    """
    This DiskPartitionList class contains various lists regarding to the DiskPartition class
    """

    @staticmethod
    def get_partitions():
        """
        Returns a list of all Partitions
        """
        return DataList(DiskPartition, {'type': DataList.where_operator.AND,
                                        'items': []})
