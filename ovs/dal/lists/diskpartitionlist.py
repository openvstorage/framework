# Copyright 2015 Open vStorage NV
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
DiskPartitionList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
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
        partitions = DataList({'object': DiskPartition,
                               'data': DataList.select.GUIDS,
                               'query': {'type': DataList.where_operator.AND,
                                         'items': []}}).data
        return DataObjectList(partitions, DiskPartition)

    @staticmethod
    def get_partition_for(mountpoint):
        """
        Returns partition object for specific mountpoint
        """
        partitions = DataList({'object': DiskPartition,
                               'data': DataList.select.GUIDS,
                               'query': {'type': DataList.where_operator.AND,
                                         'items': [('mountpoint', DataList.operator.EQUALS, mountpoint)]}}).data
        if len(partitions) == 0:
            # @todo special case when only using a directory which will get stored on the root filesystem
            return DiskPartitionList.get_partition_for('/')
        elif len(partitions) == 1:
            return DataObjectList(partitions, DiskPartition)[0]
        else:
            raise RuntimeError('Only one partition allowed for specific mountpoint {0}, got: {1]'.format(mountpoint,
                                                                                                         len(partitions)))
