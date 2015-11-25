# Copyright 2015 iNuron NV
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
