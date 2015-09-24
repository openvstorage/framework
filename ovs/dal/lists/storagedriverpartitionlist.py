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
StorageDriverPartitionList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition


class StorageDriverPartitionList(object):
    """
    This StorageDriverPartitionListList class contains various lists regarding to the StorageDriverPartitionList class
    """

    @staticmethod
    def get_partitions():
        """
        Returns a list of all StorageDriverPartitions
        """
        sd_partitions = DataList({'object': StorageDriverPartition,
                                  'data': DataList.select.GUIDS,
                                  'query': {'type': DataList.where_operator.AND,
                                            'items': []}}).data
        return DataObjectList(sd_partitions, StorageDriverPartition)

    @staticmethod
    def get_partitions_by_storagedriver_and_usage(storagedriver, usage):
        """
        Returns a list of all StorageDriverPartitions
        """
        storagedriver_guid = None if storagedriver is None else storagedriver.guid
        sd_partitions = DataList({'object': StorageDriverPartition,
                                  'data': DataList.select.GUIDS,
                                  'query': {'type': DataList.where_operator.AND,
                                            'items': [('storagedriver_guid', DataList.operator.EQUALS, storagedriver_guid),
                                                      ('usage', DataList.operator.EQUALS, usage)]}}).data
        return DataObjectList(sd_partitions, StorageDriverPartition)
