# Copyright 2014 Open vStorage NV
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
StorageDriverPartition module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.storagedriver import StorageDriver

VIRTUAL_STORAGE_LOCATION = '/mnt/storage'

class StorageDriverPartition(DataObject):
    """
    The StorageDriverPartition class represents the junction table between StorageDriver and Partitions.
    Examples:
    * my_storagedriver.partitions[0].partition
    * my_partition.storagedrivers[0].storagedriver
    """
    __properties = [Property('number', int, doc='Number of the service in case there are more than one'),
                    Property('usage', ['bfs', 'db', 'dtl', 'fragment', 'md', 'read', 'scrub', 'tmp', 'write'],
                             doc='Usage of partition')]
    __relations = [Relation('partition', DiskPartition, 'storagedrivers'),
                   Relation('storagedriver', StorageDriver, 'partitions')]
    __dynamics = [Dynamic('path', str, 86400),
                  Dynamic('mountpoint', str, 86400)]

    def _path(self):
        """
        Actual path on filesystem
        """
        if self.partition.mountpoint == '/':
            return '/'.join([VIRTUAL_STORAGE_LOCATION, self.usage + str(self.number) + '_' +
                         self.storagedriver.vpool.name])
        else:
            return '/'.join([self.partition.mountpoint, self.usage + str(self.number) + '_' +
                         self.storagedriver.vpool.name])

    def _mountpoint(self):
        """
        Mountpoint on storagedriver
        """
        return self.partition.mountpoint
