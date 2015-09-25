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
DiskPartition module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.disk import Disk


class DiskPartition(DataObject):
    """
    The DiskPartition class represents a partition on a physical Disk
    """
    __properties = [Property('id', str, doc='The partition identifier'),
                    Property('filesystem', str, mandatory=False, doc='The filesystem used on the partition'),
                    Property('state', ['OK', 'ERROR', 'MISSING'], doc='State of the partition'),
                    Property('inode', int, mandatory=False, doc='The partitions inode'),
                    Property('offset', int, doc='Offset of the partition'),
                    Property('size', int, doc='Size of the partition'),
                    Property('mountpoint', str, mandatory=False, doc='Mountpoint of partition, None if not mounted'),
                    Property('path', str, doc='The partition path'),
                    Property('roles', list, default=list(), doc='A list of claimed roles')]
    __relations = [Relation('disk', Disk, 'partitions')]
    __dynamics = []
