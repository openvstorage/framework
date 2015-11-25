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
DiskPartition module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.dal.hybrids.disk import Disk


class DiskPartition(DataObject):
    """
    The DiskPartition class represents a partition on a physical Disk
    """
    ROLES = DataObject.enumerator('Role', ['DB', 'READ', 'SCRUB', 'WRITE', 'BACKEND'])
    VIRTUAL_STORAGE_LOCATION = '/mnt/storage'

    __properties = [Property('id', str, doc='The partition identifier'),
                    Property('filesystem', str, mandatory=False, doc='The filesystem used on the partition'),
                    Property('state', ['OK', 'FAILURE', 'MISSING'], doc='State of the partition'),
                    Property('inode', int, mandatory=False, doc='The partitions inode'),
                    Property('offset', int, doc='Offset of the partition'),
                    Property('size', int, doc='Size of the partition'),
                    Property('mountpoint', str, mandatory=False, doc='Mountpoint of the partition, None if not mounted'),
                    Property('path', str, doc='The partition path'),
                    Property('roles', list, default=[], doc='A list of claimed roles')]
    __relations = [Relation('disk', Disk, 'partitions')]
    __dynamics = [Dynamic('usage', list, 120),
                  Dynamic('folder', str, 3600)]

    def _usage(self):
        """
        A dict representing this partition's usage in a more user-friendly form
        """
        dataset = []
        for junction in self.storagedrivers:
            dataset.append({'type': 'storagedriver',
                            'role': junction.role,
                            'size': junction.size,
                            'relation': junction.storagedriver_guid,
                            'folder': junction.folder})
        return dataset

    def _folder(self):
        """
        Corrected mountpoint
        """
        return DiskPartition.VIRTUAL_STORAGE_LOCATION if self.mountpoint == '/' else self.mountpoint
