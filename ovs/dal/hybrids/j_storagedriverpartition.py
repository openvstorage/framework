# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
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
from ovs.dal.hybrids.j_mdsservice import MDSService
from ovs.dal.hybrids.storagedriver import StorageDriver


class StorageDriverPartition(DataObject):
    """
    The StorageDriverPartition class represents the junction table between StorageDriver and Partitions.
    Examples:
    * my_storagedriver.partitions[0].partition
    * my_partition.storagedrivers[0].storagedriver
    """
    SUBROLE = DataObject.enumerator('Role', ['TLOG', 'MD', 'MDS', 'SCO', 'DTL', 'FD', 'FCACHE'])

    __properties = [Property('number', int, doc='Number of the service in case there is more than one'),
                    Property('size', long, mandatory=False, doc='Size in bytes configured for use'),
                    Property('role', DiskPartition.ROLES.keys(), doc='Role of the partition'),
                    Property('sub_role', SUBROLE.keys(), mandatory=False, doc='Sub-role of this StorageDriverPartition')]
    __relations = [Relation('partition', DiskPartition, 'storagedrivers'),
                   Relation('storagedriver', StorageDriver, 'partitions'),
                   Relation('mds_service', MDSService, 'storagedriver_partitions', mandatory=False)]
    __dynamics = [Dynamic('folder', str, 3600),
                  Dynamic('path', str, 3600)]

    def _folder(self):
        """
        Folder on the mountpoint
        """
        if self.sub_role:
            return '{0}_{1}_{2}_{3}'.format(self.storagedriver.vpool.name, self.role.lower(), self.sub_role.lower(), self.number)
        return '{0}_{1}_{2}'.format(self.storagedriver.vpool.name, self.role.lower(), self.number)

    def _path(self):
        """
        Actual path on filesystem, including mountpoint
        """
        return '{0}/{1}'.format(self.partition.folder, self.folder)
