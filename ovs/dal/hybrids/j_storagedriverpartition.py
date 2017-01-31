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
    SUBROLE = DataObject.enumerator('Role', ['FCACHE', 'FD', 'MD', 'MDS', 'SCO', 'TLOG'])

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
