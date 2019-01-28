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
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.dal.hybrids.disk import Disk
from ovs.constants.roles import BACKEND, DB, DTL, SCRUB, WRITE


class DiskPartition(DataObject):
    """
    The DiskPartition class represents a partition on a physical Disk
    """
    ROLES = DataObject.enumerator('Role', [BACKEND, DB, DTL, SCRUB, WRITE])
    VIRTUAL_STORAGE_LOCATION = '/mnt/storage'

    __properties = [Property('filesystem', str, mandatory=False, doc='The filesystem used on the partition'),
                    Property('state', Disk.STATES.keys(), doc='State of the partition'),
                    Property('offset', int, doc='Offset of the partition'),
                    Property('size', int, doc='Size of the partition'),
                    Property('mountpoint', str, mandatory=False, doc='Mountpoint of the partition, None if not mounted'),
                    Property('aliases', list, doc='The partition aliases'),
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
