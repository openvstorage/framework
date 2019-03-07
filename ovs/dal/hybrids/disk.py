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
Disk module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.dataobject.attributes import Dynamic


class Disk(DataObject):
    """
    The Disk class represents physical disks that are available to a storagerouter (technically, they can be
    virtual disks, but from the OS (and framework) point of view, they're considered physical)
    """
    STATES = DataObject.enumerator('state', ['OK', 'FAILURE', 'MISSING'])

    __properties = [Property('aliases', list, doc='The device aliases'),
                    Property('model', str, mandatory=False, doc='The disks model'),
                    Property('serial', str, mandatory=False, doc='The disks serial number, if available'),
                    Property('state', STATES.keys(), doc='The state of the disk'),
                    Property('name', str, doc='Name of the disk (e.g. sda)'),
                    Property('size', int, doc='Size of the disk, in bytes'),
                    Property('is_ssd', bool, doc='The type of the disk')]
    __relations = [Relation('storagerouter', StorageRouter, 'disks')]

    a_test = Dynamic(str, 1)
    a_test_implicit = Dynamic(str, 1)

    @a_test.associate_function
    def test_function(self, dynamic):
        print dynamic.func
        return self.name

    def _a_test_implicit(self, dynamic):
        print dynamic.func
        return self.name