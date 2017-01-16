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
TestDisk module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.dal.hybrids.t_testmachine import TestMachine


class TestDisk(DataObject):
    """
    This TestDisk object is used for running unittests.
    WARNING: These properties should not be changed
    """
    __properties = [Property('name', str, unique=True, doc='Name of the test disk'),
                    Property('description', str, mandatory=False, doc='Description of the test disk'),
                    Property('size', float, default=0, doc='Size of the test disk'),
                    Property('order', int, default=0, doc='Order of the test disk'),
                    Property('something', str, mandatory=False, indexed=True, doc='Some property that can be set'),
                    Property('something2', str, mandatory=False, indexed=True, doc='Some other property that can be set'),
                    Property('type', ['ONE', 'TWO'], mandatory=False, doc='Type of the test disk')]
    __relations = [Relation('machine', TestMachine, 'disks', mandatory=False),
                   Relation('storage', TestMachine, 'stored_disks', mandatory=False),
                   Relation('one', TestMachine, 'one', mandatory=False, onetoone=True),
                   Relation('parent', None, 'children', mandatory=False)]
    __dynamics = [Dynamic('used_size', int, 5),
                  Dynamic('wrong_type', int, 5),
                  Dynamic('updatable_int', int, 5),
                  Dynamic('updatable_list', list, 5),
                  Dynamic('updatable_dict', dict, 5),
                  Dynamic('updatable_string', str, 5),
                  Dynamic('predictable', int, 5)]

    # For testing purposes
    wrong_type_data = 0
    dynamic_int = 0
    dynamic_list = []
    dynamic_dict = {}
    dynamic_string = ''

    def _used_size(self):
        """
        Returns a certain fake used_size value
        """
        from random import randint
        return randint(0, self._data['size'])

    def _wrong_type(self):
        """
        Returns the wrong type, should always fail
        """
        return self.wrong_type_data

    def _updatable_int(self):
        """
        Returns an external settable value
        """
        return self.dynamic_int

    def _updatable_list(self):
        """
        Returns an external settable value
        """
        return self.dynamic_list

    def _updatable_dict(self):
        """
        Returns an external settable value
        """
        return self.dynamic_dict

    def _updatable_string(self):
        """
        Returns an external settable value
        """
        return self.dynamic_string

    def _predictable(self):
        """
        A predictable dynamic property
        """
        return self.size
