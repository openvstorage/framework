# Copyright 2014 CloudFounders NV
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
    __properties = [Property('name', str, doc='Name of the test disk'),
                    Property('description', str, mandatory=False, doc='Description of the test disk'),
                    Property('size', float, default=0, doc='Size of the test disk'),
                    Property('order', int, default=0, doc='Order of the test disk'),
                    Property('type', ['ONE', 'TWO'], mandatory=False, doc='Type of the test disk')]
    __relations = [Relation('machine', TestMachine, 'disks', mandatory=False),
                   Relation('storage', TestMachine, 'stored_disks', mandatory=False),
                   Relation('one', TestMachine, 'one', mandatory=False, onetoone=True),
                   Relation('parent', None, 'children', mandatory=False)]
    __dynamics = [Dynamic('used_size', int, 5),
                  Dynamic('wrong_type', int, 5),
                  Dynamic('updatable', int, 5)]

    # For testing purposes
    wrong_type_data = 0
    dynamic_value = 0

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

    def _updatable(self):
        """
        Returns an external settable value
        """
        return self.dynamic_value
