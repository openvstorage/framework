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
from ovs.dal.hybrids.t_testmachine import TestMachine


class TestDisk(DataObject):
    """
    This TestDisk object is used for running unittests.
    WARNING: These properties should not be changed
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':        (None, str,   'Name of the test disk'),
                  'description': (None, str,   'Description of the test disk'),
                  'size':        (0,    float, 'Size of the test disk'),
                  'order':       (0,    int,   'Order of the test disk'),
                  'type':        (None, ['ONE', 'TWO'], 'Type of the test disk')}
    _relations = {'machine': (TestMachine, 'disks'),
                  'storage': (TestMachine, 'stored_disks'),
                  'parent':  (None,        'children')}
    _expiry = {'used_size':  (5, int),
               'wrong_type': (5, int)}
    # pylint: enable=line-too-long

    # For testing purposes
    wrong_type_data = 0

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
