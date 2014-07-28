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
TestEMachine module
"""
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.t_testmachine import TestMachine
from ovs.dal.hybrids.t_testdisk import TestDisk


class TestEMachine(TestMachine):
    """
    This ExtendedDisk object is used for running unittests.
    WARNING: These properties should not be changed
    """
    __properties = [Property('extended', str, mandatory=False, doc='Extended property')]
    __relations = [Relation('the_disk', TestDisk, 'the_machines', mandatory=False)]
    __dynamics = []
