# Copyright 2016 iNuron NV
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
TestStorageDriver module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.t_teststoragerouter import TestStorageRouter
from ovs.dal.hybrids.t_testvpool import TestVPool
from ovs.dal.structures import Property, Relation


class TestStorageDriver(DataObject):
    """
    This TestStorageDriver object is used for running unittests.
    WARNING: These properties should not be changed
    """
    __properties = [Property('name', str, doc='Name of the test machine')]
    __relations = [Relation('vpool', TestVPool, 'storagedrivers'),
                   Relation('storagerouter', TestStorageRouter, 'storagedrivers')]
    __dynamics = []
