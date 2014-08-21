#!/usr/bin/python2
#  Copyright 2014 CloudFounders NV
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
Basic test module
"""
import uuid
from unittest import TestCase
from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.dal.hybrids.t_testmachine import TestMachine
from ovs.dal.datalist import DataList


class Extended(TestCase):
    """
    The extended unittestsuite will test a few more extended functions of the framework. It can be executed in
    integration tests, and if the tested codepaths change (since it's slower than the basic tests)
    """

    @classmethod
    def setUpClass(cls):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        PersistentFactory.store = DummyPersistentStore()
        PersistentFactory.store.clean()
        PersistentFactory.store.clean()
        VolatileFactory.store = DummyVolatileStore()
        VolatileFactory.store.clean()
        VolatileFactory.store.clean()

    @classmethod
    def setUp(cls):
        """
        (Re)Sets the stores on every test
        """
        PersistentFactory.store = DummyPersistentStore()
        PersistentFactory.store.clean()
        VolatileFactory.store = DummyVolatileStore()
        VolatileFactory.store.clean()

    @classmethod
    def tearDownClass(cls):
        """
        Clean up the unittest
        """
        pass

    def test_pk_stretching(self):
        """
        Validates whether the primary key lists scale correctly.
        """
        machine = TestMachine()
        keys = DataList._get_pks(machine._namespace, machine._name)
        self.assertEqual(len(list(keys)), 0, 'There should be no primary keys yet ({0})'.format(len(list(keys))))
        guids = []
        for i in xrange(0, 10000):
            guid = str(uuid.uuid4())
            guids.append(guid)
            DataList.add_pk(machine._namespace, machine._name, guid)
            keys = DataList._get_pks(machine._namespace, machine._name)
            self.assertEqual(len(list(keys)), len(guids), 'There should be no primary keys yet (got {0} instead of {1})'.format(len(list(keys)), len(guids)))
        for guid in guids:
            DataList.delete_pk(machine._namespace, machine._name, guid)
        keys = DataList._get_pks(machine._namespace, machine._name)
        self.assertEqual(len(list(keys)), 0, 'There should be no primary keys ({0})'.format(len(list(keys))))

if __name__ == '__main__':
    import unittest
    suite = unittest.TestLoader().loadTestsFromTestCase(Extended)
    unittest.TextTestRunner(verbosity=2).run(suite)
