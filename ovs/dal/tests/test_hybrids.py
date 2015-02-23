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
import sys
from unittest import TestCase
from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore
from ovs.dal.tests.mockups import FactoryModule, StorageDriver
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.dal.helpers import HybridRunner, Descriptor
from ovs.dal.relations import RelationMapper


class Hybrid(TestCase):
    """
    The basic unittestsuite will test all basic functionality of the DAL framework
    It will also try accessing all dynamic properties of all hybrids making sure
    that code actually works. This however means that all loaded 3rd party libs
    need to be mocked
    """

    @classmethod
    def setUpClass(cls):
        """
        Sets up the unittest, mocking a certain set of 3rd party libraries and extensions.
        This makes sure the unittests can be executed without those libraries installed
        """
        # Replace mocked classes
        sys.modules['ovs.extensions.hypervisor.factory'] = FactoryModule
        sys.modules['ovs.extensions.storageserver.storagedriver'] = StorageDriver

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

    def test_objectproperties(self):
        """
        Validates the correctness of all hybrid objects:
        * They should contain all required properties
        * Properties should have the correct type
        * All dynamic properties should be implemented
        """
        # Some stuff here to dynamically test all hybrid properties
        hybrid_structure = HybridRunner.get_hybrids()
        print ''
        print 'Validating hybrids...'
        for class_descriptor in hybrid_structure.values():
            cls = Descriptor().load(class_descriptor).get_object()
            print '* {0}'.format(cls.__name__)
            relation_info = RelationMapper.load_foreign_relations(cls)
            remote_properties_n = []
            remote_properties_1 = []
            if relation_info is not None:
                for key, info in relation_info.iteritems():
                    if info['list'] is True:
                        remote_properties_n.append(key)
                    else:
                        remote_properties_1.append(key)
            # Make sure certain attributes are correctly set
            self.assertIsInstance(cls._properties, list, '_properties required: {0}'.format(cls.__name__))
            self.assertIsInstance(cls._relations, list, '_relations required: {0}'.format(cls.__name__))
            self.assertIsInstance(cls._dynamics, list, '_dynamics required: {0}'.format(cls.__name__))
            # Check types
            allowed_types = [int, float, str, bool, list, dict]
            for prop in cls._properties:
                is_allowed_type = prop.property_type in allowed_types \
                    or isinstance(prop.property_type, list)
                self.assertTrue(is_allowed_type,
                                '_properties types in {0} should be one of {1}'.format(
                                    cls.__name__, str(allowed_types)
                                ))
            for dynamic in cls._dynamics:
                is_allowed_type = dynamic.return_type in allowed_types \
                    or isinstance(dynamic.return_type, list)
                self.assertTrue(is_allowed_type,
                                '_dynamics types in {0} should be one of {1}'.format(
                                    cls.__name__, str(allowed_types)
                                ))
            instance = cls()
            for prop in cls._properties:
                self.assertEqual(getattr(instance, prop.name), prop.default,
                                 'Default property set correctly')
            # Make sure the type can be instantiated
            self.assertIsNotNone(instance.guid)
            properties = []
            for item in dir(instance):
                if hasattr(cls, item) and isinstance(getattr(cls, item), property):
                    properties.append(item)
            # All expiries should be implemented
            missing_props = []
            for dynamic in instance._dynamics:
                if dynamic.name not in properties:
                    missing_props.append(dynamic.name)
                else:  # ... and should work
                    _ = getattr(instance, dynamic.name)
            self.assertEqual(len(missing_props), 0,
                             'Missing dynamic properties in {0}: {1}'.format(cls.__name__, missing_props))
            # An all properties should be either in the blueprint, relations or expiry
            missing_metadata = []
            for found_prop in properties:
                found = found_prop in [prop.name for prop in cls._properties] \
                    or found_prop in [relation.name for relation in cls._relations] \
                    or found_prop in ['{0}_guid'.format(relation.name) for relation in cls._relations] \
                    or found_prop in [dynamic.name for dynamic in cls._dynamics] \
                    or found_prop in remote_properties_n \
                    or found_prop in remote_properties_1 \
                    or found_prop in ['{0}_guids'.format(key) for key in remote_properties_n] \
                    or found_prop in ['{0}_guid'.format(key) for key in remote_properties_1] \
                    or found_prop == 'guid'
                if not found:
                    missing_metadata.append(found_prop)
            self.assertEqual(len(missing_metadata), 0,
                             'Missing metadata for properties in {0}: {1}'.format(cls.__name__, missing_metadata))
            instance.delete()

if __name__ == '__main__':
    import unittest
    suite = unittest.TestLoader().loadTestsFromTestCase(Hybrid)
    unittest.TextTestRunner(verbosity=2).run(suite)
