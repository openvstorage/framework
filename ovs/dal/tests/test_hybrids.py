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
Basic test module
"""
import sys
from unittest import TestCase
from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore
from ovs.dal.tests.mockups import FactoryModule
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.dal.helpers import HybridRunner, Descriptor
from ovs.dal.relations.relations import RelationMapper


class Basic(TestCase):
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
        for class_descriptor in hybrid_structure.values():
            cls = Descriptor().load(class_descriptor).get_object()
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
            self.assertIsInstance(cls._blueprint, dict, '_blueprint required: {0}'.format(cls.__name__))
            self.assertIsInstance(cls._relations, dict, '_relations required: {0}'.format(cls.__name__))
            self.assertIsInstance(cls._expiry, dict, '_expiry required: {0}'.format(cls.__name__))
            # Check types
            allowed_types = [int, float, str, bool, list, dict]
            for key in cls._blueprint:
                is_allowed_type = cls._blueprint[key][1] in allowed_types \
                    or isinstance(cls._blueprint[key][1], list)
                self.assertTrue(is_allowed_type,
                                '_blueprint types in {0} should be one of {1}'.format(
                                    cls.__name__, str(allowed_types)
                                ))
            for key in cls._expiry:
                is_allowed_type = cls._expiry[key][1] in allowed_types \
                    or isinstance(cls._expiry[key][1], list)
                self.assertTrue(is_allowed_type,
                                '_expiry types in {0} should be one of {1}'.format(
                                    cls.__name__, str(allowed_types)
                                ))
            instance = cls()
            for key, default in cls._blueprint.iteritems():
                self.assertEqual(getattr(instance, key), default[0],
                                 'Default property set correctly')
            # Make sure the type can be instantiated
            self.assertIsNotNone(instance.guid)
            properties = []
            for item in dir(instance):
                if hasattr(cls, item) and isinstance(getattr(cls, item), property):
                    properties.append(item)
            # All expiries should be implemented
            missing_props = []
            for attribute in instance._expiry.keys():
                if attribute not in properties:
                    missing_props.append(attribute)
                else:  # ... and should work
                    _ = getattr(instance, attribute)
            self.assertEqual(len(missing_props), 0,
                             'Missing dynamic properties in {0}: {1}'.format(cls.__name__, missing_props))
            # An all properties should be either in the blueprint, relations or expiry
            missing_metadata = []
            for prop in properties:
                found = prop in cls._blueprint \
                    or prop in cls._relations \
                    or prop in ['{0}_guid'.format(key) for key in cls._relations.keys()] \
                    or prop in cls._expiry \
                    or prop in remote_properties_n \
                    or prop in remote_properties_1 \
                    or prop in ['{0}_guids'.format(key) for key in remote_properties_n] \
                    or prop in ['{0}_guid'.format(key) for key in remote_properties_1] \
                    or prop == 'guid'
                if not found:
                    missing_metadata.append(prop)
            self.assertEqual(len(missing_metadata), 0,
                             'Missing metadata for properties in {0}: {1}'.format(cls.__name__, missing_metadata))
            instance.delete()
