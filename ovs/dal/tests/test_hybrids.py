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
Basic test module
"""
import unittest
from ovs.dal.helpers import HybridRunner, Descriptor
from ovs.dal.relations import RelationMapper
from ovs.lib.tests.helpers import Helper


class Hybrid(unittest.TestCase):
    """
    The basic unittest suite will test all basic functionality of the DAL framework
    It will also try accessing all dynamic properties of all hybrids making sure
    that code actually works. This however means that all loaded 3rd party libs
    need to be mocked
    """
    def setUp(self):
        """
        (Re)Sets the stores on every test
        """
        self.debug = False
        Helper.setup()

    def tearDown(self):
        """
        Clean up after every UnitTest
        """
        Helper.teardown()

    def _print_message(self, message):
        if self.debug is True:
            print message

    def test_objectproperties(self):
        """
        Validates the correctness of all hybrid objects:
        * They should contain all required properties
        * Properties should have the correct type
        * All dynamic properties should be implemented
        """
        # Some stuff here to dynamically test all hybrid properties
        hybrid_structure = HybridRunner.get_hybrids()
        self._print_message('')
        self._print_message('Validating hybrids...')
        for class_descriptor in hybrid_structure.values():
            cls = Descriptor().load(class_descriptor).get_object()
            self._print_message('* {0}'.format(cls.__name__))
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
            allowed_types = [int, float, long, str, bool, list, dict, set]
            unique_types = [int, float, long, str]
            indexed_types = [int, float, long, str, bool]
            for prop in cls._properties:
                is_allowed_type = prop.property_type in allowed_types \
                    or isinstance(prop.property_type, list)
                self.assertTrue(is_allowed_type,
                                '_property {0}.{1} should be one of {2}'.format(
                                    cls.__name__, prop.name, allowed_types
                                ))
                if prop.unique is True:
                    self.assertIn(prop.property_type, unique_types,
                                  '_property {0}.{1} can only be unique if it is one of {2}'.format(
                                      cls.__name__, prop.name, unique_types
                                  ))
                if prop.indexed is True:
                    self.assertIn(prop.property_type, indexed_types,
                                  '_property {0}.{1} can only be indexed if it is one of {2}'.format(
                                      cls.__name__, prop.name, indexed_types
                                  ))
            for dynamic in cls._dynamics:
                is_allowed_type = dynamic.return_type in allowed_types \
                    or isinstance(dynamic.return_type, list)
                self.assertTrue(is_allowed_type,
                                '_dynamic {0}.{1} should be one of {2}'.format(
                                    cls.__name__, dynamic.name, str(allowed_types)
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
            # All expires should be implemented
            missing_props = []
            for dynamic in instance._dynamics:
                if dynamic.name not in properties:
                    missing_props.append(dynamic.name)
            self.assertEqual(len(missing_props), 0,
                             'Missing dynamic properties in {0}: {1}'.format(cls.__name__, missing_props))
            # An all properties should be either in the blueprint, relations or expiry
            missing_metadata = []
            for found_prop in properties:
                found = found_prop in [prop.name for prop in cls._properties] \
                    or found_prop in (cls._fixed_properties if hasattr(cls, '_fixed_properties') else []) \
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
