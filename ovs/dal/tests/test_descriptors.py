# Copyright (C) 2019 iNuron NV
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
import unittest
from ovs.dal.hybrids.t_descriptor import TestDescriptor
from ovs.dal.hybrids.t_testmachine import TestMachine
from ovs.dal.hybrids.t_descriptor_relation import TestDescriptorRelation
from ovs.dal.hybrids.t_descriptor_relation_many import TestDescriptorRelationMany
from ovs.dal.hybrids.t_descriptor_relation_no_many import TestDescriptorRelationNoManyMap


class DescriptorsTest(unittest.TestCase):

    def test_property(self):
        test = TestDescriptor()
        test_prop = 'test'
        test.test_prop = test_prop
        self.assertEqual(test.test_prop, test_prop)

    def test_dynamic(self):
        test = TestDescriptor()
        # Upon definition: the function is already bound to the descriptor
        self.assertIsNotNone(TestDescriptor.test_dynamic.func)

        with self.assertRaises(AttributeError):
            test.test_dynamic = ''

        self.assertEqual(test.test_dynamic, 'test')

    def test_dynamic_implicit(self):
        """
        Test if the implicit function calling works (fetch the related function through the name)
        """
        test = TestDescriptor()
        # Upon definition: the function is not bound to the descriptor
        self.assertIsNone(TestDescriptor.test_dynamic_implicit.func)

        with self.assertRaises(AttributeError):
            test.test_dynamic_implicit = ''

        self.assertEqual(test.test_dynamic_implicit, 'test')
        # Function is now bound
        self.assertIsNotNone(TestDescriptor.test_dynamic_implicit.func)

    def test_relation(self):
        test = TestDescriptor()
        test_relation = TestDescriptorRelation()

        test.test_descriptor_relation = test_relation
        self.assertEqual(test.test_descriptor_relation, test_relation)
        self.assertEqual(test.test_descriptor_relation_guid, test_relation.guid)

    def test_relation_faulty(self):
        """
        Test faulty relations
        :return:
        """
        test_machine = TestMachine()
        test = TestDescriptor()
        with self.assertRaises(TypeError):
            test.test_descriptor_relation = test_machine

    def test_relation_many_side(self):
        test = TestDescriptorRelationMany()

        self.assertIsNone(test.test_descriptors)
        self.assertIsNone(test.test_descriptors_guids)

    def test_relation_many_side_faulty(self):
        test = TestDescriptorRelationMany()

        with self.assertRaises(LookupError):
            _ = test.test_descriptors_invalid

        with self.assertRaises(LookupError):
            _ = test.test_descriptors_guids_invalid
