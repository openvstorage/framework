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

"""
TestEMachine module
"""

from ovs.dal.dataobject import DataObject
from ovs.dal.dataobject.attributes import Property, Dynamic, Relation, RelationGuid, RelationTypes


class TestDescriptor(DataObject):
    """
    This ExtendedDisk object is used for running unittests.
    WARNING: These properties should not be changed
    """
    test_prop = Property(str)

    test_dynamic = Dynamic(str, 1)
    test_dynamic_implicit = Dynamic(str, 1)

    test_descriptor_relation = Relation('TestDescriptorRelation', relation_type=RelationTypes.ONETOMANY)
    test_descriptor_relation_guid = RelationGuid(test_descriptor_relation)

    test_descriptor_relation_many = Relation('TestDescriptorRelationMany', relation_type=RelationTypes.ONETOMANY)
    test_descriptor_relation_many_guid = RelationGuid(test_descriptor_relation_many)

    @test_dynamic.associate_function
    def a_different_name(self):
        return 'test'

    def _test_dynamic_implicit(self):
        return 'test'

    # test_relation = Relation
    # test_relation_guid = RelationGuid(test_relation)
