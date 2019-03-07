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
from ovs.dal.dataobject.attributes import Property, RelationGuid, Relation, RelationTypes


class TestDescriptorRelationMany(DataObject):
    """
    Test object to test many to one relations
    """
    test_prop = Property(str)

    test_descriptors = Relation('TestDescriptor', relation_type=RelationTypes.MANYTOONE)
    test_descriptors_guids = RelationGuid(test_descriptors)

    # Will raise a Lookup error
    test_descriptors_invalid = Relation('TestDescriptorRelationNoManyMap', relation_type=RelationTypes.MANYTOONE)
    test_descriptors_guids_invalid = RelationGuid(test_descriptors_invalid)
