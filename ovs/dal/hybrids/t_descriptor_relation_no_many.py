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
from ovs.dal.dataobject.attributes import Property


class TestDescriptorRelationNoManyMap(DataObject):
    """
    Is linked up with TestDescriptorRelationMany by his side but not on this object
    Will raise an exception upon accessing the property
    """
    test_prop = Property(str)
