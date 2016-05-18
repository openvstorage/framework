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
TestMachine module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property


class TestMachine(DataObject):
    """
    This TestMachine object is used for running unittests.
    WARNING: These properties should not be changed
    """
    __properties = [Property('name', str, doc='Name of the test machine')]
    __relations = []
    __dynamics = []
