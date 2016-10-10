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
Group module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property


class Group(DataObject):
    """
    The Group class represents a Group. A group is used to bind a set of Users to a set of Roles.
    """
    __properties = [Property('name', str, unique=True, doc='Name of the Group.'),
                    Property('description', str, mandatory=False, doc='Description of the Group.')]
    __relations = []
    __dynamics = []
