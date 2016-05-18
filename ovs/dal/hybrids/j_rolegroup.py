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
RoleGroup module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Relation
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.group import Group


class RoleGroup(DataObject):
    """
    The RoleGroup class represents the junction table between Role and Group.
    Examples:
    * my_role.groups[0].group
    * my_group.roles[0].role
    """
    __properties = []
    __relations = [Relation('role', Role, 'groups'),
                   Relation('group', Group, 'roles')]
    __dynamics = []
