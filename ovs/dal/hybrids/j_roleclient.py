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
RoleClient module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Relation
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.client import Client


class RoleClient(DataObject):
    """
    The RoleClient class represents the junction table between Role and Client.
    Examples:
    * my_role.clients[0].client
    * my_client.roles[0].role
    """
    __properties = []
    __relations = [Relation('role', Role, 'clients'),
                   Relation('client', Client, 'roles')]
    __dynamics = []
