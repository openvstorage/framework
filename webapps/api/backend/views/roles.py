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
Module for roles
"""

from backend.decorators import required_roles, return_object, return_list, load, log
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from ovs.dal.hybrids.role import Role
from ovs.dal.lists.rolelist import RoleList


class RoleViewSet(viewsets.ViewSet):
    """
    Information about Roles
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'roles'
    base_name = 'roles'

    @log()
    @required_roles(['read'])
    @return_list(Role)
    @load()
    def list(self):
        """
        Lists all available Roles
        """
        return RoleList.get_roles()

    @log()
    @required_roles(['read'])
    @return_object(Role)
    @load(Role)
    def retrieve(self, role):
        """
        Load information about a given Role
        :param role: The Role to be returned
        :type role: Role
        """
        return role
