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
RoleList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.role import Role


class RoleList(object):
    """
    This RoleList class contains various lists regarding to the Role class
    """

    @staticmethod
    def get_roles():
        """
        Returns a list of all Roles
        """
        return DataList(Role, {'type': DataList.where_operator.AND,
                               'items': []})

    @staticmethod
    def get_role_by_code(code):
        """
        Returns a single Role for the given code. Returns None if no Role was found
        """
        roles = DataList(Role, {'type': DataList.where_operator.AND,
                                'items': [('code', DataList.operator.EQUALS, code)]})
        if len(roles) == 1:
            return roles[0]
        return None

    @staticmethod
    def get_roles_by_codes(codes):
        """
        Returns a list of Roles for a list of codes
        """
        return DataList(Role, {'type': DataList.where_operator.AND,
                               'items': [('code', DataList.operator.IN, codes)]})
