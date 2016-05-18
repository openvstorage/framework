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
UserList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.user import User


class UserList(object):
    """
    This UserList class contains various lists regarding to the User class
    """

    @staticmethod
    def get_user_by_username(username):
        """
        Returns a single User for the given username. Returns None if no user was found
        """
        users = DataList(User, {'type': DataList.where_operator.AND,
                                'items': [('username', DataList.operator.EQUALS, username)]})
        if len(users) == 1:
            return users[0]
        return None

    @staticmethod
    def get_users():
        """
        Returns a list of all Users
        """
        return DataList(User, {'type': DataList.where_operator.AND,
                               'items': []})
