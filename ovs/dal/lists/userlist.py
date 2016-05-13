# Copyright 2016 iNuron NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
