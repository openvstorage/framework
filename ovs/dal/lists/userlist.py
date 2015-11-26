# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
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
from ovs.dal.dataobject import DataObjectList
from ovs.dal.hybrids.user import User
from ovs.dal.helpers import Descriptor


class UserList(object):
    """
    This UserList class contains various lists regarding to the User class
    """

    @staticmethod
    def get_user_by_username(username):
        """
        Returns a single User for the given username. Returns None if no user was found
        """
        # pylint: disable=line-too-long
        users = DataList({'object': User,
                          'data': DataList.select.GUIDS,
                          'query': {'type': DataList.where_operator.AND,
                                    'items': [('username', DataList.operator.EQUALS, username)]}}).data  # noqa
        # pylint: enable=line-too-long
        if len(users) == 1:
            return Descriptor(User, users[0]).get_object(True)
        return None

    @staticmethod
    def get_users():
        """
        Returns a list of all Users
        """
        users = DataList({'object': User,
                          'data': DataList.select.GUIDS,
                          'query': {'type': DataList.where_operator.AND,
                                    'items': []}}).data
        return DataObjectList(users, User)
