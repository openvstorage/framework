# Copyright 2014 CloudFounders NV
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
Contains various helping classes
"""
import re


class Toolbox:
    """
    This class contains generic methods
    """
    @staticmethod
    def is_uuid(string):
        """
        Checks whether a given string is a valid guid
        """
        regex = re.compile('^[0-9a-f]{22}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
        return regex.match(string)

    @staticmethod
    def is_user_in_roles(user, roles):
        """
        Checks whether a user is member of a set of roles
        """
        user_roles = [j.role.code for j in user.group.roles]
        for required_role in roles:
            if required_role not in user_roles:
                return False
        return True
