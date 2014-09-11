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
Module for roles
"""

from backend.decorators import required_roles, return_object, return_list, load
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

    @required_roles(['read'])
    @return_list(Role)
    @load()
    def list(self):
        """
        Lists all available Roles
        """
        return RoleList.get_roles()

    @required_roles(['read'])
    @return_object(Role)
    @load(Role)
    def retrieve(self, role):
        """
        Load information about a given Role
        """
        return role
