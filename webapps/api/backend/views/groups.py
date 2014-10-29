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
Module for groups
"""

from backend.decorators import required_roles, return_object, return_list, load
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from ovs.dal.hybrids.group import Group
from ovs.dal.lists.grouplist import GroupList


class GroupViewSet(viewsets.ViewSet):
    """
    Information about Groups
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'groups'
    base_name = 'groups'

    @required_roles(['read'])
    @return_list(Group)
    @load()
    def list(self):
        """
        Lists all available Groups
        """
        return GroupList.get_groups()

    @required_roles(['read'])
    @return_object(Group)
    @load(Group)
    def retrieve(self, group):
        """
        Load information about a given Group
        """
        return group
