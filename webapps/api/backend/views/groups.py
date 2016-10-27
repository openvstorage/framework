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
Module for groups
"""

from backend.decorators import required_roles, return_object, return_list, load, log
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

    @log()
    @required_roles(['read'])
    @return_list(Group)
    @load()
    def list(self):
        """
        Lists all available Groups
        """
        return GroupList.get_groups()

    @log()
    @required_roles(['read'])
    @return_object(Group)
    @load(Group)
    def retrieve(self, group):
        """
        Load information about a given Group
        :param group: The Group to be returned
        :type group: Group
        """
        return group
