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
GroupList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.group import Group


class GroupList(object):
    """
    This GroupList class contains various lists regarding to the Group class
    """

    @staticmethod
    def get_groups():
        """
        Returns a list of all Groups
        """
        return DataList(Group, {'type': DataList.where_operator.AND,
                                'items': []})
