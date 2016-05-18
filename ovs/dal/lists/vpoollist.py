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
VPoolList
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.vpool import VPool


class VPoolList(object):
    """
    This VPoolList class contains various lists regarding to the VPool class
    """

    @staticmethod
    def get_vpools():
        """
        Returns a list of all VPools
        """
        return DataList(VPool, {'type': DataList.where_operator.AND,
                                'items': []})

    @staticmethod
    def get_vpool_by_name(vpool_name):
        """
        Returns all VPools which have a given name
        """
        vpools = DataList(VPool, {'type': DataList.where_operator.AND,
                                  'items': [('name', DataList.operator.EQUALS, vpool_name)]})
        if len(vpools) == 0:
            return None
        if len(vpools) == 1:
            return vpools[0]
        else:
            raise RuntimeError('Only one vPool with name {0} should exist.'.format(vpool_name))
