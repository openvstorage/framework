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
MgmtCenterList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.mgmtcenter import MgmtCenter


class MgmtCenterList(object):
    """
    This MgmtCenterList class contains various lists regarding to the MgmtCenter class
    """

    @staticmethod
    def get_mgmtcenters():
        """
        Returns a list of MgmtCenters
        """
        return DataList(MgmtCenter, {'type': DataList.where_operator.AND,
                                     'items': []})

    @staticmethod
    def get_by_ip(ip):
        """
        Gets a mgmtCenter based on a given ip address
        """
        mgmtcenters = DataList(MgmtCenter, {'type': DataList.where_operator.AND,
                                            'items': [('ip', DataList.operator.EQUALS, ip)]})
        if len(mgmtcenters) > 0:
            return mgmtcenters[0]
        return None
