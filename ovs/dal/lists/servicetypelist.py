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
ServiceTypeList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.servicetype import ServiceType


class ServiceTypeList(object):
    """
    This ServiceTypeList class contains various lists regarding to the ServiceType class
    """

    @staticmethod
    def get_by_name(name):
        """
        Returns a single ServiceType for the given name. Returns None if no ServiceType was found
        """
        servicetypes = DataList(ServiceType, {'type': DataList.where_operator.AND,
                                              'items': [('name', DataList.operator.EQUALS, name)]})
        if len(servicetypes) == 1:
            return servicetypes[0]
        return None

    @staticmethod
    def get_servicetypes():
        """
        Returns a list of all ServiceTypes
        """
        return DataList(ServiceType, {'type': DataList.where_operator.AND,
                                      'items': []})
