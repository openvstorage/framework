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
LicenseList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.license import License


class LicenseList(object):
    """
    This LicenseList class contains various lists regarding to the License class
    """

    @staticmethod
    def get_by_component(component, return_as_list=False):
        """
        Returns a single License for the given name. Returns None if no license was found
        """
        licenses = DataList(License, {'type': DataList.where_operator.AND,
                                      'items': [('component', DataList.operator.EQUALS, component)]})
        if return_as_list is True:
            return licenses
        if len(licenses) == 1:
            return licenses[0]
        return None

    @staticmethod
    def get_licenses():
        """
        Returns a list of all Licenses
        """
        return DataList(License, {'type': DataList.where_operator.AND,
                                  'items': []})
