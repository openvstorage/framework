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
BrandingList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.branding import Branding


class BrandingList(object):
    """
    This BrandingList class contains various lists regarding to the Branding class
    """

    @staticmethod
    def get_brandings():
        """
        Returns a list of all brandings
        """
        return DataList(Branding, {'type': DataList.where_operator.AND,
                                   'items': []})
