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
BearerTokenList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.bearertoken import BearerToken


class BearerTokenList(object):
    """
    This BearerTokenList class contains various lists regarding to the BearerToken class
    """

    @staticmethod
    def get_by_access_token(access_token):
        """
        Returns a single BearerToken for the given token. Returns None if no BearerToken was found
        """
        return DataList(BearerToken, {'type': DataList.where_operator.AND,
                                      'items': [('access_token', DataList.operator.EQUALS, access_token)]})

    @staticmethod
    def get_by_refresh_token(refresh_token):
        """
        Returns a single BearerToken for the given token. Returns None if no BearerToken was found
        """
        return DataList(BearerToken, {'type': DataList.where_operator.AND,
                                      'items': [('refresh_token', DataList.operator.EQUALS, refresh_token)]})
