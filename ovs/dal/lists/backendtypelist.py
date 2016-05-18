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
BackendTypeList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.backendtype import BackendType


class BackendTypeList(object):
    """
    This BackendTypeList class contains various lists regarding to the BackendType class
    """

    @staticmethod
    def get_backend_types():
        """
        Returns a list of all Backends
        """
        return DataList(BackendType, {'type': DataList.where_operator.AND,
                                      'items': []})

    @staticmethod
    def get_backend_type_by_code(code):
        """
        Returns a single BackendType for the given code. Returns None if no BackendType was found
        """
        backendtypes = DataList(BackendType, {'type': DataList.where_operator.AND,
                                              'items': [('code', DataList.operator.EQUALS, code)]})
        if len(backendtypes) == 1:
            return backendtypes[0]
        return None
