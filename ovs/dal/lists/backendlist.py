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
BackendList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.backend import Backend


class BackendList(object):
    """
    This BackendList class contains various lists regarding to the Backend class
    """

    @staticmethod
    def get_backends():
        """
        Returns a list of all Backends
        """
        return DataList(Backend, {'type': DataList.where_operator.AND,
                                  'items': []})

    @staticmethod
    def get_by_name(name):
        """
        Retrieve a backend based on its name
        :param name: Name of the backend
        :type name: str

        :return: Backend or None
        :rtype: Backend
        """
        backends = DataList(Backend, {'type': DataList.where_operator.AND,
                                      'items': [('name', DataList.operator.EQUALS, name)]})
        if len(backends) > 1:
            raise RuntimeError('Invalid amount of Backends found: {0}'.format(len(backends)))
        if len(backends) == 0:
            return None
        return backends[0]
