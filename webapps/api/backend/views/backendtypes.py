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
Contains the BackendTypeViewSet
"""

from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from ovs.dal.lists.backendtypelist import BackendTypeList
from ovs.dal.hybrids.backendtype import BackendType
from ovs.dal.datalist import DataList
from backend.decorators import return_object, return_list, load, required_roles, log


class BackendTypeViewSet(viewsets.ViewSet):
    """
    Information about backend types
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'backendtypes'
    base_name = 'backendtypes'

    @log()
    @required_roles(['read'])
    @return_list(BackendType)
    @load()
    def list(self, query=None):
        """
        Overview of all backend types
        :param query: Optional filter for BackendTypes
        :type query: DataQuery
        """
        if query is not None:
            return DataList(BackendType, query)
        return BackendTypeList.get_backend_types()

    @log()
    @required_roles(['read'])
    @return_object(BackendType)
    @load(BackendType)
    def retrieve(self, backendtype):
        """
        Load information about a given backend type
        :param backendtype: BackendType to retrieve
        :type backendtype: BackendType
        """
        return backendtype
