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
Contains the BackendViewSet
"""

from urllib2 import HTTPError, URLError
from backend.serializers.serializers import FullSerializer
from rest_framework import status, viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.exceptions import NotAcceptable
from ovs.dal.lists.backendlist import BackendList
from ovs.dal.lists.backendtypelist import BackendTypeList
from ovs.dal.hybrids.backend import Backend
from backend.decorators import return_object, return_list, load, required_roles, log
from ovs.extensions.api.client import OVSClient


class BackendViewSet(viewsets.ViewSet):
    """
    Information about backends
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'backends'
    base_name = 'backends'

    @log()
    @required_roles(['read'])
    @return_list(Backend)
    @load()
    def list(self, backend_type=None):
        """
        Overview of all backends (from a certain type, if given) on the local node (or a remote one)
        """
        if backend_type is None:
            return BackendList.get_backends()
        return BackendTypeList.get_backend_type_by_code(backend_type).backends

    @log()
    @required_roles(['read'])
    @return_object(Backend)
    @load(Backend)
    def retrieve(self, backend):
        """
        Load information about a given backend
        """
        return backend

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load()
    def create(self, request):
        """
        Creates a Backend
        """
        serializer = FullSerializer(Backend, instance=Backend(), data=request.DATA)
        if serializer.is_valid():
            duplicate = BackendList.get_by_name(serializer.object.name)
            if duplicate is None:
                serializer.save()
                return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
