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

from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from backend.decorators import return_object, return_list, load, required_roles, log, return_plain
from backend.serializers.serializers import FullSerializer
from ovs.dal.lists.backendlist import BackendList
from ovs.dal.lists.backendtypelist import BackendTypeList
from ovs.dal.hybrids.backend import Backend
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.j_backenddomain import BackendDomain


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

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_plain()
    @load(Backend)
    def set_domains(self, backend, domain_guids):
        """
        Configures the given domains to the StorageRouter.
        :param storagerouter: The StorageRouter to update
        :type storagerouter: StorageRouter
        :param domain_guids: A list of Domain guids
        :type domain_guids: list
        :param recovery_domain_guids: A list of Domain guids to set as recovery Domain
        :type recovery_domain_guids: list
        """
        for junction in backend.domains:
            if junction.domain_guid not in domain_guids:
                junction.delete()
            else:
                domain_guids.remove(junction.domain_guid)
        for domain_guid in domain_guids:
            junction = BackendDomain()
            junction.domain = Domain(domain_guid)
            junction.backend = backend
            junction.save()
        backend.invalidate_dynamics(['regular_domains'])
