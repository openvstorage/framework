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
from backend.exceptions import HttpForbiddenException
from backend.serializers.serializers import FullSerializer
from backend.toolbox import Toolbox
from ovs.dal.lists.backendlist import BackendList
from ovs.dal.lists.backendtypelist import BackendTypeList
from ovs.dal.hybrids.backend import Backend
from ovs.dal.hybrids.client import Client
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.user import User
from ovs.dal.hybrids.j_backendclient import BackendClient
from ovs.dal.hybrids.j_backenddomain import BackendDomain
from ovs.dal.hybrids.j_backenduser import BackendUser


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
    def list(self, request, backend_type=None):
        """
        Overview of all backends (from a certain type, if given) on the local node (or a remote one)
        """
        if backend_type is None:
            possible_backends = BackendList.get_backends()
        else:
            possible_backends = BackendTypeList.get_backend_type_by_code(backend_type).backends
        backends = []
        for backend in possible_backends:
            if Toolbox.access_granted(request.client,
                                      user_rights=backend.user_rights,
                                      client_rights=backend.client_rights):
                backends.append(backend)
        return backends

    @log()
    @required_roles(['read'])
    @return_object(Backend)
    @load(Backend)
    def retrieve(self, backend, request):
        """
        Load information about a given backend
        """
        if Toolbox.access_granted(request.client,
                                  user_rights=backend.user_rights,
                                  client_rights=backend.client_rights):
            return backend
        raise HttpForbiddenException(error_description='The requesting client has no access to this Backend',
                                     error='no_ownership')

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
        :param backend: The Backend to update
        :type backend: Backend
        :param domain_guids: A list of Domain guids
        :type domain_guids: list
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

    @action()
    @log()
    @required_roles(['manage'])
    @return_plain()
    @load(Backend)
    def configure_rights(self, backend, new_rights):
        """
        Configures the access rights for this backend
        :param backend: The backend to configure
        :type backend: Backend
        :param new_rights: New access rights
        :type new_rights: dict

        Example of new_rights.
        {'users': {'guida': True,
                   'guidb': True,
                   'guidc': False},
         'clients': {'guidd': False,
                     'guide': True}}
        """
        # Users
        matched_guids = []
        for user_guid, grant in new_rights.get('users', {}).iteritems():
            found = False
            for user_right in backend.user_rights:
                if user_right.user_guid == user_guid:
                    user_right.grant = grant
                    user_right.save()
                    matched_guids.append(user_right.guid)
                    found = True
            if found is False:
                user_right = BackendUser()
                user_right.backend = backend
                user_right.user = User(user_guid)
                user_right.grant = grant
                user_right.save()
                matched_guids.append(user_right.guid)
        for user_right in backend.user_rights:
            if user_right.guid not in matched_guids:
                user_right.delete()
        # Clients
        matched_guids = []
        for client_guid, grant in new_rights.get('clients', {}).iteritems():
            found = False
            for client_right in backend.client_rights:
                if client_right.client_guid == client_guid:
                    client_right.grant = grant
                    client_right.save()
                    matched_guids.append(client_right.guid)
                    found = True
            if found is False:
                client_right = BackendClient()
                client_right.backend = backend
                client_right.client = Client(client_guid)
                client_right.grant = grant
                client_right.save()
                matched_guids.append(client_right.guid)
        for client_right in backend.client_rights:
            if client_right.guid not in matched_guids:
                client_right.delete()
        backend.invalidate_dynamics(['access_rights'])
        return backend.access_rights
