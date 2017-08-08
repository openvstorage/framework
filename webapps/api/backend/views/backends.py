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

from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from api.backend.decorators import return_object, return_list, load, required_roles, log, return_simple
from api.backend.serializers.serializers import FullSerializer
from api.backend.toolbox import ApiToolbox
from ovs.dal.lists.backendlist import BackendList
from ovs.dal.lists.backendtypelist import BackendTypeList
from ovs.dal.hybrids.backend import Backend
from ovs.dal.hybrids.client import Client
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.user import User
from ovs.dal.hybrids.j_backendclient import BackendClient
from ovs.dal.hybrids.j_backenddomain import BackendDomain
from ovs.dal.hybrids.j_backenduser import BackendUser
from ovs_extensions.api.exceptions import HttpForbiddenException, HttpNotAcceptableException
from ovs.lib.generic import GenericController


class BackendViewSet(viewsets.ViewSet):
    """
    Information about backends
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'backends'
    base_name = 'backends'

    def _validate_access(self, backend, request):
        _ = self
        if not ApiToolbox.access_granted(request.client,
                                         user_rights=backend.user_rights,
                                         client_rights=backend.client_rights):
            raise HttpForbiddenException(error_description='The requesting client has no access to this Backend',
                                         error='no_ownership')

    @log()
    @required_roles(['read'])
    @return_list(Backend)
    @load()
    def list(self, request, backend_type=None):
        """
        Overview of all backends (from a certain type, if given) on the local node (or a remote one)
        :param request: The raw request
        :type request: Request
        :param backend_type: Optional BackendType code to filter
        :type backend_type: str
        """
        if backend_type is None:
            possible_backends = BackendList.get_backends()
        else:
            possible_backends = BackendTypeList.get_backend_type_by_code(backend_type).backends
        backends = []
        for backend in possible_backends:
            if ApiToolbox.access_granted(request.client,
                                         user_rights=backend.user_rights,
                                         client_rights=backend.client_rights):
                backends.append(backend)
        return backends

    @log()
    @required_roles(['read'])
    @return_object(Backend)
    @load(Backend, validator=_validate_access)
    def retrieve(self, backend):
        """
        Load information about a given backend
        :param backend: The backend to retrieve
        :type backend: Backend
        """
        return backend

    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_object(Backend, mode='created')
    @load()
    def create(self, request):
        """
        Creates a Backend
        :param request: The raw request
        :type request: Request
        """
        serializer = FullSerializer(Backend, instance=Backend(), data=request.DATA)
        backend = serializer.deserialize()
        duplicate = BackendList.get_by_name(backend.name)
        if duplicate is None:
            backend.save()
            return backend
        raise HttpNotAcceptableException(error='duplicate',
                                         error_description='Backend with this name already exists')

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_simple()
    @load(Backend, validator=_validate_access)
    def set_domains(self, backend, domain_guids):
        """
        Configures the given domains to the StorageRouter.
        :param backend: The Backend to update
        :type backend: Backend
        :param domain_guids: A list of Domain guids
        :type domain_guids: list
        :return: None
        :rtype: None
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
        GenericController.run_backend_domain_hooks.delay(backend_guid=backend.guid)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_simple(mode='accepted')
    @load(Backend, validator=_validate_access)
    def configure_rights(self, backend, new_rights):
        """
        Configures the access rights for this backend
        :param backend: The backend to configure
        :type backend: Backend
        :param new_rights: New access rights
        :type new_rights: dict
        :return: New access rights
        :rtype: dict

        Example of `new_rights`.
        {'users': {'guid_a': True,
                   'guid_b': True,
                   'guid_c': False},
         'clients': {'guid_d': False,
                     'guid_e': True}}
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
