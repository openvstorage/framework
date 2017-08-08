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
Module for clients
"""

from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from api.backend.decorators import required_roles, return_object, return_list, load, log, return_simple
from api.backend.serializers.serializers import FullSerializer
from api.backend.toolbox import ApiToolbox
from api.oauth2.toolbox import OAuth2Toolbox
from ovs.dal.hybrids.client import Client
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.j_roleclient import RoleClient
from ovs.dal.lists.clientlist import ClientList
from ovs_extensions.api.exceptions import HttpForbiddenException, HttpNotAcceptableException


class ClientViewSet(viewsets.ViewSet):
    """
    Information about Clients
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'clients'
    base_name = 'clients'

    @log()
    @required_roles(['read'])
    @return_list(Client)
    @load()
    def list(self, request, userguid=None, ovs_type=None):
        """
        Lists all available Clients where the logged in user has access to
        :param request: Raw request
        :type request: Request
        :param userguid: User guid to filter the clients
        :type userguid: str
        :param ovs_type: Filter on the Client's ovs_type
        :type ovs_type: str
        """
        if ApiToolbox.is_client_in_roles(request.client, ['manage']):
            client_list = ClientList.get_clients()
        else:
            if ovs_type is not None and ovs_type != 'INTERNAL':
                client_list = [client for client in request.client.user.clients if client.ovs_type == ovs_type]
            else:
                client_list = [client for client in request.client.user.clients if client.ovs_type != 'INTERNAL']
        if userguid is not None:
            return [client for client in client_list if client.user_guid == userguid]
        return client_list

    @log()
    @required_roles(['read'])
    @return_object(Client)
    @load(Client)
    def retrieve(self, request, client):
        """
        Load information about a given Client
        Only the currently logged in User's Clients are accessible, or all if the logged in User has a
        system role
        :param request: Raw request
        :type request: Request
        :param client: Client to return
        :type client: Client
        """
        _ = format
        if client.guid in request.client.user.clients_guids or ApiToolbox.is_client_in_roles(request.client, ['manage']):
            return client
        raise HttpForbiddenException(error_description='Fetching client information not allowed',
                                     error='no_ownership')

    @log()
    @required_roles(['read', 'write'])
    @return_object(Client, mode='created')
    @load()
    def create(self, request, role_guids=None):
        """
        Creates a Client
        :param request: Raw request
        :type request: Request
        :param role_guids: The GUIDs of the roles where the client should get access to
        :type role_guids: str
        """
        if 'role_guids' in request.DATA:
            del request.DATA['role_guids']
        serializer = FullSerializer(Client, instance=Client(), data=request.DATA)
        client = serializer.deserialize()
        if client.user is not None:
            if client.user_guid == request.client.user_guid or ApiToolbox.is_client_in_roles(request.client, ['manage']):
                client.grant_type = 'CLIENT_CREDENTIALS'
                client.client_secret = OAuth2Toolbox.create_hash(64)
                client.save()
                if not role_guids:
                    roles = [junction.role for junction in client.user.group.roles]
                else:
                    possible_role_guids = [junction.role_guid for junction in client.user.group.roles]
                    roles = [Role(guid) for guid in role_guids if guid in possible_role_guids]
                for role in roles:
                    roleclient = RoleClient()
                    roleclient.client = client
                    roleclient.role = role
                    roleclient.save()
                return client
        raise HttpNotAcceptableException(error_description='A client must have a user',
                                         error='invalid_data')

    @log()
    @required_roles(['read', 'write'])
    @return_simple()
    @load(Client)
    def destroy(self, request, client):
        """
        Deletes a user
        :param request: Raw request
        :type request: Request
        :param client: The Client to be deleted
        :type client: Client
        :return: None
        :rtype: None
        """
        if client.user_guid == request.client.user_guid or ApiToolbox.is_client_in_roles(request.client, ['manage']):
            for token in client.tokens:
                for junction in token.roles.itersafe():
                    junction.delete()
                token.delete()
            for junction in client.roles.itersafe():
                junction.delete()
            client.delete()
        else:
            return HttpForbiddenException(error_description='Deleting this client is now allowed',
                                          error='no_ownership')
