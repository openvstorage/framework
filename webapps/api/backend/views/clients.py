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

from oauth2.toolbox import Toolbox as OAuth2Toolbox
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from backend.exceptions import HttpForbiddenException
from backend.serializers.serializers import FullSerializer
from backend.decorators import required_roles, return_object, return_list, load, log
from backend.toolbox import Toolbox
from ovs.dal.hybrids.client import Client
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.j_roleclient import RoleClient
from ovs.dal.lists.clientlist import ClientList


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
        """
        if Toolbox.is_client_in_roles(request.client, ['manage']):
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
        """
        _ = format
        if client.guid in request.client.user.clients_guids or Toolbox.is_client_in_roles(request.client, ['manage']):
            return client
        raise HttpForbiddenException(error_description='Fetching client information not allowed',
                                     error='no_ownership')

    @log()
    @required_roles(['read', 'write'])
    @load()
    def create(self, request, role_guids=None):
        """
        Creates a Client
        """
        if 'role_guids' in request.DATA:
            del request.DATA['role_guids']
        serializer = FullSerializer(Client, instance=Client(), data=request.DATA)
        if serializer.is_valid():
            client = serializer.object
            if client.user is not None:
                if client.user_guid == request.client.user_guid or Toolbox.is_client_in_roles(request.client, ['manage']):
                    client.grant_type = 'CLIENT_CREDENTIALS'
                    client.client_secret = OAuth2Toolbox.create_hash(64)
                    serializer.save()
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
                    return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @log()
    @required_roles(['read', 'write'])
    @load(Client)
    def destroy(self, request, client):
        """
        Deletes a user
        """
        if client.user_guid == request.client.user_guid or Toolbox.is_client_in_roles(request.client, ['manage']):
            for token in client.tokens:
                for junction in token.roles.itersafe():
                    junction.delete()
                token.delete()
            for junction in client.roles.itersafe():
                junction.delete()
            client.delete()
            return Response(status=status.HTTP_204_NO_CONTENT)
        return HttpForbiddenException(error_description='Deleting this client is now allowed',
                                      error='no_ownership')
