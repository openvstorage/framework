# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module for clients
"""

from backend.serializers.serializers import FullSerializer
from backend.decorators import required_roles, expose, return_object, return_list, discover
from backend.toolbox import Toolbox
from oauth2.toolbox import Toolbox as OAuth2Toolbox
from rest_framework import status, viewsets
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from ovs.dal.hybrids.client import Client
from ovs.dal.hybrids.j_roleclient import RoleClient
from ovs.dal.lists.clientlist import ClientList


class ClientViewSet(viewsets.ViewSet):
    """
    Information about Clients
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'clients'
    base_name = 'clients'

    @expose(internal=True)
    @required_roles(['view'])
    @return_list(Client)
    @discover()
    def list(self, request, userguid=None):
        """
        Lists all available Clients where the logged in user has access to
        """
        if Toolbox.is_client_in_roles(request.client, ['system']):
            client_list = ClientList.get_clients()
        else:
            client_list = [client for client in request.client.user.clients if client.ovs_type != 'FRONTEND']
        if userguid is not None:
            return [client for client in client_list if client.user_guid == userguid]
        return client_list

    @expose(internal=True)
    @required_roles(['view'])
    @return_object(Client)
    @discover(Client)
    def retrieve(self, request, client):
        """
        Load information about a given Client
        Only the currently logged in User's Clients are accessible, or all if the logged in User has a
        system role
        """
        _ = format
        if client.guid in request.client.user.clients_guids or Toolbox.is_client_in_roles(request.client, ['system']):
            return client
        raise PermissionDenied('Fetching user information not allowed')

    @expose(internal=True)
    @required_roles(['view', 'create', 'system'])
    @discover()
    def create(self, request):
        """
        Creates a Client
        """
        serializer = FullSerializer(Client, instance=Client(), data=request.DATA)
        if serializer.is_valid():
            obj = serializer.object
            if obj.user is not None:
                obj.grant_type = 'CLIENT_CREDENTIALS'
                obj.client_secret = OAuth2Toolbox.create_hash(64)
                serializer.save()
                for junction in obj.user.group.roles:
                    roleclient = RoleClient()
                    roleclient.client = obj
                    roleclient.role = junction.role
                    roleclient.save()
                return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @expose(internal=True)
    @required_roles(['view', 'delete', 'system'])
    @discover(Client)
    def destroy(self, client):
        """
        Deletes a user
        """
        for token in client.tokens:
            for junction in token.roles.itersafe():
                junction.delete()
            token.delete()
        client.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
