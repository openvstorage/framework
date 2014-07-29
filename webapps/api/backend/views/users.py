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
Module for users
"""

from backend.serializers.user import PasswordSerializer
from backend.serializers.serializers import FullSerializer
from backend.decorators import required_roles, expose, discover, return_object, return_list
from backend.toolbox import Toolbox
from rest_framework import status, viewsets
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from ovs.dal.hybrids.user import User
from ovs.dal.lists.userlist import UserList
import hashlib


class UserViewSet(viewsets.ViewSet):
    """
    Information about Users
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'users'
    base_name = 'users'

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @return_list(User)
    @discover()
    def list(self, request):
        """
        Lists all available Users where the logged in user has access to
        """
        if Toolbox.is_client_in_roles(request.client, ['system']):
            return UserList.get_users()
        else:
            return [request.client.user]

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @return_object(User)
    @discover(User)
    def retrieve(self, request, user):
        """
        Load information about a given User
        Only the currently logged in User is accessible, or all if the logged in User has a
        system role
        """
        if user.guid == request.client.user_guid or Toolbox.is_client_in_roles(request.client, ['system']):
            return user
        raise PermissionDenied('Fetching user information not allowed')

    @expose(internal=True)
    @required_roles(['view', 'create', 'system'])
    @discover()
    def create(self, request):
        """
        Creates a User
        """
        serializer = FullSerializer(User, instance=User(), data=request.DATA)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @expose(internal=True)
    @required_roles(['view', 'delete', 'system'])
    @discover(User)
    def destroy(self, user):
        """
        Deletes a user
        """
        for client in user.clients:
            for token in client.tokens:
                for junction in token.roles.itersafe():
                    junction.delete()
                token.delete()
            client.delete()
        user.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @discover(User)
    def set_password(self, request, user):
        """
        Sets the password of a given User. A logged in User can only changes its own password,
        or all passwords if the logged in User has a system role
        """
        if user.guid == request.client.user_guid or Toolbox.is_client_in_roles(request.client, ['update', 'system']):
            serializer = PasswordSerializer(data=request.DATA)
            if serializer.is_valid():
                if user.password == hashlib.sha256(str(serializer.data['current_password'])).hexdigest():
                    user.password = hashlib.sha256(str(serializer.data['new_password'])).hexdigest()
                    user.save()
                    # Now, invalidate all access tokens granted
                    for client in user.clients:
                        for token in client.tokens:
                            for junction in token.roles:
                                junction.delete()
                            token.delete()
                    return Response(serializer.data, status=status.HTTP_202_ACCEPTED)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        raise PermissionDenied('Updating password not allowed')
