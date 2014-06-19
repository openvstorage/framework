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
from backend.decorators import required_roles, expose, validate, get_object, get_list
from backend.toolbox import Toolbox
from rest_framework import status, viewsets
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.hybrids.user import User
from ovs.dal.lists.userlist import UserList
from django.http import Http404
import hashlib


class UserViewSet(viewsets.ViewSet):
    """
    Information about Users
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'users'
    base_name = 'users'

    @staticmethod
    def _get_object(guid):
        """
        Gets a User object, raises a 404 in case the User doesn't exist
        """
        try:
            return User(guid)
        except ObjectNotFoundException:
            raise Http404

    @expose(internal=True, customer=True)
    @required_roles(['view', 'system'])
    @get_list(User)
    def list(self, request, format=None, hints=None):
        """
        Lists all available Users
        """
        _ = request, format, hints
        return UserList.get_users()

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @validate(User)
    @get_object(User)
    def retrieve(self, request, obj):
        """
        Load information about a given User
        Only the currently logged in User is accessible, or all if the logged in User has a
        system role
        """
        _ = format
        loggedin_user = UserList.get_user_by_username(request.user.username)
        if obj.username == loggedin_user.username or Toolbox.is_user_in_roles(loggedin_user, ['system']):
            return obj
        raise PermissionDenied('Fetching user information not allowed')

    @expose(internal=True)
    @required_roles(['view', 'create', 'system'])
    def create(self, request, format=None):
        """
        Creates a User
        """
        _ = format
        serializer = FullSerializer(User, instance=User(), data=request.DATA)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @expose(internal=True)
    @required_roles(['view', 'delete', 'system'])
    def destroy(self, request, pk=None, format=None):
        """
        Deletes a user
        """
        _ = request, format
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        else:
            user = UserViewSet._get_object(pk)
            user.delete()
            return Response(status=status.HTTP_204_NO_CONTENT)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @validate(User)
    def set_password(self, request, obj):
        """
        Sets the password of a given User. A logged in User can only changes its own password,
        or all passwords if the logged in User has a system role
        """
        loggedin_user = UserList.get_user_by_username(request.user.username)
        if obj.username == loggedin_user.username or Toolbox.is_user_in_roles(loggedin_user, ['update', 'system']):
            serializer = PasswordSerializer(data=request.DATA)
            if serializer.is_valid():
                if obj.password == hashlib.sha256(str(serializer.data['current_password'])).hexdigest():
                    obj.password = hashlib.sha256(str(serializer.data['new_password'])).hexdigest()
                    obj.save()
                    return Response(serializer.data, status=status.HTTP_202_ACCEPTED)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        raise PermissionDenied('Updating password not allowed')
