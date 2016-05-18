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
Module for users
"""

import hashlib
import random
import string
from backend.serializers.user import PasswordSerializer
from backend.serializers.serializers import FullSerializer
from backend.decorators import required_roles, load, return_object, return_list, log
from backend.toolbox import Toolbox
from rest_framework import status, viewsets
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from ovs.dal.hybrids.user import User
from ovs.dal.hybrids.client import Client
from ovs.dal.hybrids.j_roleclient import RoleClient
from ovs.dal.lists.userlist import UserList


class UserViewSet(viewsets.ViewSet):
    """
    Information about Users
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'users'
    base_name = 'users'

    @log()
    @required_roles(['read'])
    @return_list(User)
    @load()
    def list(self, request):
        """
        Lists all available Users where the logged in user has access to
        """
        if Toolbox.is_client_in_roles(request.client, ['manage']):
            return UserList.get_users()
        else:
            return [request.client.user]

    @log()
    @required_roles(['read'])
    @return_object(User)
    @load(User)
    def retrieve(self, request, user):
        """
        Load information about a given User
        Only the currently logged in User is accessible, or all if the logged in User has a
        system role
        """
        if user.guid == request.client.user_guid or Toolbox.is_client_in_roles(request.client, ['manage']):
            return user
        raise PermissionDenied('Fetching user information not allowed')

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load()
    def create(self, request):
        """
        Creates a User
        """
        serializer = FullSerializer(User, instance=User(), data=request.DATA, allow_passwords=True)
        if serializer.is_valid():
            user = serializer.object
            if UserList.get_user_by_username(user.username) is not None:
                return Response('User already exists', status=status.HTTP_303_SEE_OTHER)
            user.save()
            pw_client = Client()
            pw_client.ovs_type = 'INTERNAL'
            pw_client.grant_type = 'PASSWORD'
            pw_client.user = user
            pw_client.save()
            cc_client = Client()
            cc_client.ovs_type = 'INTERNAL'
            cc_client.grant_type = 'CLIENT_CREDENTIALS'
            cc_client.client_secret = ''.join(random.choice(string.ascii_letters +
                                                            string.digits +
                                                            '|_=+*#@!/-[]{}<>.?,\'";:~')
                                              for _ in range(128))
            cc_client.user = user
            cc_client.save()
            for junction in user.group.roles:
                for client in [cc_client, pw_client]:
                    roleclient = RoleClient()
                    roleclient.client = client
                    roleclient.role = junction.role
                    roleclient.save()
            serializer = FullSerializer(User, instance=user)
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load(User)
    def destroy(self, request, user):
        """
        Deletes a user
        """
        if request.client.user_guid == user.guid:
            raise PermissionDenied('A user cannot delete itself')
        for client in user.clients:
            for token in client.tokens:
                for junction in token.roles.itersafe():
                    junction.delete()
                token.delete()
            for junction in client.roles.itersafe():
                junction.delete()
            client.delete()
        user.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load(User)
    def partial_update(self, contents, user, request):
        """
        Update a User
        """
        contents = None if contents is None else contents.split(',')
        serializer = FullSerializer(User, contents=contents, instance=user, data=request.DATA)
        if serializer.is_valid():
            if user.guid == request.client.user_guid:
                raise PermissionDenied('A user cannot update itself')
            serializer.save()
            return Response(serializer.data, status=status.HTTP_202_ACCEPTED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @action()
    @log()
    @required_roles(['read', 'write'])
    @load(User)
    def set_password(self, request, user):
        """
        Sets the password of a given User. A logged in User can only changes its own password,
        or all passwords if the logged in User has a system role
        """
        if user.guid == request.client.user_guid or Toolbox.is_client_in_roles(request.client, ['manage']):
            serializer = PasswordSerializer(data=request.DATA)
            if serializer.is_valid():
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
