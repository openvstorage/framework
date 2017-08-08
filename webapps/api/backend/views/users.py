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

import random
import string
import hashlib
from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from api.backend.decorators import required_roles, load, return_object, return_list, log, return_simple
from api.backend.serializers.serializers import FullSerializer
from api.backend.toolbox import ApiToolbox
from ovs.dal.hybrids.client import Client
from ovs.dal.hybrids.j_roleclient import RoleClient
from ovs.dal.hybrids.user import User
from ovs.dal.lists.userlist import UserList
from ovs_extensions.api.exceptions import HttpForbiddenException, HttpNotAcceptableException


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
        :param request: The raw request
        :type request: Request
        """
        if ApiToolbox.is_client_in_roles(request.client, ['manage']):
            return UserList.get_users()
        else:
            return [request.client.user]

    @log()
    @required_roles(['read'])
    @return_object(User)
    @load(User)
    def retrieve(self, request, user):
        """
        Load information about a given User. Only the currently logged in User is accessible, or all if the logged in User has a manage role
        :param request: The raw request
        :type request: Request
        :param user: The user to load
        :type user: User
        """
        if user.guid == request.client.user_guid or ApiToolbox.is_client_in_roles(request.client, ['manage']):
            return user
        raise HttpForbiddenException(error_description='Fetching user information not allowed',
                                     error='no_ownership')

    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_object(User, mode='created')
    @load()
    def create(self, request):
        """
        Creates a User
        :param request: The raw request
        :type request: Request
        """
        serializer = FullSerializer(User, instance=User(), data=request.DATA, allow_passwords=True)
        user = serializer.deserialize()
        if UserList.get_user_by_username(user.username) is not None:
            raise HttpNotAcceptableException(error='duplicate',
                                             error_description='User with this username already exists')
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
        return user

    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_simple()
    @load(User)
    def destroy(self, request, user):
        """
        Deletes a user
        :param request: The raw request
        :type request: Request
        :param user: The user to delete
        :type user: User
        :return: None
        :rtype: None
        """
        if request.client.user_guid == user.guid:
            raise HttpForbiddenException(error_description='A user cannot delete itself',
                                         error='impossible_request')
        for client in user.clients:
            for token in client.tokens:
                for junction in token.roles.itersafe():
                    junction.delete()
                token.delete()
            for junction in client.roles.itersafe():
                junction.delete()
            client.delete()
        user.delete()
        return None

    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_object(User, mode='accepted')
    @load(User)
    def partial_update(self, contents, user, request):
        """
        Update a User
        :param request: The raw request
        :type request: Request
        :param user: The user to update
        :type user: User
        :param contents: The contents to update/return
        :type contents: str
        """
        contents = None if contents is None else contents.split(',')
        serializer = FullSerializer(User, contents=contents, instance=user, data=request.DATA)
        user = serializer.deserialize()
        if user.guid == request.client.user_guid:
            raise HttpForbiddenException(error_description='A user cannot update itself',
                                         error='impossible_request')
        user.save()
        return user

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_object(User, mode='accepted')
    @load(User)
    def set_password(self, request, user, new_password):
        """
        Sets the password of a given User. A logged in User can only changes its own password, or all passwords if the logged in User has a manage role
        :param request: The raw request
        :type request: Request
        :param user: The user to update the password from
        :type user: User
        :param new_password: The new password to be set
        :type new_password: str
        """
        if user.guid == request.client.user_guid or ApiToolbox.is_client_in_roles(request.client, ['manage']):
            user.password = hashlib.sha256(str(new_password)).hexdigest()
            user.save()
            for client in user.clients:
                for token in client.tokens:
                    for junction in token.roles:
                        junction.delete()
                    token.delete()
            return user
        raise HttpForbiddenException(error_description='Updating password not allowed',
                                     error='impossible_request')
