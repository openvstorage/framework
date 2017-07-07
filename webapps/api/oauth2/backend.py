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
Contains the OAuth 2 authentication/authorization backends
"""
import time
from django.contrib.auth.models import User as DUser
from rest_framework.authentication import BaseAuthentication
from ovs.dal.lists.bearertokenlist import BearerTokenList
from ovs_extensions.api.exceptions import HttpUnauthorizedException


class OAuth2Backend(BaseAuthentication):
    """
    OAuth 2 based authentication for Bearer tokens
    """

    def authenticate(self, request, **kwargs):
        """
        Authenticate method
        """
        _ = self
        if 'HTTP_AUTHORIZATION' not in request.META:
            return None
        authorization_type, access_token = request.META['HTTP_AUTHORIZATION'].split(' ')
        if authorization_type != 'Bearer':
            raise HttpUnauthorizedException(error='invalid_authorization_type',
                                            error_description='Invalid authorization type specified')

        tokens = BearerTokenList.get_by_access_token(access_token)
        if len(tokens) != 1:
            raise HttpUnauthorizedException(error='invalid_token',
                                            error_description='Invalid token passed')
        token = tokens[0]
        if token.expiration < time.time():
            for junction in token.roles.itersafe():
                junction.delete()
            token.delete()
            raise HttpUnauthorizedException(error='token_expired',
                                            error_description='The token passed is expired')

        user = token.client.user
        if not user.is_active:
            raise HttpUnauthorizedException(error='inactive_user',
                                            error_description='Inactive user')
        request.client = token.client
        request.token = token

        try:
            duser = DUser.objects.get(username=user.username)
        except DUser.DoesNotExist:
            duser = DUser.objects.create_user(user.username, 'nobody@example.com')
            duser.is_active = user.is_active
            duser.is_staff = False
            duser.is_superuser = False
            duser.save()

        if 'native_django' in kwargs and kwargs['native_django'] is True:
            return duser
        return duser, None

    def get_user(self, user_id):
        """
        Get_user method
        """
        _ = self
        try:
            return DUser.objects.get(pk=user_id)
        except DUser.DoesNotExist:
            return None
