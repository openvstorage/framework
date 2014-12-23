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
Contains the OAuth 2 authentication/authorization backends
"""
import time
from django.contrib.auth.models import User as DUser
from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed
from ovs.dal.lists.bearertokenlist import BearerTokenList


class OAuth2Backend(BaseAuthentication):
    """
    OAuth 2 based authentication for Bearer tokens
    """

    def authenticate(self, request):
        """
        Authenticate method
        """
        if 'HTTP_AUTHORIZATION' not in request.META:
            return None
        authorization_type, access_token = request.META['HTTP_AUTHORIZATION'].split(' ')
        if authorization_type != 'Bearer':
            raise AuthenticationFailed('invalid_authorization_type')

        tokens = BearerTokenList.get_by_access_token(access_token)
        if len(tokens) != 1:
            raise AuthenticationFailed('invalid_token')
        token = tokens[0]
        if token.expiration < time.time():
            for junction in token.roles.itersafe():
                junction.delete()
            token.delete()
            raise AuthenticationFailed('token_expired')

        user = token.client.user
        if not user.is_active:
            raise AuthenticationFailed('inactive_user')
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
