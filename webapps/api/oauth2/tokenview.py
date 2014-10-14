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
Token views
"""

import hashlib
import base64
import time
from django.views.generic import View
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse, HttpResponseBadRequest
from oauth2.decorators import json_response, limit
from oauth2.toolbox import Toolbox
from ovs.dal.lists.userlist import UserList
from ovs.dal.lists.rolelist import RoleList
from ovs.dal.hybrids.client import Client


class OAuth2TokenView(View):
    """
    Implements OAuth 2 token views
    """

    @json_response()
    @limit(amount=5, per=60, timeout=60)
    def post(self, request, *args, **kwargs):
        """
        Handles token post
        """
        _ = args, kwargs
        if 'grant_type' not in request.POST:
            return HttpResponseBadRequest, {'error': 'invalid_request'}
        grant_type = request.POST['grant_type']
        scopes = None
        if 'scope' in request.POST:
            scopes = RoleList.get_roles_by_codes(request.POST['scope'].split(' '))
        if grant_type == 'password':
            # Resource Owner Password Credentials Grant
            if 'username' not in request.POST or 'password' not in request.POST:
                return HttpResponseBadRequest, {'error': 'invalid_request'}
            username = request.POST['username']
            password = request.POST['password']
            user = UserList.get_user_by_username(username)
            if user is None or user.password != hashlib.sha256(password).hexdigest():
                return HttpResponseBadRequest, {'error': 'invalid_client'}
            if user.is_active is False:
                return HttpResponseBadRequest, {'error': 'inactive_user'}
            clients = [client for client in user.clients if client.ovs_type == 'FRONTEND' and client.grant_type == 'PASSWORD']
            if len(clients) != 1:
                return HttpResponseBadRequest, {'error': 'unauthorized_client'}
            client = clients[0]
            try:
                access_token, _ = Toolbox.generate_tokens(client, generate_access=True, scopes=scopes)
                access_token.expiration = int(time.time() + 86400)
                access_token.save()
            except ValueError as error:
                return HttpResponseBadRequest, {'error': str(error)}
            Toolbox.clean_tokens(client)
            return HttpResponse, {'access_token': access_token.access_token,
                                  'token_type': 'bearer',
                                  'expires_in': 86400}
        elif grant_type == 'client_credentials':
            # Client Credentials
            if 'HTTP_AUTHORIZATION' not in request.META:
                return HttpResponseBadRequest, {'error': 'missing_header'}
            _, password_hash = request.META['HTTP_AUTHORIZATION'].split(' ')
            client_id, client_secret = base64.decodestring(password_hash).split(':', 1)
            try:
                client = Client(client_id)
                if client.grant_type != 'CLIENT_CREDENTIALS':
                    return HttpResponseBadRequest, {'error': 'invalid_grant'}
                if not client.user.is_active:
                    return HttpResponseBadRequest, {'error': 'inactive_user'}
                try:
                    access_token, _ = Toolbox.generate_tokens(client, generate_access=True, scopes=scopes)
                except ValueError as error:
                    return HttpResponseBadRequest, {'error': str(error)}
                Toolbox.clean_tokens(client)
                return HttpResponse, {'access_token': access_token.access_token,
                                      'token_type': 'bearer',
                                      'expires_in': 3600}
            except:
                return HttpResponseBadRequest, {'error': 'invalid_client'}
        else:
            return HttpResponseBadRequest, {'error': 'unsupported_grant_type'}

    @csrf_exempt
    def dispatch(self, request, *args, **kwargs):
        """
        Pass through method to add the CSRF exempt
        """
        return super(OAuth2TokenView, self).dispatch(request, *args, **kwargs)
