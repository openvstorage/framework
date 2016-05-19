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
Token views
"""

import hashlib
import base64
import time
from django.views.generic import View
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse, HttpResponseBadRequest
from oauth2.decorators import auto_response, limit, log
from oauth2.toolbox import Toolbox
from ovs.dal.lists.userlist import UserList
from ovs.dal.lists.rolelist import RoleList
from ovs.dal.hybrids.client import Client
from ovs.log.log_handler import LogHandler


class OAuth2TokenView(View):
    """
    Implements OAuth 2 token views
    """

    @log()
    @auto_response()
    @limit(amount=5, per=60, timeout=60)
    def post(self, request, *args, **kwargs):
        """
        Handles token post
        """
        logger = LogHandler.get('api', 'oauth2')
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
            clients = [client for client in user.clients if client.ovs_type == 'INTERNAL' and client.grant_type == 'PASSWORD']
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
            client_id, client_secret = base64.b64decode(password_hash).split(':', 1)
            try:
                client = Client(client_id)
                if client.grant_type != 'CLIENT_CREDENTIALS':
                    return HttpResponseBadRequest, {'error': 'invalid_grant'}
                if client.client_secret != client_secret:
                    return HttpResponseBadRequest, {'error': 'invalid_client'}
                if not client.user.is_active:
                    return HttpResponseBadRequest, {'error': 'inactive_user'}
                try:
                    access_token, _ = Toolbox.generate_tokens(client, generate_access=True, scopes=scopes)
                except ValueError as error:
                    return HttpResponseBadRequest, {'error': str(error)}
                try:
                    Toolbox.clean_tokens(client)
                except Exception as error:
                    logger.error('Error during session cleanup: {0}'.format(error))
                return HttpResponse, {'access_token': access_token.access_token,
                                      'token_type': 'bearer',
                                      'expires_in': 3600}
            except Exception as ex:
                logger.exception('Error matching client: {0}'.format(ex))
                return HttpResponseBadRequest, {'error': 'invalid_client'}
        else:
            return HttpResponseBadRequest, {'error': 'unsupported_grant_type'}

    @csrf_exempt
    def dispatch(self, request, *args, **kwargs):
        """
        Pass through method to add the CSRF exempt
        """
        return super(OAuth2TokenView, self).dispatch(request, *args, **kwargs)
