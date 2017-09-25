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

import time
import json
import base64
import hashlib
from django.views.generic import View
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
from api.oauth2.decorators import auto_response, limit, log
from api.oauth2.toolbox import OAuth2Toolbox
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.lists.userlist import UserList
from ovs.dal.lists.rolelist import RoleList
from ovs.dal.hybrids.client import Client
from ovs_extensions.api.exceptions import HttpBadRequestException
from ovs.extensions.generic.logger import Logger


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
        logger = Logger('api')
        _ = args, kwargs
        if 'grant_type' not in request.POST:
            raise HttpBadRequestException(error='invalid_request',
                                          error_description='No grant type specified')
        grant_type = request.POST['grant_type']
        scopes = None
        if 'scope' in request.POST:
            scopes = RoleList.get_roles_by_codes(request.POST['scope'].split(' '))
        if grant_type == 'password':
            # Resource Owner Password Credentials Grant
            if 'username' not in request.POST or 'password' not in request.POST:
                raise HttpBadRequestException(error='invalid_request',
                                              error_description='Invalid request')
            username = request.POST['username']
            password = request.POST['password']
            user = UserList.get_user_by_username(username)
            if user is None or user.password != hashlib.sha256(password).hexdigest():
                raise HttpBadRequestException(error='invalid_client',
                                              error_description='Invalid client')
            if user.is_active is False:
                raise HttpBadRequestException(error='inactive_user',
                                              error_description='User is inactive')
            clients = [client for client in user.clients if client.ovs_type == 'INTERNAL' and client.grant_type == 'PASSWORD']
            if len(clients) != 1:
                raise HttpBadRequestException(error='unauthorized_client',
                                              error_description='Client is unauthorized')
            client = clients[0]
            try:
                access_token, _ = OAuth2Toolbox.generate_tokens(client, generate_access=True, scopes=scopes)
                access_token.expiration = int(time.time() + 86400)
                access_token.save()
            except ValueError as error:
                if error.message == 'invalid_scope':
                    raise HttpBadRequestException(error='invalid_scope',
                                                  error_description='Invalid scope requested')
                raise
            OAuth2Toolbox.clean_tokens(client)
            return HttpResponse(json.dumps({'access_token': access_token.access_token,
                                            'token_type': 'bearer',
                                            'expires_in': 86400}),
                                content_type='application/json')
        elif grant_type == 'client_credentials':
            # Client Credentials
            if 'HTTP_AUTHORIZATION' not in request.META:
                raise HttpBadRequestException(error='missing_header',
                                              error_description='Authorization header missing')
            _, password_hash = request.META['HTTP_AUTHORIZATION'].split(' ')
            client_id, client_secret = base64.b64decode(password_hash).split(':', 1)
            try:
                client = Client(client_id)
                if client.grant_type != 'CLIENT_CREDENTIALS':
                    raise HttpBadRequestException(error='invalid_grant',
                                                  error_description='The given grant type is not supported')
                if client.client_secret != client_secret:
                    raise HttpBadRequestException(error='invalid_client',
                                                  error_description='Invalid client')
                if not client.user.is_active:
                    raise HttpBadRequestException(error='inactive_user',
                                                  error_description='User is inactive')
                try:
                    access_token, _ = OAuth2Toolbox.generate_tokens(client, generate_access=True, scopes=scopes)
                except ValueError as error:
                    if error.message == 'invalid_scope':
                        raise HttpBadRequestException(error='invalid_scope',
                                                      error_description='Invalid scope requested')
                    raise
                try:
                    OAuth2Toolbox.clean_tokens(client)
                except Exception as error:
                    logger.error('Error during session cleanup: {0}'.format(error))
                return HttpResponse(json.dumps({'access_token': access_token.access_token,
                                                'token_type': 'bearer',
                                                'expires_in': 3600}),
                                    content_type='application/json')
            except HttpBadRequestException:
                raise
            except ObjectNotFoundException as ex:
                logger.warning('Error matching client: {0}'.format(ex))
                raise HttpBadRequestException(error='invalid_client',
                                              error_description='Client could not be found')
            except Exception as ex:
                logger.exception('Error matching client: {0}'.format(ex))
                raise HttpBadRequestException(error='invalid_client',
                                              error_description='Error loading client')
        else:
            raise HttpBadRequestException(error='unsupported_grant_type',
                                          error_description='Unsupported grant type')

    @csrf_exempt
    def dispatch(self, request, *args, **kwargs):
        """
        Pass through method to add the CSRF exempt
        """
        return super(OAuth2TokenView, self).dispatch(request, *args, **kwargs)
