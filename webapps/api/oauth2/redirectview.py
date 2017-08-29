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
import requests
import datetime
from django.views.generic import View
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponseRedirect
from api.oauth2.decorators import auto_response, limit, log
from api.oauth2.toolbox import OAuth2Toolbox
from ovs.dal.lists.clientlist import ClientList
from ovs.dal.lists.rolelist import RoleList
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger


class OAuth2RedirectView(View):
    """
    Implements OAuth 2 redirect views
    """
    _logger = Logger('oauth2')

    @log()
    @auto_response()
    @limit(amount=20, per=60, timeout=60)
    def get(self, request, *args, **kwargs):
        """
        Handles token post
        """
        _ = args, kwargs
        html_endpoint = Configuration.get('/ovs/framework/webapps|html_endpoint')
        if 'code' not in request.GET:
            OAuth2RedirectView._logger.error('Got OAuth2 redirection request without code')
            return HttpResponseRedirect(html_endpoint)
        code = request.GET['code']
        if 'state' not in request.GET:
            OAuth2RedirectView._logger.error('Got OAuth2 redirection request without state')
            return HttpResponseRedirect(html_endpoint)
        state = request.GET['state']
        if 'error' in request.GET:
            error = request.GET['error']
            description = request.GET['error_description'] if 'error_description' in request.GET else ''
            OAuth2RedirectView._logger.error('Error {0} during OAuth2 redirection request: {1}'.format(error, description))
            return HttpResponseRedirect(html_endpoint)

        base_url = Configuration.get('/ovs/framework/webapps|oauth2.token_uri')
        client_id = Configuration.get('/ovs/framework/webapps|oauth2.client_id')
        client_secret = Configuration.get('/ovs/framework/webapps|oauth2.client_secret')
        redirect_uri = 'https://{0}/api/oauth2/redirect/'.format(request.META['HTTP_HOST'])
        parameters = {'grant_type': 'authorization_code',
                      'redirect_uri': redirect_uri,
                      'state': state,
                      'code': code}
        headers = {'Accept': 'application/json'}
        raw_response = requests.post(url=base_url, data=parameters, headers=headers, auth=(client_id, client_secret), verify=False)
        response = raw_response.json()
        if 'error' in response:
            error = response['error']
            description = response['error_description'] if 'error_description' in response else ''
            OAuth2RedirectView._logger.error('Error {0} during OAuth2 redirection access token: {1}'.format(error, description))
            return HttpResponseRedirect(html_endpoint)

        token = response['access_token']
        expires_in = response['expires_in']

        clients = ClientList.get_by_types('INTERNAL', 'CLIENT_CREDENTIALS')
        client = None
        for current_client in clients:
            if current_client.user.group.name == 'administrators':
                client = current_client
                break
        if client is None:
            OAuth2RedirectView._logger.error('Could not find INTERNAL CLIENT_CREDENTIALS client in administrator group.')
            return HttpResponseRedirect(html_endpoint)

        roles = RoleList.get_roles_by_codes(['read', 'write', 'manage'])
        access_token, _ = OAuth2Toolbox.generate_tokens(client, generate_access=True, scopes=roles)
        access_token.expiration = int(time.time() + expires_in)
        access_token.access_token = token
        access_token.save()

        expires = datetime.datetime.now() + datetime.timedelta(minutes=2)
        response = HttpResponseRedirect(html_endpoint)
        response.set_cookie('state', state, expires=expires, secure=True)
        response.set_cookie('accesstoken', token, expires=expires, secure=True)

        return response

    @csrf_exempt
    def dispatch(self, request, *args, **kwargs):
        """
        Pass through method to add the CSRF exempt
        """
        return super(OAuth2RedirectView, self).dispatch(request, *args, **kwargs)
