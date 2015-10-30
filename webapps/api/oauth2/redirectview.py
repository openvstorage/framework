# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Token views
"""

import time
import urllib
import base64
import requests
import datetime
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.system import System
from django.views.generic import View
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponseRedirect
from oauth2.decorators import auto_response, limit, log
from ovs.log.logHandler import LogHandler
from ovs.dal.lists.clientlist import ClientList
from ovs.dal.lists.rolelist import RoleList
from oauth2.toolbox import Toolbox

logger = LogHandler.get('api', 'oauth2')


class OAuth2RedirectView(View):
    """
    Implements OAuth 2 redirect views
    """

    @log()
    @auto_response()
    @limit(amount=20, per=60, timeout=60)
    def get(self, request, *args, **kwargs):
        """
        Handles token post
        """
        _ = args, kwargs
        html_endpoint = Configuration.get('ovs.webapps.html_endpoint')
        if 'code' not in request.GET:
            logger.error('Got OAuth2 redirection request without code')
            return HttpResponseRedirect, html_endpoint
        code = request.GET['code']
        if 'state' not in request.GET:
            logger.error('Got OAuth2 redirection request without state')
            return HttpResponseRedirect, html_endpoint
        state = request.GET['state']
        if 'error' in request.GET:
            error = request.GET['error']
            description = request.GET['error_description'] if 'error_description' in request.GET else ''
            logger.error('Error {0} during OAuth2 redirection request: {1}'.format(error, description))
            return HttpResponseRedirect, html_endpoint

        base_url = Configuration.get('ovs.webapps.oauth2.token_uri')
        client_id = Configuration.get('ovs.webapps.oauth2.client_id')
        client_secret = Configuration.get('ovs.webapps.oauth2.client_secret')
        parameters = {'grant_type': 'authorization_code',
                      'redirect_url': 'https://{0}/api/oauth2/redirect/'.format(System.get_my_storagerouter().ip),
                      'client_id': client_id,
                      'code': code}
        url = '{0}?{1}'.format(base_url, urllib.urlencode(parameters))
        headers = {'Accept': 'application/json',
                   'Authorization': 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(client_id, client_secret)).strip())}
        raw_response = requests.post(url=url, headers=headers, verify=False)
        response = raw_response.json()
        if 'error' in response:
            error = response['error']
            description = response['error_description'] if 'error_description' in response else ''
            logger.error('Error {0} during OAuth2 redirection access token: {1}'.format(error, description))
            return HttpResponseRedirect, html_endpoint

        token = response['access_token']
        expires_in = response['expires_in']

        clients = ClientList.get_by_types('INTERNAL', 'CLIENT_CREDENTIALS')
        client = None
        for current_client in clients:
            if current_client.user.group.name == 'administrators':
                client = current_client
                break
        if client is None:
            logger.error('Could not find INTERNAL CLIENT_CREDENTIALS client in administrator group.')
            return HttpResponseRedirect, html_endpoint

        roles = RoleList.get_roles_by_codes(['read', 'write', 'manage'])
        access_token, _ = Toolbox.generate_tokens(client, generate_access=True, scopes=roles)
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
