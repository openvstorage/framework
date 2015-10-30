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
Metadata views
"""

import json
import time
from ovs.log.logHandler import LogHandler
from ovs.extensions.generic.system import System
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.api.client import OVSClient
from django.views.generic import View
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse, HttpResponseBadRequest
from django.conf import settings
from oauth2.decorators import auto_response, limit, authenticated
from backend.decorators import required_roles, load
from ovs.dal.lists.bearertokenlist import BearerTokenList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.backendtypelist import BackendTypeList

logger = LogHandler.get('api', name='metadata')


class MetadataView(View):
    """
    Implements retrieval of generic metadata about the services
    """

    @auto_response()
    @limit(amount=60, per=60, timeout=60)
    def get(self, request, *args, **kwargs):
        """
        Fetches metadata
        """
        _ = args, kwargs
        data = {'authenticated': False,
                'authentication_state': None,
                'authentication_metadata': {},
                'username': None,
                'userguid': None,
                'roles': [],
                'identification': {},
                'storagerouter_ips': [sr.ip for sr in StorageRouterList.get_storagerouters()],
                'versions': list(settings.VERSION),
                'plugins': {}}
        try:
            # Gather plugin metadata
            plugins = {}
            # - Backends. BackendType plugins must set the has_plugin flag on True
            for backend_type in BackendTypeList.get_backend_types():
                if backend_type.has_plugin is True:
                    if backend_type.code not in plugins:
                        plugins[backend_type.code] = []
                    plugins[backend_type.code] += ['backend', 'gui']
            # - Generic plugins, as added to the configuration file(s)
            generic_plugins = Configuration.get('ovs.plugins.generic')
            for plugin_name in generic_plugins:
                if plugin_name not in plugins:
                    plugins[plugin_name] = []
                plugins[plugin_name] += ['gui']
            data['plugins'] = plugins

            # Fill identification
            data['identification'] = {'cluster_id': Configuration.get('ovs.support.cid')}

            # Get authentication metadata
            authentication_metadata = {'ip': System.get_my_storagerouter().ip}
            for key in ['mode', 'authorize_uri', 'client_id', 'scope']:
                if Configuration.exists('ovs.webapps.oauth2.{0}'.format(key)):
                    authentication_metadata[key] = Configuration.get('ovs.webapps.oauth2.{0}'.format(key))
            data['authentication_metadata'] = authentication_metadata

            # Gather authorization metadata
            if 'HTTP_AUTHORIZATION' not in request.META:
                return HttpResponse, dict(data.items() + {'authentication_state': 'unauthenticated'}.items())
            authorization_type, access_token = request.META['HTTP_AUTHORIZATION'].split(' ')
            if authorization_type != 'Bearer':
                return HttpResponse, dict(data.items() + {'authentication_state': 'invalid_authorization_type'}.items())
            tokens = BearerTokenList.get_by_access_token(access_token)
            if len(tokens) != 1:
                return HttpResponse, dict(data.items() + {'authentication_state': 'invalid_token'}.items())
            token = tokens[0]
            if token.expiration < time.time():
                for junction in token.roles.itersafe():
                    junction.delete()
                token.delete()
                return HttpResponse, dict(data.items() + {'authentication_state': 'token_expired'}.items())

            # Gather user metadata
            user = token.client.user
            if not user.is_active:
                return HttpResponse, dict(data.items() + {'authentication_state': 'inactive_user'}.items())
            roles = [j.role.code for j in token.roles]

            return HttpResponse, dict(data.items() + {'authenticated': True,
                                                      'authentication_state': 'authenticated',
                                                      'username': user.username,
                                                      'userguid': user.guid,
                                                      'roles': roles,
                                                      'plugins': plugins}.items())
        except Exception as ex:
            logger.exception('Unexpected exception: {0}'.format(ex))
            return HttpResponse, dict(data.items() + {'authentication_state': 'unexpected_exception'}.items())

    @csrf_exempt
    def dispatch(self, request, *args, **kwargs):
        """
        Pass through method to add the CSRF exempt
        """
        return super(MetadataView, self).dispatch(request, *args, **kwargs)


def relay(*args, **kwargs):
    """
    Relays any call to another node.
    Assume this example:
    * A user wants to execute a HTTP GET on /api/storagerouters/
    ** /api/<call>
    * He'll have to execute a HTTP GET on /api/relay/<call>
    ** This will translate to /apt/relay/storagerouters/
    Parameters:
    * Mandatory: ip, port, client_id, client_secret
    * All other parameters will be passed through to the speicified node
    """

    @authenticated()
    @required_roles(['read'])
    @load()
    def _relay(_, ip, port, client_id, client_secret, version, request):
        path = '/{0}'.format(request.path.replace('/api/relay/', ''))
        method = request.META['REQUEST_METHOD'].lower()
        client = OVSClient(ip, port, credentials=(client_id, client_secret), version=version, raw_response=True)
        if not hasattr(client, method):
            return HttpResponseBadRequest, 'Method not available in relay'
        client_kwargs = {'params': request.GET}
        if method != 'get':
            client_kwargs['data'] = request.POST
        call_response = getattr(client, method)(path, **client_kwargs)
        response = HttpResponse(call_response.text,
                                content_type='application/json',
                                status=call_response.status_code)
        for header, value in call_response.headers.iteritems():
            response[header] = value
        return response

    try:
        return _relay(*args, **kwargs)
    except Exception as ex:
        message = str(ex)
        status_code = 400
        if hasattr(ex, 'detail'):
            message = ex.detail
        if hasattr(ex, 'status_code'):
            status_code = ex.status_code
        logger.exception('Error relaying call: {0}'.format(message))
        return HttpResponse(json.dumps({'error': message}), content_type='application/json', status=status_code)
