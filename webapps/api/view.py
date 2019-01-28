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
Metadata views
"""

import json
import time
import logging
from django.views.generic import View
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
from django.conf import settings
from api.backend.decorators import required_roles, load
from api.middleware import OVSMiddleware
from api.oauth2.decorators import auto_response, limit, authenticated
from ovs.dal.lists.backendtypelist import BackendTypeList
from ovs.dal.lists.bearertokenlist import BearerTokenList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs_extensions.api.client import OVSClient
from ovs_extensions.api.exceptions import HttpMethodNotAllowedException
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.system import System
from ovs.extensions.packages.packagefactory import PackageFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory


class MetadataView(View):
    """
    Implements retrieval of generic metadata about the services
    """
    _logger = logging.getLogger(__name__)

    @auto_response()
    @limit(amount=60, per=60, timeout=60)
    def get(self, request, *args, **kwargs):
        """
        Fetches metadata
        """
        _ = args, kwargs
        data = {'release': {'name': ''},
                'authenticated': False,
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
            # Gather release name
            try:
                data['release']['name'] = PackageFactory.get_release_name()
            except:
                MetadataView._logger.exception('Could not load release name')

            # Gather plugin metadata
            plugins = {}
            # - Backends. BackendType plugins must set the has_plugin flag on True
            for backend_type in BackendTypeList.get_backend_types():
                if backend_type.has_plugin is True:
                    if backend_type.code not in plugins:
                        plugins[backend_type.code] = []
                    plugins[backend_type.code] += ['backend', 'gui']
            # - Generic plugins, as added to the configuration file(s)
            generic_plugins = Configuration.get('/ovs/framework/plugins/installed|generic')
            for plugin_name in generic_plugins:
                if plugin_name not in plugins:
                    plugins[plugin_name] = []
                plugins[plugin_name] += ['gui']
            data['plugins'] = plugins

            # Fill identification
            data['identification'] = {'cluster_id': Configuration.get('/ovs/framework/cluster_id')}

            # Get authentication metadata
            authentication_metadata = {'ip': System.get_my_storagerouter().ip}
            for key in ['mode', 'authorize_uri', 'client_id', 'scope']:
                if Configuration.exists('/ovs/framework/webapps|oauth2.{0}'.format(key)):
                    authentication_metadata[key] = Configuration.get('/ovs/framework/webapps|oauth2.{0}'.format(key))
            data['authentication_metadata'] = authentication_metadata

            # Gather authorization metadata
            if 'HTTP_AUTHORIZATION' not in request.META:
                return dict(data.items() + {'authentication_state': 'unauthenticated'}.items())
            authorization_type, access_token = request.META['HTTP_AUTHORIZATION'].split(' ')
            if authorization_type != 'Bearer':
                return dict(data.items() + {'authentication_state': 'invalid_authorization_type'}.items())
            tokens = BearerTokenList.get_by_access_token(access_token)
            if len(tokens) != 1:
                return dict(data.items() + {'authentication_state': 'invalid_token'}.items())
            token = tokens[0]
            if token.expiration < time.time():
                for junction in token.roles.itersafe():
                    junction.delete()
                token.delete()
                return dict(data.items() + {'authentication_state': 'token_expired'}.items())

            # Gather user metadata
            user = token.client.user
            if not user.is_active:
                return dict(data.items() + {'authentication_state': 'inactive_user'}.items())
            roles = [j.role.code for j in token.roles]

            return dict(data.items() + {'authenticated': True,
                                        'authentication_state': 'authenticated',
                                        'username': user.username,
                                        'userguid': user.guid,
                                        'roles': roles,
                                        'plugins': plugins}.items())
        except Exception as ex:
            MetadataView._logger.exception('Unexpected exception: {0}'.format(ex))
            return dict(data.items() + {'authentication_state': 'unexpected_exception'}.items())

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
    * All other parameters will be passed through to the specified node
    """

    logger = logging.getLogger(__name__)

    @authenticated()
    @required_roles(['read'])
    @load()
    def _relay(_, ip, port, client_id, client_secret, raw_version, request):
        path = '/{0}'.format(request.path.replace('/api/relay/', ''))
        method = request.META['REQUEST_METHOD'].lower()
        client = OVSClient(ip, port,
                           credentials=(client_id, client_secret),
                           version=raw_version,
                           raw_response=True,
                           cache_store=VolatileFactory.get_client())
        if not hasattr(client, method):
            raise HttpMethodNotAllowedException(error='unavailable_call',
                                                error_description='Method not available in relay')
        client_kwargs = {'params': request.GET}
        if method != 'get':
            client_kwargs['data'] = request.POST
        call_response = getattr(client, method)(path, **client_kwargs)
        response = HttpResponse(call_response.text,
                                content_type='application/json',
                                status=call_response.status_code)
        for header, value in call_response.headers.iteritems():
            response[header] = value
        response['OVS-Relay'] = '{0}:{1}'.format(ip, port)
        return response

    try:
        return _relay(*args, **kwargs)
    except Exception as ex:
        if OVSMiddleware.is_own_httpexception(ex):
            # noinspection PyUnresolvedReferences
            return HttpResponse(ex.data,
                                status=ex.status_code,
                                content_type='application/json')
        message = str(ex)
        status_code = 400
        if hasattr(ex, 'detail'):
            message = ex.detail
        if hasattr(ex, 'status_code'):
            status_code = ex.status_code
        logger.exception('Error relaying call: {0}'.format(message))
        return HttpResponse(json.dumps({'error_description': message,
                                        'error': 'relay_error'}),
                            content_type='application/json',
                            status=status_code)
