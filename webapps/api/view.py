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
Metadata views
"""

import time
from ovs.log.logHandler import LogHandler
from django.views.generic import View
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
from django.conf import settings
from oauth2.decorators import json_response
from ovs.dal.lists.bearertokenlist import BearerTokenList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.backendtypelist import BackendTypeList

logger = LogHandler('api', name='metadata')


class MetadataView(View):
    """
    Implements retrieval of generic metadata about the services
    """

    @json_response()
    def get(self, request, *args, **kwargs):
        """
        Fetches metadata
        """
        _ = args, kwargs
        data = {'authenticated': False,
                'authentication_state': None,
                'username': None,
                'userguid': None,
                'roles': [],
                'storagerouter_ips': [sr.ip for sr in StorageRouterList.get_storagerouters()],
                'versions': list(settings.VERSION),
                'plugins': {}}
        try:
            # Gather authorization metadata
            if 'HTTP_AUTHORIZATION' not in request.META:
                return HttpResponse, dict(data.items() + {'authentication_state': 'unauthenticated'}.items())
            authorization_type, access_token = request.META['HTTP_AUTHORIZATION'].split(' ')
            if authorization_type != 'Bearer':
                return HttpResponse, dict(data.items() + {'authentication_state': 'invalid authorization type'}.items())
            tokens = BearerTokenList.get_by_access_token(access_token)
            if len(tokens) != 1:
                return HttpResponse, dict(data.items() + {'authentication_state': 'invalid token'}.items())
            token = tokens[0]
            if token.expiration < time.time():
                for junction in token.roles.itersafe():
                    junction.delete()
                token.delete()
                return HttpResponse, dict(data.items() + {'authentication_state': 'token expired'}.items())

            # Gather user metadata
            user = token.client.user
            if not user.is_active:
                return HttpResponse, dict(data.items() + {'authentication_state': 'inactive user'}.items())
            roles = [j.role.code for j in token.roles]

            # Gather plugin metadata
            plugins = {}
            # - Backends. BackendType plugins must set the has_plugin flag on True
            backend_types = [backend_types.code for backend_types in BackendTypeList.get_backend_types() if backend_types.has_plugin is True]
            if backend_types:
                plugins['backend_types'] = backend_types

            return HttpResponse, dict(data.items() + {'authenticated': True,
                                                      'authentication_state': 'authenticated',
                                                      'username': user.username,
                                                      'userguid': user.guid,
                                                      'roles': roles,
                                                      'plugins': plugins}.items())
        except Exception as ex:
            logger.exception('Unexpected exception: {0}'.format(ex))
            return HttpResponse, dict(data.items() + {'authentication_state': 'unexpected exception'}.items())

    @csrf_exempt
    def dispatch(self, request, *args, **kwargs):
        """
        Pass through method to add the CSRF exempt
        """
        return super(MetadataView, self).dispatch(request, *args, **kwargs)
