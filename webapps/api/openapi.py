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

import re
import json
import time
from django.views.generic import View
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
from backend.decorators import required_roles, load
from backend.exceptions import HttpBadRequestException
from oauth2.decorators import auto_response, limit, authenticated
from ovs.dal.lists.bearertokenlist import BearerTokenList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.backendtypelist import BackendTypeList
from ovs.extensions.generic.system import System
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.api.client import OVSClient
from ovs.log.log_handler import LogHandler


class OpenAPIView(View):
    """
    Implements retrieval of generic metadata about the services
    """
    _logger = LogHandler.get('api', name='openapi')

    @auto_response()
    @load()
    def get(self, request):
        """
        returns OpenAPI specs
        """
        path = request.path
        data = {}
        if re.match('^.*/swagger\.json$', path):
            version = settings.VERSION[-1]
            data = {'swagger': '2.0',
                    'info': {'title': 'Open vStorage',
                             'description': 'The Open vStorage API',
                             'version': str(version)},
                    'basePath': '/api',
                    'schemes': ['https'],
                    'consumes': ['application/json'],
                    'produces': ['application/json; version={0}'.format(version)],
                    'paths': {'/': {'get': {'summary': 'Retrieve API metadata',
                                            'operationId': 'api',
                                            'responses': {'200': {'descirption': 'API metadata',
                                                                  'schema': {'type': 'object',
                                                                             'title': 'APIMetadata',
                                                                             'properties': {'username': {'type': 'string',
                                                                                                         'description': 'The logged in username or null if none available'}},
                                                                             'required': ['username']}}}}}},
                    'securityDefinitions': {'oauth2': {'type': 'oauth2',
                                                       'flow': 'password',
                                                       'tokenUrl': 'oauth2/token',
                                                       'scopes': {'read': 'Read access',
                                                                  'write': 'Write access',
                                                                  'manage': 'Management access'}}},
                    'security': [{'oauth2': ['read', 'write', 'manage']}]}
        return data

    @csrf_exempt
    def dispatch(self, request, *args, **kwargs):
        """
        Pass through method to add the CSRF exempt
        """
        return super(OpenAPIView, self).dispatch(request, *args, **kwargs)

