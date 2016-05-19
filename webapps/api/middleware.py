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
Middleware module
"""

import re
from django.http import HttpResponse
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.log.log_handler import LogHandler


class OVSMiddleware(object):
    """
    Middleware object
    """

    def process_exception(self, request, exception):
        """
        Logs information about the given error
        """
        _ = self, request
        logger = LogHandler.get('api', 'middleware')
        logger.exception('An unhandled exception occurred: {0}'.format(exception))

    def process_request(self, request):
        """
        Processes requests
        """
        _ = self
        # Processes CORS preflight requests
        if request.method == 'OPTIONS' and 'HTTP_ACCESS_CONTROL_REQUEST_METHOD' in request.META:
            return HttpResponse()
        # Validate version
        path = request.path
        regex = re.compile('^(.*; )?version=(?P<version>([0-9]+|\*)?)(;.*)?$')
        if path != '/api/' and '/api/oauth2/' not in path:
            if 'HTTP_ACCEPT' not in request.META or regex.match(request.META['HTTP_ACCEPT']) is None:
                return HttpResponseNotAcceptable(
                    '{"error": "The version required by the client should be added to the Accept header. E.g.: \'Accept: application/json; version=1\'"}',
                    content_type='application/json'
                )
        return None

    def process_response(self, request, response):
        """
        Processes responses
        """
        _ = self
        # Process CORS responses
        if 'HTTP_ORIGIN' in request.META:
            storagerouters = StorageRouterList.get_storagerouters()
            allowed_origins = ['https://{0}'.format(storagerouter.ip) for storagerouter in storagerouters]
            if request.META['HTTP_ORIGIN'] in allowed_origins:
                response['Access-Control-Allow-Origin'] = request.META['HTTP_ORIGIN']
                response['Access-Control-Allow-Headers'] = 'x-requested-with, content-type, accept, origin, authorization'
                response['Access-Control-Allow-Methods'] = 'GET, POST, PUT, PATCH, DELETE, OPTIONS'
        return response


class HttpResponseNotAcceptable(HttpResponse):
    status_code = 406
