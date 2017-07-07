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
import json
import time
from django.http import HttpResponse
from api.helpers import OVSResponse
from ovs.dal.exceptions import MissingMandatoryFieldsException
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
        if OVSMiddleware.is_own_httpexception(exception):
            return HttpResponse(exception.data,
                                status=exception.status_code,
                                content_type='application/json')
        if isinstance(exception, MissingMandatoryFieldsException):
            return HttpResponse(json.dumps({'error': 'invalid_data',
                                            'error_description': exception.message}),
                                status=400,
                                content_type='application/json')
        logger.exception('An unhandled exception occurred: {0}'.format(exception))
        return HttpResponse(
            json.dumps({'error': 'internal_server',
                        'error_description': exception.message}),
            status=500,
            content_type='application/json'
        )

    def process_request(self, request):
        """
        Processes requests
        """
        _ = self
        start = time.time()
        # Processes CORS preflight requests
        if request.method == 'OPTIONS' and 'HTTP_ACCESS_CONTROL_REQUEST_METHOD' in request.META:
            return HttpResponse()
        # Validate version
        path = request.path
        regex = re.compile('^(.*; )?version=(?P<version>([0-9]+|\*)?)(;.*)?$')
        if path != '/api/' and '/api/oauth2/' not in path and '/swagger.json' not in path:
            if 'HTTP_ACCEPT' not in request.META or regex.match(request.META['HTTP_ACCEPT']) is None:
                return OVSResponse(
                    json.dumps({'error': 'missing_header',
                                'error_description': "The version required by the client should be added to the Accept header. E.g.: 'Accept: application/json; version=1'"}),
                    status=406,
                    content_type='application/json',
                    timings={'total': [time.time() - start, 'Total']}
                )
        request._entry_time = time.time()
        return None

    def process_response(self, request, response):
        """
        Processes responses
        """
        _ = self
        # Timings
        if isinstance(response, OVSResponse):
            if hasattr(request, '_entry_time'):
                response.timings['total'] = [time.time() - request._entry_time, 'Total']
            response.build_timings()
        # Process CORS responses
        if 'HTTP_ORIGIN' in request.META:
            path = request.path
            storagerouters = StorageRouterList.get_storagerouters()
            allowed_origins = ['https://{0}'.format(storagerouter.ip) for storagerouter in storagerouters]
            if request.META['HTTP_ORIGIN'] in allowed_origins or '/swagger.json' in path:
                response['Access-Control-Allow-Origin'] = request.META['HTTP_ORIGIN']
                response['Access-Control-Allow-Headers'] = 'x-requested-with, content-type, accept, origin, authorization'
                response['Access-Control-Allow-Methods'] = 'GET, POST, PUT, PATCH, DELETE, OPTIONS'
        return response

    @staticmethod
    def is_own_httpexception(exception):
        """
        This is some sad, sad code and the only known workaround to ceck whether the given exception
        is an instance of one of our own exceptions. No, isinstance doesn't work as it somehow is convinced
        that the same classes imported from relatively a different path are in fact different classes.
        """
        bases = exception.__class__.__bases__
        if len(bases) != 1:
            return False
        base = bases[0]
        if base.__name__ != 'HttpException' and not base.__module__.endswith('api.exceptions'):
            return False
        if not exception.__class__.__module__.endswith('api.exceptions'):
            return False
        return True
