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
Middleware module
"""

import re
from django.http import HttpResponse
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.log.logHandler import LogHandler

logger = LogHandler('api', 'middleware')
regex = re.compile('^(.*; )?version=(?P<version>([0-9]+|\*)?)(;.*)?$')


class OVSMiddleware(object):
    """
    Middleware object
    """

    def process_exception(self, request, exception):
        """
        Logs information about the given error
        """
        _ = self, request
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
                response['Access-Control-Allow-Credentials'] = 'true'
                response['Access-Control-Allow-Headers'] = 'x-requested-with, content-type, accept, origin, authorization, x-csrftoken'
                response['Access-Control-Allow-Methods'] = 'GET, POST, PUT, PATCH, DELETE, OPTIONS'
        return response


class HttpResponseNotAcceptable(HttpResponse):
    status_code = 406
