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
CORS middleware module
"""

from django import http
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.log.logHandler import LogHandler

logger = LogHandler('ovs.api', 'CORS middleware')


class CORSMiddleware(object):
    """
    CORS middleware object
    """

    def process_request(self, request):
        """
        Handle CORS preflight requests
        """
        _ = self
        if request.method == 'OPTIONS' and 'HTTP_ACCESS_CONTROL_REQUEST_METHOD' in request.META:
            logger.debug('Allow CORS preflight')
            response = http.HttpResponse()
            return response
        return None

    def process_response(self, request, response):
        """
        Processes CORS headers
        """
        _ = self
        if 'HTTP_ORIGIN' in request.META:
            vsas = VMachineList.get_vsas()
            allowed_origins = ['https://{0}'.format(vsa.ip) for vsa in vsas]
            if request.META['HTTP_ORIGIN'] in allowed_origins:
                logger.debug('Set CORS preflight headers')
                response['Access-Control-Allow-Origin'] = request.META['HTTP_ORIGIN']
                response['Access-Control-Allow-Credentials'] = 'true'
                response['Access-Control-Allow-Headers'] = 'x-requested-with, content-type, accept, origin, authorization, x-csrftoken'
                response['Access-Control-Allow-Methods'] = 'GET, POST, PUT, PATCH, DELETE, OPTIONS'
        return response
