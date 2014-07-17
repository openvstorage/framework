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

import hashlib
import base64
import time
from django.views.generic import View
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse, HttpResponseBadRequest, HttpResponseForbidden
from oauth2.decorators import json_response
from oauth2.toolbox import Toolbox
from ovs.dal.lists.userlist import UserList
from ovs.dal.lists.rolelist import RoleList
from ovs.dal.hybrids.client import Client
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.lists.bearertokenlist import BearerTokenList


class MetadataView(View):
    """
    Implements OAuth 2 metadata views (out of OAuth 2 spec)
    """

    @json_response()
    def get(self, request, *args, **kwargs):
        """
        Fetches OAuth 2 metadata
        * Who am I
        """
        try:
            _ = args, kwargs
            if 'HTTP_AUTHORIZATION' not in request.META:
                return HttpResponse, {'loggedin': False,
                                      'reason': 'unauthorized'}
            authorization_type, access_token = request.META['HTTP_AUTHORIZATION'].split(' ')
            if authorization_type != 'Bearer':
                return HttpResponse, {'loggedin': False,
                                      'reason': 'invalid authorization type'}

            tokens = BearerTokenList.get_by_access_token(access_token)
            if len(tokens) != 1:
                return HttpResponse, {'loggedin': False,
                                      'reason': 'invalid token'}
            token = tokens[0]
            if token.expiration < time.time():
                for junction in token.roles.itersafe():
                    junction.delete()
                token.delete()
                return HttpResponse, {'loggedin': False,
                                      'reason': 'token expired'}

            user = token.client.user
            if not user.is_active:
                return HttpResponse, {'loggedin': False,
                                      'reason': 'inactive user'}

            return HttpResponse, {'loggedin': True,
                                  'userguid': user.guid,
                                  'username': user.username}
        except:
            return HttpResponse, {'loggedin': False,
                                  'reason': 'unexpected exception'}

    @csrf_exempt
    def dispatch(self, request, *args, **kwargs):
        """
        Pass through method to add the CSRF exempt
        """
        return super(MetadataView, self).dispatch(request, *args, **kwargs)
