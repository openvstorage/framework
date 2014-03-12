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
Module for generic functionality
"""
from backend.decorators import expose
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from ovs.dal.lists.vmachinelist import VMachineList
from django.http import Http404


class GenericViewSet(viewsets.ViewSet):
    """
    Generic
    """

    @expose(internal=True)
    def list(self, request, format=None):
        """
        Dummy implementation
        """
        _ = request, format
        return Response([{'guid': '0'}])

    @expose(internal=True)
    def retrieve(self, request, pk=None, format=None):
        """
        Retrieve generic information
        """
        _ = format, request
        if pk != '0':
            raise Http404
        vsa_ips = []
        for vsa in VMachineList.get_vsas():
            vsa_ips.append(vsa.ip)
        data = {'vsa_ips': vsa_ips}
        return Response(data)
