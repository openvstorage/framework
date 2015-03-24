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
Contains the BackendViewSet
"""

from urllib2 import HTTPError, URLError
from backend.serializers.serializers import FullSerializer
from rest_framework import status, viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.exceptions import NotAcceptable
from ovs.dal.lists.backendlist import BackendList
from ovs.dal.lists.backendtypelist import BackendTypeList
from ovs.dal.hybrids.backend import Backend
from backend.decorators import return_object, return_list, load, required_roles, log
from ovs.extensions.api.client import OVSClient


class BackendViewSet(viewsets.ViewSet):
    """
    Information about backends
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'backends'
    base_name = 'backends'

    @log()
    @required_roles(['read'])
    @return_list(Backend)
    @load()
    def list(self, backend_type=None, ip=None, port=None, client_id=None, client_secret=None, contents=None):
        """
        Overview of all backends (from a certain type, if given) on the local node (or a remote one)
        """
        if ip is None:
            if backend_type is None:
                return BackendList.get_backends()
            return BackendTypeList.get_backend_type_by_code(backend_type).backends
        client = OVSClient(ip, port, client_id, client_secret)
        try:
            remote_backends = client.get('/backends/', params={'backend_type': backend_type,
                                                               'contents': '' if contents is None else contents})
        except (HTTPError, URLError):
            raise NotAcceptable('Could not load remote backends')
        backend_list = []
        for entry in remote_backends['data']:
            backend = type('Backend', (), entry)()
            backend_list.append(backend)
        return backend_list

    @log()
    @required_roles(['read'])
    @return_object(Backend)
    @load(Backend)
    def retrieve(self, backend):
        """
        Load information about a given backend
        """
        return backend

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load()
    def create(self, request):
        """
        Creates a Backend
        """
        serializer = FullSerializer(Backend, instance=Backend(), data=request.DATA)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
