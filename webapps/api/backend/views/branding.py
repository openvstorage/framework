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
Contains the BrandingViewSet
"""
from rest_framework import status, viewsets
from rest_framework.response import Response
from ovs.dal.lists.brandinglist import BrandingList
from ovs.dal.hybrids.branding import Branding
from backend.serializers.serializers import FullSerializer
from backend.decorators import expose, validate
from backend.toolbox import Toolbox


class BrandingViewSet(viewsets.ViewSet):
    """
    Information about branding
    """

    @expose(internal=True)
    def list(self, request, format=None):
        """
        Overview of all brandings
        """
        _ = format
        brands = BrandingList.get_brandings()
        pmachines, serializer, contents = Toolbox.handle_list(brands, request)
        serialized = serializer(Branding, contents=contents, instance=brands, many=True)
        return Response(serialized.data, status=status.HTTP_200_OK)

    @expose(internal=True)
    @validate(Branding)
    def retrieve(self, request, obj):
        """
        Load information about a given branding
        """
        contents = Toolbox.handle_retrieve(request)
        return Response(FullSerializer(Branding, contents=contents, instance=obj).data, status=status.HTTP_200_OK)
