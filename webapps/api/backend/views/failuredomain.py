# Copyright 2015 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module for failure domains
"""

from backend.decorators import load
from backend.decorators import log
from backend.decorators import required_roles
from backend.decorators import return_list
from backend.decorators import return_object
from backend.serializers.serializers import FullSerializer
from ovs.dal.hybrids.failuredomain import FailureDomain
from ovs.dal.lists.failuredomainlist import FailureDomainList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from rest_framework import status
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.exceptions import NotAcceptable
from rest_framework.response import Response


class FailureDomainViewSet(viewsets.ViewSet):
    """
    Information about FailureDomains
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'failure_domains'
    base_name = 'failure_domains'

    @log()
    @required_roles(['read'])
    @return_list(FailureDomain)
    @load()
    def list(self):
        """
        Lists all available Failure Domains
        """
        return FailureDomainList.get_failure_domains()

    @log()
    @required_roles(['read'])
    @return_object(FailureDomain)
    @load(FailureDomain)
    def retrieve(self, failuredomain):
        """
        Load information about a given Failure Domain
        """
        return failuredomain

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load(FailureDomain)
    def destroy(self, failuredomain):
        """
        Deletes a FailureDomain
        """
        if len(failuredomain.primary_storagerouters) > 0 or len(failuredomain.secondary_storagerouters) > 0:
            raise NotAcceptable('The given FailureDomain is still in use')
        failuredomain.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load()
    def create(self, request, contents=None):
        """
        Creates a new Failure Domain
        """
        contents = None if contents is None else contents.split(',')
        serializer = FullSerializer(FailureDomain, contents=contents, instance=FailureDomain(), data=request.DATA)
        if serializer.is_valid():
            serializer.save()

            return Response(serializer.data, status=status.HTTP_202_ACCEPTED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load(FailureDomain)
    def partial_update(self, failuredomain, request, contents=None):
        """
        Update a Failure Domain
        """
        contents = None if contents is None else contents.split(',')
        serializer = FullSerializer(FailureDomain, contents=contents, instance=failuredomain, data=request.DATA)
        if serializer.is_valid():
            serializer.save()

            return Response(serializer.data, status=status.HTTP_202_ACCEPTED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
