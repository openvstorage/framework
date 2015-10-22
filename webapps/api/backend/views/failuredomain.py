# Copyright 2015 Open vStorage NV
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
from rest_framework.response import Response


class FailureDomainViewSet(viewsets.ViewSet):
    """
    Information about FailureDomains
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'failure_domain'
    base_name = 'failure_domain'

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
    @load()
    def create(self, name, city, address, country, primary, secondary=None):
        """
        Creates a new failure domain
        """
        if secondary is None:
            secondary = []

        failure_domain = FailureDomain()
        failure_domain.name = name
        failure_domain.city = city
        failure_domain.address = address
        failure_domain.country = country
        failure_domain.save()

        for storage_router in StorageRouterList.get_storagerouters():
            if storage_router.guid in primary:
                if storage_router.primary_failure_domain:
                    raise RuntimeError('Primary domain for storagerouter {0} was already set to {1}'.format(storage_router.name, storage_router.primary_failure_domain.name))
                storage_router.primary_failure_domain = failure_domain
                storage_router.save()
            elif storage_router.guid in secondary:
                if storage_router.secondary_failure_domain:
                    raise RuntimeError('Secondary domain for storagerouter {0} was already set to {1}'.format(storage_router.name, storage_router.secondary_failure_domain.name))
                storage_router.secondary_failure_domain = failure_domain
                storage_router.save()

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load(FailureDomain)
    def partial_update(self, contents, failuredomain, request, city, name, address, country, primary, secondary=None):
        """
        Update a Failure Domain
        """
        contents = None if contents is None else contents.split(',')
        serializer = FullSerializer(FailureDomain, contents=contents, instance=failuredomain, data=request.DATA)
        if serializer.is_valid():
            primary = set(primary.split(','))
            secondary = set(secondary.split(',')) if secondary is not None else set([])
            if primary.intersection(secondary):
                raise ValueError('A storagerouter cannot have the same failure domain for both primary and backup')

            failuredomain.name = name
            failuredomain.city = city
            failuredomain.address = address
            failuredomain.country = country
            failuredomain.save()
            for storage_router in StorageRouterList.get_storagerouters():
                # Clear the failure domains from all storagerouter if equal to current failure domain
                if storage_router.primary_failure_domain is not None and storage_router.primary_failure_domain.guid == failuredomain.guid:
                    storage_router.primary_failure_domain = None
                if storage_router.secondary_failure_domain is not None and storage_router.secondary_failure_domain.guid == failuredomain.guid:
                    storage_router.secondary_failure_domain = None

                if storage_router.guid in primary:
                    if storage_router.primary_failure_domain:  # A storage router can only belong to 1 failure domain, which should have been cleared by now
                        raise RuntimeError('Primary domain for storagerouter {0} was already set to {1}'.format(storage_router.name, storage_router.primary_failure_domain.name))
                    storage_router.primary_failure_domain = failuredomain
                elif storage_router.guid in secondary:
                    if storage_router.secondary_failure_domain:
                        raise RuntimeError('Secondary domain for storagerouter {0} was already set to {1}'.format(storage_router.name, storage_router.secondary_failure_domain.name))
                    storage_router.secondary_failure_domain = failuredomain
                storage_router.save()

            serializer.save()
            return Response(serializer.data, status=status.HTTP_202_ACCEPTED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
