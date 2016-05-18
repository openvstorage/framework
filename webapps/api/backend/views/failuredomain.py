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
