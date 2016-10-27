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
Module for domains
"""

from backend.decorators import load, log, required_roles, return_list, return_object, return_simple
from backend.exceptions import HttpNotAcceptableException
from backend.serializers.serializers import FullSerializer
from ovs.dal.hybrids.domain import Domain
from ovs.dal.lists.domainlist import DomainList
from rest_framework import status, viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response


class DomainViewSet(viewsets.ViewSet):
    """
    Information about Domains
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'domains'
    base_name = 'domains'

    @log()
    @required_roles(['read'])
    @return_list(Domain)
    @load()
    def list(self):
        """
        Lists all available Domains
        """
        return DomainList.get_domains()

    @log()
    @required_roles(['read'])
    @return_object(Domain)
    @load(Domain)
    def retrieve(self, domain):
        """
        Load information about a given Domain
        :param domain: The domain to be retrieved
        :type domain: Domain
        """
        return domain

    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_object(Domain, mode='created')
    @load()
    def create(self, request, contents=None):
        """
        Creates a new Domain
        :param request: The raw request:
        :type request: Request
        :param contents: Requested contents (serializer hint)
        :type contents: str
        """
        contents = None if contents is None else contents.split(',')
        serializer = FullSerializer(Domain, contents=contents, instance=Domain(), data=request.DATA)
        domain = serializer.object
        domain.save()
        return domain

    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_simple()
    @load(Domain)
    def destroy(self, domain):
        """
        Deletes a Domain
        :param domain: The domain to return
        :type domain: Domain
        :return: None
        :rtype: None
        """
        if len(domain.storagerouters) > 0 or len(domain.backends) > 0 or len(domain.vdisks_dtl) > 0:
            raise HttpNotAcceptableException(error_description='The given Domain is still in use',
                                             error='in_use')
        domain.delete()

    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_object(Domain, mode='accepted')
    @load(Domain)
    def partial_update(self, domain, request, contents=None):
        """
        Update a Failure Domain
        :param domain: The domain to update
        :type domain: Domain
        :param request: The raw request
        :type request: Request
        :param contents: Contents to be updated/returned
        :type contents: str
        """
        contents = None if contents is None else contents.split(',')
        serializer = FullSerializer(Domain, contents=contents, instance=domain, data=request.DATA)
        domain = serializer.object
        domain.save()
        return domain
