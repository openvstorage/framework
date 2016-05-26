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

from backend.decorators import load, log, required_roles, return_list, return_object, return_plain
from backend.serializers.serializers import FullSerializer
from celery.task.control import revoke
from ovs.dal.hybrids.backend import Backend
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.j_backendomain import BackendDomain
from ovs.dal.hybrids.j_vdiskdomain import VDiskDomain
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.lists.domainlist import DomainList
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.vdisk import VDiskController
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.exceptions import NotAcceptable
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
        """
        return domain

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load()
    def create(self, request, contents=None):
        """
        Creates a new Domain
        """
        contents = None if contents is None else contents.split(',')
        serializer = FullSerializer(Domain, contents=contents, instance=Domain(), data=request.DATA)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_202_ACCEPTED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load(Domain)
    def destroy(self, domain):
        """
        Deletes a Domain
        """
        if len(domain.storagerouters) > 0 or len(domain.backends) > 0 or len(domain.vdisks_dtl) > 0:
            raise NotAcceptable('The given Domain is still in use')
        domain.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load(Domain)
    def partial_update(self, domain, request, contents=None):
        """
        Update a Failure Domain
        """
        contents = None if contents is None else contents.split(',')
        serializer = FullSerializer(Domain, contents=contents, instance=domain, data=request.DATA)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_202_ACCEPTED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_plain()
    @load(Domain)
    def link(self, domain, object_type, object_guid, backup=False):
        """
        Link the domain with the specified object
        :param domain: Domain to link to an object
        :type domain: Domain

        :param object_type: Type of the object to link the domain to (backend, storagerouter or vdisk)
        :type object_type: str

        :param object_guid: Guid of the object to link the domain to
        :type object_guid: str

        :param backup: Domain is used as backup domain or regular domain
        :type backup: bool
        """
        change = False
        if object_type == 'backend':
            backend = Backend(object_guid)
            if not any(dom for dom in backend.domains if dom.domain_guid == domain.guid):
                junction = BackendDomain()
                junction.domain = domain
                junction.backend = backend
                junction.save()
        elif object_type == 'storagerouter':
            storagerouter = StorageRouter(object_guid)
            if any(dom for dom in storagerouter.domains if dom.domain_guid == domain.guid and dom.backup is not backup):
                raise ValueError('New{0} domain {1} is already part of the{2} domain'.format(' backup' if backup is True else '', domain.name, '' if backup is True else ' backup'))
            if not any(dom for dom in storagerouter.domains if dom.domain_guid == domain.guid and dom.backup is backup):
                change = True
                junction = StorageRouterDomain()
                junction.domain = domain
                junction.backup = backup
                junction.storagerouter = storagerouter
                junction.save()
                storagerouter.invalidate_dynamics(['regular_domains', 'backup_domains'])
        elif object_type == 'vdisk':
            vdisk = VDisk(object_guid)
            if not any(dom for dom in vdisk.domains_dtl if dom.domain_guid == domain.guid):
                change = True
                junction = VDiskDomain()
                junction.domain = domain
                junction.vdisk = vdisk
                junction.save()
        else:
            raise NotAcceptable('Only object types "backend", "storagerouter" and "vdisk" are allowed to link to a domain')

        # Schedule a task to run after 60 seconds, re-schedule task if another identical task gets triggered
        if change is True:
            cache = VolatileFactory.get_client()
            task_id_domain = cache.get(DomainViewSet.DOMAIN_CHANGE_KEY)
            task_id_backup = cache.get(DomainViewSet.FAILURE_DOMAIN_CHANGE_KEY)
            if task_id_domain:
                revoke(task_id_domain)  # If key exists, task was already scheduled. If task is already running, the revoke message will be ignored
            if task_id_backup:
                revoke(task_id_backup)
            async_mds_result = MDSServiceController.mds_checkup.s().apply_async(countdown=60)
            async_dtl_result = VDiskController.dtl_checkup.s().apply_async(countdown=60)
            cache.set(DomainViewSet.DOMAIN_CHANGE_KEY, async_mds_result.id, 600)  # Store the task id
            cache.set(DomainViewSet.FAILURE_DOMAIN_CHANGE_KEY, async_dtl_result.id, 600)  # Store the task id
        return True

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_plain()
    @load(Domain)
    def unlink(self, domain, object_type, object_guid):
        """
        Unlink the domain from the specified object
        :param domain: Domain to unlink from an object
        :type domain: Domain

        :param object_type: Type of the object to unlink the domain from (backend, storagerouter or vdisk)
        :type object_type: str

        :param object_guid: Guid of the object to unlink the domain from
        :type object_guid: str
        """
        if object_type == 'backend':
            attr_name = 'domains'
            object_to_unlink = Backend(object_guid)
        elif object_type == 'storagerouter':
            attr_name = 'domains'
            object_to_unlink = StorageRouter(object_guid)
        elif object_type == 'vdisk':
            attr_name = 'domains_dtl'
            object_to_unlink = VDisk(object_guid)
        else:
            raise NotAcceptable('Only object types "backend", "storagerouter" and "vdisk" are allowed to link to a domain')

        # Remove the link
        change = False
        for junction in getattr(object_to_unlink, attr_name):
            if junction.domain == domain:
                change = object_type in ['storagerouter', 'vdisk']
                junction.delete()
                object_to_unlink.invalidate_dynamics(['regular_domains', 'backup_domains'])
                break

        # Verify if the domain is still in use by another object
        for dom in DomainList.get_domains():
            if dom == domain and len(dom.backends) == 0 and len(dom.storagerouters) == 0 and len(dom.vdisks_dtl) == 0:
                dom.delete()
                break

        # Schedule a task to run after 60 seconds, re-schedule task if another identical task gets triggered
        if change is True:
            cache = VolatileFactory.get_client()
            task_id_domain = cache.get(DomainViewSet.DOMAIN_CHANGE_KEY)
            task_id_backup = cache.get(DomainViewSet.FAILURE_DOMAIN_CHANGE_KEY)
            if task_id_domain:
                revoke(task_id_domain)  # If key exists, task was already scheduled. If task is already running, the revoke message will be ignored
            if task_id_backup:
                revoke(task_id_backup)
            async_mds_result = MDSServiceController.mds_checkup.s().apply_async(countdown=60)
            async_dtl_result = VDiskController.dtl_checkup.s().apply_async(countdown=60)
            cache.set(DomainViewSet.DOMAIN_CHANGE_KEY, async_mds_result.id, 600)  # Store the task id
            cache.set(DomainViewSet.FAILURE_DOMAIN_CHANGE_KEY, async_dtl_result.id, 600)  # Store the task id
        return True
