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
VPool module
"""
from backend.decorators import required_roles, load, return_list, return_object, return_task, log
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.vpoollist import VPoolList
from ovs.lib.storagerouter import StorageRouterController
from ovs.lib.vpool import VPoolController
from rest_framework import viewsets
from rest_framework.decorators import link, action
from rest_framework.exceptions import NotAcceptable
from rest_framework.permissions import IsAuthenticated


class VPoolViewSet(viewsets.ViewSet):
    """
    Information about vPools
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'vpools'
    base_name = 'vpools'

    @log()
    @required_roles(['read'])
    @return_list(VPool, 'name')
    @load()
    def list(self):
        """
        Overview of all vPools
        """
        return VPoolList.get_vpools()

    @log()
    @required_roles(['read'])
    @return_object(VPool)
    @load(VPool)
    def retrieve(self, vpool):
        """
        Load information about a given vPool
        :param vpool: vPool object to retrieve
        """
        return vpool

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VPool)
    def sync_vmachines(self, vpool):
        """
        Syncs the vMachine of this vPool
        :param vpool: vPool to synchronize
        """
        return VPoolController.sync_with_hypervisor.delay(vpool.guid)

    @link()
    @log()
    @required_roles(['read'])
    @return_list(StorageRouter)
    @load(VPool)
    def storagerouters(self, vpool, hints):
        """
        Retrieves a list of StorageRouters, serving a given vPool
        :param vpool: vPool to retrieve the storagerouter information for
        :param hints: Dictionary with hints
        """
        if hints.get('full', False) is True:
            return [storagedriver.storagerouter for storagedriver in vpool.storagedrivers]
        return [storagedriver.storagerouter_guid for storagedriver in vpool.storagedrivers]

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(VPool)
    def shrink_vpool(self, vpool, storagerouter_guid):
        """
        Remove the storagedriver linking the specified vPool and storagerouter_guid
        :param vpool: vPool to shrink (or delete if its the last storagerouter linked to it)
        :param storagerouter_guid: Guid of the Storage Router
        :return: Celery tasks' async result
        """
        sr = StorageRouter(storagerouter_guid)
        sd_guid = None
        sd_guids = [storagedriver.guid for storagedriver in vpool.storagedrivers]
        for storagedriver in sr.storagedrivers:
            if storagedriver.guid in sd_guids:
                sd_guid = storagedriver.guid
                break
        if sd_guid is None:
            raise NotAcceptable('Storage Router {0} is not a member of vPool {1}'.format(sr.name, vpool.name))
        return StorageRouterController.remove_storagedriver.s(sd_guid).apply_async(routing_key='sr.{0}'.format(sr.machine_id))

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(VPool)
    def update_storagedrivers(self, vpool, storagedriver_guid, version, storagerouter_guids=None, storagedriver_guids=None):
        """
        Update Storage Drivers for a given vPool (both adding and removing Storage Drivers)
        :param vpool: vPool to update
        :param storagedriver_guid: Storage Driver to update
        :param version: Version of API
        :param storagerouter_guids: Storage Router guids
        :param storagedriver_guids: Storage Driver guids
        :return: Celery task
        """
        if version > 1:
            raise NotAcceptable('Only available in API version 1')
        storagerouters = []
        if storagerouter_guids is not None:
            if storagerouter_guids.strip() != '':
                for storagerouter_guid in storagerouter_guids.strip().split(','):
                    storagerouter = StorageRouter(storagerouter_guid)
                    storagerouters.append((storagerouter.ip, storagerouter.machine_id))
        valid_storagedriver_guids = []
        if storagedriver_guids is not None:
            if storagedriver_guids.strip() != '':
                for storagedriver_guid in storagedriver_guids.strip().split(','):
                    storagedriver = StorageDriver(storagedriver_guid)
                    if storagedriver.vpool_guid != vpool.guid:
                        raise NotAcceptable('Given Storage Driver does not belong to this vPool')
                    valid_storagedriver_guids.append(storagedriver.guid)

        storagedriver = StorageDriver(storagedriver_guid)
        parameters = {'connection_host': None if vpool.connection is None else vpool.connection.split(':')[0],
                      'connection_port': None if vpool.connection is None else int(vpool.connection.split(':')[1]),
                      'connection_username': vpool.login,
                      'connection_password': vpool.password,
                      'storage_ip': storagedriver.storage_ip,
                      'type': vpool.backend_type.code,
                      'vpool_name': vpool.name}
        for field in parameters:
            if isinstance(parameters[field], basestring):
                parameters[field] = str(parameters[field])

        return StorageRouterController.update_storagedrivers.delay(valid_storagedriver_guids, storagerouters, parameters)

    @link()
    @required_roles(['read'])
    @return_task()
    @load(VPool, max_version=3)
    def get_configuration(self, vpool):
        """
        Retrieve the configuration settings for this vPool
        Currently we are able to configure the following settings (via GUI)
          - DTL enabled
          - DTL mode  (no sync, async, sync)
          - DTL location  (where DTL is configured to)
          - SCO size  (4MB - 128 MB)
          - Dedupe mode  (deduped aka ContentBased, non-deduped aka LocationBased)
          - Write buffer  (Amount of data allowed not immediately being put on backend)
          - Cache strategy  (no cache, cache on read, cache on write)
        :param vpool: vPool to retrieve configuration for
        :type vpool: Guid of the vPool
        """
        return vpool.configuration
