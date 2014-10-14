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
VPool module
"""
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import link, action
from rest_framework.exceptions import NotAcceptable
from ovs.dal.lists.vpoollist import VPoolList
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.lib.vpool import VPoolController
from ovs.lib.storagerouter import StorageRouterController
from ovs.dal.hybrids.storagedriver import StorageDriver
from backend.decorators import required_roles, load, return_list, return_object, return_task


class VPoolViewSet(viewsets.ViewSet):
    """
    Information about vPools
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'vpools'
    base_name = 'vpools'

    @required_roles(['read'])
    @return_list(VPool, 'name')
    @load()
    def list(self):
        """
        Overview of all vPools
        """
        return VPoolList.get_vpools()

    @required_roles(['read'])
    @return_object(VPool)
    @load(VPool)
    def retrieve(self, vpool):
        """
        Load information about a given vPool
        """
        return vpool

    @action()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VPool)
    def sync_vmachines(self, vpool):
        """
        Syncs the vMachine of this vPool
        """
        return VPoolController.sync_with_hypervisor.delay(vpool.guid)

    @link()
    @required_roles(['read'])
    @return_list(StorageRouter)
    @load(VPool)
    def storagerouters(self, vpool, hints):
        """
        Retrieves a list of StorageRouters, serving a given vPool
        """
        storagerouter_guids = []
        storagerouter = []
        for storagedriver in vpool.storagedrivers:
            storagerouter_guids.append(storagedriver.storagerouter_guid)
            if hints['full'] is True:
                storagerouter.append(storagedriver.storagerouter)
        return storagerouter if hints['full'] is True else storagerouter_guids

    @action()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(VPool)
    def update_storagedrivers(self, vpool, storagedriver_guid, storagerouter_guids=None, storagedriver_guids=None):
        """
        Update Storage Drivers for a given vPool (both adding and removing Storage Drivers)
        """
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
        parameters = {'vpool_name':            vpool.name,
                      'type':                  vpool.type,
                      'connection_host':       None if vpool.connection is None else vpool.connection.split(':')[0],
                      'connection_port':       None if vpool.connection is None else int(vpool.connection.split(':')[1]),
                      'connection_timeout':    0,  # Not in use anyway
                      'connection_username':   vpool.login,
                      'connection_password':   vpool.password,
                      'mountpoint_bfs':        storagedriver.mountpoint_bfs,
                      'mountpoint_temp':       storagedriver.mountpoint_temp,
                      'mountpoint_md':         storagedriver.mountpoint_md,
                      'mountpoint_readcache1': storagedriver.mountpoint_readcache1,
                      'mountpoint_readcache2': storagedriver.mountpoint_readcache2,
                      'mountpoint_writecache': storagedriver.mountpoint_writecache,
                      'mountpoint_foc':        storagedriver.mountpoint_foc,
                      'storage_ip':            storagedriver.storage_ip,
                      'vrouter_port':          storagedriver.port}
        for field in parameters:
            if not parameters[field] is int:
                parameters[field] = str(parameters[field])

        return StorageRouterController.update_storagedrivers.delay(valid_storagedriver_guids, storagerouters, parameters)

    @action()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(VPool)
    def set_configparams(self, vpool, configparams):
        """
        Sets configuration parameters to a given vpool/vdisk. Items not passed are (re)set.
        """
        return VPoolController.set_configparams.delay(vpool_guid=vpool.guid, configparams=configparams)
