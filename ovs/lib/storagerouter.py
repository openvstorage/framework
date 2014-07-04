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
StorageRouter module
"""

from ovs.celery import celery
from ovs.dal.hybrids.storageappliance import StorageAppliance
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.storageserver.storagerouter import StorageRouterClient


class StorageRouterController(object):
    """
    Contains all BLL related to Storage Routers
    """

    @staticmethod
    @celery.task(name='ovs.storagerouter.move_away')
    def move_away(storageappliance_guid):
        """
        Moves away all vDisks from all Storage Routers this Storage Appliance is serving
        """
        storagerouters = StorageAppliance(storageappliance_guid).storagerouters
        if len(storagerouters) > 0:
            storagerouter_client = StorageRouterClient().load(storagerouters[0].vpool)
            for storagerouter in storagerouters:
                storagerouter_client.mark_node_offline(str(storagerouter.storagerouter_id))

    @staticmethod
    @celery.task(name='ovs.storagerouter.update_status')
    def update_status(storagerouter_id):
        """
        Sets Storage Router offline in case hypervisor management Center
        reports the hypervisor pmachine related to this Storage Router
        as unavailable.
        """
        pmachine = PMachineList.get_by_storagerouter_id(storagerouter_id)
        if pmachine.mgmtcenter:
            # Update status
            pmachine.invalidate_dynamics(['host_status'])
            host_status = pmachine.host_status
            if host_status != 'RUNNING':
                # Host is stopped
                storagerouter = StorageRouterList.get_by_storagerouter_id(storagerouter_id)
                storagerouter_client = StorageRouterClient().load(storagerouter.vpool)
                storagerouter_client.mark_node_offline(str(storagerouter.storagerouter_id))
        else:
            # No management Center, cannot update status via api
            #TODO: should we try manually (ping, ssh)?
            pass
