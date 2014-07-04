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
VolumeStorageRouter module
"""

from ovs.celery import celery
from ovs.dal.hybrids.storageappliance import StorageAppliance
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.dal.lists.volumestoragerouterlist import VolumeStorageRouterList
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient


class VolumeStorageRouterController(object):
    """
    Contains all BLL related to VolumeStorageRouters
    """

    @staticmethod
    @celery.task(name='ovs.vsr.move_away')
    def move_away(storageappliance_guid):
        """
        Moves away all vDisks from all VSRs this Storage Appliance is serving
        """
        vsrs = StorageAppliance(storageappliance_guid).vsrs
        if len(vsrs) > 0:
            vsr_client = VolumeStorageRouterClient().load(vsrs[0].vpool)
            for vsr in vsrs:
                vsr_client.mark_node_offline(str(vsr.vsrid))

    @staticmethod
    @celery.task(name='ovs.vsr.update_status')
    def update_status(vsrid):
        """
        Sets volumerouter offline in case hypervisor management Center
         reports the hypervisor pmachine related to this volumestoragerouter
         as unavailable.
        """
        pmachine = PMachineList.get_by_vsrid(vsrid)
        if pmachine.mgmtcenter:
            # Update status
            pmachine.invalidate_dynamics(['host_status'])
            host_status = pmachine.host_status
            if host_status != 'RUNNING':
                # Host is stopped
                vsr = VolumeStorageRouterList.get_by_vsrid(vsrid)
                vsr_client = VolumeStorageRouterClient().load(vsr.vpool)
                vsr_client.mark_node_offline(str(vsr.vsrid))
        else:
            # No management Center, cannot update status via api
            #TODO: should we try manually (ping, ssh)?
            pass
