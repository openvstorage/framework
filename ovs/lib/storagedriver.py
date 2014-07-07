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
StorageDriver module
"""

from ovs.celery import celery
from ovs.dal.hybrids.storageappliance import StorageAppliance
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.extensions.storageserver.storagedriver import StorageDriverClient


class StorageDriverController(object):
    """
    Contains all BLL related to Storage Drivers
    """

    @staticmethod
    @celery.task(name='ovs.storagedriver.move_away')
    def move_away(storageappliance_guid):
        """
        Moves away all vDisks from all Storage Drivers this Storage Appliance is serving
        """
        storagedrivers = StorageAppliance(storageappliance_guid).storagedrivers
        if len(storagedrivers) > 0:
            storagedriver_client = StorageDriverClient().load(storagedrivers[0].vpool)
            for storagedriver in storagedrivers:
                storagedriver_client.mark_node_offline(str(storagedriver.storagedriver_id))

    @staticmethod
    @celery.task(name='ovs.storagedriver.update_status')
    def update_status(storagedriver_id):
        """
        Sets Storage Driver offline in case hypervisor management Center
        reports the hypervisor pmachine related to this Storage Driver
        as unavailable.
        """
        pmachine = PMachineList.get_by_storagedriver_id(storagedriver_id)
        if pmachine.mgmtcenter:
            # Update status
            pmachine.invalidate_dynamics(['host_status'])
            host_status = pmachine.host_status
            if host_status != 'RUNNING':
                # Host is stopped
                storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
                storagedriver_client = StorageDriverClient().load(storagedriver.vpool)
                storagedriver_client.mark_node_offline(str(storagedriver.storagedriver_id))
        else:
            # No management Center, cannot update status via api
            #TODO: should we try manually (ping, ssh)?
            pass
