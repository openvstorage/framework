# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
VDisk module
"""

from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import action, link
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.lib.vdisk import VDiskController
from backend.decorators import required_roles, load, return_list, return_object, return_task, log


class VDiskViewSet(viewsets.ViewSet):
    """
    Information about vDisks
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'vdisks'
    base_name = 'vdisks'

    @log()
    @required_roles(['read'])
    @return_list(VDisk)
    @load()
    def list(self, vmachineguid=None, vpoolguid=None):
        """
        Overview of all vDisks
        """
        if vmachineguid is not None:
            vmachine = VMachine(vmachineguid)
            return vmachine.vdisks
        elif vpoolguid is not None:
            vpool = VPool(vpoolguid)
            return vpool.vdisks
        return VDiskList.get_vdisks()

    @log()
    @required_roles(['read'])
    @return_object(VDisk)
    @load(VDisk)
    def retrieve(self, vdisk):
        """
        Load information about a given vDisk
        """
        return vdisk

    @action()
    @log()
    @required_roles(['read', 'write'])
    @return_task()
    @load(VDisk)
    def rollback(self, vdisk, timestamp):
        """
        Rollbacks a vDisk to a given timestamp
        """
        return VDiskController.rollback.delay(diskguid=vdisk.guid,
                                              timestamp=timestamp)

    @action()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(VDisk)
    def set_config_params(self, vdisk, new_config_params, old_config_params):
        """
        Sets configuration parameters to a given vdisk.
        """
        return VDiskController.set_config_params.delay(vdisk_guid=vdisk.guid, new_config_params=new_config_params, old_config_params=old_config_params)

    @link()
    @required_roles(['read'])
    @return_task()
    @load(VDisk)
    def get_config_params(self, vdisk):
        """
        Retrieve the configuration parameters for the given disk from the storagedriver.
        """
        return VDiskController.get_config_params.delay(vdisk_guid=vdisk.guid)
