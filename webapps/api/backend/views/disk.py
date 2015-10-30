# Copyright 2015 iNuron NV
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
Disk module
"""

from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from ovs.dal.lists.disklist import DiskList
from ovs.dal.hybrids.disk import Disk
from ovs.dal.hybrids.storagerouter import StorageRouter
from backend.decorators import required_roles, load, return_list, return_object, log


class DiskViewSet(viewsets.ViewSet):
    """
    Information about disks
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'disks'
    base_name = 'disks'

    @log()
    @required_roles(['read'])
    @return_list(Disk)
    @load()
    def list(self, storagerouterguid=None):
        """
        Overview of all disks
        """
        if storagerouterguid is not None:
            storagerouter = StorageRouter(storagerouterguid)
            return storagerouter.disks
        return DiskList.get_disks()

    @log()
    @required_roles(['read'])
    @return_object(Disk)
    @load(Disk)
    def retrieve(self, disk):
        """
        Load information about a given disk
        """
        return disk
