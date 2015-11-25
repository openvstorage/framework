# Copyright 2015 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
DiskPartition module
"""

from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from ovs.dal.lists.diskpartitionlist import DiskPartitionList
from ovs.dal.hybrids.disk import Disk
from ovs.dal.hybrids.diskpartition import DiskPartition
from backend.decorators import required_roles, load, return_list, return_object, log


class DiskPartitionViewSet(viewsets.ViewSet):
    """
    Information about DiskPartitions
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'diskpartitions'
    base_name = 'diskpartitions'

    @log()
    @required_roles(['read'])
    @return_list(DiskPartition)
    @load()
    def list(self, diskguid=None):
        """
        Overview of all disks
        """
        if diskguid is not None:
            disk = Disk(diskguid)
            return disk.partitions
        return DiskPartitionList.get_partitions()

    @log()
    @required_roles(['read'])
    @return_object(DiskPartition)
    @load(DiskPartition)
    def retrieve(self, diskpartition):
        """
        Load information about a given diskpartition
        """
        return diskpartition
