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
VDisk module
"""

from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import action
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.storageappliance import StorageAppliance
from ovs.dal.hybrids.vpool import VPool
from ovs.lib.vdisk import VDiskController
from backend.decorators import required_roles, expose, validate, get_list, get_object, celery_task


class VDiskViewSet(viewsets.ViewSet):
    """
    Information about vDisks
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'vdisks'
    base_name = 'vdisks'

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @get_list(VDisk)
    def list(self, request, format=None, hints=None):
        """
        Overview of all vDisks
        """
        _ = format, hints
        vmachineguid = request.QUERY_PARAMS.get('vmachineguid', None)
        vpoolguid = request.QUERY_PARAMS.get('vpoolguid', None)
        if vmachineguid is not None:
            vmachine = VMachine(vmachineguid)
            return vmachine.vdisks
        elif vpoolguid is not None:
            vpool = VPool(vpoolguid)
            return vpool.vdisks
        return VDiskList.get_vdisks()

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @validate(VDisk)
    @get_object(VDisk)
    def retrieve(self, request, obj):
        """
        Load information about a given vDisk
        """
        _ = request
        return obj

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VDisk)
    @celery_task()
    def rollback(self, request, obj):
        """
        Rollbacks a vDisk to a given timestamp
        """
        _ = format
        return VDiskController.rollback.delay(diskguid=obj.guid,
                                              timestamp=request.DATA['timestamp'])

