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
PMachine module
"""

from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import action
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.hybrids.storagerouter import StorageRouter
from backend.decorators import required_roles, expose, validate, get_list, get_object


class StorageRouterViewSet(viewsets.ViewSet):
    """
    Information about StorageRouters
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'storagerouters'
    base_name = 'storagerouters'

    @expose(internal=True)
    @required_roles(['view'])
    @get_list(StorageRouter)
    def list(self, request, format=None, hints=None):
        """
        Overview of all StorageRouters
        """
        _ = request, format, hints
        return StorageRouterList.get_storagerouters()

    @expose(internal=True)
    @required_roles(['view'])
    @validate(StorageRouter)
    @get_object(StorageRouter)
    def retrieve(self, request, obj):
        """
        Load information about a given StorageRouter
        """
        _ = request
        return obj

    @action()
    @expose(internal=True)
    @validate(StorageRouter)
    def can_be_deleted(self, request, obj):
        """
        Checks whether a Storage Router can be deleted
        """
        _ = request
        result = True
        storageappliance = obj.storageappliance
        pmachine = storageappliance.pmachine
        vmachines = VMachineList.get_customer_vmachines()
        vpools_guids = [vmachine.vpool_guid for vmachine in vmachines if vmachine.vpool_guid is not None]
        pmachine_guids = [vmachine.pmachine_guid for vmachine in vmachines]
        vpool = obj.vpool

        if pmachine.guid in pmachine_guids and vpool.guid in vpools_guids:
            result = False
        if any(vdisk for vdisk in vpool.vdisks if vdisk.storagerouter_id == obj.storagerouter_id):
            result = False
        return Response(result, status=status.HTTP_200_OK)
