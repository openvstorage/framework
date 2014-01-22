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
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import link
from ovs.dal.lists.vpoollist import VPoolList
from ovs.dal.hybrids.vpool import VPool
from backend.serializers.serializers import SimpleSerializer, FullSerializer
from backend.decorators import required_roles, expose, validate


class VPoolViewSet(viewsets.ViewSet):
    """
    Information about vPools
    """
    permission_classes = (IsAuthenticated,)

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    def list(self, request, format=None):
        """
        Overview of all vPools
        """
        _ = request, format
        vpools = VPoolList.get_vpools().reduced
        serializer = SimpleSerializer(vpools, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @validate(VPool)
    def retrieve(self, request, obj):
        """
        Load information about a given vPool
        """
        _ = request
        return Response(FullSerializer(VPool, instance=obj).data, status=status.HTTP_200_OK)

    @link()
    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @validate(VPool)
    def count_disks(self, request, obj):
        """
        Returns the amount of vDisks on the vPool
        """
        _ = request
        return Response(len(obj.vdisks), status=status.HTTP_200_OK)

    @link()
    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @validate(VPool)
    def count_machines(self, request, obj):
        """
        Returns the amount of vMachines on the vPool
        """
        _ = request
        vmachine_guids = []
        for disk in obj.vdisks:
            if disk.vmachine is not None and disk.vmachine.guid not in vmachine_guids:
                vmachine_guids.append(disk.vmachine.guid)
        return Response(len(vmachine_guids), status=status.HTTP_200_OK)
