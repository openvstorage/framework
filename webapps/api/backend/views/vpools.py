# license see http://www.openvstorage.com/licenses/opensource/
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
