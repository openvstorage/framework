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
from ovs.dal.exceptions import ObjectNotFoundException
from backend.serializers.serializers import SimpleSerializer, FullSerializer
from backend.decorators import required_roles, expose


class VPoolViewSet(viewsets.ViewSet):
    """
    Information about machines
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
    def retrieve(self, request, pk=None, format=None):
        """
        Load information about a given vPool
        """
        _ = request, format
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            vpool = VPool(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        return Response(FullSerializer(VPool, instance=vpool).data, status=status.HTTP_200_OK)

    @link()
    @expose(internal=True, customer=True)
    @required_roles(['view'])
    def count_disks(self, request, pk=None, format=None):
        """
        Returns the amount of disks
        """
        _ = request, format
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            vpool = VPool(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        return Response(len(vpool.vdisks), status=status.HTTP_200_OK)

    @link()
    @expose(internal=True, customer=True)
    @required_roles(['view'])
    def count_machines(self, request, pk=None, format=None):
        """
        Returns the amount of machines
        """
        _ = request, format
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            vpool = VPool(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        vmachine_guids = []
        for disk in vpool.vdisks:
            if disk.vmachine is not None and disk.vmachine.guid not in vmachine_guids:
                vmachine_guids.append(disk.vmachine.guid)
        return Response(len(vmachine_guids), status=status.HTTP_200_OK)
