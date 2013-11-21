"""
VDisk module
"""
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.exceptions import ObjectNotFoundException
from backend.serializers.serializers import SimpleSerializer, FullSerializer
from backend.decorators import required_roles


class VDiskViewSet(viewsets.ViewSet):
    """
    Information about machines
    """
    permission_classes = (IsAuthenticated,)

    @required_roles(['view'])
    def list(self, request, format=None):
        """
        Overview of all machines
        """
        _ = request, format
        vmachineguid = self.request.QUERY_PARAMS.get('vmachineguid', None)
        if vmachineguid is None:
            vdisks = VDiskList.get_vdisks().reduced
        else:
            vdisks = VMachine(vmachineguid).disks.reduced
        serializer = SimpleSerializer(vdisks, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @required_roles(['view'])
    def retrieve(self, request, pk=None, format=None):
        """
        Load information about a given task
        """
        _ = request, format
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            vdisk = VDisk(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        return Response(FullSerializer(VDisk, instance=vdisk).data, status=status.HTTP_200_OK)
