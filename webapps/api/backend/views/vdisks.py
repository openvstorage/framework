# license see http://www.openvstorage.com/licenses/opensource/
"""
VDisk module
"""
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import link, action
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.volumestoragerouterlist import VolumeStorageRouterList
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vmachine import VMachine
from ovs.lib.vdisk import VDiskController
from ovs.dal.exceptions import ObjectNotFoundException
from backend.serializers.serializers import SimpleSerializer, FullSerializer
from backend.decorators import required_roles, expose


class VDiskViewSet(viewsets.ViewSet):
    """
    Information about machines
    """
    permission_classes = (IsAuthenticated,)

    @expose(internal=True, customer=True)
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
            vmachine = VMachine(vmachineguid)
            if vmachine.is_internal:
                vdisks = []
                for vsr in vmachine.served_vsrs:
                    vdisks += vsr.vpool.vdisks.reduced
            else:
                vdisks = vmachine.vdisks.reduced
        serializer = SimpleSerializer(vdisks, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @expose(internal=True, customer=True)
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

    @link()
    @expose(internal=True, customer=True)
    @required_roles(['view'])
    def get_vsa(self, request, pk=None, format=None):
        """
        Returns the guid of VSA machine
        """
        _ = request, format
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            vdisk = VDisk(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        vsa_vmachine_guid = None
        if vdisk.vsrid:
            vsr = VolumeStorageRouterList.get_volumestoragerouter_by_vsrid(vdisk.vsrid)
            vsa_vmachine_guid = vsr.serving_vmachine.guid
        return Response(vsa_vmachine_guid, status=status.HTTP_200_OK)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    def rollback(self, request, pk=None, format=None):
        """
        Clones a machine
        """
        _ = format
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            vdisk = VDisk(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        task = VDiskController.rollback.s(diskguid=vdisk.guid,
                                          timestamp=request.DATA['timestamp']).apply_async()
        return Response(task.id, status=status.HTTP_200_OK)

