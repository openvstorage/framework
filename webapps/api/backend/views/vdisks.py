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
from backend.serializers.serializers import SimpleSerializer, FullSerializer
from backend.decorators import required_roles, expose, validate


class VDiskViewSet(viewsets.ViewSet):
    """
    Information about vDisks
    """
    permission_classes = (IsAuthenticated,)

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    def list(self, request, format=None):
        """
        Overview of all vDisks
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
    @validate(VDisk)
    def retrieve(self, request, obj):
        """
        Load information about a given vDisk
        """
        _ = request
        return Response(FullSerializer(VDisk, instance=obj).data, status=status.HTTP_200_OK)

    @link()
    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @validate(VDisk)
    def get_vsa(self, request, obj):
        """
        Returns the guid of the VSA serving the vDisk
        """
        _ = request
        vsa_vmachine_guid = None
        if obj.vsrid:
            vsr = VolumeStorageRouterList.get_by_vsrid(obj.vsrid)
            vsa_vmachine_guid = vsr.serving_vmachine.guid
        return Response(vsa_vmachine_guid, status=status.HTTP_200_OK)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VDisk)
    def rollback(self, request, obj):
        """
        Rollbacks a vDisk to a given timestamp
        """
        _ = format
        task = VDiskController.rollback.delay(diskguid=obj.guid,
                                              timestamp=request.DATA['timestamp'])
        return Response(task.id, status=status.HTTP_200_OK)

