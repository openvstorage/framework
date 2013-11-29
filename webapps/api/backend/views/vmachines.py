# license see http://www.openvstorage.com/licenses/opensource/
"""
VMachine module
"""
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import action
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.hybrids.vmachine import VMachine
from ovs.lib.vmachine import VMachineController
from ovs.dal.exceptions import ObjectNotFoundException
from backend.serializers.serializers import SimpleSerializer, FullSerializer
from backend.decorators import required_roles, expose


class VMachineViewSet(viewsets.ViewSet):
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
        vmachines = VMachineList.get_vmachines().reduced
        serializer = SimpleSerializer(vmachines, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    def retrieve(self, request, pk=None, format=None):
        """
        Load information about a given vMachine
        """
        _ = request, format
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            vmachine = VMachine(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        return Response(FullSerializer(VMachine, instance=vmachine).data, status=status.HTTP_200_OK)

    @expose(internal=True, customer=True)
    @required_roles(['view', 'delete'])
    def destroy(self, request, pk=None):
        """
        Deletes a machine
        """
        _ = request
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            vmachine = VMachine(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        task = VMachineController.delete.s(machineguid=vmachine.guid).apply_async()
        return Response(task.id, status=status.HTTP_200_OK)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    def clone(self, request, pk=None, format=None):
        """
        Clones a machine
        """
        _ = format
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            vmachine = VMachine(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        task = VMachineController.clone.s(parentmachineguid=vmachine.guid,
                                          disks=request.DATA['disks'],
                                          name=request.DATA['name']).apply_async()
        return Response(task.id, status=status.HTTP_200_OK)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    def snapshot(self, request, pk=None, format=None):
        """
        Snapshots a given machine
        """
        _ = format
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            vmachine = VMachine(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        task = VMachineController.snapshot.s(machineguid=vmachine.guid,
                                             name=request.DATA['name'],
                                             consistent=request.DATA['consistent']).apply_async()
        return Response(task.id, status=status.HTTP_200_OK)
