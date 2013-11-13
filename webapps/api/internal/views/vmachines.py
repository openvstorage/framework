from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import action
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.hybrids.vmachine import VMachine
from ovs.lib.vmachine import VMachineController
from ovs.dal.exceptions import ObjectNotFoundException
from backend.serializers.serializers import SimpleSerializer, FullSerializer
from backend.decorators import required_roles


class VMachineViewSet(viewsets.ViewSet):
    """
    Information about machines
    """
    permission_classes = (IsAuthenticated,)

    @required_roles(['view'])
    def list(self, request, format=None):
        """
        Overview of all machines
        """
        vmachines = VMachineList.get_vmachines().reduced
        serializer = SimpleSerializer(vmachines, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @required_roles(['view'])
    def retrieve(self, request, pk=None, format=None):
        """
        Load information about a given task
        """
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            vmachine = VMachine(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        return Response(FullSerializer(VMachine, instance=vmachine).data, status=status.HTTP_200_OK)

    @required_roles(['view', 'delete'])
    def destroy(self, request, pk=None):
        """
        Deletes a machine
        """
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            vmachine = VMachine(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        task = VMachineController.delete.s(machineguid=vmachine.guid).apply_async()
        return Response(task.id, status=status.HTTP_200_OK)

    @action()
    @required_roles(['view', 'create'])
    def clone(self, request, pk=None, format=None):
        """
        Clones a machine
        """
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            vmachine = VMachine(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        # POC, assuming data is correct
        task = VMachineController.clone.s(parentmachineguid=pk,
                                          disks=request.DATA['disks'],
                                          name=request.DATA['name']).apply_async()
        return Response(task.id, status=status.HTTP_200_OK)
