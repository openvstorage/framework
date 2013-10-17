from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from ovs.lib.vmachine import vMachine as LibVMachine
from ovs.dal.hybrids.vmachine import vMachine as HybridVMachine
from ovs.dal.exceptions import ObjectNotFoundException
from backend.serializers.vmachine import VMachineSerializer
from backend.serializers.vmachine import SimpleVMachineSerializer


class VMachineViewSet(viewsets.ViewSet):
    """
    Information about machines
    """
    permission_classes = (IsAuthenticated,)

    def list(self, request, format=None):
        """
        Overview of all machines
        """
        users = LibVMachine.get_vmachines()
        serializer = SimpleVMachineSerializer(users, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def retrieve(self, request, pk=None, format=None):
        """
        Load information about a given task
        """
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            vmachine = HybridVMachine(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        return Response(VMachineSerializer(vmachine).data, status=status.HTTP_200_OK)