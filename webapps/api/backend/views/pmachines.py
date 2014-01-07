# license see http://www.openvstorage.com/licenses/opensource/
"""
PMachine module
"""
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from ovs.dal.lists.pmachinelist import PMachineList
from ovs.dal.hybrids.pmachine import PMachine
from backend.serializers.serializers import SimpleSerializer, FullSerializer
from backend.decorators import required_roles, expose, validate


class PMachineViewSet(viewsets.ViewSet):
    """
    Information about pMachines
    """
    permission_classes = (IsAuthenticated,)

    @expose(internal=True)
    @required_roles(['view'])
    def list(self, request, format=None):
        """
        Overview of all vPools
        """
        _ = request, format
        pmachines = PMachineList.get_pmachines().reduced
        serializer = SimpleSerializer(pmachines, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @expose(internal=True)
    @required_roles(['view'])
    @validate(PMachine)
    def retrieve(self, request, obj):
        """
        Load information about a given pMachine
        """
        _ = request
        return Response(FullSerializer(PMachine, instance=obj).data, status=status.HTTP_200_OK)
