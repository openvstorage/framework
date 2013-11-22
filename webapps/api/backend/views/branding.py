# license see http://www.openvstorage.com/licenses/opensource/
"""
Contains the BrandingViewSet
"""
from rest_framework import status, viewsets
from rest_framework.response import Response
from ovs.dal.lists.brandinglist import BrandingList
from ovs.dal.hybrids.branding import Branding
from ovs.dal.exceptions import ObjectNotFoundException
from backend.serializers.serializers import FullSerializer
from backend.decorators import internal


class BrandingViewSet(viewsets.ViewSet):
    """
    Information about branding
    """

    @internal()
    def list(self, request, format=None):
        """
        Overview of all brandings
        """
        _ = request, format
        brands = BrandingList.get_brandings()
        serializer = FullSerializer(Branding, instance=brands, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @internal()
    def retrieve(self, request, pk=None, format=None):
        """
        Load information about a given task
        """
        _ = request, format
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        try:
            branding = Branding(pk)
        except ObjectNotFoundException:
            return Response(status=status.HTTP_404_NOT_FOUND)
        return Response(FullSerializer(Branding, instance=branding).data, status=status.HTTP_200_OK)
