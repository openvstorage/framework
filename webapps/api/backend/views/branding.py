# license see http://www.openvstorage.com/licenses/opensource/
"""
Contains the BrandingViewSet
"""
from rest_framework import status, viewsets
from rest_framework.response import Response
from ovs.dal.lists.brandinglist import BrandingList
from ovs.dal.hybrids.branding import Branding
from backend.serializers.serializers import FullSerializer
from backend.decorators import expose, validate


class BrandingViewSet(viewsets.ViewSet):
    """
    Information about branding
    """

    @expose(internal=True)
    def list(self, request, format=None):
        """
        Overview of all brandings
        """
        _ = request, format
        brands = BrandingList.get_brandings()
        serializer = FullSerializer(Branding, instance=brands, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @expose(internal=True)
    @validate(Branding)
    def retrieve(self, request, obj):
        """
        Load information about a given task
        """
        _ = request
        return Response(FullSerializer(Branding, instance=obj).data, status=status.HTTP_200_OK)
