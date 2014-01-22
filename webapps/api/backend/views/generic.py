# license see http://www.openvstorage.com/licenses/opensource/
"""
Module for generic functionality
"""
from backend.decorators import expose
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from ovs.dal.lists.vmachinelist import VMachineList
from django.http import Http404


class GenericViewSet(viewsets.ViewSet):
    """
    Generic
    """

    @expose(internal=True)
    def list(self, request, format=None):
        """
        Dummy implementation
        """
        _ = request, format
        return Response([{'guid': '0'}])

    @expose(internal=True)
    def retrieve(self, request, pk=None, format=None):
        """
        Retrieve generic information
        """
        _ = format, request
        if pk != '0':
            raise Http404
        vsa_ips = []
        for vsa in VMachineList.get_vsas():
            vsa_ips.append(vsa.ip)
        data = {'vsa_ips': vsa_ips}
        return Response(data)
