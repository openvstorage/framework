# license see http://www.openvstorage.com/licenses/opensource/
"""
Django views module for Customer API
"""
from rest_framework.views import APIView
from rest_framework.response import Response


class APIRoot(APIView):
    def get(self, request, format=None):
        _ = request, format
        return Response({})

