import time
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework.permissions import IsAuthenticated
from ovs.lib.vdisk import VDiskController


class TestViewSet(viewsets.ViewSet):
    """
    A test viewset that we can use for POC stuff
    """
    permission_classes = (IsAuthenticated,)

    def list(self, request, format=None):
        syncStart = time.time()
        data = VDiskController().listVolumes()
        syncElapsed = (time.time() - syncStart)
        asyncStart = time.time()
        reference = VDiskController().listVolumes.apply_async()
        data = reference.wait()
        asyncElapsed = (time.time() - asyncStart)
        return Response({'Elapsed Sync/Async': [syncElapsed, asyncElapsed],
                         'data': data,
                         'detail-url': reverse('tasks-detail', kwargs={'pk': reference.id}, request=request)}, status=status.HTTP_200_OK)

