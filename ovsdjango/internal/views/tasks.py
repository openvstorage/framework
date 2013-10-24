from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import link
from celery.task.control import inspect
from ovs.celery import celery


class TaskViewSet(viewsets.ViewSet):
    """
    Information about celery tasks
    """
    permission_classes = (IsAuthenticated,)

    def list(self, request, format=None):
        """
        Overview of active, scheduled, reserved and revoked tasks
        """
        inspector = inspect()
        data = {'active'   : inspector.active(),
                'scheduled': inspector.scheduled(),
                'reserved' : inspector.reserved(),
                'revoked'  : inspector.revoked()}
        return Response(data, status=status.HTTP_200_OK)

    def retrieve(self, request, pk=None, format=None):
        """
        Load information about a given task
        """
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        result = celery.AsyncResult(pk)
        data = {'id'        : result.id,
                'status'    : result.status,
                'successful': result.successful(),
                'failed'    : result.failed(),
                'ready'     : result.ready(),
                'result'    : result.result if result.successful() else result.result.message}
        return Response(data, status=status.HTTP_200_OK)

    @link()
    def get(self, request, pk=None, format=None):
        """
        Gets a given task's result
        """
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        result = celery.AsyncResult(pk)
        return Response(result.get(), status=status.HTTP_200_OK)