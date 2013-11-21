"""
Module for working with celery tasks
"""
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import link
from backend.decorators import required_roles
from celery.task.control import inspect
from ovs.celery import celery


class TaskViewSet(viewsets.ViewSet):
    """
    Information about celery tasks
    """
    permission_classes = (IsAuthenticated,)

    @required_roles(['view'])
    def list(self, request, format=None):
        """
        Overview of active, scheduled, reserved and revoked tasks
        """
        _ = request, format
        inspector = inspect()
        data = {'active'   : inspector.active(),
                'scheduled': inspector.scheduled(),
                'reserved' : inspector.reserved(),
                'revoked'  : inspector.revoked()}
        return Response(data, status=status.HTTP_200_OK)

    @required_roles(['view'])
    def retrieve(self, request, pk=None, format=None):
        """
        Load information about a given task
        """
        _ = request, format
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        result = celery.AsyncResult(pk)
        if result.successful():
            result_data = result.result
        else:
            result_data = str(result.result) if result.result is not None else None
        data = {'id'        : result.id,
                'status'    : result.status,
                'successful': result.successful(),
                'failed'    : result.failed(),
                'ready'     : result.ready(),
                'result'    : result_data}
        return Response(data, status=status.HTTP_200_OK)

    @link()
    @required_roles(['view'])
    def get(self, request, pk=None, format=None):
        """
        Gets a given task's result
        """
        _ = request, format
        if pk is None:
            return Response(status=status.HTTP_404_NOT_FOUND)
        result = celery.AsyncResult(pk)
        return Response(result.get(), status=status.HTTP_200_OK)
