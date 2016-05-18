# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
Module for working with celery tasks
"""

from backend.decorators import load
from backend.decorators import log
from backend.decorators import required_roles
from celery.task.control import inspect
from ovs.celery_run import celery
from rest_framework import status
from rest_framework import viewsets
from rest_framework.decorators import link
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response


class TaskViewSet(viewsets.ViewSet):
    """
    Information about celery tasks
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'tasks'
    base_name = 'tasks'

    @log()
    @required_roles(['read'])
    @load()
    def list(self):
        """
        Overview of active, scheduled, reserved and revoked tasks
        """
        inspector = inspect()
        data = {'active': inspector.active(),
                'scheduled': inspector.scheduled(),
                'reserved': inspector.reserved(),
                'revoked': inspector.revoked()}
        return Response(data, status=status.HTTP_200_OK)

    @log()
    @required_roles(['read'])
    @load()
    def retrieve(self, pk):
        """
        Load information about a given task
        :param pk: Primary key
        """
        result = celery.AsyncResult(pk)
        result_status = result.status
        result_successful = result.successful()
        if result_successful is True:
            result_data = result.result
        else:
            result_data = str(result.result) if result.result is not None else None
        data = {'id': result.id,
                'status': result_status,
                'successful': result_successful,
                'failed': result.failed(),
                'ready': result.ready(),
                'result': result_data}
        return Response(data, status=status.HTTP_200_OK)

    @link()
    @log()
    @required_roles(['read'])
    @load()
    def get(self, pk):
        """
        Gets a given task's result
        :param pk: Primary key
        """
        result = celery.AsyncResult(pk)
        return Response(result.get(), status=status.HTTP_200_OK)
