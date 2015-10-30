# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
MgmtCenter module
"""

from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from ovs.dal.lists.mgmtcenterlist import MgmtCenterList
from ovs.dal.hybrids.mgmtcenter import MgmtCenter
from backend.serializers.serializers import FullSerializer
from rest_framework.exceptions import NotAcceptable
from rest_framework.response import Response
from rest_framework import status
from backend.decorators import required_roles, load, return_object, return_list, log
from ovs.log.logHandler import LogHandler
from ovs.lib.mgmtcenter import MgmtCenterController

from celery.exceptions import TimeoutError

logger = LogHandler.get('api', 'mgmtcenters')


class MgmtCenterViewSet(viewsets.ViewSet):
    """
    Information about mgmtCenters
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'mgmtcenters'
    base_name = 'mgmtcenters'

    @log()
    @required_roles(['read'])
    @return_list(MgmtCenter)
    @load()
    def list(self):
        """
        Overview of all mgmtCenters
        """
        return MgmtCenterList.get_mgmtcenters()

    @log()
    @required_roles(['read'])
    @return_object(MgmtCenter)
    @load(MgmtCenter)
    def retrieve(self, mgmtcenter):
        """
        Load information about a given mgmtCenter
        """
        return mgmtcenter

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load(MgmtCenter)
    def destroy(self, mgmtcenter):
        """
        Deletes a Management center
        """
        mgmtcenter.delete(abandon=['pmachines'])
        return Response(status=status.HTTP_204_NO_CONTENT)

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load()
    def create(self, request):
        """
        Creates a Management Center
        """
        serializer = FullSerializer(MgmtCenter, instance=MgmtCenter(), data=request.DATA, allow_passwords=True)
        if serializer.is_valid():
            mgmt_center = serializer.object
            duplicate = MgmtCenterList.get_by_ip(mgmt_center.ip)
            if duplicate is None:
                mgmt_center.save()
                try:
                    task_id = MgmtCenterController.test_connection.apply_async(kwargs = {'mgmt_center_guid': mgmt_center.guid}).id
                    task = MgmtCenterController.test_connection.AsyncResult(task_id)
                except:
                    mgmt_center.delete()
                    raise
                try:
                    is_mgmt_center = task.get(timeout = 60,
                                              propagate = True)
                except TimeoutError:
                    mgmt_center.delete()
                    logger.error('Timed out waiting for test_connection')
                    raise NotAcceptable('Timed out waiting for test_connection')
                except Exception as ex:
                    # propagate reraises the exception raised in the task
                    mgmt_center.delete()
                    logger.error('Task exception %s' % ex)
                    raise NotAcceptable('Task exception')
                if is_mgmt_center is True:
                    return Response(serializer.data, status=status.HTTP_201_CREATED)
                elif is_mgmt_center is None:
                    mgmt_center.delete()
                    raise NotAcceptable('The given information is invalid.')
                elif is_mgmt_center is False:
                    mgmt_center.delete()
                    raise NotAcceptable('The given information is not for a Management center.')
                else:
                    mgmt_center.delete()
                    raise NotAcceptable('Unexpected result %s' % is_mgmt_center)
            else:
                raise NotAcceptable('A Management Center with this ip already exists.')
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
