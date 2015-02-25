# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
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
from ovs.extensions.hypervisor.factory import Factory
from backend.serializers.serializers import FullSerializer
from rest_framework.exceptions import NotAcceptable
from rest_framework.response import Response
from rest_framework import status
from backend.decorators import required_roles, load, return_object, return_list, log
from ovs.log.logHandler import LogHandler

logger = LogHandler('api', 'mgmtcenters')


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
        mgmtcenter.delete(abandon=True)
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
                try:
                    mgmt_center_client = Factory.get_mgmtcenter(mgmt_center=mgmt_center)
                    is_mgmt_center = mgmt_center_client.test_connection()
                except Exception as ex:
                    logger.debug('Management center testing: {0}'.format(ex))
                    raise NotAcceptable('The given information is invalid.')
                if not is_mgmt_center:
                    raise NotAcceptable('The given information is not for a Management center.')
                mgmt_center.save()
                return Response(serializer.data, status=status.HTTP_201_CREATED)
            else:
                raise NotAcceptable('A Mangement Center with this ip already exists.')
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
