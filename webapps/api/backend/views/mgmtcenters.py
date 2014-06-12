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
from backend.decorators import required_roles, expose, validate, get_object, get_list
from ovs.log.logHandler import LogHandler

logger = LogHandler('api', 'mgmtcenters')


class MgmtCenterViewSet(viewsets.ViewSet):
    """
    Information about mgmtCenters
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'mgmtcenters'
    base_name = 'mgmtcenters'

    @expose(internal=True)
    @required_roles(['view'])
    @get_list(MgmtCenter)
    def list(self, request, format=None, hints=None):
        """
        Overview of all mgmtCenters
        """
        _ = request, format, hints
        return MgmtCenterList.get_mgmtcenters()

    @expose(internal=True)
    @required_roles(['view'])
    @validate(MgmtCenter)
    @get_object(MgmtCenter)
    def retrieve(self, request, obj):
        """
        Load information about a given mgmtCenter
        """
        _ = request
        return obj

    @expose(internal=True, customer=True)
    @required_roles(['delete'])
    @validate(MgmtCenter)
    def destroy(self, request, obj):
        """
        Deletes a Management center
        """
        _ = request
        obj.delete(abandon=True)
        return Response({}, status=status.HTTP_200_OK)

    @expose(internal=True)
    @required_roles(['view', 'create', 'system'])
    def create(self, request, format=None):
        """
        Creates a Management Center
        """
        _ = format
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
