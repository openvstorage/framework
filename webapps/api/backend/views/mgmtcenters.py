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
from backend.decorators import required_roles, expose, validate, get_object, get_list


class MgmtCenterViewSet(viewsets.ViewSet):
    """
    Information about mgmtCenters
    """
    permission_classes = (IsAuthenticated,)

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
