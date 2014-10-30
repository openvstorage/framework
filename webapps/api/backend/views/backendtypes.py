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
Contains the BackendTypeViewSet
"""

import json
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated
from ovs.dal.lists.backendtypelist import BackendTypeList
from ovs.dal.hybrids.backendtype import BackendType
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from backend.decorators import return_object, return_list, load, required_roles, log


class BackendTypeViewSet(viewsets.ViewSet):
    """
    Information about backend types
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'backendtypes'
    base_name = 'backendtypes'

    @log()
    @required_roles(['read'])
    @return_list(BackendType)
    @load()
    def list(self, query=None):
        """
        Overview of all backend types
        """
        if query is not None:
            query = json.loads(query)
            query_result = DataList({'object': BackendType,
                                     'data': DataList.select.GUIDS,
                                     'query': query}).data
            return DataObjectList(query_result, BackendType)
        return BackendTypeList.get_backend_types()

    @log()
    @required_roles(['read'])
    @return_object(BackendType)
    @load(BackendType)
    def retrieve(self, backendtype):
        """
        Load information about a given backend type
        """
        return backendtype
