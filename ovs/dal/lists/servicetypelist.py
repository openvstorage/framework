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
ServiceTypeList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobject import DataObjectList
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.helpers import Descriptor


class ServiceTypeList(object):
    """
    This ServiceTypeList class contains various lists regarding to the ServiceType class
    """

    @staticmethod
    def get_by_name(name):
        """
        Returns a single ServiceType for the given name. Returns None if no ServiceType was found
        """
        servicetypes = DataList({'object': ServiceType,
                                 'data': DataList.select.GUIDS,
                                 'query': {'type': DataList.where_operator.AND,
                                           'items': [('name', DataList.operator.EQUALS, name)]}}).data
        if len(servicetypes) == 1:
            return Descriptor(ServiceType, servicetypes[0]).get_object(True)
        return None

    @staticmethod
    def get_servicetypes():
        """
        Returns a list of all ServiceTypes
        """
        servicetypes = DataList({'object': ServiceType,
                                 'data': DataList.select.GUIDS,
                                 'query': {'type': DataList.where_operator.AND,
                                           'items': []}}).data
        return DataObjectList(servicetypes, ServiceType)
