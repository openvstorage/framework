# Copyright 2016 iNuron NV
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
from ovs.dal.hybrids.servicetype import ServiceType


class ServiceTypeList(object):
    """
    This ServiceTypeList class contains various lists regarding to the ServiceType class
    """

    @staticmethod
    def get_by_name(name):
        """
        Returns a single ServiceType for the given name. Returns None if no ServiceType was found
        """
        servicetypes = DataList(ServiceType, {'type': DataList.where_operator.AND,
                                              'items': [('name', DataList.operator.EQUALS, name)]})
        if len(servicetypes) == 1:
            return servicetypes[0]
        return None

    @staticmethod
    def get_servicetypes():
        """
        Returns a list of all ServiceTypes
        """
        return DataList(ServiceType, {'type': DataList.where_operator.AND,
                                      'items': []})
