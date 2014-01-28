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
PMachineList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.pmachine import PMachine


class PMachineList(object):
    """
    This PMachineList class contains various lists regarding to the PMachine class
    """

    @staticmethod
    def get_pmachines():
        """
        Returns a list of all PMachines
        """
        pmachines = DataList({'object': PMachine,
                              'data': DataList.select.DESCRIPTOR,
                              'query': {'type': DataList.where_operator.AND,
                                        'items': []}}).data
        return DataObjectList(pmachines, PMachine)

    @staticmethod
    def get_by_ip(ip):
        """
        Gets a pmachine based on a given ip address
        """
        pmachines = DataList({'object': PMachine,
                              'data': DataList.select.DESCRIPTOR,
                              'query': {'type': DataList.where_operator.AND,
                                        'items': [('ip', DataList.operator.EQUALS, ip)]}}).data
        if pmachines:
            return DataObjectList(pmachines, PMachine)[0]
        return None
