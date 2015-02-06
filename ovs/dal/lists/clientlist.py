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
ClientList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobject import DataObjectList
from ovs.dal.hybrids.client import Client


class ClientList(object):
    """
    This ClientList class contains various lists regarding to the Client class
    """

    @staticmethod
    def get_clients():
        """
        Returns a list of all Clients, except internal types
        """
        clients = DataList({'object': Client,
                            'data': DataList.select.GUIDS,
                            'query': {'type': DataList.where_operator.AND,
                                      'items': [('ovs_type', DataList.operator.NOT_EQUALS, 'INTERNAL')]}}).data
        return DataObjectList(clients, Client)

    @staticmethod
    def get_by_types(ovs_type, grant_type):
        """
        Returns a list of all internal Clients
        """
        clients = DataList({'object': Client,
                            'data': DataList.select.GUIDS,
                            'query': {'type': DataList.where_operator.AND,
                                      'items': [('ovs_type', DataList.operator.EQUALS, ovs_type),
                                                ('grant_type', DataList.operator.EQUALS, grant_type)]}}).data
        return DataObjectList(clients, Client)
