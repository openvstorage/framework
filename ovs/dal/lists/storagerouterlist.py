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
StorageRouterList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.storagerouter import StorageRouter


class StorageRouterList(object):
    """
    This StorageRouterList class contains various lists regarding to the StorageRouter class
    """

    @staticmethod
    def get_storagerouters():
        """
        Returns a list of all StorageRouters
        """
        storagerouters = DataList({'object': StorageRouter,
                                   'data': DataList.select.GUIDS,
                                   'query': {'type': DataList.where_operator.AND,
                                             'items': []}}).data
        return DataObjectList(storagerouters, StorageRouter)

    @staticmethod
    def get_masters():
        """
        Get all MASTER StorageRouters
        """
        storagerouters = DataList({'object': StorageRouter,
                                   'data': DataList.select.GUIDS,
                                   'query': {'type': DataList.where_operator.AND,
                                             'items': [('node_type', DataList.operator.EQUALS, 'MASTER')]}}).data
        return DataObjectList(storagerouters, StorageRouter)

    @staticmethod
    def get_by_machine_id(machine_id):
        """
        Returns a StorageRouter by its machine_id
        """
        storagerouters = DataList({'object': StorageRouter,
                                   'data': DataList.select.GUIDS,
                                   'query': {'type': DataList.where_operator.AND,
                                             'items': [('machine_id', DataList.operator.EQUALS, machine_id)]}}).data
        srs = DataObjectList(storagerouters, StorageRouter)
        if len(srs) == 0:
            return None
        if len(srs) == 1:
            return srs[0]
        raise RuntimeError('There should be only one StorageRouter with machine_id: {0}'.format(machine_id))

    @staticmethod
    def get_by_ip(ip):
        """
        Returns a StorageRouter by its ip
        """
        storagerouters = DataList({'object': StorageRouter,
                                   'data': DataList.select.GUIDS,
                                   'query': {'type': DataList.where_operator.AND,
                                             'items': [('ip', DataList.operator.EQUALS, ip)]}}).data
        srs = DataObjectList(storagerouters, StorageRouter)
        if len(srs) == 0:
            return None
        if len(srs) == 1:
            return srs[0]
        raise RuntimeError('There should be only one StorageRouter with ip: {0}'.format(ip))
