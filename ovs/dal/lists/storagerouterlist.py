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
StorageRouterList module
"""
from ovs.dal.datalist import DataList
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
        return DataList(StorageRouter, {'type': DataList.where_operator.AND,
                                        'items': []})

    @staticmethod
    def get_slaves():
        """
        Get all SLAVE StorageRouters
        """
        return DataList(StorageRouter, {'type': DataList.where_operator.AND,
                                        'items': [('node_type', DataList.operator.EQUALS, 'EXTRA')]})

    @staticmethod
    def get_masters():
        """
        Get all MASTER StorageRouters
        """
        return DataList(StorageRouter, {'type': DataList.where_operator.AND,
                                        'items': [('node_type', DataList.operator.EQUALS, 'MASTER')]})

    @staticmethod
    def get_by_machine_id(machine_id):
        """
        Returns a StorageRouter by its machine_id
        """
        storagerouters = DataList(StorageRouter, {'type': DataList.where_operator.AND,
                                                  'items': [('machine_id', DataList.operator.EQUALS, machine_id)]})
        if len(storagerouters) == 0:
            return None
        if len(storagerouters) == 1:
            return storagerouters[0]
        raise RuntimeError('There should be only one StorageRouter with machine_id: {0}'.format(machine_id))

    @staticmethod
    def get_by_ip(ip):
        """
        Returns a StorageRouter by its ip
        """
        storagerouters = DataList(StorageRouter, {'type': DataList.where_operator.AND,
                                                  'items': [('ip', DataList.operator.EQUALS, ip)]})
        if len(storagerouters) == 0:
            return None
        if len(storagerouters) == 1:
            return storagerouters[0]
        raise RuntimeError('There should be only one StorageRouter with ip: {0}'.format(ip))

    @staticmethod
    def get_by_name(name):
        """
        Returns a StorageRouter by its name
        """
        storagerouters = DataList(StorageRouter, {'type': DataList.where_operator.AND,
                                                  'items': [('name', DataList.operator.EQUALS, name)]})
        if len(storagerouters) == 0:
            return None
        if len(storagerouters) == 1:
            return storagerouters[0]
        raise RuntimeError('There should be only one StorageRouter with name: {0}'.format(name))
