# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

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

    @staticmethod
    def get_primary_storagerouters_for_domain(domain):
        """
        Retrieve a Storage Router pool of Storage Routers using the specified Domain as primary Domain
        :param domain: Domain to filter on
        :type domain: Domain

        :return: List of Storage Routers
        :rtype: list
        """
        return [junction.storagerouter for junction in domain.storagerouters if junction.backup is False]

    @staticmethod
    def get_secondary_storagerouters_for_domain(domain):
        """
        Retrieve a Storage Router pool of Storage Routers using the specified Domain as secondary Domain
        :param domain: Domain to filter on
        :type domain: Domain

        :return: List of Storage Routers
        :rtype: list
        """
        return [junction.storagerouter for junction in domain.storagerouters if junction.backup is True]
