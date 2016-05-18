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
PMachineList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.lists.storagedriverlist import StorageDriverList


class PMachineList(object):
    """
    This PMachineList class contains various lists regarding to the PMachine class
    """

    @staticmethod
    def get_pmachines():
        """
        Returns a list of all PMachines
        """
        return DataList(PMachine, {'type': DataList.where_operator.AND,
                                   'items': []})

    @staticmethod
    def get_by_ip(ip):
        """
        Gets a pmachine based on a given ip address
        """
        pmachines = DataList(PMachine, {'type': DataList.where_operator.AND,
                                        'items': [('ip', DataList.operator.EQUALS, ip)]})
        if len(pmachines) > 0:
            return pmachines[0]
        return None

    @staticmethod
    def get_by_storagedriver_id(storagedriver_id):
        """
        Get pMachine that hosts a given storagedriver_id
        """
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        if storagedriver is None:
            raise RuntimeError('StorageDriver {0} could not be found'.format(storagedriver_id))
        storagerouter = storagedriver.storagerouter
        if storagerouter is None:
            raise RuntimeError('StorageDriver {0} not linked to a StorageRouter'.format(storagedriver.name))
        pmachine = storagerouter.pmachine
        if pmachine is None:
            raise RuntimeError('StorageRouter {0} not linked to a pMachine'.format(storagerouter.name))
        return pmachine
