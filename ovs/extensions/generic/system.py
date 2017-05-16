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
Generic system module, executing statements on local node
"""

from ovs_extensions.packages.packagefactory import PackageFactory
from ovs_extensions.generic.system import System as _System


class System(_System):
    """
    Generic helper class
    """

    OVS_ID_FILE = '/etc/openvstorage_id'
    RELEASE_NAME_FILE = '/opt/OpenvStorage/config/release_name'
    _machine_id = {}

    def __init__(self):
        """
        Dummy init method
        """
        raise RuntimeError('System is a static class')

    @staticmethod
    def get_release_name(client=None):
        try:
            if client is not None:
                return client.run(['cat', System.RELEASE_NAME_FILE]).strip()
            with open(System.RELEASE_NAME_FILE, 'r') as the_file:
                return the_file.read().strip()
        except:
            manager = PackageFactory.get_manager()
            return manager.get_release_name()

    @staticmethod
    def get_my_storagerouter():
        """
        Returns unique machine storagerouter id
        :return: Storage Router this is executed on
        :rtype: StorageRouter
        """
        from ovs.dal.lists.storagerouterlist import StorageRouterList
        storagerouter = StorageRouterList.get_by_machine_id(System.get_my_machine_id())
        if storagerouter is None:
            raise RuntimeError('Could not find the local StorageRouter')
        return storagerouter
