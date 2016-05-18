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
Generic volatile factory.
"""
import unittest
from ovs.extensions.db.etcd.configuration import EtcdConfiguration


class VolatileFactory(object):
    """
    The VolatileFactory will generate certain default clients.
    """

    @staticmethod
    def get_client(client_type=None):
        """
        Returns a volatile storage client
        """
        if not hasattr(VolatileFactory, 'store') or VolatileFactory.store is None:
            if hasattr(unittest, 'running_tests') and getattr(unittest, 'running_tests'):
                client_type = 'dummy'
            if client_type is None:
                client_type = EtcdConfiguration.get('/ovs/framework/stores|volatile')

            VolatileFactory.store = None
            if client_type == 'memcache':
                from ovs.extensions.storage.volatile.memcachestore import MemcacheStore
                nodes = EtcdConfiguration.get('/ovs/framework/memcache|endpoints')
                VolatileFactory.store = MemcacheStore(nodes)
            if client_type == 'dummy':
                from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore
                VolatileFactory.store = DummyVolatileStore()

        if VolatileFactory.store is None:
            raise RuntimeError('Invalid client_type specified')
        return VolatileFactory.store
