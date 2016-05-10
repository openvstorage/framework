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
