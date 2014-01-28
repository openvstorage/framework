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
Generic volatile factory.
"""
from configobj import ConfigObj
import os
from ovs.plugin.provider.configuration import Configuration


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
            if client_type is None:
                client_type = Configuration.get('ovs.core.storage.volatile')

            VolatileFactory.store = None
            if client_type == 'memcache':
                from ovs.extensions.storage.volatile.memcachestore import MemcacheStore
                memcache_servers = list()
                memcache_config = ConfigObj(os.path.join(Configuration.get('ovs.core.cfgdir'), 'memcacheclient.cfg'))
                nodes = memcache_config.get('main')['nodes'] if type(memcache_config.get('main')['nodes']) == list else [memcache_config.get('main')['nodes'],]
                for node in nodes:
                    location = memcache_config.get(node)['location']
                    memcache_servers.append(location)
                VolatileFactory.store = MemcacheStore(memcache_servers)
            if client_type == 'default':
                from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore
                VolatileFactory.store = DummyVolatileStore()

        if VolatileFactory.store is None:
            raise RuntimeError('Invalid client_type specified')
        return VolatileFactory.store
