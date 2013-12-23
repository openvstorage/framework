# license see http://www.openvstorage.com/licenses/opensource/
"""
Generic volatile factory.
"""
import ConfigParser
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
            parser = ConfigParser.RawConfigParser()
            if client_type is None:
                client_type = Configuration.get('ovs.core.storage.volatile')

            VolatileFactory.store = None
            if client_type == 'memcache':
                from ovs.extensions.storage.volatile.memcachestore import MemcacheStore
                memcache_servers = list()
                parser.read(os.path.join(Configuration.get('ovs.core.cfgdir'), 'memcacheclient.cfg'))
                nodes = parser.get('main', 'nodes').split(',')
                for node in nodes:
                    location = parser.get(node, 'location')
                    memcache_servers.append(location)
                VolatileFactory.store = MemcacheStore(memcache_servers)
            if client_type == 'default':
                from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore
                VolatileFactory.store = DummyVolatileStore()

        if VolatileFactory.store is None:
            raise RuntimeError('Invalid client_type specified')
        return VolatileFactory.store
