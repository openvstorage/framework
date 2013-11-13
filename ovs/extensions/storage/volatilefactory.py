"""
Generic volatile factory.
"""
import ConfigParser


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
                parser.read('/opt/openvStorage/config/storage.cfg')
                client_type = parser.get('main', 'volatile')

            VolatileFactory.store = None
            if client_type == 'memcache':
                from ovs.extensions.storage.volatile.memcachestore import MemcacheStore
                parser.read('/opt/openvStorage/config/memcache.cfg')
                node = parser.get('main', 'local_node')
                location = parser.get(node, 'location')
                VolatileFactory.store = MemcacheStore([location])
            if client_type == 'default':
                from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore
                VolatileFactory.store = DummyVolatileStore()

        if VolatileFactory.store is None:
            raise RuntimeError('Invalid client_type specified')
        return VolatileFactory.store