# license see http://www.openvstorage.com/licenses/opensource/
"""
Generic volatile factory.
"""
from JumpScale import j


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
                client_type = j.application.config.get('ovs.core.storage.volatile')

            VolatileFactory.store = None
            if client_type == 'memcache':
                from ovs.extensions.storage.volatile.memcachestore import MemcacheStore
                location = '{}:{}'.format(j.application.config.get('ovs.core.memcache.localnode.ip'),
                                          j.application.config.get('ovs.core.memcache.localnode.port'))
                VolatileFactory.store = MemcacheStore([location])
            if client_type == 'default':
                from ovs.extensions.storage.volatile.dummystore import DummyVolatileStore
                VolatileFactory.store = DummyVolatileStore()

        if VolatileFactory.store is None:
            raise RuntimeError('Invalid client_type specified')
        return VolatileFactory.store
