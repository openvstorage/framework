# license see http://www.openvstorage.com/licenses/opensource/
"""
Generic persistent factory.
"""
from JumpScale import j


class PersistentFactory(object):
    """
    The PersistentFactory will generate certain default clients.
    """

    @staticmethod
    def get_client(client_type=None):
        """
        Returns a persistent storage client
        """

        if not hasattr(PersistentFactory, 'store') or PersistentFactory.store is None:
            if client_type is None:
                client_type = j.application.config.get('ovs.core.storage.persistent')

            PersistentFactory.store = None
            if client_type == 'arakoon':
                from ovs.extensions.storage.persistent.arakoonstore import ArakoonStore
                cluster = j.application.config.get('ovs.core.db.arakoon.clusterid')
                PersistentFactory.store = ArakoonStore(cluster)
            if client_type == 'default':
                from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
                PersistentFactory.store = DummyPersistentStore()

        if PersistentFactory.store is None:
            raise RuntimeError('Invalid client_type specified')
        return PersistentFactory.store
