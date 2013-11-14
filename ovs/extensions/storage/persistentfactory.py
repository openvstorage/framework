"""
Generic persistent factory.
"""
import ConfigParser


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
            parser = ConfigParser.RawConfigParser()
            if client_type is None:
                parser.read('/opt/openvStorage/config/storage.cfg')
                client_type = parser.get('main', 'persistent')

            PersistentFactory.store = None
            if client_type == 'arakoon':
                from ovs.extensions.storage.persistent.arakoonstore import ArakoonStore
                parser.read('/opt/openvStorage/config/arakoon.cfg')
                cluster = parser.get('main', 'cluster')
                node = parser.get('main', 'local_node')
                name = parser.get(node, 'name')
                ip = parser.get(node, 'ip')
                port = int(parser.get(node, 'port'))
                PersistentFactory.store = ArakoonStore(cluster, {name: ([ip], port)})
            if client_type == 'default':
                from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
                PersistentFactory.store = DummyPersistentStore()

        if PersistentFactory.store is None:
            raise RuntimeError('Invalid client_type specified')
        return PersistentFactory.store
