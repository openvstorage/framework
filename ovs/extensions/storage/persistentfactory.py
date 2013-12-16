# license see http://www.openvstorage.com/licenses/opensource/
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
                parser.read('/opt/OpenvStorage/config/storage.cfg')
                client_type = parser.get('main', 'persistent')

            PersistentFactory.store = None
            if client_type == 'arakoon':
                from ovs.extensions.storage.persistent.arakoonstore import ArakoonStore
                parser.read('/opt/OpenvStorage/config/arakoon/ovsdb/ovsdb_client.cfg')
                cluster = parser.get('global', 'cluster_id')
                clusterNodes = parser.get('global', 'cluster')
                node_dict = {}
                for node in clusterNodes.split(",") :
                    node = node.strip()
                    ip  = parser.get(node, "ip")
                    port = parser.get(node, "client_port")
                    ip_port = ([ip,], port)
                    node_dict.update({node: ip_port})
                PersistentFactory.store = ArakoonStore(cluster, node_dict)
            if client_type == 'default':
                from ovs.extensions.storage.persistent.dummystore import DummyPersistentStore
                PersistentFactory.store = DummyPersistentStore()

        if PersistentFactory.store is None:
            raise RuntimeError('Invalid client_type specified')
        return PersistentFactory.store
