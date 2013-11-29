# license see http://www.openvstorage.com/licenses/opensource/
"""
RelationMapper module
"""
from ovs.dal.helpers import HybridRunner, Descriptor
from ovs.extensions.storage.volatilefactory import VolatileFactory


class RelationMapper(object):
    """
    The RelationMapper is responsible for loading the relational structure
    of the hybrid objects.
    """

    @staticmethod
    def load_foreign_relations(object_type):
        """
        This method will return a mapping of all relations towards a certain hybrid object type.
        The resulting mapping will be stored in volatile storage so it can be fetched faster
        """
        relation_key = 'ovs_relations_%s' % object_type.__name__.lower()
        volatile = VolatileFactory.get_client()
        relation_info = volatile.get(relation_key)
        if relation_info is None:
            relation_info = {}
            for cls in HybridRunner.get_hybrids():
                for key, item in cls._relations.iteritems():
                    if item[0].__name__ == object_type.__name__:
                        relation_info[item[1]] = {'class': Descriptor(cls).descriptor,
                                                  'key': key}
            volatile.set(relation_key, relation_info)
        return relation_info
