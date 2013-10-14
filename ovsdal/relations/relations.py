from ovsdal.storedobject import StoredObject
from ovsdal.helpers import HybridRunner, Descriptor


class RelationMapper(StoredObject):
    @staticmethod
    def load_foreign_relations(object_type):
        relation_key = 'ovs_relations_%s' % object_type.__name__.lower()
        relation_info = StoredObject.volatile.get(relation_key)
        if relation_info is None:
            relation_info = {}
            for cls in HybridRunner.get_hybrids():
                for key, item in cls._relations.iteritems():
                    if item[0].__name__ == object_type.__name__:
                        relation_info[item[1]] = {'class': Descriptor(cls).descriptor,
                                                  'key'  : key}
            StoredObject.volatile.set(relation_key, relation_info)
        return relation_info
