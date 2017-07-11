# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

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

    cache = {}

    @staticmethod
    def load_foreign_relations(object_type):
        """
        This method will return a mapping of all relations towards a certain hybrid object type.
        The resulting mapping will be stored in volatile storage so it can be fetched faster
        """
        relation_key = 'ovs_relations_{0}'.format(object_type.__name__.lower())
        if relation_key in RelationMapper.cache:
            return RelationMapper.cache[relation_key]
        volatile = VolatileFactory.get_client()
        relation_info = volatile.get(relation_key)
        if relation_info is not None:
            RelationMapper.cache[relation_key] = relation_info
            return relation_info
        relation_info = {}
        hybrid_structure = HybridRunner.get_hybrids()
        for class_descriptor in hybrid_structure.values():  # Extended objects
            cls = Descriptor().load(class_descriptor).get_object()
            # noinspection PyProtectedMember
            for relation in cls._relations:
                if relation.foreign_type is None:
                    remote_class = cls
                else:
                    identifier = Descriptor(relation.foreign_type).descriptor['identifier']
                    if identifier in hybrid_structure and identifier != hybrid_structure[identifier]['identifier']:
                        remote_class = Descriptor().load(hybrid_structure[identifier]).get_object()
                    else:
                        remote_class = relation.foreign_type
                itemname = remote_class.__name__
                if itemname == object_type.__name__:
                    relation_info[relation.foreign_key] = {'class': Descriptor(cls).descriptor,
                                                           'key': relation.name,
                                                           'list': not relation.onetoone}
        RelationMapper.cache[relation_key] = relation_info
        volatile.set(relation_key, relation_info)
        return relation_info
