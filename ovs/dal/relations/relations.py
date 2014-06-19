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
RelationMapper module
"""
from ovs.dal.helpers import HybridRunner, Descriptor, Toolbox
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
            Toolbox.log_cache_hit('relations', False)
            relation_info = {}
            for cls in HybridRunner.get_hybrids():
                for key, item in cls._relations.iteritems():
                    if item[0] is None:
                        itemname = cls.__name__
                    else:
                        itemname = item[0].__name__
                    if itemname == object_type.__name__:
                        relation_info[item[1]] = {'class': Descriptor(cls).descriptor,
                                                  'key': key,
                                                  'list': item[2] if len(item) == 3 else True}
            volatile.set(relation_key, relation_info)
        else:
            Toolbox.log_cache_hit('relations', True)
        return relation_info
