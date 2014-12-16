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
DataList module
"""

import hashlib
import json
import copy
from random import randint
from ovs.dal.helpers import Descriptor, Toolbox, HybridRunner
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.generic.volatilemutex import VolatileMutex
from ovs.dal.relations import RelationMapper


class DataList(object):
    """
    The DataList is a class that provide query functionality for the hybrid DAL
    """

    # Test hooks for unit tests
    test_hooks = {}

    class Select(object):
        """
        The Select class provides enum-alike properties for what to select
        """
        GUIDS = 'GUIDS'
        COUNT = 'COUNT'

    class WhereOperator(object):
        """
        The WhereOperator class provides enum-alike properties for the Where-operators
        """
        AND = 'AND'
        OR = 'OR'

    class Operator(object):
        """
        The Operator class provides enum-alike properties for equalitation-operators
        """
        # In case more operators are required, add them here, and implement them in
        # the _evaluate method below
        EQUALS = 'EQUALS'
        NOT_EQUALS = 'NOT_EQUALS'
        LT = 'LT'
        GT = 'GT'
        IN = 'IN'

    select = Select()
    where_operator = WhereOperator()
    operator = Operator()
    namespace = 'ovs_list'
    cachelink = 'ovs_listcache'
    partsize_pks = 5000

    def __init__(self, query, key=None, load=True):
        """
        Initializes a DataList class with a given key (used for optional caching) and a given query
        """
        # Initialize super class
        super(DataList, self).__init__()

        if key is not None:
            self._key = key
        else:
            identifier = copy.deepcopy(query)
            identifier['object'] = identifier['object'].__name__
            self._key = hashlib.sha256(json.dumps(identifier)).hexdigest()
        self._key = '{0}_{1}'.format(DataList.namespace, self._key)
        self._volatile = VolatileFactory.get_client()
        self._persistent = PersistentFactory.get_client()
        self._query = query
        self.data = None
        self.from_cache = False
        self._can_cache = True
        if load is True:
            self._load()

    def _exec_and(self, instance, items):
        """
        Executes a given set of query items against the instance in an "AND" scope
        This means the first False will cause the scope to return False
        """
        for item in items:
            if isinstance(item, dict):
                # Recursive
                if item['type'] == DataList.where_operator.AND:
                    result = self._exec_and(instance, item['items'])
                else:
                    result = self._exec_or(instance, item['items'])
                if result is False:
                    return False
            else:
                if self._evaluate(instance, item) is False:
                    return False
        return True

    def _exec_or(self, instance, items):
        """
        Executes a given set of query items against the instance in an "OR" scope
        This means the first True will cause the scope to return True
        """
        for item in items:
            if isinstance(item, dict):
                # Recursive
                if item['type'] == DataList.where_operator.AND:
                    result = self._exec_and(instance, item['items'])
                else:
                    result = self._exec_or(instance, item['items'])
                if result is True:
                    return True
            else:
                if self._evaluate(instance, item) is True:
                    return True
        return False

    def _evaluate(self, instance, item):
        """
        Evaluates a single query item comparing a given value with a given instance property
        It will keep track of which properties are used, making sure the query result
        will get invalidated when such property is updated
        """
        path = item[0].split('.')
        value = instance
        if value is None:
            return False
        itemcounter = 0
        for pitem in path:
            itemcounter += 1
            if pitem in (dynamic.name for dynamic in value.__class__._dynamics):
                self._can_cache = False
            value = getattr(value, pitem)
            if value is None and itemcounter != len(path):
                return False  # Fail the filter

        # Apply operators
        if item[1] == DataList.operator.NOT_EQUALS:
            return value != item[2]
        if item[1] == DataList.operator.EQUALS:
            return value == item[2]
        if item[1] == DataList.operator.GT:
            return value > item[2]
        if item[1] == DataList.operator.LT:
            return value < item[2]
        if item[1] == DataList.operator.IN:
            return value in item[2]
        raise NotImplementedError('The given operator {} is not yet implemented.'.format(item[1]))

    def _load(self):
        """
        Tries to load the result for the given key from the volatile cache, or executes the query
        if not yet available. Afterwards (if a key is given), the result will be (re)cached
        """
        self.data = self._volatile.get(self._key) if self._key is not None else None
        if self.data is None:
            # The query should be a dictionary:
            #     {'object': Disk,  # Object on which the query should be executed
            #      'data'  : DataList.select.XYZ,  # The requested result
            #      'query' : <query>}  # The actual query
            # Where <query> is a query(group) dictionary:
            #     {'type' : DataList.where_operator.ABC,  # Whether the items should be AND/OR
            #      'items': <items>}  # The items in the group
            # Where the <items> is any combination of one or more <filter> or <query>
            # A <filter> tuple example:
            #     (<field>, DataList.operator.GHI, <value>)  # For example EQUALS
            # The field is any property you would also find on the given object. In case of
            # properties, you can dot as far as you like. This means you can combine AND and OR
            # in any possible combination

            Toolbox.log_cache_hit('datalist', False)
            hybrid_structure = HybridRunner.get_hybrids()

            items = self._query['query']['items']
            query_type = self._query['query']['type']
            query_data = self._query['data']
            query_object = self._query['object']
            query_object_id = Descriptor(query_object).descriptor['identifier']
            if query_object_id in hybrid_structure and query_object_id != hybrid_structure[query_object_id]['identifier']:
                query_object = Descriptor().load(hybrid_structure[query_object_id]).get_object()

            invalidations = {query_object.__name__.lower(): ['__all']}
            DataList._build_invalidations(invalidations, query_object, items)

            for class_name in invalidations:
                key = '{0}_{1}'.format(DataList.cachelink, class_name)
                mutex = VolatileMutex('listcache_{0}'.format(class_name))
                try:
                    mutex.acquire(60)
                    cache_list = Toolbox.try_get(key, {})
                    current_fields = cache_list.get(self._key, [])
                    current_fields = list(set(current_fields + ['__all'] + invalidations[class_name]))
                    cache_list[self._key] = current_fields
                    self._volatile.set(key, cache_list)
                    self._persistent.set(key, cache_list)
                finally:
                    mutex.release()

            self.from_cache = False
            namespace = query_object()._namespace
            name = query_object.__name__.lower()
            guids = DataList.get_pks(namespace, name)

            if query_data == DataList.select.COUNT:
                self.data = 0
            else:
                self.data = []

            for guid in guids:
                try:
                    instance = query_object(guid)
                    if query_type == DataList.where_operator.AND:
                        include = self._exec_and(instance, items)
                    elif query_type == DataList.where_operator.OR:
                        include = self._exec_or(instance, items)
                    else:
                        raise NotImplementedError('The given operator is not yet implemented.')
                    if include:
                        if query_data == DataList.select.COUNT:
                            self.data += 1
                        elif query_data == DataList.select.GUIDS:
                            self.data.append(guid)
                        else:
                            raise NotImplementedError('The given selector type is not implemented')
                except ObjectNotFoundException:
                    pass

            if 'post_query' in DataList.test_hooks:
                DataList.test_hooks['post_query'](self)

            if self._key is not None and len(guids) > 0 and self._can_cache:
                invalidated = False
                for class_name in invalidations:
                    key = '{0}_{1}'.format(DataList.cachelink, class_name)
                    cache_list = Toolbox.try_get(key, {})
                    if self._key not in cache_list:
                        invalidated = True
                # If the key under which the list should be saved was already invalidated since the invalidations
                # were saved, the returned list is most likely outdated. This is OK for this result, but the list
                # won't get cached
                if invalidated is False:
                    self._volatile.set(self._key, self.data, 300 + randint(0, 300))  # Cache between 5 and 10 minutes
        else:
            Toolbox.log_cache_hit('datalist', True)
            self.from_cache = True
        return self

    @staticmethod
    def _build_invalidations(invalidations, object_type, items):
        """
        Builds an invalidation set out of a given object type and query items. It will use type information
        to build the invalidations, and not the actual data.
        """
        def add(class_name, field):
            if class_name not in invalidations:
                invalidations[class_name] = []
            if field not in invalidations[class_name]:
                invalidations[class_name].append(field)

        for item in items:
            if isinstance(item, dict):
                # Recursive
                DataList._build_invalidations(invalidations, object_type, item['items'])
            else:
                path = item[0].split('.')
                value = object_type
                itemcounter = 0
                for pitem in path:
                    itemcounter += 1
                    class_name = value.__name__.lower()
                    if pitem == 'guid':
                        # The guid is a final value which can't be changed so it shouldn't be taken into account
                        break
                    elif pitem in (prop.name for prop in value._properties):
                        # The pitem is in the blueprint, so it's a simple property (e.g. vmachine.name)
                        add(class_name, pitem)
                        break
                    elif pitem in (relation.name for relation in value._relations):
                        # The pitem is in the relations, so it's a relation property (e.g. vdisk.vmachine)
                        add(class_name, pitem)
                        relation = [relation for relation in value._relations if relation.name == pitem][0]
                        if relation.foreign_type is not None:
                            value = relation.foreign_type
                        continue
                    elif pitem.endswith('_guid') and pitem.replace('_guid', '') in (relation.name for relation in value._relations):
                        # The pitem is the guid pointing to a relation, so it can be handled like a simple property (e.g. vdisk.vmachine_guid)
                        add(class_name, pitem.replace('_guid', ''))
                        break
                    elif pitem in (dynamic.name for dynamic in value._dynamics):
                        # The pitem is a dynamic property, which will be ignored anyway
                        break
                    else:
                        # No blueprint and no relation, it might be a foreign relation (e.g. vmachine.vdisks)
                        # this means the pitem most likely contains an index
                        cleaned_pitem = pitem.split('[')[0]
                        relations = RelationMapper.load_foreign_relations(value)
                        if relations is not None:
                            if cleaned_pitem in relations:
                                value = Descriptor().load(relations[cleaned_pitem]['class']).get_object()
                                add(value.__name__.lower(), relations[cleaned_pitem]['key'])
                                continue
                    raise RuntimeError('Invalid path given: {0}, currently pointing to {1}'.format(path, pitem))

    @staticmethod
    def get_relation_set(remote_class, remote_key, own_class, own_key, own_guid):
        """
        This method will get a DataList for a relation.
        On a cache miss, the relation DataList will be rebuild and due to the nature of the full table scan, it will
        update all relations in the mean time.
        """

        # Example:
        # * remote_class = vDisk
        # * remote_key = vmachine
        # * own_class = vMachine
        # * own_key = vdisks
        # Called to load the vMachine.vdisks list (resulting in a possible scan of vDisk objects)
        # * own_guid = this vMachine object's guid

        volatile = VolatileFactory.get_client()
        own_name = own_class.__name__.lower()
        datalist = DataList({}, '{0}_{1}_{2}'.format(own_name, own_guid, remote_key), load=False)
        reverse_key = 'ovs_reverseindex_{0}_{1}'.format(own_name, own_guid)

        # Check whether the requested information is available in cache
        reverse_index = volatile.get(reverse_key)
        if reverse_index is not None and own_key in reverse_index:
            Toolbox.log_cache_hit('datalist', True)
            datalist.data = reverse_index[own_key]
            datalist.from_cache = True
            return datalist

        Toolbox.log_cache_hit('datalist', False)
        mutex = VolatileMutex('reverseindex')
        remote_name = remote_class.__name__.lower()
        blueprint_object = remote_class()  # vDisk object
        foreign_guids = {}

        remote_namespace = blueprint_object._namespace
        remote_keys = DataList.get_pks(remote_namespace, remote_name)
        handled_flows = []
        for guid in remote_keys:
            try:
                instance = remote_class(guid)
                for relation in blueprint_object._relations:  # E.g. vmachine or vpool relation
                    if relation.foreign_type is None:
                        classname = remote_name
                        foreign_namespace = blueprint_object._namespace
                    else:
                        classname = relation.foreign_type.__name__.lower()
                        foreign_namespace = relation.foreign_type()._namespace
                    if classname not in foreign_guids:
                        foreign_guids[classname] = DataList.get_pks(foreign_namespace, classname)
                    flow = '{0}_{1}'.format(classname, relation.foreign_key)
                    if flow not in handled_flows:
                        handled_flows.append(flow)
                        try:
                            mutex.acquire(60)
                            for foreign_guid in foreign_guids[classname]:
                                reverse_key = 'ovs_reverseindex_{0}_{1}'.format(classname, foreign_guid)
                                reverse_index = volatile.get(reverse_key)
                                if reverse_index is None:
                                    reverse_index = {}
                                if relation.foreign_key not in reverse_index:
                                    reverse_index[relation.foreign_key] = []
                                    volatile.set(reverse_key, reverse_index)
                        finally:
                            mutex.release()
                    key = getattr(instance, '{0}_guid'.format(relation.name))
                    if key is not None:
                        try:
                            mutex.acquire(60)
                            reverse_index = volatile.get('ovs_reverseindex_{0}_{1}'.format(classname, key))
                            if reverse_index is None:
                                reverse_index = {}
                            if relation.foreign_key not in reverse_index:
                                reverse_index[relation.foreign_key] = []
                            if guid not in reverse_index[relation.foreign_key]:
                                reverse_index[relation.foreign_key].append(guid)
                                volatile.set('ovs_reverseindex_{0}_{1}'.format(classname, key), reverse_index)
                        finally:
                            mutex.release()
            except ObjectNotFoundException:
                pass

        try:
            mutex.acquire(60)
            reverse_key = 'ovs_reverseindex_{0}_{1}'.format(own_name, own_guid)
            reverse_index = volatile.get(reverse_key)
            if reverse_index is None:
                reverse_index = {}
            if own_key not in reverse_index:
                reverse_index[own_key] = []
                volatile.set(reverse_key, reverse_index)
            datalist.data = reverse_index[own_key]
            datalist.from_cache = False
        finally:
            mutex.release()
        return datalist

    @staticmethod
    def get_pks(namespace, name):
        """
        This method will load the primary keys for a given namespace and name
        """
        persistent = PersistentFactory.get_client()
        prefix = '{0}_{1}_'.format(namespace, name)
        return set([key.replace(prefix, '') for key in persistent.prefix(prefix, max_elements=-1)])
