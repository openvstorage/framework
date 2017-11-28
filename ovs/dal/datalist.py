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
DataList module
"""

import json
import copy
import random
import hashlib
from random import randint
from ovs.dal.helpers import Descriptor, HybridRunner
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.dal.relations import RelationMapper


# noinspection PyProtectedMember
class DataList(object):
    """
    The DataList is a class that provide query functionality for the hybrid DAL
    """

    # Test hooks for unit tests
    _test_hooks = {}

    class WhereOperator(object):
        """
        The WhereOperator class provides enum-alike properties for the Where-operators
        """
        AND = 'AND'
        OR = 'OR'

    class Operator(object):
        """
        The Operator class provides enum-alike properties for equation-operators
        """
        # In case more operators are required, add them here, and implement them in
        # the _evaluate method below
        EQUALS = 'EQUALS'
        CONTAINS = 'CONTAINS'
        NOT_EQUALS = 'NOT_EQUALS'
        LT = 'LT'
        GT = 'GT'
        IN = 'IN'

    where_operator = WhereOperator()
    operator = Operator()
    NAMESPACE = 'ovs_list'
    CACHELINK = 'ovs_listcache'

    def __init__(self, object_type, query=None, key=None, guids=None):
        """
        Initializes a DataList class with a given key (used for optional caching) and a given query
        :param object_type: The type of the objects that have to be queried
        :param query: The query to execute. Example: {'type': DataList.where_operator.AND, 'items': [('storagedriver_id', DataList.operator.EQUALS, storagedriver_id)]}
        When query is None, it will default to a query which will not do any filtering
        :type query: dict or NoneType
        :param key: A key under which the result must be cached
        :type key: str
        :param guids: List of guids to use as a base
        These guids should be guids of objects related to the object_type param. If no object related to the guid could be found, these guids will not be included in the result
        When guids is None, it will default to querying all items
        :type guids: list[basestring] or NoneType
        """
        # Validation
        self._validate_guids(guids)
        self._validate_query(query)

        # Defaults
        if query is None:
            query = {'type': DataList.where_operator.AND, 'items': []}

        super(DataList, self).__init__()

        self._volatile = VolatileFactory.get_client()
        self._persistent = PersistentFactory.get_client()
        self._query = query
        self._can_cache = True
        self._object_type = object_type
        self._data = {}
        self._objects = {}
        self._guids = None
        self._executed = False
        self._shallow_sort = True
        self._provided_guids = guids
        self._provided_keys = None  # Conversion of guids to keys, cached for faster lookup
        self._key = None
        self._provided_key = False  # Keep track whether a key was explicitly set
        self.from_cache = None
        self.from_index = 'none'

        self.set_key(key)

    @property
    def guids(self):
        """
        Gets the resulting guids
        """
        if self._executed is False and self._guids is None:
            self._execute_query()
        return self._guids

    def set_key(self, key=None, reset=False):
        """
        Sets the caching key
        Won't override the key when a key was giving on initializing
        :param key: Key to explicitly use
        :type key: str
        :param reset: Reset the key to a default one for this list
        :type reset: bool
        :return: None
        :rtype: NoneType
        """
        if key is not None:
            self._key = '{0}_{1}'.format(DataList.NAMESPACE, key)
            self._provided_key = True
            # Unsure whether or not the same query would apply
            self._volatile.delete(self._key)
        elif self._provided_key is False or reset is True:
            identifier = copy.deepcopy(self._query)
            identifier['object'] = self._object_type.__name__
            # Order matters so keeping order in cache too
            identifier['guids'] = 'None' if self._provided_guids is None else ','.join(self._provided_guids)
            self._key = '{0}_{1}'.format(DataList.NAMESPACE, hashlib.sha256(json.dumps(identifier)).hexdigest())

    def _reset_list(self):
        """
        Resets everything about the DataList
        :return: None
        :rtype: NoneType
        """
        # Force query to rerun
        self._executed = False
        self._guids = None
        self._data = {}
        self._objects = {}
        # Reset index information
        self.from_index = 'none'
        # Reset caching info
        self._can_cache = True

    def set_query(self, query):
        """
        Sets the query to apply to a different query
        :param query: The query to perform. If this query is different from the previous one, the previously cached result will be replaced
        with results of this query. If the query is identical and the result is cached, it will return the cached result
        If None is supplied, a default query which does not filter anything will be set
        :type query: dict or NoneType
        Definitions:
        * <query>: Should be a dictionary:
                   {'type' : DataList.where_operator.XYZ,
                    'items': <items>}
        * <filter>: A tuple defining a single expression:
                    (<field>, DataList.operator.XYZ, <value> [, <ignore_case>])
                    The field is any property you would also find on the given object. In case of
                    properties, you can dot as far as you like.
        * <items>: A list of one or more <query> or <filter> items. This means the query structure is recursive and
                   complex queries are possible
        :return: None
        :rtype: NoneType
        """
        if query is None:
            query = {'type': DataList.where_operator.AND, 'items': []}
        self._validate_query(query)
        self._query = query
        if self._provided_key is True:
            # Cache has to be reset as it is no longer valid
            self._volatile.delete(self._key)
        else:
            self.set_key()
        self._reset_list()

    def set_guids(self, guids):
        """
        Sets up a list of guids to apply the query too
        :param guids: List of guids to query on or None in case you wish to query all items again
        :type guids: list[basestring] or NoneType
        :return: None
        :rtype: NoneType
        """
        self._validate_guids(guids)
        self._provided_guids = guids
        self._provided_keys = None
        if self._provided_key is True:
            # Cache has to be reset as it is no longer valid
            self._volatile.delete(self._key)
        else:
            self.set_key()
        self._reset_list()

    @staticmethod
    def _validate_query(query):
        """
        Validates if a query is of the format we'd expect
        :param query: Query to perform
        :type query: dict
        :return: None
        :rtype: NoneType
        :raises: ValueError if the query is not valid
        """
        if query is None or (isinstance(query, dict) and all((k in query for k in ("type", "items")))):
            return
        raise ValueError('Query can be None or a dict containing \'type\' and \'items\'')

    @staticmethod
    def _validate_guids(guids):
        """
        Validates if the supplied guids are valid
        :param guids: Guids to check for
        :type guids: list[basestring]
        :return: None
        :rtype: NoneType
        :raises: ValueError if the guids are not valid
        """
        if guids is None or (isinstance(guids, list) and (len(guids) > 0 and all((isinstance(guid, basestring) for guid in guids)))):
            return
        raise ValueError('Specified guids should be a list of guids or None')

    #######################
    # Query functionality #
    #######################

    def _get_keys_from_index(self, indexed_properties, items, where_operator):
        """
        Builds a set of keys that were retrieved from the indexes.
        :param indexed_properties: A list of all indexed properties
        :param items: The query items
        :param where_operator: The WHERE operator
        :return: Set of keys or None
        Returns None when no indexes could be applied, empty set when indexes could be applied but values do not match
        :rtype: set{basestring} or NoneType
        """
        if not self._can_use_indexes(indexed_properties, items, where_operator):
            raise RuntimeError('A request for loading data from indexes is aborted since the query is not index-safe.')

        object_key = 'ovs_data_{0}_{{0}}'.format(self._object_type.__name__.lower())
        base_index_prefix = 'ovs_index_{0}|{{0}}|{{1}}'.format(self._object_type.__name__.lower())
        keys = None
        for item in items[:]:
            if isinstance(item, dict):
                indexed_keys = self._get_keys_from_index(indexed_properties, item['items'], item['type'])
                if indexed_keys is not None:
                    if keys is None:
                        keys = indexed_keys
                    elif where_operator == DataList.where_operator.AND:
                        keys &= indexed_keys  # intersect keys
                    else:
                        keys |= indexed_keys  # Unify keys
                    if self.from_index == 'none':
                        self.from_index = 'full'
                elif self.from_index == 'full':
                    self.from_index = 'partial'
            else:
                # Item consists of: ( <field>, <operator>, <value>, <ignore_case>(optional) )
                if item[0] in indexed_properties:
                    if item[1] == DataList.operator.EQUALS:
                        if item[0] == 'guid':
                            indexed_keys = {object_key.format(item[2])}
                        else:
                            index_key = base_index_prefix.format(item[0], hashlib.sha1(str(item[2])).hexdigest())
                            # [item for sublist in mainlist for item in sublist] - shitty nested list comprehensions
                            indexed_keys = set(str(key)
                                               for keys_set in self._persistent.get_multi([index_key], must_exist=False)
                                               if keys_set is not None
                                               for key in keys_set)
                        if keys is None:
                            keys = indexed_keys
                        elif where_operator == DataList.where_operator.AND:
                            keys &= indexed_keys
                        else:
                            keys |= indexed_keys
                        if self.from_index == 'none':
                            self.from_index = 'full'
                        items.remove(item)
                    elif item[1] == DataList.operator.IN and isinstance(item[2], list):
                        if item[0] == 'guid':
                            indexed_keys = set(object_key.format(sub_item) for sub_item in item[2])
                        else:
                            index_keys = [base_index_prefix.format(item[0], hashlib.sha1(str(sub_item)).hexdigest())
                                          for sub_item in item[2]]
                            # [item for sublist in mainlist for item in sublist] - shitty nested list comprehensions
                            indexed_keys = set(str(key)
                                               for keys_set in self._persistent.get_multi(index_keys, must_exist=False)
                                               if keys_set is not None
                                               for key in keys_set)
                        if keys is None:
                            keys = indexed_keys
                        elif where_operator == DataList.where_operator.AND:
                            keys &= indexed_keys
                        else:
                            keys |= indexed_keys
                        if self.from_index == 'none':
                            self.from_index = 'full'
                        items.remove(item)
                    elif self.from_index == 'full':
                        self.from_index = 'partial'
                elif self.from_index == 'full':
                    self.from_index = 'partial'
        return keys

    def _can_use_indexes(self, indexed_properties, query_items, where_operator):
        """
        Validates the given query to decide whether it's possible to use indexes.
        Indexes are possible UNLESS there is a query to a non-indexed property inside an OR block
        :param indexed_properties: The names of all indexed properties
        :param query_items: The query items
        :param where_operator: The WHERE operator
        :return: Whether or not it's possible to use indexes
        """
        if where_operator not in [DataList.where_operator.AND, DataList.where_operator.OR]:
            raise NotImplementedError('Invalid where operator specified')

        for item in query_items:
            if isinstance(item, dict):
                possible = self._can_use_indexes(indexed_properties, item['items'], item['type'])
                if possible is False:
                    return False
            elif item[0] not in indexed_properties and where_operator == DataList.where_operator.OR:
                return False
        return True

    def _data_generator(self, prefix, query_items, query_type):
        """
        Generator that yields key-value pairs for the given prefix. If indexes are available an can be
        used, it yields only the relevant data that is referred to by the indexes
        :param prefix: The prefix to be returned, if not using indexes
        :param query_items: The query items
        :param query_type: The WHERE operator
        :return: A generator that yields key-value pairs for the data to be filtered
        """
        if self._provided_guids is not None:
            if self._provided_keys is None:
                # Build and cache the keys
                self._provided_keys = ['{0}{1}'.format(prefix, guid) for guid in self._provided_guids]

        indexed_properties = [prop.name for prop in self._object_type._properties if prop.indexed is True] + ['guid']
        use_indexes = self._can_use_indexes(indexed_properties, query_items, query_type)
        if use_indexes is True:
            keys = self._get_keys_from_index(indexed_properties, query_items, query_type)
            if keys is not None:
                if self.from_index == 'none':
                    self.from_index = 'full'

                if self._provided_guids is not None:
                    # Keys is a set which can contain more keys for objects than requested, thus intersect to query for the requested objects
                    # Set lookups ~=O(1) are faster than list lookups O(n)
                    # Provided keys is a list to maintain order
                    # This is a bit slower than set intersection (O(min(len(s), len(t))) to maintain order (now O(n))
                    keys = [x for x in self._provided_keys if x in keys]

                keys = list(keys)

                if 'data_generator' in DataList._test_hooks:
                    DataList._test_hooks['data_generator'](self)

                for index, value in enumerate(self._persistent.get_multi(keys, must_exist=False)):
                    if value is not None:
                        yield keys[index], value
            else:
                use_indexes = False
        if use_indexes is False:
            if self._provided_guids is not None:
                entries = list(self._persistent.get_multi(self._provided_keys, must_exist=False))
                for index, key in enumerate(self._provided_keys):
                    # Discard keys for which no data could be found
                    if entries[index] is None:
                        continue
                    yield key, entries[index]
            else:
                for item in self._persistent.prefix_entries(prefix):
                    # Item is a list with [key, value] so casting to tuple to yield the same as with indexes
                    yield tuple(item)

    def _filter(self, instance, items, where_operator):
        """
        Executes a given set of query items against the instance in an "AND" scope
        This means the first False will cause the scope to return False
        :param instance: An instance of this lists object_type, or a dict with 'guid' and 'data'
        :param items: The query items
        :param where_operator: The WHERE operator
        """
        if where_operator not in [DataList.where_operator.AND, DataList.where_operator.OR]:
            raise NotImplementedError('Invalid where operator specified')
        if len(items) == 0:
            return True, instance
        return_value = where_operator == DataList.where_operator.OR
        for item in items:
            if isinstance(item, dict):
                result, instance = self._filter(instance, item['items'], item['type'])
                if result == return_value:
                    return return_value, instance
            else:
                result, instance = self._evaluate(instance, item)
                if result == return_value:
                    return return_value, instance
        return not return_value, instance

    def _evaluate(self, instance, item):
        """
        Evaluates a single query item comparing a given value with a given instance property
        It will keep track of which properties are used, making sure the query result
        will get invalidated when such property is updated.
        :param instance: An instance of this lists object_type, or a dict with 'guid' and 'data'
        :param item: A single query entry to be evaluated
        """
        # Find value to evaluate
        value = None
        found = False
        if '.' not in item[0] and isinstance(instance, dict):
            pitem = item[0]
            if pitem in (prop.name for prop in self._object_type._properties):
                value = instance['data'][pitem]
                found = True
        if found is False:
            if isinstance(instance, dict):
                instance = self._object_type(instance['guid'])
            path = item[0].split('.')
            value = instance
            itemcounter = 0
            for pitem in path:
                itemcounter += 1
                if pitem in (dynamic.name for dynamic in value.__class__._dynamics):
                    self._can_cache = False
                value = getattr(value, pitem)
                if value is None and itemcounter != len(path):
                    return False, instance  # This would mean a NoneType error

        # Apply operators
        ignorecase = len(item) == 4 and item[3] is False
        if item[1] == DataList.operator.NOT_EQUALS:
            if ignorecase is True:
                return value.lower() != item[2].lower(), instance
            return value != item[2], instance
        if item[1] == DataList.operator.EQUALS:
            if ignorecase is True:
                return value.lower() == item[2].lower(), instance
            return value == item[2], instance
        if item[1] == DataList.operator.GT:
            return value > item[2], instance
        if item[1] == DataList.operator.LT:
            return value < item[2], instance
        if item[1] == DataList.operator.IN:
            if ignorecase is True:
                if isinstance(item[2], list):
                    return value.lower() in [x.lower() for x in item[2]], instance
                else:
                    return value.lower() in item[2].lower(), instance
            return value in item[2], instance
        if item[1] == DataList.operator.CONTAINS:
            if ignorecase is True:
                return item[2].lower() in value.lower(), instance
            return item[2] in value, instance
        raise NotImplementedError('Invalid operator specified')

    def _execute_query(self):
        """
        Tries to load the result for the given key from the volatile cache, or executes the query
        if not yet available. Afterwards (if a key is given), the result will be (re)cached
        Definitions:
        * <query>: Should be a dictionary:
                   {'type' : DataList.where_operator.XYZ,
                    'items': <items>}
        * <filter>: A tuple defining a single expression:
                    (<field>, DataList.operator.XYZ, <value> [, <ignore_case>])
                    The field is any property you would also find on the given object. In case of
                    properties, you can dot as far as you like.
        * <items>: A list of one or more <query> or <filter> items. This means the query structure is recursive and
                   complex queries are possible
        """
        from ovs.dal.dataobject import DataObject
        hybrid_structure = HybridRunner.get_hybrids()
        query_object_id = Descriptor(self._object_type).descriptor['identifier']
        if query_object_id in hybrid_structure and query_object_id != hybrid_structure[query_object_id]['identifier']:
            self._object_type = Descriptor().load(hybrid_structure[query_object_id]).get_object()
        object_type_name = self._object_type.__name__.lower()
        prefix = '{0}_{1}_'.format(DataObject.NAMESPACE, object_type_name)

        if self._guids is not None:
            keys = ['{0}{1}'.format(prefix, guid) for guid in self._guids]
            entries = list(self._persistent.get_multi(keys, must_exist=False))

            self._data = {}
            self._objects = {}
            for index, guid in enumerate(self._guids[:]):
                if entries[index] is None:
                    self._guids.remove(guid)
                else:
                    self._data[guid] = {'data': entries[index],
                                        'guid': guid}
            self._executed = True
            return

        cached_data = self._volatile.get(self._key)
        if cached_data is None:
            self.from_cache = False

            query_type = self._query['type']
            query_items = self._query['items']

            invalidations = {object_type_name: ['__all']}
            DataList._build_invalidations(invalidations, self._object_type, query_items)
            transaction = self._persistent.begin_transaction()
            for class_name, fields in invalidations.iteritems():
                key = '{0}_{1}|{{0}}|{{1}}'.format(DataList.CACHELINK, class_name)
                for field in fields:
                    self._persistent.set(key.format(self._key, field), 0, transaction=transaction)
            self._persistent.apply_transaction(transaction)

            self._guids = []
            self._data = {}
            self._objects = {}
            elements = 0
            for key, data in self._data_generator(prefix, query_items, query_type):
                elements += 1
                try:
                    guid = key.replace(prefix, '')
                    result, instance = self._filter({'data': data, 'guid': guid}, query_items, query_type)
                    if result is True:
                        self._guids.append(guid)
                        self._data[guid] = {'data': data, 'guid': guid}
                        if not isinstance(instance, dict):
                            self._objects[guid] = instance
                except ObjectNotFoundException:
                    pass

            if 'post_query' in DataList._test_hooks:
                DataList._test_hooks['post_query'](self)

            if self._key is not None and elements > 0 and self._can_cache:
                self._volatile.set(self._key, self._guids, 300 + randint(0, 300))  # Cache between 5 and 10 minutes
                # Check whether the cache was invalidated and should be removed again
                for class_name in invalidations:
                    key = '{0}_{1}|{{0}}|'.format(DataList.CACHELINK, class_name)
                    if len(list(self._persistent.prefix(key.format(self._key)))) == 0:
                        self._volatile.delete(self._key)
                        break
            self._executed = True
        else:
            self.from_cache = True
            self._guids = cached_data

            # noinspection PyTypeChecker
            keys = ['{0}{1}'.format(prefix, guid) for guid in self._guids]
            entries = list(self._persistent.get_multi(keys, must_exist=False))

            self._data = {}
            self._objects = {}
            for index, guid in enumerate(self._guids[:]):
                if entries[index] is None:
                    self._guids.remove(guid)
                else:
                    self._data[guid] = {'data': entries[index],
                                        'guid': guid}
            self._executed = True

    @staticmethod
    def _build_invalidations(invalidations, object_type, items):
        """
        Builds an invalidation set out of a given object type and query items. It will use type information
        to build the invalidations, and not the actual data.
        :param invalidations: A by-ref dict containing all invalidations for this list
        :param object_type: The object type for this invalidations run
        :param items: The query items that need to be used for building invalidations
        :return: None. This invalidations is passed by reference and will be updated by reference
        :rtype: NoneType
        """
        def _add(cname, field):
            if cname not in invalidations:
                invalidations[cname] = []
            if field not in invalidations[cname]:
                invalidations[cname].append(field)

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
                        # The pitem is in the properties, so it's a simple property (e.g. vmachine.name)
                        _add(class_name, pitem)
                        break
                    elif pitem in (relation.name for relation in value._relations):
                        # The pitem is in the relations, so it's a relation property (e.g. vdisk.vmachine)
                        _add(class_name, pitem)
                        relation = [relation for relation in value._relations if relation.name == pitem][0]
                        if relation.foreign_type is not None:
                            value = relation.foreign_type
                        continue
                    elif pitem.endswith('_guid') and pitem.replace('_guid', '') in (relation.name for relation in value._relations):
                        # The pitem is the guid pointing to a relation, so it can be handled like a simple property (e.g. vdisk.vmachine_guid)
                        _add(class_name, pitem.replace('_guid', ''))
                        break
                    elif pitem in (dynamic.name for dynamic in value._dynamics):
                        # The pitem is a dynamic property, which will be ignored anyway
                        break
                    else:
                        # No property and no relation, it might be a foreign relation (e.g. vmachine.vdisks)
                        # this means the pitem most likely contains an index
                        cleaned_pitem = pitem.split('[')[0]
                        relations = RelationMapper.load_foreign_relations(value)
                        if relations is not None:
                            if cleaned_pitem in relations:
                                value = Descriptor().load(relations[cleaned_pitem]['class']).get_object()
                                _add(value.__name__.lower(), relations[cleaned_pitem]['key'])
                                continue
                    raise RuntimeError('Invalid path given: {0}, currently pointing to {1}'.format(path, pitem))

    @staticmethod
    def get_relation_set(remote_class, remote_key, own_class, own_key, own_guid):
        """
        This method will get a DataList for a relation.
        On a cache miss, the relation DataList will be rebuild and due to the nature of the full table scan, it will
        update all relations in the mean time.
        For below parameter information, use following example: We called "my_vmachine.vdisks".
        :param remote_class: The class of the remote part of the relation (e.g. VDisk)
        :param remote_key: The key in the remote_class that points to us (e.g. vmachine)
        :param own_class: The class of the base object of the relation (e.g. VMachine)
        :param own_key: The key in this class pointing to the remote classes (e.g. vdisks)
        :param own_guid: The guid of this object instance (e.g. the guid of my_vmachine)
        """

        # Example:
        # * remote_class = VDisk
        # * remote_key = vmachine
        # * own_class = VMachine
        # * own_key = vdisks
        # Called to load the vMachine.vdisks list (resulting in a possible scan of vDisk objects)
        # * own_guid = this vMachine object's guid

        persistent = PersistentFactory.get_client()
        own_name = own_class.__name__.lower()
        datalist = DataList(remote_class, key='{0}_{1}_{2}'.format(own_name, own_guid, remote_key))

        reverse_key = 'ovs_reverseindex_{0}_{1}|{2}|'.format(own_name, own_guid, own_key)
        datalist._guids = [guid.replace(reverse_key, '') for guid in persistent.prefix(reverse_key)]
        return datalist

    ######################
    # List functionality #
    ######################

    def _get_object(self, requested_guid):
        """
        Yields an instance with a given guid, or a fake class with only a guid property in case
        of a reduced list
        :param requested_guid: The guid of the object to be returned
        """
        if requested_guid in self._objects:
            requested_object = self._objects[requested_guid]
            if requested_object.updated_on_datastore():
                self._objects[requested_guid] = self._object_type(requested_guid)
                return self._objects[requested_guid]
            return requested_object
        elif requested_guid in self._data:
            self._objects[requested_guid] = self._object_type(requested_guid, data=self._data[requested_guid]['data'])
            return self._objects[requested_guid]
        self._objects[requested_guid] = self._object_type(requested_guid)
        return self._objects[requested_guid]

    def update(self, other):
        """
        This method merges in a datalist, preserving objects that might already
        be cached. It also maintains previous sorting, appending new items to the end of the list.
        There result is:
        * Both lists must have guids available
        * Only entries (guids) from the given list
        * Sorting (guids) of this list
        * Cached objects from both lists
        :param other: The list that must be used to update this lists query results
        :type other: ovs.dal.datalist.DataList
        """
        # Validating and ensure that the guids are available
        if not isinstance(other, DataList):
            raise TypeError('Both operands should be of type DataList')
        if Descriptor(self._object_type) != Descriptor(other._object_type):
            raise TypeError('Both operands should contain the same data')
        if self._executed is False and self._guids is None:
            self._guids = []
            self._data = {}
            self._objects = {}
            self._executed = True
        if other._executed is False and other._guids is None:
            other._execute_query()
        # Maintaining order is very important here
        old_guids = self._guids[:]
        new_guids = other._guids
        self._guids = []
        for guid in old_guids:
            if guid in new_guids:
                self._guids.append(guid)
        # noinspection PyTypeChecker
        for guid in new_guids:
            if guid not in self._guids:
                self._guids.append(guid)
        # Cleaning out old cached objects
        for guid in self._data.keys():
            if guid not in self._guids:
                del self._data[guid]
        for guid in self._objects.keys():
            if guid not in self._guids:
                del self._objects[guid]

    def index(self, value):
        """
        Returns the index of a given value (hybrid)
        :param value: Value to search index of (must be a hybrid)
        """
        if self._executed is False and self._guids is None:
            self._execute_query()
        return self._guids.index(value.guid)

    def count(self, value):
        """
        Returns the count for a given value (hybrid)
        :param value: Value to count occurrences for (must be a hybrid)
        """
        if self._executed is False and self._guids is None:
            self._execute_query()
        return self._guids.count(value.guid)

    def sort(self, key=None, reverse=False):
        """
        Sorts the list with a given set of parameters.
        However, the sorting will be applied to the guids only
        """
        if self._executed is False:
            self._execute_query()

        if key is None:
            return self._guids.sort(reverse=reverse)

        def _extract_key(guid):
            if self._shallow_sort is True:
                try:
                    type_dict = {'guid': guid}
                    type_dict.update(self._data[guid]['data'])
                    return key(type(self._object_type.__name__, (), type_dict))
                except AttributeError:
                    self._shallow_sort = False
            return key(self._get_object(guid))

        self._shallow_sort = True
        self._guids.sort(key=_extract_key, reverse=reverse)

    def reverse(self):
        """
        Reverses the list
        """
        if self._executed is False and self._guids is None:
            self._execute_query()
        self._guids.reverse()

    def loadunsafe(self):
        """
        Loads all objects (to use on e.g. sorting)
        """
        if self._executed is False:
            self._execute_query()
        for guid in self._guids:
            if guid not in self._objects:
                self._get_object(guid)

    def loadsafe(self):
        """
        Loads all objects (to use on e.g. sorting), but not caring about objects that doesn't exist
        """
        if self._executed is False:
            self._execute_query()
        for guid in self._guids:
            if guid not in self._objects:
                try:
                    self._get_object(guid)
                except ObjectNotFoundException:
                    pass

    def load(self):
        """
        Loads all objects
        """
        return self.loadsafe()

    def __add__(self, other):
        """
        __add__ operator for DataList
        :param other: A DataList instance that must be added to this instance
        :type other: ovs.dal.datalist.DataList
        """
        if not isinstance(other, DataList):
            raise TypeError('Both operands should be of type DataList')
        if Descriptor(self._object_type) != Descriptor(other._object_type):
            raise TypeError('Both operands should contain the same data')
        if self._executed is False and self._guids is None:
            self._execute_query()
        if other._executed is False and other._guids is None:
            other._execute_query()
        new_datalist = DataList(self._object_type)
        guids = self._guids[:]
        # noinspection PyTypeChecker
        for guid in other._guids:
            if guid not in guids:
                guids.append(guid)
        new_datalist._guids = guids
        return new_datalist

    def __radd__(self, other):
        """
        __radd__ operator for DataList
        :param other: Something that must be added to this instance. None, an empty list or a DataList is supported
        """
        # This will typically called when "other" is no DataList.
        if other is None:
            return self
        if isinstance(other, list) and other == []:
            return self
        return self.__add__(other)

    def iterloaded(self):
        """
        Allows to iterate only over the objects that are already loaded
        preventing unnecessary object loading
        """
        if self._executed is False:
            self._execute_query()
        for guid in self._guids:
            if guid in self._objects:
                yield self._objects[guid]

    def iterunsafe(self):
        """
        Yields object instances
        """
        if self._executed is False:
            self._execute_query()
        for guid in self._guids:
            yield self._get_object(guid)

    def itersafe(self):
        """
        Yields object instances, but not caring about objects that doesn't exist
        """
        if self._executed is False:
            self._execute_query()
        for guid in self._guids:
            try:
                yield self._get_object(guid)
            except ObjectNotFoundException:
                pass

    def __iter__(self):
        """
        Yields object instances
        """
        return self.itersafe()

    def __len__(self):
        """
        Returns the length of the list
        """
        if self._executed is False and self._guids is None:
            self._execute_query()
        # noinspection PyTypeChecker
        return len(self._guids)

    def __getitem__(self, item):
        """
        Provide indexer behavior to the list
        :param item: The index accessor used (can be a slice instance, or a number)
        """
        if self._executed is False:
            self._execute_query()

        if isinstance(item, slice):
            guids = self._guids[item.start:item.stop]
            new_datalist = DataList(self._object_type)
            new_datalist._guids = guids
            new_datalist._executed = True  # Will always be True at this point, since _execute_query is executed if False
            new_datalist._data = dict((key, copy.deepcopy(value)) for key, value in self._data.iteritems() if key in guids)
            new_datalist._objects = dict((key, value.clone()) for key, value in self._objects.iteritems() if key in guids)
            return new_datalist
        else:
            guid = self._guids[item]
            return self._get_object(guid)

    def remove(self, item):
        """
        Remove an item from the DataList
        :param item: Guid or hybrid object (of the correct type)
        """
        if self._executed is False and self._guids is None:
            self._execute_query()

        guid = None
        if isinstance(item, basestring):
            if item in self._guids:
                guid = item
        else:
            if Descriptor(self._object_type) != Descriptor(item.__class__):
                raise TypeError('Item should be of type {0}'.format(self._object_type))
            guid = item.guid
        if guid is None:
            raise ValueError('Item not in list')
        self._guids.remove(guid)
        self._objects = dict(item for item in self._objects.iteritems() if item[0] in self._guids)

    def pop(self, index):
        """
        Pop an item from the DataList at the specified index
        :param index: Index of item to pop
        """
        if self._executed is False and self._guids is None:
            self._execute_query()

        if not isinstance(index, int):
            raise ValueError('Index must be an integer')
        self._guids.pop(index)
        self._objects = dict(item for item in self._objects.iteritems() if item[0] in self._guids)

    def shuffle(self):
        """
        Randomly shuffle the items in the DataList
        """
        if self._executed is False and self._guids is None:
            self._execute_query()
        random.shuffle(self._guids)

    def __repr__(self):
        """
        A short self-representation
        """
        return '<DataList (type: {0}, executed: {1}, at: {2})>'.format(self._object_type.__name__, self._executed, hex(id(self)))
