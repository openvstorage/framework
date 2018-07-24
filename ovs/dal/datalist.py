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

    def __init__(self, object_type, query, key=None):
        """
        Initializes a DataList class with a given key (used for optional caching) and a given query
        :param object_type: The type of the objects that have to be queried
        :param query: The query to execute
        :param key: A key under which the result must be cached
        """
        super(DataList, self).__init__()

        if key is not None:
            self._key = '{0}_{1}'.format(DataList.NAMESPACE, key)
        else:
            identifier = copy.deepcopy(query)
            identifier['object'] = object_type.__name__
            self._key = '{0}_{1}'.format(DataList.NAMESPACE, hashlib.sha256(json.dumps(identifier)).hexdigest())

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

        self.from_cache = None
        self.from_index = 'none'

    @property
    def guids(self):
        """
        Gets the resulting guids
        """
        if self._executed is False and self._guids is None:
            self._execute_query()
        return self._guids

    #######################
    # Query functionality #
    #######################

    def _get_keys_from_index(self, indexed_properties, items, where_operator):
        """
        Builds a generator that only yields data filtered by possible indexes where possible.
        :param indexed_properties: A list of all indexed properties
        :param items: The query items
        :param where_operator: The WHERE operator
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
                        keys &= indexed_keys
                    else:
                        keys |= indexed_keys
                    if self.from_index == 'none':
                        self.from_index = 'full'
                elif self.from_index == 'full':
                    self.from_index = 'partial'
            else:
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
        indexed_properties = [prop.name for prop in self._object_type._properties if prop.indexed is True] + ['guid']
        use_indexes = self._can_use_indexes(indexed_properties, query_items, query_type)
        if use_indexes is True:
            keys = self._get_keys_from_index(indexed_properties, query_items, query_type)
            if keys is not None:
                if self.from_index == 'none':
                    self.from_index = 'full'
                keys = list(keys)

                if 'data_generator' in DataList._test_hooks:
                    DataList._test_hooks['data_generator'](self)

                for index, value in enumerate(self._persistent.get_multi(keys, must_exist=False)):
                    if value is not None:
                        yield keys[index], value
            else:
                use_indexes = False
        if use_indexes is False:
            for item in self._persistent.prefix_entries(prefix):
                yield item

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

            start_references = {object_type_name: ['__all']}
            # Providing the arguments for thread safety. State could change if query would be set in a different thread
            class_references = self._get_referenced_fields(start_references, self._object_type, query_items)
            transaction = self._persistent.begin_transaction()
            for class_name, fields in class_references.iteritems():
                for field in fields:
                    key = self.generate_persistent_cache_key(class_name, self._key, field)
                    self._persistent.set(key, 0, transaction=transaction)
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
                if self.cache_invalidated(class_references):
                    # Pointers were removed. Remove the cached data
                    self.remove_cached_data()
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

    def remove_cached_data(self):
        # type: () -> None
        """
        Removes all cached data
        :return: None
        :rtype: NoneType
        """
        self._volatile.delete(self._key)

    def cache_invalidated(self, references=None):
        """
        Check if the cache was already invalidated
        When a DataObject saves/deleted, all list-caches persistent keys are removed within that code part.
        This can race with this list caching the results of a query. This should be checked after saving the list to remove the data.
        :param references: Class and field references taken from the query. Regenerated if not given
        :type references: dict
        :return: True if invalidated else False
        :rtype: bool
        """
        references = references or self._get_referenced_fields()
        # own key first
        # Check if any pointers were removed
        class_pointer_lengths = []
        for class_name in references.iterkeys():
            key = self.generate_persistent_cache_key(class_name, self._key)
            class_pointer_lengths.append(len(list(self._persistent.prefix(key))))
        return any(pointer == 0 for pointer in class_pointer_lengths)
        # # Fields first
        # persistent_keys = []
        # # List all possible keys that the list can cache under
        # for class_name, fields in references.iteritems():
        #     for field in fields:
        #         persistent_keys.append(self.generate_persistent_cache_key(class_name, self._key, field))
        # data = list(self._persistent.get_multi(persistent_keys, must_exist=False))  # type: list
        # return any(item is None for item in data)

    def _get_referenced_fields(self, references=None, object_type=None, query_items=None):
        # type: (Optional[dict], Optional[type], Optional[list]) -> dict
        """
        Retrieve an overview of all fields included in the query
        The fields are mapped by the class name. This mapping is used for nested properties
        :param references: A by-ref dict containing all references for this list (Providing None will generate a new dict)
        :param object_type: The object type for this references run (Providing None will use the current object type)
        :param query_items: The query items that need to be used for building references (Providing None will use the current query)
        :return: A dict containing all classes referenced within the itens together with the fields of those classes
        Example: {disk: ['__all', 'model'], 'storagerouter': ['name']}
        where disk with model X was requested on storagerouter with name Y
        :rtype: dict
        """
        def add_reference(c_name, f_name):
            """
            :param c_name of the class to add
            :param f_name: Name of the field to add
            Add a reference to the dict
            """
            if c_name not in references:
                references[c_name] = []
            if f_name not in references[c_name]:
                references[c_name].append(f_name)

        # All fields are referenced by default.
        references = references or {self._object_type.__name__.lower(): ['__all']}
        object_type = object_type or self._object_type
        query_items = query_items or self._query['items']

        for query_item in query_items:
            if isinstance(query_item, dict):
                # Recursive, items are added by reference
                self._get_referenced_fields(references, object_type, query_item['items'])
            else:
                field = query_item[0]
                field_paths = field.split('.')
                current_object_type = object_type
                item_counter = 0
                # Handle nesting of properties
                for property_item in field_paths:
                    item_counter += 1
                    class_name = current_object_type.__name__.lower()
                    # Determine which property type it is:
                    # Options are: relation (both direction), dynamic, simple
                    if property_item == 'guid':
                        # The guid is a final value which can't be changed so it shouldn't be taken into account
                        break
                    elif property_item in (prop.name for prop in current_object_type._properties):
                        # The property_item is in the properties, so it's a simple property (e.g. vmachine.name)
                        add_reference(class_name, property_item)
                        break
                    elif property_item in (relation.name for relation in current_object_type._relations):
                        # The property_item is in the relations, so it's a relation property (e.g. vdisk.vmachine)
                        add_reference(class_name, property_item)
                        relation = [relation for relation in current_object_type._relations if relation.name == property_item][0]
                        if relation.foreign_type is not None:
                            current_object_type = relation.foreign_type
                        continue
                    elif property_item.endswith('_guid') and property_item.replace('_guid', '') in (relation.name for relation in current_object_type._relations):
                        # The property_item is the guid pointing to a relation, so it can be handled like a simple property (e.g. vdisk.vmachine_guid)
                        add_reference(class_name, property_item.replace('_guid', ''))
                        break
                    elif property_item in (dynamic.name for dynamic in current_object_type._dynamics):
                        # The property_item is a dynamic property, which will be ignored anyway
                        break
                    else:
                        # No property and no relation, it might be a foreign relation (e.g. vmachine.vdisks)
                        # this means the property_item most likely contains an index
                        cleaned_property_item = property_item.split('[')[0]
                        relations = RelationMapper.load_foreign_relations(current_object_type)
                        if relations is not None:
                            if cleaned_property_item in relations:
                                current_object_type = Descriptor().load(relations[cleaned_property_item]['class']).get_object()
                                add_reference(current_object_type.__name__.lower(), relations[cleaned_property_item]['key'])
                                continue
                    raise RuntimeError('Invalid path given: {0}, currently pointing to {1}'.format(field_paths, property_item))
        return references

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
        datalist = DataList(remote_class, {}, '{0}_{1}_{2}'.format(own_name, own_guid, remote_key))

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
        new_datalist = DataList(self._object_type, {})
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
            new_datalist = DataList(self._object_type, {})
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

    @classmethod
    def generate_persistent_cache_key(cls, class_name=None, identifier=None, property_name=None):
        # type: (str, str, str) -> str
        """
        Generate the pointer to the cache key
        Providing None will skip that part
        The persistent DB is used for prefixing support. These DB keys point towards the key in the volatile store.
        :param class_name: Name of the the class
        :type class_name: str
        :param identifier: ID of the list. Also serves as the key where the data will be cached
        :type identifier: str
        :param property_name: Name of the property
        :type property_name: str
        :return: The generated key
        :rtype: str
        """
        parts = '{0}_{{0}}|{{0}}|{{0}}'.format(cls.CACHELINK).split('|')
        arg_parts = [class_name, identifier, property_name]
        key = ''
        for index, arg_part in enumerate(arg_parts):
            if arg_part is None:
                if index == 0:
                    # Special case. Return CACHELINK_
                    return parts[index].format('')
                return key
            part_to_add = '|{1}'.format(key, parts[index].format(arg_part))
            if key == '':
                part_to_add = part_to_add.split('|')[-1]
            key = '{0}{1}'.format(key, part_to_add)
        return key

    @classmethod
    def get_key_parts(cls, list_key):
        # type: (str) -> Tuple[str, str, str]
        """
        Returns all parts of the key
        :param list_key: Key of the list
        :type list_key: str
        :return: The parts of the key
        :rtype: Tuple[str, str, str]
        """
        namespace_class, cache_key, field = list_key.split('|')
        class_name = namespace_class.replace('{0}_'.format(cls.CACHELINK), '')
        return class_name, cache_key, field

    @classmethod
    def extract_cache_key(cls, list_key):
        # type: (str) -> str
        """
        Extract the cache key from a complete list key
        :param list_key: Key of the list
        :type list_key: str
        :return: The extracted key
        :rtype: str
        """
        # Format class|key|prop
        class_name, cache_key, field = cls.get_key_parts(list_key)
        return cache_key
