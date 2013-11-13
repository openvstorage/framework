"""
DataList module
"""
from storedobject import StoredObject
from helpers import Descriptor, Toolbox
from exceptions import ObjectNotFoundException


class DataList(StoredObject):
    """
    The DataList is a class that provide query functionality for the hybrid DAL
    """
    class Select(object):
        """
        The Select class provides enum-alike properties for what to select
        """
        DESCRIPTOR = 'DESCRIPTOR'
        COUNT      = 'COUNT'

    class WhereOperator(object):
        """
        The WhereOperator class provides enum-alike properties for the Where-operators
        """
        AND = 'AND'
        OR  = 'OR'

    class Operator(object):
        """
        The Operator class provides enum-alike properties for equalitation-operators
        """
        # In case more operators are required, add them here, and implement them in
        # the _evaluate method below
        EQUALS    = 'EQUALS'
        LT        = 'LT'
        GT        = 'GT'

    select = Select()
    where_operator = WhereOperator()
    operator = Operator()
    namespace = 'ovs_list'
    cachelink = 'ovs_listcache'

    def __init__(self, key, query):
        """
        Initializes a DataList class with a given key (used for optional caching) and a given query
        """
        # Initialize super class
        super(DataList, self).__init__()

        self._key = None if key is None else ('%s_%s' % (DataList.namespace, key))
        self._query = query
        self._invalidation = {}
        self.data = None
        self.from_cache = False
        self._load()

    @staticmethod
    def get_pks(namespace, name):
        """
        This method will load the primary keys for a given namespace and name (typically, for ovs_data_*)
        """
        key = 'ovs_primarykeys_%s' % name
        keys = StoredObject.volatile.get(key)
        if keys is None:
            keys = StoredObject.persistent.prefix('%s_%s_' % (namespace, name))
        return keys

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
            self._add_invalidation(value.__class__.__name__.lower(), pitem)
            target_class = value._relations.get(pitem, None)
            value = getattr(value, pitem)
            if value is None and itemcounter != len(path):
                # We loaded a None in the middle of our path
                if target_class is not None:
                    self._add_invalidation(target_class[0].__name__.lower(), path[itemcounter])
                return False  # Fail the filter

        # Apply operators
        if item[1] == DataList.operator.EQUALS:
            return value == item[2]
        if item[1] == DataList.operator.GT:
            return value > item[2]
        if item[1] == DataList.operator.LT:
            return value < item[2]
        raise NotImplementedError('The given where_operator is not yet implemented.')

    def _load(self):
        """
        Tries to load the result for the given key from the volatile cache, or executes the query if not
        yet available. Afterwards (if a key is given), the result will be (re)cached
        """
        self.data = StoredObject.volatile.get(self._key) if self._key is not None else None
        if self.data is None:
            # The query should be a dictionary:
            #     {'object': Disk,                           # Object on which the query should be executed
            #      'data'  : DataList.select.XYZ,            # The requested result; a list of object descriptors, or a count
            #      'query' : <query>}                        # The actual query
            # Where <query> is a query(group) dictionary:
            #     {'type' : DataList.where_operator.ABC,     # Defines whether the given items should be considered in an AND or OR group
            #      'items': <items>}                         # The items in the group
            # Where the <items> is any combination of one or more <filter> or <query>
            # A <filter> tuple example:
            #     (<field>, DataList.operator.GHI, <value>)  # The operator can be for example EQUALS
            # The field is any property you would also find on the given object. In case of properties, you can dot as far as you like
            # This means you can combine AND and OR in any possible combination

            items        = self._query['query']['items']
            query_type   = self._query['query']['type']
            query_data   = self._query['data']
            query_object = self._query['object']

            self.from_cache = False
            namespace = query_object()._namespace
            name = query_object.__name__.lower()
            base_key = '%s_%s_' % (namespace, name)
            keys = DataList.get_pks(namespace, name)

            if query_data == DataList.select.COUNT:
                self.data = 0
            else:
                self.data = []

            self._add_invalidation(name, '__all')
            for key in keys:
                guid = key.replace(base_key, '')
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
                        elif query_data == DataList.select.DESCRIPTOR:
                            self.data.append(Descriptor(query_object, guid).descriptor)
                        else:
                            raise NotImplementedError('The given selector type is not yet implemented.')
                except ObjectNotFoundException:
                    pass

            if self._key is not None and len(keys) > 0:
                StoredObject.volatile.set(self._key, self.data)
            self._update_listinvalidation()
        else:
            self.from_cache = True
        return self

    def _add_invalidation(self, object_name, field):
        """
        This method adds an invalidation to an internal list that will be saved when the
        query is completed
        """
        field_list = self._invalidation.get(object_name, [])
        field_list.append(field)
        self._invalidation[object_name] = field_list
        pass

    def _update_listinvalidation(self):
        """
        This method will save the list invalidation mapping to volatile and persistent storage
        """
        if self._key is not None:
            for object_name, field_list in self._invalidation.iteritems():
                key = '%s_%s' % (DataList.cachelink, object_name)
                cache_list = Toolbox.try_get(key, {})
                for field in field_list:
                    list_list = cache_list.get(field, [])
                    if self._key not in list_list:
                        list_list.append(self._key)
                    cache_list[field] = list_list
                StoredObject.volatile.set(key, cache_list)
                StoredObject.persistent.set(key, cache_list)