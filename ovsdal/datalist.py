from storedobject import StoredObject
from helpers import Descriptor
from exceptions import ObjectNotFoundException


class DataList(StoredObject):
    class Select(object):
        OBJECT = 'OBJECT'
        COUNT  = 'COUNT'

    class WhereOperator(object):
        AND = 'AND'
        OR  = 'OR'

    class Operator(object):
        EQUALS    = 'EQUALS'
        LT        = 'LT'
        GT        = 'GT'

    select = Select()
    where_operator = WhereOperator()
    operator = Operator()
    namespace = 'ovs_list'

    def __init__(self, key, query, load=True):
        # Initialize super class
        super(DataList, self).__init__()

        self._key = None if key is None else ('%s_%s' % (DataList.namespace, key))
        self._query = query
        self.data = None
        if load:
            self.load()

    @staticmethod
    def get_pks(namespace, name):
        key = 'ovs_primarykeys_%s' % name
        keys = StoredObject.volatile.get(key)
        if keys is None:
            keys = StoredObject.persistent.prefix('%s_%s_' % (namespace, name))
        return keys

    def _exec_and(self, instance, items):
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
                if DataList._evaluate(instance, item) is False:
                    return False
        return True

    def _exec_or(self, instance, items):
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
                if DataList._evaluate(instance, item) is True:
                    return True
        return False

    @staticmethod
    def _evaluate(instance, item):
        path = item[0].split('.')
        value = instance
        for pitem in path:
            if value is not None:
                value = getattr(value, pitem)
            else:
                return False
        if item[1] == DataList.operator.EQUALS:
            return value == item[2]
        if item[1] == DataList.operator.GT:
            return value > item[2]
        if item[1] == DataList.operator.LT:
            return value < item[2]

    def load(self):
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

            namespace = self._query['object']()._namespace
            name = self._query['object'].__name__.lower()
            base_key = '%s_%s_' % (namespace, name)
            keys = DataList.get_pks(namespace, name)
            if self._query['data'] == DataList.select.COUNT:
                self.data = 0
            else:
                self.data = []

            for key in keys:
                guid = key.replace(base_key, '')
                try:
                    instance = self._query['object'](guid)
                    if self._query['query']['type'] == DataList.where_operator.AND:
                        include = self._exec_and(instance, self._query['query']['items'])
                    else:
                        include = self._exec_or(instance, self._query['query']['items'])
                except ObjectNotFoundException:
                    include = False
                if include:
                    if self._query['data'] == DataList.select.COUNT:
                        self.data += 1
                    else:
                        self.data.append(Descriptor(self._query['object'], guid).descriptor)

            if self._key is not None:
                StoredObject.volatile.set(self._key, self.data)
        return self
