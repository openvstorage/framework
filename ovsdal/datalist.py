from storedobject import StoredObject
from helpers import Descriptor
from relations.relations import Relation


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

    def __init__(self, key, query, load=True):
        self._key = key
        self._query = query
        self.data = None
        if load:
            self.load()

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
        if isinstance(instance._blueprint[item[0]], Relation):
            # @TODO: We should support advanced recursive queries.
            # For now, we assume we want to query the guid
            value = instance._data[item[0]]['guid']
        else:
            value = getattr(instance, item[0])
        if item[1] == DataList.operator.EQUALS:
            return value == item[2]
        if item[1] == DataList.operator.GT:
            return value > item[2]
        if item[1] == DataList.operator.LT:
            return value < item[2]

    def load(self):
        self.data = StoredObject.volatile.get(self._key)
        if self.data is None:
            # The query should be a dictionary of the following format / example:
            #
            # {'object': Disk,
            #  'data'  : DataList.select.OBJECT,
            #  'query'  : {'type' : DataList.where_operator.AND,
            #              'items': [(machine, DataList.operator.EQUALS, self.guid),
            #                        (size,    DataList.operator.GT,     10000),
            #                        {'type' : DataList.where_operator.OR,
            #                         'items': [(used_size, DataList.operator.LT, 100),
            #                                   (used_size, DataList.operator.GT, 500)]]]}})
            #
            # Which should result in a SQL-alike query behavior:
            #
            # SELECT *
            # FROM Disk
            # WHERE Disk.machine == <self.guid>
            # AND Disk.size > 10000
            # AND (Disk.used_size < 100
            #      OR Disk.used_size > 500)

            base_key = '%s_%s_' % (self._query['object']().namespace, self._query['object'].__name__.lower())
            keys = StoredObject.persistent.prefix(base_key)
            if self._query['data'] == DataList.select.COUNT:
                self.data = 0
            else:
                self.data = []

            for key in keys:
                guid = key.replace(base_key, '')
                instance = self._query['object'](guid)
                if self._query['query']['type'] == DataList.where_operator.AND:
                    include = self._exec_and(instance, self._query['query']['items'])
                else:
                    include = self._exec_or(instance, self._query['query']['items'])
                if include:
                    if self._query['data'] == DataList.select.COUNT:
                        self.data += 1
                    else:
                        self.data.append(Descriptor(self._query['object'], guid).descriptor)

            StoredObject.volatile.set(self._key, self.data)
        return self
