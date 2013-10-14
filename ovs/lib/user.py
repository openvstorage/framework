from ovsdal.datalist import DataList
from ovsdal.helpers import Descriptor
from ovsdal.hybrids.user import User as HybridUser


class User(object):
    @staticmethod
    def get_user_by_username(username):
        users = DataList(key   = 'user_%s' % username,
                         query = {'object': HybridUser,
                                  'data'  : DataList.select.DESCRIPTOR,
                                  'query' : {'type' : DataList.where_operator.AND,
                                             'items': [('username', DataList.operator.EQUALS, username)]}}).data
        if len(users) == 1:
            return Descriptor().load(users[0]).get_object(True)
        return None

    @staticmethod
    def get_users():
        users = DataList(key   = 'users',
                         query = {'object': HybridUser,
                                  'data': DataList.select.DESCRIPTOR,
                                  'query': {'type': DataList.where_operator.AND,
                                            'items': []}}).data
        return [Descriptor().load(user).get_object(True) for user in users]

