# license see http://www.openvstorage.com/licenses/opensource/
"""
UserList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobject import DataObjectList
from ovs.dal.hybrids.user import User
from ovs.dal.helpers import Descriptor


class UserList(object):
    """
    This UserList class contains various lists regarding to the User class
    """

    @staticmethod
    def get_user_by_username(username):
        """
        Returns a single User for the given username. Returns None if no user was found
        """
        # pylint: disable=line-too-long
        users = DataList(key='user_%s' % username,
                         query={'object': User,
                                'data': DataList.select.DESCRIPTOR,
                                'query': {'type': DataList.where_operator.AND,
                                          'items': [('username', DataList.operator.EQUALS, username)]}}).data  # noqa
        # pylint: enable=line-too-long
        if len(users) == 1:
            return Descriptor().load(users[0]).get_object(True)
        return None

    @staticmethod
    def get_users():
        """
        Returns a list of all Users
        """
        users = DataList(key='users',
                         query={'object': User,
                                'data': DataList.select.DESCRIPTOR,
                                'query': {'type': DataList.where_operator.AND,
                                          'items': []}}).data
        return DataObjectList(users, User)
