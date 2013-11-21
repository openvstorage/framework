"""
User module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.group import Group


class User(DataObject):
    """
    The User class represents a user account that can use the product.
    """
    _blueprint = {'username': (None, str, 'Username of the user'),
                  'password': (None, str, 'Password of the user'),
                  'email': (None, str, 'Email address of the user'),
                  'is_active': (False, bool, 'Indicates whether the user is active'),
                  'language': ('en-US', ['en-US'], 'Language of the user')}
    _relations = {'group': (Group, 'users')}
    _expiry = {}
