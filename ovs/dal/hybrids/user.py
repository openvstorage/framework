"""
User module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.group import Group


class User(DataObject):
    """
    The User class represents a user account that can use the product.
    """
    _blueprint = {'username': (None, str),
                  'password': (None, str),
                  'email': (None, str),
                  'is_active': (False, bool),
                  'language': ('en-US', ['en-US'])}
    _relations = {'group': (Group, 'users')}
    _expiry = {}
