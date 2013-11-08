from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.group import Group


class User(DataObject):
    _blueprint = {'username': (None,  str),
                  'password': (None,  str),
                  'email': (None,  str),
                  'is_active': (False, bool),
                  'language': ('en-US', str)}
    _relations = {'group': (Group, 'users')}
    _expiry = {}