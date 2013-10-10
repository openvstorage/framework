from ovsdal.dataobject import DataObject
from ovsdal.hybrids.group import Group
from ovsdal.relations.relations import Relation


class User(DataObject):
    _blueprint = {'username': None,
                  'password': None,
                  'email'   : None,
                  'group'   : Relation(Group, 'users')}
    _expiry = {}