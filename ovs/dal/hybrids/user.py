# license see http://www.openvstorage.com/licenses/opensource/
"""
User module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.group import Group


class User(DataObject):
    """
    The User class represents a User.  A user is an individual who can perform actions 
    on objects in Open vStorage.
    """
    # pylint: disable=line-too-long
    _blueprint = {'username':  (None,    str,  'Username of the User.'),
                  'password':  (None,    str,  'Password of the User.'),
                  'email':     (None,    str,  'Email address of the User.'),
                  'is_active': (False,   bool, 'Indicates whether the User is active.'),
                  'language':  ('en-US', ['en-US', 'nl-NL'], 'Language of the User.')}
    _relations = {'group': (Group, 'users')}
    _expiry = {}
    # pylint: enable=line-too-long
