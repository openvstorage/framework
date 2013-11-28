# license see http://www.openvstorage.com/licenses/opensource/
"""
Group module
"""
from ovs.dal.dataobject import DataObject


class Group(DataObject):
    """
    The Group class represents a Group. A group is used to bind a set of Users to a set of Roles.
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':        (None, str, 'Name of the Group.'),
                  'description': (None, str, 'Description of the Group.')}
    _relations = {}
    _expiry = {}
    # pylint: enable=line-too-long
