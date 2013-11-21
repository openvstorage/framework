"""
Role module
"""
from ovs.dal.dataobject import DataObject


class Role(DataObject):
    """
    The Role class is used to represent a certain role which is allowed to execute a certain set of
    actions. Example; a "viewer" role can view all data without explicitly be able to update
    the data
    """
    _blueprint = {'name': (None, str, 'Name of the role'),
                  'code': (None, str, 'Contains a code which is referenced from the API code'),
                  'description': (None, str, 'Description of the role')}
    _relations = {}
    _expiry = {}
