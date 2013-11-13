"""
Role module
"""
from ovs.dal.dataobject import DataObject


class Role(DataObject):
    """
    The Role class is used to represent a certain role which is allowed to execute a certain set of actions.
    Example; a "viewer" role can view all data without explicitly be able to update the data
    """
    _blueprint = {'name': (None, str),
                  'code': (None, str),
                  'description': (None, str)}
    _relations = {}
    _expiry = {}
