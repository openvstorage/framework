"""
VPool module
"""
from ovs.dal.dataobject import DataObject


class VPool(DataObject):
    """
    The VPool class represents a vPool. A vPool covers a given backend
    """
    _blueprint = {'name': (None, str),
                  'description': (None, str),
                  'size': (None, int)}
    _relations = {}
    _expiry = {}
