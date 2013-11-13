"""
TestMachine module
"""
from ovs.dal.dataobject import DataObject


class TestMachine(DataObject):
    """
    This TestMachine object is used for running unittests.
    WARNING: These properties should not be changed
    """
    _blueprint = {'name': (None, str)}
    _relations = {}
    _expiry = {}