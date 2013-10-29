from ovs.dal.dataobject import DataObject


class TestMachine(DataObject):
    _blueprint = {'name': (None, str)}
    _relations = {}
    _expiry = {}