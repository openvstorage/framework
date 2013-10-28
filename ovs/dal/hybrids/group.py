from ovs.dal.dataobject import DataObject


class Group(DataObject):
    _blueprint = {'name': (None, str)}
    _relations = {}
    _expiry = {}