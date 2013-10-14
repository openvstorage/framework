from ovs.dal.dataobject import DataObject


class Group(DataObject):
    _blueprint = {'name': None}
    _relations = {}
    _expiry = {}