from ovs.dal.dataobject import DataObject


class Role(DataObject):
    _blueprint = {'name': (None, str),
                  'code': (None, str),
                  'description': (None, str)}
    _relations = {}
    _expiry = {}
