from ovs.dal.dataobject import DataObject


class vPool(DataObject):
    _blueprint = {'name'       : (None, str),
                  'description': (None, str),
                  'size'       : (None, int)}
    _relations = {}
    _expiry = {}