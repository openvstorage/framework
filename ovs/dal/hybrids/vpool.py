from ovs.dal.dataobject import DataObject


class vPool(DataObject):
    _blueprint = {'name'       : None,
                  'description': None,
                  'size'       : None,}
    _relations = {}
    _expiry = {}