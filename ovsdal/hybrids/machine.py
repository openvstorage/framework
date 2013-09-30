from ovsdal.dataobject import DataObject


class Machine(DataObject):
    _blueprint = {'name': None,
                  'description': None}
    _objectexpiry = 300
    _expiry = {}