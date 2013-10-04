from ovsdal.dataobject import DataObject


class Machine(DataObject):
    _blueprint = {'name': None,
                  'description': None}
    _expiry = {}