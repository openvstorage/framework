from ovsdal.dataobject import DataObject


class Machine(DataObject):
    _blueprint = {'name'       : None,
                  'description': None,
                  'hvtype'     : None,
                  'username'   : None,
                  'password'   : None,
                  'ip'         : None}
    _relations = {}
    _expiry = {}