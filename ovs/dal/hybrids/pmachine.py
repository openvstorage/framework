from ovs.dal.dataobject import DataObject


class pMachine(DataObject):
    _blueprint = {'name'       : None,
                  'description': None,
                  'hvtype'     : None,
                  'username'   : None,
                  'password'   : None,
                  'ip'         : None}
    _relations = {}
    _expiry = {}

    def __str__(self):
        return str(self._data)