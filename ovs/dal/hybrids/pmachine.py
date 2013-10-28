from ovs.dal.dataobject import DataObject


class pMachine(DataObject):
    _blueprint = {'name'       : (None, str),
                  'description': (None, str),
                  'hvtype'     : (None, str),
                  'username'   : (None, str),
                  'password'   : (None, str),
                  'ip'         : (None, str)}
    _relations = {}
    _expiry = {}