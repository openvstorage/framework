from ovs.dal.dataobject import DataObject



class vMachine(DataObject):
    _blueprint = {'name'       : None,
                  'description': None,
                  'hvtype'     : None}
    _relations = {}
    _expiry = {}
    
