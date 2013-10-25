from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.pmachine import pMachine


class vMachine(DataObject):
    _blueprint = {'name'        : None,
                  'description' : None,
                  'hvtype'      : None,
                  'cpu'         : 1,
                  'memory'      : 1024,
                  'vmid'        : None,
                  'template'    : True}
    _relations = {'node': (pMachine, 'guests')}
    _expiry = {'iops': 30,
               'backend_size': 120}

    @property
    def iops(self):
        def get_data():
            from random import randint
            return randint(50, 250)
        return self._backend_property(get_data)

    @property
    def backend_size(self):
        def get_data():
            from random import randint
            return randint(0, 500)
        return self._backend_property(get_data)