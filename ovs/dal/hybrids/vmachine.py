from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.pmachine import pMachine


class vMachine(DataObject):
    _blueprint = {'name'       : (None, str),
                  'description': (None, str),
                  'hvtype'     : (None, str),
                  'cpu'        : (1,    int),
                  'memory'     : (1024, int),
                  'vmid'       : (None, str),
                  'template'   : (True, bool),
                  'system'     : (False, bool)}
    _relations = {'node': (pMachine, 'guests')}
    _expiry = {'iops': 30,
               'stored_data': 120,
               'cache': 60,
               'latency': 15,
               'read_speed': 30,
               'write_speed': 30}

    @property
    def iops(self):
        def get_data():
            from random import randint
            return randint(50, 250)
        return self._backend_property(get_data)

    @property
    def stored_data(self):
        def get_data():
            from random import randint
            return randint(0, 500)
        return self._backend_property(get_data)

    @property
    def cache(self):
        def get_data():
            from random import randint
            return randint(100, 200)
        return self._backend_property(get_data)

    @property
    def latency(self):
        def get_data():
            from random import randint
            return randint(10, 125)
        return self._backend_property(get_data)

    @property
    def read_speed(self):
        def get_data():
            from random import randint
            return randint(0, 250)
        return self._backend_property(get_data)

    @property
    def write_speed(self):
        def get_data():
            from random import randint
            return randint(0, 250)
        return self._backend_property(get_data)
