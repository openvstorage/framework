from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids._testmachine import TestMachine


class TestDisk(DataObject):
    _blueprint = {'name'       : (None, str),
                  'description': (None, str),
                  'size'       : (0,    int)}
    _relations = {'machine': (TestMachine, 'disks'),
                  'storage': (TestMachine, 'stored_disks')}
    _expiry = {'used_size': 5}

    @property
    def used_size(self):
        def get_data():
            # Simulate fetching real data
            from random import randint
            return randint(0, self._data['size'])
        return self._backend_property(get_data)