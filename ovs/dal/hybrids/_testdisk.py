"""
TestDisk module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids._testmachine import TestMachine


class TestDisk(DataObject):
    """
    This TestDisk object is used for running unittests.
    WARNING: These properties should not be changed
    """
    _blueprint = {'name': (None, str),
                  'description': (None, str),
                  'size': (0, int)}
    _relations = {'machine': (TestMachine, 'disks'),
                  'storage': (TestMachine, 'stored_disks')}
    _expiry = {'used_size': 5}

    @property
    def used_size(self):
        """
        Returns a certain fake used_size value
        """

        def get_data():
            """
            Loads the actualy fake data
            """
            from random import randint

            return randint(0, self._data['size'])

        return self._backend_property(get_data)
