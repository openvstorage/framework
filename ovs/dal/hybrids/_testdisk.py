# license see http://www.openvstorage.com/licenses/opensource/
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
    # pylint: disable=line-too-long
    _blueprint = {'name':        (None, str,   'Name of the test disk'),
                  'description': (None, str,   'Description of the test disk'),
                  'size':        (0,    float, 'Size of the test disk'),
                  'order':       (0,    int,   'Order of the test disk'),
                  'type':        (None, ['ONE', 'TWO'], 'Type of the test disk')}
    _relations = {'machine': (TestMachine, 'disks'),
                  'storage': (TestMachine, 'stored_disks')}
    _expiry = {'used_size':  (5, int),
               'wrong_type': (5, int)}
    # pylint: enable=line-too-long

    # For testing purposes
    wrong_type_data = 0

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

    @property
    def wrong_type(self):
        """
        Returns the wrong type, should always fail
        """

        def get_data():
            """
            Loads the actual data
            """
            return self.wrong_type_data

        return self._backend_property(get_data)
