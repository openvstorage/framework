"""
VMachine module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.pmachine import PMachine


class VMachine(DataObject):
    """
    A VMachine represents a virtual machine in the model. A virtual machine is
    always served by a hypervisor
    """
    _blueprint = {'name': (None, str),
                  'description': (None, str),
                  'hvtype': (None, str, 'Hypervisor type serving the VMachine'),
                  'cpu': (1, int),
                  'memory': (1024, int),
                  'vmid': (None, str, 'Identifier of the VMachine on the hypervisor'),
                  'template': (True, bool),
                  'system': (False, bool)}
    _relations = {'node': (PMachine, 'guests')}
    _expiry = {'iops': 1,
               'stored_data': 120,
               'cache': 60,
               'latency': 15,
               'read_speed': 30,
               'write_speed': 30}

    @property
    def iops(self):
        """
        Returns the IOPS counter for this VM
        """

        def get_data():
            """
            Loads the actual data
            """
            import time
            return time.time()

        return self._backend_property(get_data)

    @property
    def stored_data(self):
        """
        Returns the amount of stored data for this VM
        """

        def get_data():
            """
            Loads the actual data
            """
            from random import randint

            return randint(0, 500)

        return self._backend_property(get_data)

    @property
    def cache(self):
        """
        Returns the cache hits percentage on this VM
        """

        def get_data():
            """
            Loads the actual data
            """
            from random import randint

            return randint(100, 200)

        return self._backend_property(get_data)

    @property
    def latency(self):
        """
        Returns the latency for this VM
        """

        def get_data():
            """
            Loads the actual data
            """
            from random import randint

            return randint(10, 125)

        return self._backend_property(get_data)

    @property
    def read_speed(self):
        """
        Returns the current read speed for this VM
        """

        def get_data():
            """
            Loads the actual data
            """
            from random import randint

            return randint(0, 250)

        return self._backend_property(get_data)

    @property
    def write_speed(self):
        """
        Returns the current write speed for this VM
        """

        def get_data():
            """
            Loads the actual data
            """
            from random import randint

            return randint(0, 250)

        return self._backend_property(get_data)
