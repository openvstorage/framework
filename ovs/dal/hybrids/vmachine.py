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
    # pylint: disable=line-too-long
    _blueprint = {'name':         (None,  str,  'Name of the virtual machine'),
                  'description':  (None,  str,  'Description of the virtual machine'),
                  'hypervisorid': (None,  str,  'Identifier of the VMachine on the hypervisor'),
                  'template':     (True,  bool, 'Indicates whether this virtual machine is a template'),
                  'system':       (False, bool, 'Indicates whether this virtual machine represents the system'),
                  'hvtype':       (None,  ['HYPERV', 'VMWARE', 'XEN'], 'Hypervisor type serving the VMachine')}
    _relations = {}
    _expiry = {'snapshots':               60,
               'status':                  30,
               'storage_server':          30,
               'cache_hits':               5,
               'cache_misses':             5,
               'read_operations':          5,
               'write_operations':         5,
               'bytes_read':               5,
               'bytes_written':            5,
               'backend_read_operations':  5,
               'backend_write_operations': 5,
               'backend_bytes_read':       5,
               'backend_bytes_written':    5,
               'stored_data':              5,
               'foc_status':               5}
    # pylint: enable=line-too-long

    @property
    def snapshots(self):
        """
        Fetches a list of snapshots for this virtual machine
        """

        def get_data():
            """
            Loads the actual data
            """
            return None

        return self._backend_property(get_data)

    @property
    def status(self):
        """
        Fetches the status of the volume
        """

        def get_data():
            """
            Loads the actual data
            """
            return None

        return self._backend_property(get_data)

    @property
    def storage_server(self):
        """
        Returns the storage server on which the virtual disk is stored
        """

        def get_data():
            """
            Loads the actual data
            """
            return None

        return self._backend_property(get_data)

    @property
    def cache_hits(self):
        """
        Loads the cache hits (counter)
        """

        def get_data():
            """
            Loads the actual data
            """
            return sum([d.cache_hits for d in self.disks])

        return self._backend_property(get_data)

    @property
    def cache_misses(self):
        """
        Loads the cache misses (counter)
        """

        def get_data():
            """
            Loads the actual data
            """
            return sum([d.cache_misses for d in self.disks])

        return self._backend_property(get_data)

    @property
    def read_operations(self):
        """
        Loads the read operations (counter)
        """

        def get_data():
            """
            Loads the actual data
            """
            return sum([d.read_operations for d in self.disks])

        return self._backend_property(get_data)

    @property
    def write_operations(self):
        """
        Loads the write operations (counter)
        """

        def get_data():
            """
            Loads the actual data
            """
            return sum([d.write_operations for d in self.disks])

        return self._backend_property(get_data)

    @property
    def bytes_read(self):
        """
        Loads the total of bytes read (counter)
        """

        def get_data():
            """
            Loads the actual data
            """
            return sum([d.bytes_read for d in self.disks])

        return self._backend_property(get_data)

    @property
    def bytes_written(self):
        """
        Loads the bytes written (counter)
        """

        def get_data():
            """
            Loads the actual data
            """
            return sum([d.bytes_written for d in self.disks])

        return self._backend_property(get_data)

    @property
    def backend_read_operations(self):
        """
        Loads the backend read operations (counter)
        """

        def get_data():
            """
            Loads the actual data
            """
            return sum([d.backend_read_operations for d in self.disks])

        return self._backend_property(get_data)

    @property
    def backend_write_operations(self):
        """
        Loads the backend write operations
        """

        def get_data():
            """
            Loads the actual data
            """
            return sum([d.backend_write_operations for d in self.disks])

        return self._backend_property(get_data)

    @property
    def backend_bytes_read(self):
        """
        Loads the bytes read (counter)
        """

        def get_data():
            """
            Loads the actual data
            """
            return sum([d.backend_bytes_read for d in self.disks])

        return self._backend_property(get_data)

    @property
    def backend_bytes_written(self):
        """
        Loads the bytes written (counter)
        """

        def get_data():
            """
            Loads the actual data
            """
            return sum([d.backend_bytes_written for d in self.disks])

        return self._backend_property(get_data)

    @property
    def stored_data(self):
        """
        Loads the stored data (counter)
        """

        def get_data():
            """
            Loads the actual data
            """
            return sum([d.stored_data for d in self.disks])

        return self._backend_property(get_data)

    @property
    def foc_status(self):
        """
        Loads the FOC status
        """

        def get_data():
            """
            Loads the actual data
            """
            return None

        return self._backend_property(get_data)
