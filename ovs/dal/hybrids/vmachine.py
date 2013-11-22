# license see http://www.openvstorage.com/licenses/opensource/
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
                  'is_template':  (True,  bool, 'Indicates whether this virtual machine is a template'),
                  'is_vsa':       (False, bool, 'Indicates whether this virtual machine represents a VSA'),
                  'hvtype':       (None,  ['HYPERV', 'VMWARE', 'XEN'], 'Hypervisor type serving the VMachine')}
    _relations = {}
    _expiry = {'snapshots':               (60, list),
               'status':                  (30, str),
               'vsaid':                   (30, str),
               'cache_hits':               (5, int),
               'cache_misses':             (5, int),
               'read_operations':          (5, int),
               'write_operations':         (5, int),
               'bytes_read':               (5, int),
               'bytes_written':            (5, int),
               'backend_read_operations':  (5, int),
               'backend_write_operations': (5, int),
               'backend_bytes_read':       (5, int),
               'backend_bytes_written':    (5, int),
               'stored_data':              (5, int),
               'foc_status':               (5, str)}
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
    def vsaid(self):
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
