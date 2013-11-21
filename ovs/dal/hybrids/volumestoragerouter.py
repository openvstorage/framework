"""
VolumeStorageRouter module
"""
from ovs.dal.dataobject import DataObject


class VolumeStorageRouter(DataObject):
    """
    The VolumeStorageRouter class represents a volume storage router
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':        (None, str, 'Name of the VSR'),
                  'description': (None, str, 'Description of the VSR'),
                  'port':        (None, int, 'Port on which the VSR is listening'),
                  'ip':          (None, str, 'IP address on which the VSR is listening')}
    _relations = {}
    _expiry = {'status':                  30,
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
               'stored_data':              5}
    # pylint: enable=line-too-long

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
    def cache_hits(self):
        """
        Loads the cache hits (counter)
        """

        def get_data():
            """
            Loads the actual data
            """
            return sum([j.vpool.cache_hits for j in self.vpools])

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
            return sum([j.vpool.cache_misses for j in self.vpools])

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
            return sum([j.vpool.read_operations for j in self.vpools])

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
            return sum([j.vpool.write_operations for j in self.vpools])

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
            return sum([j.vpool.bytes_read for j in self.vpools])

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
            return sum([j.vpool.bytes_written for j in self.vpools])

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
            return sum([j.vpool.backend_read_operations for j in self.vpools])

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
            return sum([j.vpool.backend_write_operations for j in self.vpools])

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
            return sum([j.vpool.backend_bytes_read for j in self.vpools])

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
            return sum([j.vpool.backend_bytes_written for j in self.vpools])

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
            return sum([j.vpool.stored_data for j in self.vpools])

        return self._backend_property(get_data)
