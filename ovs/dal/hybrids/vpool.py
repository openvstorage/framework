"""
VPool module
"""
from ovs.dal.dataobject import DataObject


class VPool(DataObject):
    """
    The VPool class represents a vPool. A vPool covers a given backend
    """
    _blueprint = {'name': (None, str),
                  'description': (None, str),
                  'size': (None, int),
                  'backend_type': (None, ['S3', 'FILESYSTEM']),
                  'backend_login': (None, str),
                  'backend_password': (None, str),
                  'backend_connection': (None, str)}
    _relations = {}
    _expiry = {'status': 10,
               'cache_hits': 5,
               'cache_misses': 5,
               'read_operations': 5,
               'write_operations': 5,
               'bytes_read': 5,
               'bytes_written': 5,
               'backend_read_operations': 5,
               'backend_write_operations': 5,
               'backend_bytes_read': 5,
               'backend_bytes_written': 5,
               'stored_data': 5}

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
    def volumestoragerouterid(self):
        """
        Returns the VSR on which the virtual disk is stored
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

