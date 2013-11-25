# license see http://www.openvstorage.com/licenses/opensource/
"""
VPool module
"""
from ovs.dal.dataobject import DataObject


class VPool(DataObject):
    """
    The VPool class represents a vPool. A vPool covers a given backend
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':               (None, str, 'Name of the virtual pool'),
                  'description':        (None, str, 'Description of the virtual pool'),
                  'size':               (None, int, 'Size of the virtual pool'),
                  'backend_login':      (None, str, 'Login for the backend'),
                  'backend_password':   (None, str, 'Password for the backend'),
                  'backend_connection': (None, str, 'Connection for the backend'),
                  'backend_type':       (None, ['S3', 'FILESYSTEM'], 'Type of the backend')}
    _relations = {}
    _expiry = {'status':                  (10, str),
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
               'stored_data':              (5, int)}
    # pylint: enable=line-too-long

    def _status(self):
        """
        Fetches the status of the volume
        """
        _ = self
        return None

    def _cache_hits(self):
        """
        Loads the cache hits (counter)
        """
        return sum([d.cache_hits for d in self.disks])

    def _cache_misses(self):
        """
        Loads the cache misses (counter)
        """
        return sum([d.cache_misses for d in self.disks])

    def _read_operations(self):
        """
        Loads the read operations (counter)
        """
        return sum([d.read_operations for d in self.disks])

    def _write_operations(self):
        """
        Loads the write operations (counter)
        """
        return sum([d.write_operations for d in self.disks])

    def _bytes_read(self):
        """
        Loads the total of bytes read (counter)
        """
        return sum([d.bytes_read for d in self.disks])

    def _bytes_written(self):
        """
        Loads the bytes written (counter)
        """
        return sum([d.bytes_written for d in self.disks])

    def _backend_read_operations(self):
        """
        Loads the backend read operations (counter)
        """
        return sum([d.backend_read_operations for d in self.disks])

    def _backend_write_operations(self):
        """
        Loads the backend write operations
        """
        return sum([d.backend_write_operations for d in self.disks])

    def _backend_bytes_read(self):
        """
        Loads the bytes read (counter)
        """
        return sum([d.backend_bytes_read for d in self.disks])

    def _backend_bytes_written(self):
        """
        Loads the bytes written (counter)
        """
        return sum([d.backend_bytes_written for d in self.disks])

    def _stored_data(self):
        """
        Loads the stored data (counter)
        """
        return sum([d.stored_data for d in self.disks])
