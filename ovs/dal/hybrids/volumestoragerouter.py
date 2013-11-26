# license see http://www.openvstorage.com/licenses/opensource/
"""
VolumeStorageRouter module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.vmachine import VMachine


class VolumeStorageRouter(DataObject):
    """
    The VolumeStorageRouter class represents a volume storage router
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':        (None, str, 'Name of the VSR'),
                  'description': (None, str, 'Description of the VSR'),
                  'port':        (None, int, 'Port on which the VSR is listening'),
                  'ip':          (None, str, 'IP address on which the VSR is listening'),
                  'vsrid':       (None, str, 'Internal volumedriver reference ID')}
    _relations = {'vpool':            (VPool,    'vsrs'),
                  'serving_vmachine': (VMachine, 'served_vsrs')}
    _expiry = {'status':                  (30, str),
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
        return self.vpool.cache_hits

    def _cache_misses(self):
        """
        Loads the cache misses (counter)
        """
        return self.vpool.cache_misses

    def _read_operations(self):
        """
        Loads the read operations (counter)
        """
        return self.vpool.read_operations

    def _write_operations(self):
        """
        Loads the write operations (counter)
        """
        return self.vpool.write_operations

    def _bytes_read(self):
        """
        Loads the total of bytes read (counter)
        """
        return self.vpool.bytes_read

    def _bytes_written(self):
        """
        Loads the bytes written (counter)
        """
        return self.vpool.bytes_written

    def _backend_read_operations(self):
        """
        Loads the backend read operations (counter)
        """
        return self.vpool.backend_read_operations

    def _backend_write_operations(self):
        """
        Loads the backend write operations
        """
        return self.vpool.backend_write_operations

    def _backend_bytes_read(self):
        """
        Loads the bytes read (counter)
        """
        return self.vpool.backend_bytes_read

    def _backend_bytes_written(self):
        """
        Loads the bytes written (counter)
        """
        return self.vpool.backend_bytes_written

    def _stored_data(self):
        """
        Loads the stored data (counter)
        """
        _ = self
        return self.vpool.stored_data
