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
                  'is_vtemplate': (True,  bool, 'Indicates whether this virtual machine is a template'),
                  'is_internal':  (False, bool, 'Indicates whether this virtual machine represents an internal machine'),
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

    def _snapshots(self):
        """
        Fetches a list of snapshots for this virtual machine
        """
        _ = self
        return None

    def _status(self):
        """
        Fetches the status of the volume
        """
        _ = self
        return None

    def _vsaid(self):
        """
        Returns the storage server on which the virtual disk is stored
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

    def _foc_status(self):
        """
        Loads the FOC status
        """
        _ = self
        return None
