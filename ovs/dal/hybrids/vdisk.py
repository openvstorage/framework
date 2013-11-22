# license see http://www.openvstorage.com/licenses/opensource/
"""
VDisk module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient

_vsrClient = VolumeStorageRouterClient().load()


class VDisk(DataObject):
    """
    The VDisk class represents a virtual disk that can be used by virtual machines. It has
    a one-to-one link with the volumedriver which is responsible for that particular volume
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':              (None,  str,  'Name of the virtual disk'),
                  'description':       (None,  str,  'Description of the virtual disk'),
                  'size':              (0,     int,  'Size of the virtual disk'),
                  'devicename':        (None,  str,  'The name of the container file backing the vDisk'),
                  'order':             (None,  int,  'Order of the virtual disk in which they are attached'),
                  'volumeid':          (None,  str,  'Volume ID representing the virtual disk'),
                  'parentsnapshot':    (None,  str,  'Points to a parent voldrvsnapshotid'),
                  'children':          ([],    list, 'List of child vDisks'),  # @TODO: discuss purpose of field, there might be a better solution
                  'retentionpolicyid': (None,  str,  'Retention policy used by the virtual disk'),
                  'snapshotpolicyid':  (None,  str,  'Snapshot polity used by the virtual disk'),
                  'tags':              ([],    list, 'Tags of the virtual disk'),
                  'has_autobackup':    (False, bool, 'Indicates whether this disk has autobackup'),
                  'type':             ('DSSVOL', ['DSSVOL'], 'Type of the virtual disk')}
    _relations = {'machine': (VMachine, 'disks'),
                  'vpool':   (VPool,    'disks')}
    _expiry = {'snapshots':               (60, list),
               'status':                  (30, str),
               'vsaid':                   (30, str),
               'volumestoragerouterid':   (30, str),
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
        Fetches a list of snapshots for this virtual disk
        """

        def get_data():
            """
            Loads the actual data
            """
            return _vsrClient.listSnapShots(self.volumeid)

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
            return 0

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
            return 0

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
            import time
            return int(time.time())

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
            import time
            return int(time.time())

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
            return 0

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
            return 0

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
            return 0

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
            return 0

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
            return 0

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
            return 0

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
            return 0

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
