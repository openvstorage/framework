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
    _blueprint = {'name'               : (None,                  str),
                  'description'        : ('Test disk',           str),
                  'size'               : (100,                   int),
                  'type'               : ('DSSVOL',              str),
                  'role'               : ('BOOT',                str),  # BOOT, DATA, TEMP
                  'devicename'         : ('123456789-flat.vmdk', str),
                  'order'              : (None,                  int),
                  'volumeid'           : (None,                  str),
                  'parentsnapshot'     : (None,                  str),
                  'children'           : ([],                    list),
                  'retentionpolicyid'  : (None,                  str),
                  'snapshotpolicyid'   : (None,                  str),
                  'tags'               : ([],                    list),
                  'replicationguid'    : (None,                  str),
                  'environmentguid'    : (None,                  str),
                  'autobackup'         : (False,                 bool),
                  'templatesnapshot'   : (None,                  str)}
    _relations = {'machine': (VMachine, 'disks'),
                  'vpool'  : (VPool,    'disks')}
    _expiry = {'used_size': 5,  # Timeout in seconds of individual RO properties
               'snapshots': 60,
               'status': 30,
               'storage_server': 30}

    @property
    def used_size(self):
        """
        The used_size is the amount of data that is in use on the backend
        """
        def get_data():
            """
            Loads the actual data
            """
            # Simulate fetching real data
            from random import randint
            return randint(0, self._data['size'])
        return self._backend_property(get_data)

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
            return 'ATTACHED'
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
            return _vsrClient.info(self.volumeid)
        return self._backend_property(get_data)

