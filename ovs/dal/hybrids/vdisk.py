from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.vmachine import vMachine
from ovs.dal.hybrids.vpool import vPool
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient

_vsrClient = VolumeStorageRouterClient().load()


class vDisk(DataObject):
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
                  'retentionpolicyguid': (None,                  str),
                  'snapshotpolicyguid' : (None,                  str),
                  'tags'               : ([],                    list),
                  'replicationguid'    : (None,                  str),
                  'environmentguid'    : (None,                  str),
                  'cloudspaceguid'     : (None,                  str),
                  'autobackup'         : (False,                 bool),
                  'templatesnapshot'   : (None,                  str)}
    _relations = {'machine': (vMachine, 'disks'),
                  'vpool'  : (vPool,    'disks')}
    _expiry = {'used_size': 5,  # Timeout in seconds of individual RO properties
               'snapshots': 60,
               'status'   : 30}

    @property
    def used_size(self):
        def get_data():
            # Simulate fetching real data
            from random import randint
            return randint(0, self._data['size'])
        return self._backend_property(get_data)

    @property
    def snapshots(self):
        def get_data():
            return _vsrClient.listSnapShots(self.volumeid)
        return self._backend_property(get_data)

    @property
    def status(self):
        def get_data():
            return 'ATTACHED'
        return self._backend_property(get_data)

