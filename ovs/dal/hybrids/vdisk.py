from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.vmachine import vMachine
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient

_vsrClient = VolumeStorageRouterClient().load()


class vDisk(DataObject):
    _blueprint = {'name' : None,  # All persistent stored fields, with default value
                  'description' : 'Test disk',
                  'size' : 100,
                  'vpoolguid' : None,  #BACKEND
                  'type' : 'DSSVOL',
                  'role' : 'BOOT',  # BOOT, DATA, TEMP
                  'devicename' : '123456789-flat.vmdk',
                  'order' : None,
                  'volumeid' : None,
                  'parentsnapshot' : None,
                  'children' : [],
                  'retentionpolicyguid' : None,
                  'snapshotpolicyguid' : None,
                  'tags' : [],
                  'replicationguid' : None,
                  'environmentguid' : None,
                  'cloudspaceguid' : None,
                  'autobackup' : False}
    _relations = {'machine': (vMachine, 'disks')}
    _expiry = {'used_size': 5,  # Timeout in seconds of individual RO properties
               'snapshots': 60,
               'status': 30,
               'volumedriverid': 30}

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

