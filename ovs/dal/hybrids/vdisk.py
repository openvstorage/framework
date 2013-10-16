from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.vmachine import vMachine


class vDisk(DataObject):
    _blueprint = {'name'             : None,  # All persistent stored fields, with default value
                  'description'      : 'Test disk',
                  'size'             : 100,
                  'vpoolguid'        : None, #BACKEND
                  'type'             : 'DSSVOL',
                  'devicename'       : '123456789.vmdk',
                  'retentionpolicyid': None,
                  'snapshotpolicyid' : None,
                  'tags'             : None,
                  'replicationguid'  : None,
                  'environmentguid'  : None,
                  'cloudspaceguid'   : None,
                  'autobackup'       : False}
    _relations = {'machine': (vMachine, 'disks'),
                  'storage': (vMachine, 'stored_disks')}
    _expiry = {'used_size': 5,  # Timeout in seconds of individual RO properties
               'snapshots': 10,
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
            # Simulate fetching real data
            from random import randint
            snapshots = []
            for i in xrange(0, randint(1, 10)):
                snapshots.append({'consistent': randint(0, 1) == 1, 'id': i})
            return snapshots
        return self._backend_property(get_data)

    @property
    def status(self):
        def get_data():
            return 'ATTACHED'
        return self._backend_property(get_data)

    @property
    def volumedriverid(self):
        def get_data():
            return 1
        return self._backend_property(get_data)
