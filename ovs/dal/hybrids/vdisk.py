# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
VDisk module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.extensions.storageserver.storagedriver import StorageDriverClient
from ovs.extensions.storage.volatilefactory import VolatileFactory
import pickle
import time


class VDisk(DataObject):
    """
    The VDisk class represents a vDisk. A vDisk is a Virtual Disk served by Open vStorage.
    vDisks can be part of a vMachine or stand-alone.
    """
    __properties = [Property('name', str, mandatory=False, doc='Name of the vDisk.'),
                    Property('description', str, mandatory=False, doc='Description of the vDisk.'),
                    Property('size', int, doc='Size of the vDisk in Bytes.'),
                    Property('devicename', str, doc='The name of the container file (e.g. the VMDK-file) describing the vDisk.'),
                    Property('order', int, mandatory=False, doc='Order with which vDisk is attached to a vMachine. None if not attached to a vMachine.'),
                    Property('volume_id', str, mandatory=False, doc='ID of the vDisk in the Open vStorage Volume Driver.'),
                    Property('parentsnapshot', str, mandatory=False, doc='Points to a parent voldrvsnapshotid. None if there is no parent Snapshot'),
                    Property('cinder_id', str, mandatory=False, doc='Cinder Volume ID, for volumes managed through Cinder')]
    __relations = [Relation('vmachine', VMachine, 'vdisks', mandatory=False),
                   Relation('vpool', VPool, 'vdisks'),
                   Relation('parent_vdisk', None, 'child_vdisks', mandatory=False)]
    __dynamics = [Dynamic('snapshots', list, 60),
                  Dynamic('info', dict, 60),
                  Dynamic('statistics', dict, 5),
                  Dynamic('storagedriver_id', str, 60),
                  Dynamic('storagerouter_guid', str, 15)]

    def __init__(self, *args, **kwargs):
        """
        Initializes a vDisk, setting up its additional helpers
        """
        DataObject.__init__(self, *args, **kwargs)
        self._frozen = False
        self.storagedriver_client = None
        self._frozen = True
        self.reload_client()

    def _snapshots(self):
        """
        Fetches a list of Snapshots for the vDisk
        """
        snapshots = []
        if self.volume_id and self.vpool:
            volume_id = str(self.volume_id)
            try:
                voldrv_snapshots = self.storagedriver_client.list_snapshots(volume_id)
            except:
                voldrv_snapshots = []
            for guid in voldrv_snapshots:
                snapshot = self.storagedriver_client.info_snapshot(volume_id, guid)
                # @todo: to be investigated howto handle during set as template
                if snapshot.metadata:
                    metadata = pickle.loads(snapshot.metadata)
                    snapshots.append({'guid': guid,
                                      'timestamp': metadata['timestamp'],
                                      'label': metadata['label'],
                                      'is_consistent': metadata['is_consistent'],
                                      'is_automatic': metadata.get('is_automatic', True),
                                      'stored': int(snapshot.stored)})
        return snapshots

    def _info(self):
        """
        Fetches the info (see Volume Driver API) for the vDisk.
        """
        if self.volume_id and self.vpool:
            try:
                vdiskinfo = self.storagedriver_client.info_volume(str(self.volume_id))
            except:
                vdiskinfo = StorageDriverClient().empty_info()
        else:
            vdiskinfo = StorageDriverClient().empty_info()

        vdiskinfodict = {}
        for key, value in vdiskinfo.__class__.__dict__.items():
            if type(value) is property:
                vdiskinfodict[key] = getattr(vdiskinfo, key)
                if key == 'object_type':
                    vdiskinfodict[key] = str(vdiskinfodict[key])
        return vdiskinfodict

    def _statistics(self, dynamic):
        """
        Fetches the Statistics for the vDisk.
        """
        client = StorageDriverClient()
        volatile = VolatileFactory.get_client()
        prev_key = '{0}_{1}'.format(self._key, 'statistics_previous')
        # Load data from volumedriver
        if self.volume_id and self.vpool:
            try:
                vdiskstats = self.storagedriver_client.statistics_volume(str(self.volume_id))
            except:
                vdiskstats = client.empty_statistics()
        else:
            vdiskstats = client.empty_statistics()
        # Load volumedriver data in dictionary
        vdiskstatsdict = {}
        for key, value in vdiskstats.__class__.__dict__.items():
            if type(value) is property and key in client.stat_counters:
                vdiskstatsdict[key] = getattr(vdiskstats, key)
        # Precalculate sums
        for key, items in client.stat_sums.iteritems():
            vdiskstatsdict[key] = 0
            for item in items:
                vdiskstatsdict[key] += vdiskstatsdict[item]
        vdiskstatsdict['timestamp'] = time.time()
        # Calculate delta's based on previously loaded dictionary
        previousdict = volatile.get(prev_key, default={})
        for key in vdiskstatsdict.keys():
            if key in client.stat_keys:
                delta = vdiskstatsdict['timestamp'] - previousdict.get('timestamp',
                                                                       vdiskstatsdict['timestamp'])
                if delta < 0:
                    vdiskstatsdict['{0}_ps'.format(key)] = 0
                elif delta == 0:
                    vdiskstatsdict['{0}_ps'.format(key)] = previousdict.get('{0}_ps'.format(key), 0)
                else:
                    vdiskstatsdict['{0}_ps'.format(key)] = (vdiskstatsdict[key] - previousdict[key]) / delta
        volatile.set(prev_key, vdiskstatsdict, dynamic.timeout * 10)
        # Returning the dictionary
        return vdiskstatsdict

    def _storagedriver_id(self):
        """
        Returns the Volume Storage Driver ID to which the vDisk is connected.
        """
        return self.info.get('vrouter_id', None)

    def _storagerouter_guid(self):
        """
        Loads the vDisks StorageRouter guid
        """
        if not self.storagedriver_id:
            return None
        from ovs.dal.hybrids.storagedriver import StorageDriver
        storagedrivers = DataObjectList(
            DataList({'object': StorageDriver,
                      'data': DataList.select.DESCRIPTOR,
                      'query': {'type': DataList.where_operator.AND,
                                'items': [('storagedriver_id', DataList.operator.EQUALS, self.storagedriver_id)]}}).data,
            StorageDriver
        )
        if len(storagedrivers) == 1:
            return storagedrivers[0].storagerouter_guid
        return None

    def reload_client(self):
        """
        Reloads the StorageDriver Client
        """
        if self.vpool:
            self._frozen = False
            self.storagedriver_client = StorageDriverClient().load(self.vpool)
            self._frozen = True
