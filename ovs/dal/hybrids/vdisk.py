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
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.extensions.storageserver.storagerouter import StorageRouterClient
from ovs.extensions.storage.volatilefactory import VolatileFactory
import pickle
import time


class VDisk(DataObject):
    """
    The VDisk class represents a vDisk. A vDisk is a Virtual Disk served by Open vStorage.
    vDisks can be part of a vMachine or stand-alone.
    """
    # pylint: disable=line-too-long
    __blueprint = {'name':              (None, str, 'Name of the vDisk.'),
                   'description':       (None, str, 'Description of the vDisk.'),
                   'size':              (0, int, 'Size of the vDisk in Bytes.'),
                   'devicename':        (None, str, 'The name of the container file (e.g. the VMDK-file) describing the vDisk.'),
                   'order':             (None, int, 'Order with which vDisk is attached to a vMachine. None if not attached to a vMachine.'),
                   'volume_id':         (None, str, 'ID of the vDisk in the Open vStorage Volume Driver.'),
                   'parentsnapshot':    (None, str, 'Points to a parent voldrvsnapshotid. None if there is no parent Snapshot'),
                   'retentionpolicyid': (None, str, 'Retention policy used by the vDisk.'),
                   'snapshotpolicyid':  (None, str, 'Snapshot policy used by the vDisk.'),
                   'tags':              (list(), list, 'Tags of the vDisk.'),
                   'has_autobackup':    (False, bool, 'Indicates whether this vDisk has autobackup enabled.'),
                   'type':              ('DSSVOL', ['DSSVOL'], 'Type of the vDisk.')}
    __relations = {'vmachine':     (VMachine, 'vdisks'),
                   'vpool':        (VPool, 'vdisks'),
                   'parent_vdisk': (None, 'child_vdisks')}
    __expiry = {'snapshots':             (60, list),
                'info':                  (60, dict),
                'statistics':             (5, dict),
                'storagerouter_id':      (60, str),
                'storageappliance_guid': (15, str)}
    # pylint: enable=line-too-long

    def __init__(self, *args, **kwargs):
        """
        Initializes a vDisk, setting up it's additional helpers
        """
        DataObject.__init__(self, *args, **kwargs)
        if self.vpool:
            self._frozen = False
            self.storagerouter_client = StorageRouterClient().load(self.vpool)
            self._frozen = True

    def _snapshots(self):
        """
        Fetches a list of Snapshots for the vDisk
        """
        snapshots = []
        if self.volume_id and self.vpool:
            volume_id = str(self.volume_id)
            try:
                voldrv_snapshots = self.storagerouter_client.list_snapshots(volume_id)
            except:
                voldrv_snapshots = []
            for guid in voldrv_snapshots:
                snapshot = self.storagerouter_client.info_snapshot(volume_id, guid)
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
                vdiskinfo = self.storagerouter_client.info_volume(str(self.volume_id))
            except:
                vdiskinfo = StorageRouterClient().empty_info()
        else:
            vdiskinfo = StorageRouterClient().empty_info()

        vdiskinfodict = {}
        for key, value in vdiskinfo.__class__.__dict__.items():
            if type(value) is property:
                vdiskinfodict[key] = getattr(vdiskinfo, key)
                if key == 'object_type':
                    vdiskinfodict[key] = str(vdiskinfodict[key])
        return vdiskinfodict

    def _statistics(self):
        """
        Fetches the Statistics for the vDisk.
        """
        client = StorageRouterClient()
        volatile = VolatileFactory.get_client()
        prev_key = '{0}_{1}'.format(self._key, 'statistics_previous')
        # Load data from volumedriver
        if self.volume_id and self.vpool:
            try:
                vdiskstats = self.storagerouter_client.statistics_volume(str(self.volume_id))
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
        volatile.set(prev_key, vdiskstatsdict, self._expiry['statistics'][0] * 10)
        # Returning the dictionary
        return vdiskstatsdict

    def _storagerouter_id(self):
        """
        Returns the Volume Storage Router ID to which the vDisk is connected.
        """
        return self.info.get('vrouter_id', None)

    def _storageappliance_guid(self):
        """
        Loads the vDisks StorageAppliance guid
        """
        if not self.storagerouter_id:
            return None
        from ovs.dal.hybrids.storagerouter import StorageRouter
        storagerouters = DataObjectList(
            DataList({'object': StorageRouter,
                      'data': DataList.select.DESCRIPTOR,
                      'query': {'type': DataList.where_operator.AND,
                                'items': [('storagerouter_id', DataList.operator.EQUALS, self.storagerouter_id)]}}).data,
            StorageRouter
        )
        if len(storagerouters) == 1:
            return storagerouters[0].storageappliance_guid
        return None
