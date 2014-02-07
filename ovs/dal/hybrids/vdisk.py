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
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient
from ovs.extensions.storage.volatilefactory import VolatileFactory
import pickle
import time


class VDisk(DataObject):
    """
    The VDisk class represents a vDisk. A vDisk is a Virtual Disk served by Open vStorage.
    vDisks can be part of a vMachine or stand-alone.
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':              (None, str, 'Name of the vDisk.'),
                  'description':       (None, str, 'Description of the vDisk.'),
                  'size':              (0, int, 'Size of the vDisk in Bytes.'),
                  'devicename':        (None, str, 'The name of the container file (e.g. the VMDK-file) describing the vDisk.'),
                  'order':             (None, int, 'Order with which vDisk is attached to a vMachine. None if not attached to a vMachine.'),
                  'volumeid':          (None, str, 'ID of the vDisk in the Open vStorage Volume Driver.'),
                  'parentsnapshot':    (None, str, 'Points to a parent voldrvsnapshotid. None if there is no parent Snapshot'),
                  'retentionpolicyid': (None, str, 'Retention policy used by the vDisk.'),
                  'snapshotpolicyid':  (None, str, 'Snapshot policy used by the vDisk.'),
                  'tags':              (list(), list, 'Tags of the vDisk.'),
                  'has_autobackup':    (False, bool, 'Indicates whether this vDisk has autobackup enabled.'),
                  'type':              ('DSSVOL', ['DSSVOL'], 'Type of the vDisk.')}
    _relations = {'vmachine':     (VMachine, 'vdisks'),
                  'vpool':        (VPool, 'vdisks'),
                  'parent_vdisk': (None, 'child_vdisks')}
    _expiry = {'snapshots':  (60, list),
               'info':       (60, dict),
               'statistics':  (5, dict),
               'vsrid':      (60, str),
               'vsa_guid':   (15, str)}
    # pylint: enable=line-too-long

    def __init__(self, *args, **kwargs):
        """
        Initializes a vDisk, setting up it's additional helpers
        """
        DataObject.__init__(self, *args, **kwargs)
        if self.vpool:
            self._frozen = False
            self.vsr_client = VolumeStorageRouterClient().load(vpool=self.vpool)
            self._frozen = True

    def _snapshots(self):
        """
        Fetches a list of Snapshots for the vDisk
        """
        snapshots = []
        if self.volumeid and self.vpool:
            volumeid = str(self.volumeid)
            try:
                voldrv_snapshots = self.vsr_client.list_snapshots(volumeid)
            except:
                voldrv_snapshots = []
            for guid in voldrv_snapshots:
                snapshot = self.vsr_client.info_snapshot(volumeid, guid)
                # @todo: to be investigated howto handle during set as template
                if snapshot.metadata:
                    metadata = pickle.loads(snapshot.metadata)
                    snapshots.append({'guid': guid,
                                      'timestamp': metadata['timestamp'],
                                      'label': metadata['label'],
                                      'is_consistent': metadata['is_consistent']})
        return snapshots

    def _info(self):
        """
        Fetches the info (see Volume Driver API) for the vDisk.
        """
        if self.volumeid and self.vpool:
            try:
                vdiskinfo = self.vsr_client.info_volume(str(self.volumeid))
            except:
                vdiskinfo = VolumeStorageRouterClient().empty_info()
        else:
            vdiskinfo = VolumeStorageRouterClient().empty_info()

        vdiskinfodict = {}
        for key, value in vdiskinfo.__class__.__dict__.items():
            if type(value) is property:
                vdiskinfodict[key] = getattr(vdiskinfo, key)
                if key == 'volume_type':
                    vdiskinfodict[key] = str(vdiskinfodict[key])
        return vdiskinfodict

    def _statistics(self):
        """
        Fetches the Statistics for the vDisk.
        """
        client = VolumeStorageRouterClient()
        volatile = VolatileFactory.get_client()
        prev_key = '%s_%s' % (self._key, 'statistics_previous')
        # Load data from volumedriver
        if self.volumeid and self.vpool:
            try:
                vdiskstats = self.vsr_client.statistics_volume(str(self.volumeid))
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
                if delta == 0:
                    vdiskstatsdict['%s_ps' % key] = previousdict.get('%s_ps' % key, 0)
                else:
                    vdiskstatsdict['%s_ps' % key] = (vdiskstatsdict[key] - previousdict[key]) / delta
        volatile.set(prev_key, vdiskstatsdict, self._expiry['statistics'][0] * 10)
        # Returning the dictionary
        return vdiskstatsdict

    def _vsrid(self):
        """
        Returns the Volume Storage Router ID to which the vDisk is connected.
        """
        return self.info.get('vrouter_id', None)

    def _vsa_guid(self):
        """
        Loads the vDisks VSA guid
        """
        if not self.vsrid:
            return None
        from ovs.dal.hybrids.volumestoragerouter import VolumeStorageRouter
        volumestoragerouters = DataObjectList(
            DataList({'object': VolumeStorageRouter,
                      'data': DataList.select.DESCRIPTOR,
                      'query': {'type': DataList.where_operator.AND,
                                'items': [('vsrid', DataList.operator.EQUALS, self.vsrid)]}}).data,
            VolumeStorageRouter
        )
        if len(volumestoragerouters) == 1:
            return volumestoragerouters[0].serving_vmachine_guid
        return None
