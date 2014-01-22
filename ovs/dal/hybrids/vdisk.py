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
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient
import pickle
import time

_vsr_client = VolumeStorageRouterClient().load()


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
               'statistics':  (4, dict),
               'vsrid':      (60, str)}
    # pylint: enable=line-too-long

    def _snapshots(self):
        """
        Fetches a list of Snapshots for the vDisk
        """

        snapshots = []
        if self.volumeid:
            volumeid = str(self.volumeid)
            try:
                voldrv_snapshots = _vsr_client.list_snapshots(volumeid)
            except:
                voldrv_snapshots = []
            for guid in voldrv_snapshots:
                snapshot = _vsr_client.info_snapshot(volumeid, guid)
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
        if self.volumeid:
            try:
                vdiskinfo = _vsr_client.info_volume(str(self.volumeid))
            except:
                vdiskinfo = _vsr_client.empty_info()
        else:
            vdiskinfo = _vsr_client.empty_info()

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
        if self.volumeid:
            try:
                vdiskstats = _vsr_client.statistics_volume(str(self.volumeid))
            except:
                vdiskstats = _vsr_client.empty_statistics()
        else:
            vdiskstats = _vsr_client.empty_statistics()

        vdiskstatsdict = {}
        for key, value in vdiskstats.__class__.__dict__.items():
            if type(value) is property:
                vdiskstatsdict[key] = getattr(vdiskstats, key)

        vdiskstatsdict['timestamp'] = time.time()
        return vdiskstatsdict

    def _vsrid(self):
        """
        Returns the Volume Storage Router ID to which the vDisk is connected.
        """
        return self.info.get('vrouter_id', None)
