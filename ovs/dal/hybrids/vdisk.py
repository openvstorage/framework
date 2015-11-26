# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
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
from ovs.dal.hybrids.failuredomain import FailureDomain
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
                    Property('parentsnapshot', str, mandatory=False, doc='Points to a parent storage driver parent ID. None if there is no parent Snapshot'),
                    Property('cinder_id', str, mandatory=False, doc='Cinder Volume ID, for volumes managed through Cinder')]
    __relations = [Relation('vmachine', VMachine, 'vdisks', mandatory=False),
                   Relation('vpool', VPool, 'vdisks'),
                   Relation('parent_vdisk', None, 'child_vdisks', mandatory=False),
                   Relation('secondary_failure_domain', FailureDomain, 'secondary_vdisks', mandatory=False)]
    __dynamics = [Dynamic('snapshots', list, 60),
                  Dynamic('info', dict, 60),
                  Dynamic('statistics', dict, 4, locked=True),
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
                # @todo: to be investigated how to handle during set as template
                if snapshot.metadata:
                    metadata = pickle.loads(snapshot.metadata)
                    snapshots.append({'guid': guid,
                                      'timestamp': metadata['timestamp'],
                                      'label': metadata['label'],
                                      'is_consistent': metadata['is_consistent'],
                                      'is_automatic': metadata.get('is_automatic', True),
                                      'in_backend': snapshot.in_backend,
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
                vdiskinfo = StorageDriverClient.EMPTY_INFO()
        else:
            vdiskinfo = StorageDriverClient.EMPTY_INFO()

        vdiskinfodict = {}
        for key, value in vdiskinfo.__class__.__dict__.items():
            if type(value) is property:
                objectvalue = getattr(vdiskinfo, key)
                if key == 'object_type':
                    vdiskinfodict[key] = str(objectvalue)
                elif key == 'metadata_backend_config':
                    vdiskinfodict[key] = {}
                    if hasattr(objectvalue, 'node_configs') and callable(objectvalue.node_configs):
                        vdiskinfodict[key] = []
                        for nodeconfig in objectvalue.node_configs():
                            vdiskinfodict[key].append({'ip': nodeconfig.address(),
                                                       'port': nodeconfig.port()})
                else:
                    vdiskinfodict[key] = objectvalue
        return vdiskinfodict

    def _statistics(self, dynamic):
        """
        Fetches the Statistics for the vDisk.
        """
        statistics = {}
        for key in StorageDriverClient.STAT_KEYS:
            statistics[key] = 0
            statistics['{0}_ps'.format(key)] = 0
        for key, value in self.fetch_statistics().iteritems():
            statistics[key] += value
        statistics['timestamp'] = time.time()
        VDisk.calculate_delta(self._key, dynamic, statistics)
        return statistics

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
                      'data': DataList.select.GUIDS,
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
            self.storagedriver_client = StorageDriverClient.load(self.vpool)
            self._frozen = True

    def fetch_statistics(self):
        """
        Loads statistics from this vDisk - returns unprocessed data
        """
        # Load data from volumedriver
        if self.volume_id and self.vpool:
            try:
                vdiskstats = self.storagedriver_client.statistics_volume(str(self.volume_id))
                vdiskinfo = self.storagedriver_client.info_volume(str(self.volume_id))
            except:
                vdiskstats = StorageDriverClient.EMPTY_STATISTICS()
                vdiskinfo = StorageDriverClient.EMPTY_INFO()
        else:
            vdiskstats = StorageDriverClient.EMPTY_STATISTICS()
            vdiskinfo = StorageDriverClient.EMPTY_INFO()
        # Load volumedriver data in dictionary
        vdiskstatsdict = {}
        try:
            pc = vdiskstats.performance_counters
            vdiskstatsdict['backend_data_read'] = pc.backend_read_request_size.sum()
            vdiskstatsdict['backend_data_written'] = pc.backend_write_request_size.sum()
            vdiskstatsdict['backend_read_operations'] = pc.backend_read_request_size.events()
            vdiskstatsdict['backend_write_operations'] = pc.backend_write_request_size.events()
            vdiskstatsdict['data_read'] = pc.read_request_size.sum()
            vdiskstatsdict['data_written'] = pc.write_request_size.sum()
            vdiskstatsdict['read_operations'] = pc.read_request_size.events()
            vdiskstatsdict['write_operations'] = pc.write_request_size.events()
            for key in ['cluster_cache_hits', 'cluster_cache_misses', 'metadata_store_hits',
                        'metadata_store_misses', 'sco_cache_hits', 'sco_cache_misses']:
                vdiskstatsdict[key] = getattr(vdiskstats, key)
            # Do some more manual calculations
            block_size = vdiskinfo.lba_size * vdiskinfo.cluster_multiplier
            if block_size == 0:
                block_size = 4096
            vdiskstatsdict['4k_read_operations'] = vdiskstatsdict['data_read'] / block_size
            vdiskstatsdict['4k_write_operations'] = vdiskstatsdict['data_written'] / block_size
            # Pre-calculate sums
            for key, items in StorageDriverClient.STAT_SUMS.iteritems():
                vdiskstatsdict[key] = 0
                for item in items:
                    vdiskstatsdict[key] += vdiskstatsdict[item]
        except:
            pass
        return vdiskstatsdict

    @staticmethod
    def calculate_delta(key, dynamic, current_stats):
        """
        Calculate statistics deltas
        """
        volatile = VolatileFactory.get_client()
        prev_key = '{0}_{1}'.format(key, 'statistics_previous')
        previous_stats = volatile.get(prev_key, default={})
        for key in current_stats.keys():
            if key in StorageDriverClient.STAT_KEYS:
                delta = current_stats['timestamp'] - previous_stats.get('timestamp', current_stats['timestamp'])
                if delta < 0:
                    current_stats['{0}_ps'.format(key)] = 0
                elif delta == 0:
                    current_stats['{0}_ps'.format(key)] = previous_stats.get('{0}_ps'.format(key), 0)
                else:
                    current_stats['{0}_ps'.format(key)] = max(0, (current_stats[key] - previous_stats[key]) / delta)
        volatile.set(prev_key, current_stats, dynamic.timeout * 10)
