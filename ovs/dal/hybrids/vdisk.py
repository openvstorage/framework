# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
VDisk module
"""

import time
import pickle
from datetime import datetime
from ovs.dal.datalist import DataList
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.extensions.storageserver.storagedriver import \
    MaxRedirectsExceededException, VolumeRestartInProgressException, \
    FSMetaDataClient, ObjectRegistryClient, StorageDriverClient
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.log.log_handler import LogHandler


class VDisk(DataObject):
    """
    The VDisk class represents a vDisk. A vDisk is a Virtual Disk served by Open vStorage.
    """
    _logger = LogHandler.get('dal', name='hybrid')

    __properties = [Property('name', str, mandatory=False, doc='Name of the vDisk.'),
                    Property('description', str, mandatory=False, doc='Description of the vDisk.'),
                    Property('size', int, doc='Size of the vDisk in Bytes.'),
                    Property('devicename', str, doc='The name of the container file (e.g. the VMDK-file) describing the vDisk.'),
                    Property('volume_id', str, mandatory=False, indexed=True, doc='ID of the vDisk in the Open vStorage Volume Driver.'),
                    Property('parentsnapshot', str, mandatory=False, doc='Points to a parent storage driver parent ID. None if there is no parent Snapshot'),
                    Property('cinder_id', str, mandatory=False, doc='Cinder Volume ID, for volumes managed through Cinder'),
                    Property('has_manual_dtl', bool, default=False, doc='Indicates whether the default DTL location has been overruled by customer'),
                    Property('pagecache_ratio', float, default=1.0, doc='Ratio of the volume\'s metadata pages that needs to be cached'),
                    Property('metadata', dict, default=dict(), doc='Contains fixed metadata about the volume (e.g. lba_size, ...)')]
    __relations = [Relation('vpool', VPool, 'vdisks'),
                   Relation('parent_vdisk', None, 'child_vdisks', mandatory=False)]
    __dynamics = [Dynamic('dtl_status', str, 60),
                  Dynamic('snapshots', list, 30),
                  Dynamic('info', dict, 60),
                  Dynamic('statistics', dict, 4),
                  Dynamic('storagedriver_id', str, 60),
                  Dynamic('storagerouter_guid', str, 15),
                  Dynamic('is_vtemplate', bool, 60),
                  Dynamic('edge_clients', list, 30)]
    _fixed_properties = ['storagedriver_client', 'objectregistry_client', 'fsmetadata_client']

    def __init__(self, *args, **kwargs):
        """
        Initializes a vDisk, setting up its additional helpers
        """
        DataObject.__init__(self, *args, **kwargs)
        self._frozen = False
        self._storagedriver_client = None
        self._objectregistry_client = None
        self._fsmetadata_client = None
        self._frozen = True

    @property
    def storagedriver_client(self):
        """
        Client used for communication between StorageDriver and framework
        :return: StorageDriverClient
        """
        if self._storagedriver_client is None:
            self.reload_client('storagedriver')
        return self._storagedriver_client

    @property
    def objectregistry_client(self):
        """
        Client used for communication between StorageDriver OR and framework
        :return: ObjectRegistryClient
        """
        if self._objectregistry_client is None:
            self.reload_client('objectregistry')
        return self._objectregistry_client

    @property
    def fsmetadata_client(self):
        """
        Client used for communications between StorageDriver FS metadata and framework
        :return: FileSystemMetaDataClient
        """
        if self._fsmetadata_client is None:
            self.reload_client('filesystem_metadata')
        return self._fsmetadata_client

    def _dtl_status(self):
        """
        Retrieve the DTL status for a vDisk
        """
        sd_status = self.info.get('failover_mode', 'UNKNOWN').lower()
        if sd_status == '':
            sd_status = 'unknown'
        if sd_status != 'ok_standalone':
            return sd_status

        # Verify whether 'ok_standalone' is the correct status for this vDisk
        vpool_dtl = self.vpool.configuration['dtl_enabled']
        if self.has_manual_dtl is True or vpool_dtl is False:
            return 'disabled'

        domains = []
        possible_dtl_targets = set()
        for sr in StorageRouterList.get_storagerouters():
            if sr.guid == self.storagerouter_guid:
                domains = [junction.domain for junction in sr.domains]
            elif len(sr.storagedrivers) > 0:
                possible_dtl_targets.add(sr)

        if len(domains) > 0:
            possible_dtl_targets = set()
            for domain in domains:
                possible_dtl_targets.update(StorageRouterList.get_primary_storagerouters_for_domain(domain))

        if len(possible_dtl_targets) == 0:
            return sd_status
        return 'checkup_required'

    def _snapshots(self):
        """
        Fetches a list of Snapshots for the vDisk
        """
        snapshots = []
        if self.volume_id and self.vpool:
            volume_id = str(self.volume_id)
            voldrv_snapshots = []
            try:
                try:
                    voldrv_snapshots = self.storagedriver_client.list_snapshots(volume_id, req_timeout_secs=2)
                except VolumeRestartInProgressException:
                    time.sleep(0.5)
                    voldrv_snapshots = self.storagedriver_client.list_snapshots(volume_id, req_timeout_secs=2)
            except:
                pass

            for snap_id in voldrv_snapshots:
                snapshot = self.storagedriver_client.info_snapshot(volume_id, snap_id, req_timeout_secs=2)
                if snapshot.metadata:
                    metadata = pickle.loads(snapshot.metadata)
                    if isinstance(metadata, dict):
                        snapshots.append({'guid': snap_id,
                                          'timestamp': metadata['timestamp'],
                                          'label': metadata['label'],
                                          'is_consistent': metadata['is_consistent'],
                                          'is_automatic': metadata.get('is_automatic', True),
                                          'is_sticky': metadata.get('is_sticky', False),
                                          'in_backend': snapshot.in_backend,
                                          'stored': int(snapshot.stored)})
                else:
                    snapshots.append({'guid': snap_id,
                                      'timestamp': time.mktime(datetime.strptime(snapshot.timestamp.strip(), '%c').timetuple()),
                                      'label': snap_id,
                                      'is_consistent': False,
                                      'is_automatic': False,
                                      'is_sticky': False,
                                      'in_backend': snapshot.in_backend,
                                      'stored': int(snapshot.stored)})
        return snapshots

    def _info(self):
        """
        Fetches the info (see Volume Driver API) for the vDisk.
        """
        vdiskinfo = StorageDriverClient.EMPTY_INFO()
        max_redirects = False
        if self.volume_id and self.vpool:
            try:
                try:
                    vdiskinfo = self.storagedriver_client.info_volume(str(self.volume_id), req_timeout_secs=2)
                except VolumeRestartInProgressException:
                    time.sleep(0.5)
                    vdiskinfo = self.storagedriver_client.info_volume(str(self.volume_id), req_timeout_secs=2)
            except MaxRedirectsExceededException:
                max_redirects = True
            except:
                pass

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
        vdiskinfodict['live_status'] = 'NON-RUNNING' if max_redirects is True else ('RUNNING' if vdiskinfodict['halted'] is False else 'HALTED')
        return vdiskinfodict

    def _statistics(self, dynamic):
        """
        Fetches the Statistics for the vDisk.
        """
        statistics = {}
        for key, value in self.fetch_statistics().iteritems():
            statistics[key] = value
        statistics['timestamp'] = time.time()
        VDisk.calculate_delta(self._key, dynamic, statistics)
        return statistics

    def _storagedriver_id(self):
        """
        Returns the Volume Storage Driver ID to which the vDisk is connected.
        """
        vdisk_object = self.objectregistry_client.find(str(self.volume_id))
        if vdisk_object is not None:
            return vdisk_object.node_id()
        return None

    def _storagerouter_guid(self):
        """
        Loads the vDisks StorageRouter guid
        """
        if not self.storagedriver_id:
            return None
        from ovs.dal.hybrids.storagedriver import StorageDriver
        sds = DataList(StorageDriver, {'type': DataList.where_operator.AND,
                                       'items': [('storagedriver_id', DataList.operator.EQUALS, self.storagedriver_id)]})
        if len(sds) == 1:
            return sds[0].storagerouter_guid
        return None

    def _is_vtemplate(self):
        """
        Returns whether the vdisk is a template
        """
        vdisk_object = self.objectregistry_client.find(str(self.volume_id))
        if vdisk_object is not None:
            return str(vdisk_object.object_type()) == 'TEMPLATE'
        return False

    def _edge_clients(self):
        """
        Retrieves all edge clients
        """
        clients = {}
        for storagedriver in self.vpool.storagedrivers:
            for client in storagedriver.edge_clients:
                if client['object_id'] == self.volume_id:
                    clients[client['key']] = client
        return clients.values()

    def fetch_statistics(self):
        """
        Loads statistics from this vDisk - returns unprocessed data
        """
        # Load data from volumedriver
        vdiskstats = StorageDriverClient.EMPTY_STATISTICS()
        if self.volume_id and self.vpool:
            try:
                vdiskstats = self.storagedriver_client.statistics_volume(str(self.volume_id), req_timeout_secs=2)
            except Exception as ex:
                VDisk._logger.error('Error loading statistics_volume from {0}: {1}'.format(self.volume_id, ex))
        # Load volumedriver data in dictionary
        return VDisk.extract_statistics(vdiskstats, self)

    @staticmethod
    def extract_statistics(stats, vdisk):
        statsdict = {}
        try:
            pc = stats.performance_counters
            for counter, info in {'backend_read_request_size': {'sum': 'backend_data_read',
                                                                'events': 'backend_read_operations',
                                                                'distribution': 'backend_read_operations_distribution'},
                                  'backend_read_request_usecs': {'sum': 'backend_read_latency',
                                                                 'distribution': 'backend_read_latency_distribution'},
                                  'backend_write_request_size': {'sum': 'backend_data_written',
                                                                 'events': 'backend_write_operations',
                                                                 'distribution': 'backend_write_operations_distribution'},
                                  'backend_write_request_usecs': {'sum': 'backend_write_latency',
                                                                  'distribution': 'backend_write_latency_distribution'},
                                  'read_request_size': {'sum': 'data_read',
                                                        'events': 'read_operations',
                                                        'distribution': 'read_operations_distribution'},
                                  'read_request_usecs': {'sum': 'read_latency',
                                                         'distribution': 'read_latency_distribution'},
                                  'write_request_size': {'sum': 'data_written',
                                                         'events': 'write_operations',
                                                         'distribution': 'write_operations_distribution'},
                                  'write_request_usecs': {'sum': 'write_latency',
                                                          'distribution': 'write_latency_distribution'},
                                  'unaligned_read_request_size': {'sum': 'unaligned_data_read',
                                                                  'events': 'unaligned_read_operations',
                                                                  'distribution': 'unaligned_read_operations_distribution'},
                                  'unaligned_read_request_usecs': {'sum': 'unaligned_read_latency',
                                                                   'distribution': 'unaligned_read_latency_distribution'},
                                  'unaligned_write_request_size': {'sum': 'unaligned_data_written',
                                                                   'events': 'unaligned_write_operations',
                                                                   'distribution': 'unaligned_write_operations_distribution'},
                                  'unaligned_write_request_usecs': {'sum': 'unaligned_write_latency',
                                                                    'distribution': 'unaligned_write_latency_distribution'}}.iteritems():
                if hasattr(pc, counter):
                    counter_object = getattr(pc, counter)
                    for method, target in info.iteritems():
                        if hasattr(counter_object, method):
                            statsdict[target] = getattr(counter_object, method)()

            for key in ['cluster_cache_hits', 'cluster_cache_misses', 'metadata_store_hits',
                        'metadata_store_misses', 'sco_cache_hits', 'sco_cache_misses', 'stored',
                        'partial_read_fast', 'partial_read_slow']:
                if hasattr(stats, key):
                    statsdict[key] = getattr(stats, key)
            # Do some more manual calculations
            block_size = 0
            if vdisk is not None:
                block_size = vdisk.metadata.get('lba_size', 0) * vdisk.metadata.get('cluster_multiplier', 0)
            if block_size == 0:
                block_size = 4096
            for key, source in {'4k_read_operations': 'data_read',
                                '4k_write_operations': 'data_written',
                                '4k_unaligned_read_operations': 'unaligned_data_read',
                                '4k_unaligned_write_operations': 'unaligned_data_written'}.iteritems():
                statsdict[key] = statsdict.get(source, 0) / block_size
            # Pre-calculate sums
            for key, items in StorageDriverClient.STAT_SUMS.iteritems():
                statsdict[key] = 0
                for item in items:
                    statsdict[key] += statsdict[item]
        except:
            pass
        return statsdict

    @staticmethod
    def calculate_delta(key, dynamic, current_stats):
        """
        Calculate statistics deltas
        :param key: Key to retrieve from volatile factory
        :param dynamic:
        :param current_stats: Current statistics to compare with
        :return: None
        """
        volatile = VolatileFactory.get_client()
        prev_key = '{0}_{1}'.format(key, 'statistics_previous')
        previous_stats = volatile.get(prev_key, default={})
        for key in current_stats.keys():
            if key == 'timestamp'or '_latency' in key or '_distribution' in key:
                continue
            delta = current_stats['timestamp'] - previous_stats.get('timestamp', current_stats['timestamp'])
            if delta < 0:
                current_stats['{0}_ps'.format(key)] = 0
            elif delta == 0:
                current_stats['{0}_ps'.format(key)] = previous_stats.get('{0}_ps'.format(key), 0)
            else:
                current_stats['{0}_ps'.format(key)] = max(0, (current_stats[key] - previous_stats[key]) / delta)
        volatile.set(prev_key, current_stats, dynamic.timeout * 10)

    def reload_client(self, client):
        """
        Reloads the StorageDriverClient or ObjectRegistryClient
        """
        if self.vpool_guid:
            self._frozen = False
            if client == 'storagedriver':
                self._storagedriver_client = StorageDriverClient.load(self.vpool)
            elif client == 'objectregistry':
                self._objectregistry_client = ObjectRegistryClient.load(self.vpool)
            elif client == 'filesystem_metadata':
                self._fsmetadata_client = FSMetaDataClient.load(self.vpool)
            self._frozen = True
