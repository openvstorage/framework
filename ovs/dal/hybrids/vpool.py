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
VPool module
"""
import time
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.backendtype import BackendType
from ovs.dal.structures import Dynamic, Property, Relation
from ovs.extensions.storageserver.storagedriver import StorageDriverClient, StorageDriverConfiguration


class VPool(DataObject):
    """
    The VPool class represents a vPool. A vPool is a Virtual Storage Pool, a Filesystem, used to
    deploy vDisks. a vPool can span multiple Storage Drivers and connects to a single Storage BackendType.
    """
    STATUSES = DataObject.enumerator('Status', ['DELETING', 'EXTENDING', 'FAILURE', 'INSTALLING', 'RUNNING', 'SHRINKING'])

    __properties = [Property('name', str, doc='Name of the vPool'),
                    Property('description', str, mandatory=False, doc='Description of the vPool'),
                    Property('size', int, mandatory=False, doc='Size of the vPool expressed in Bytes. Set to zero if not applicable.'),
                    Property('login', str, mandatory=False, doc='Login/Username for the Storage BackendType.'),
                    Property('password', str, mandatory=False, doc='Password for the Storage BackendType.'),
                    Property('connection', str, mandatory=False, doc='Connection (IP, URL, Domain name, Zone, ...) for the Storage BackendType.'),
                    Property('metadata', dict, mandatory=False, doc='Metadata for the backends, as used by the Storage Drivers.'),
                    Property('rdma_enabled', bool, default=False, doc='Has the vpool been configured to use RDMA for DTL transport, which is only possible if all storagerouters are RDMA capable'),
                    Property('status', STATUSES.keys(), doc='Status of the vPool')]
    __relations = [Relation('backend_type', BackendType, 'vpools', doc='Type of storage backend.')]
    __dynamics = [Dynamic('configuration', dict, 3600),
                  Dynamic('statistics', dict, 4),
                  Dynamic('identifier', str, 120)]
    _fixed_properties = ['storagedriver_client']

    def __init__(self, *args, **kwargs):
        """
        Initializes a vPool, setting up its additional helpers
        """
        DataObject.__init__(self, *args, **kwargs)
        self._frozen = False
        self._storagedriver_client = None
        self._frozen = True

    @property
    def storagedriver_client(self):
        """
        Client used for communication between Storage Driver and framework
        :return: StorageDriverClient
        """
        if self._storagedriver_client is None:
            self.reload_client()
        return self._storagedriver_client

    def _configuration(self):
        """
        VPool configuration
        """
        if not self.storagedrivers or not self.storagedrivers[0].storagerouter:
            return {}

        storagedriver_config = StorageDriverConfiguration('storagedriver', self.guid, self.storagedrivers[0].storagedriver_id)
        storagedriver_config.load()

        dtl = storagedriver_config.configuration.get('distributed_transaction_log', {})
        file_system = storagedriver_config.configuration.get('filesystem', {})
        volume_router = storagedriver_config.configuration.get('volume_router', {})
        volume_manager = storagedriver_config.configuration.get('volume_manager', {})

        dtl_mode = file_system.get('fs_dtl_mode', StorageDriverClient.VOLDRV_DTL_ASYNC)
        dedupe_mode = volume_manager.get('read_cache_default_mode', StorageDriverClient.VOLDRV_CONTENT_BASED)
        cluster_size = volume_manager.get('default_cluster_size', 4096) / 1024
        dtl_transport = dtl.get('dtl_transport', StorageDriverClient.VOLDRV_DTL_TRANSPORT_TCP)
        cache_strategy = volume_manager.get('read_cache_default_behaviour', StorageDriverClient.VOLDRV_CACHE_ON_READ)
        sco_multiplier = volume_router.get('vrouter_sco_multiplier', 1024)
        dtl_config_mode = file_system.get('fs_dtl_config_mode', StorageDriverClient.VOLDRV_DTL_AUTOMATIC_MODE)
        tlog_multiplier = volume_manager.get('number_of_scos_in_tlog', 20)
        non_disposable_sco_factor = volume_manager.get('non_disposable_scos_factor', 12)

        sco_size = sco_multiplier * cluster_size / 1024  # SCO size is in MiB ==> SCO multiplier * cluster size (4 KiB by default)
        write_buffer = tlog_multiplier * sco_size * non_disposable_sco_factor

        dtl_mode = StorageDriverClient.REVERSE_DTL_MODE_MAP[dtl_mode]
        dtl_enabled = dtl_config_mode == StorageDriverClient.VOLDRV_DTL_AUTOMATIC_MODE
        if dtl_enabled is False:
            dtl_mode = StorageDriverClient.FRAMEWORK_DTL_NO_SYNC

        return {'sco_size': sco_size,
                'dtl_mode': dtl_mode,
                'dedupe_mode': StorageDriverClient.REVERSE_DEDUPE_MAP[dedupe_mode],
                'dtl_enabled': dtl_enabled,
                'cluster_size': cluster_size,
                'write_buffer': write_buffer,
                'dtl_transport': StorageDriverClient.REVERSE_DTL_TRANSPORT_MAP[dtl_transport],
                'cache_strategy': StorageDriverClient.REVERSE_CACHE_MAP[cache_strategy],
                'tlog_multiplier': tlog_multiplier}

    def _statistics(self, dynamic):
        """
        Aggregates the Statistics (IOPS, Bandwidth, ...) of each vDisk served by the vPool.
        """
        from ovs.dal.hybrids.vdisk import VDisk
        statistics = {}
        for key in StorageDriverClient.STAT_KEYS:
            statistics[key] = 0
            statistics['{0}_ps'.format(key)] = 0
        for storagedriver in self.storagedrivers:
            for key, value in storagedriver.fetch_statistics().iteritems():
                statistics[key] += value
        statistics['timestamp'] = time.time()
        VDisk.calculate_delta(self._key, dynamic, statistics)
        return statistics

    def _identifier(self):
        """
        An identifier of this vPool in its current configuration state
        """
        return '{0}_{1}'.format(self.guid, '_'.join(self.storagedrivers_guids))

    def reload_client(self):
        """
        Reloads the StorageDriver Client
        """
        self._frozen = False
        self._storagedriver_client = StorageDriverClient.load(self)
        self._frozen = True
