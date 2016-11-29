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
from ovs.dal.structures import Dynamic, Property
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonClusterConfig
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.storageserver.storagedriver import StorageDriverClient, ObjectRegistryClient, StorageDriverConfiguration
from volumedriver.storagerouter.storagerouterclient import ArakoonNodeConfig, ClusterRegistry


class VPool(DataObject):
    """
    The VPool class represents a vPool. A vPool is a Virtual Storage Pool, a Filesystem, used to
    deploy vDisks. a vPool can span multiple Storage Drivers and connects to a single Storage BackendType.
    """
    STATUSES = DataObject.enumerator('Status', ['DELETING', 'EXTENDING', 'FAILURE', 'INSTALLING', 'RUNNING', 'SHRINKING'])

    __properties = [Property('name', str, unique=True, doc='Name of the vPool'),
                    Property('description', str, mandatory=False, doc='Description of the vPool'),
                    Property('size', int, mandatory=False, doc='Size of the vPool expressed in Bytes. Set to zero if not applicable.'),
                    Property('login', str, mandatory=False, doc='Login/Username for the Storage BackendType.'),
                    Property('password', str, mandatory=False, doc='Password for the Storage BackendType.'),
                    Property('connection', str, mandatory=False, doc='Connection (IP, URL, Domain name, Zone, ...) for the Storage BackendType.'),
                    Property('metadata', dict, mandatory=False, doc='Metadata for the backends, as used by the Storage Drivers.'),
                    Property('rdma_enabled', bool, default=False, doc='Has the vpool been configured to use RDMA for DTL transport, which is only possible if all storagerouters are RDMA capable'),
                    Property('status', STATUSES.keys(), doc='Status of the vPool')]
    __relations = []
    __dynamics = [Dynamic('configuration', dict, 3600),
                  Dynamic('statistics', dict, 4),
                  Dynamic('identifier', str, 120)]
    _fixed_properties = ['storagedriver_client', 'objectregistry_client', 'clusterregistry_client']

    def __init__(self, *args, **kwargs):
        """
        Initializes a vPool, setting up its additional helpers
        """
        DataObject.__init__(self, *args, **kwargs)
        self._frozen = False
        self._storagedriver_client = None
        self._objectregistry_client = None
        self._clusterregistry_client = None
        self._frozen = True

    @property
    def storagedriver_client(self):
        """
        Client used for communication between Storage Driver and framework
        :return: StorageDriverClient
        """
        if self._storagedriver_client is None:
            self.reload_client('storagedriver')
        return self._storagedriver_client

    @property
    def objectregistry_client(self):
        """
        Client used for communication between Storage Driver OR and framework
        :return: ObjectRegistryClient
        """
        if self._objectregistry_client is None:
            self.reload_client('objectregistry')
        return self._objectregistry_client

    @property
    def clusterregistry_client(self):
        """
        Client used for making changes to the StorageDriver's Cluster Registry
        :return: ClusterRegistry client
        """
        if self._clusterregistry_client is None:
            self.reload_client('clusterregistry')
        return self._clusterregistry_client

    def _configuration(self):
        """
        VPool configuration
        """
        if not self.storagedrivers or not self.storagedrivers[0].storagerouter:
            return {}

        storagedriver_config = StorageDriverConfiguration('storagedriver', self.guid, self.storagedrivers[0].storagedriver_id)
        storagedriver_config.load()

        dtl = storagedriver_config.configuration['distributed_transaction_log']
        file_system = storagedriver_config.configuration['filesystem']
        volume_router = storagedriver_config.configuration['volume_router']
        volume_manager = storagedriver_config.configuration['volume_manager']

        dtl_host = file_system['fs_dtl_host']
        dtl_mode = file_system.get('fs_dtl_mode', StorageDriverClient.VOLDRV_DTL_ASYNC)
        cluster_size = volume_manager['default_cluster_size'] / 1024
        dtl_transport = dtl['dtl_transport']
        sco_multiplier = volume_router['vrouter_sco_multiplier']
        dtl_config_mode = file_system['fs_dtl_config_mode']
        tlog_multiplier = volume_manager['number_of_scos_in_tlog']
        non_disposable_sco_factor = volume_manager['non_disposable_scos_factor']

        sco_size = sco_multiplier * cluster_size / 1024  # SCO size is in MiB ==> SCO multiplier * cluster size (4 KiB by default)
        write_buffer = tlog_multiplier * sco_size * non_disposable_sco_factor
        dtl_enabled = not (dtl_config_mode == StorageDriverClient.VOLDRV_DTL_MANUAL_MODE and dtl_host == '')

        return {'sco_size': sco_size,
                'dtl_mode': StorageDriverClient.REVERSE_DTL_MODE_MAP[dtl_mode] if dtl_enabled is True else 'no_sync',
                'dtl_enabled': dtl_enabled,
                'cluster_size': cluster_size,
                'write_buffer': write_buffer,
                'dtl_transport': StorageDriverClient.REVERSE_DTL_TRANSPORT_MAP[dtl_transport],
                'dtl_config_mode': dtl_config_mode,
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

    def reload_client(self, client):
        """
        Reloads the StorageDriverClient, ObjectRegistryClient or ClusterRegistry client
        """
        self._frozen = False
        if client == 'storagedriver':
            self._storagedriver_client = StorageDriverClient.load(self)
        elif client == 'objectregistry':
            self._objectregistry_client = ObjectRegistryClient.load(self)
        elif client == 'clusterregistry':
            arakoon_cluster_name = str(Configuration.get('/ovs/framework/arakoon_clusters|voldrv'))
            config = ArakoonClusterConfig(cluster_id=arakoon_cluster_name, filesystem=False)
            config.load_config()
            arakoon_node_configs = []
            for node in config.nodes:
                arakoon_node_configs.append(ArakoonNodeConfig(str(node.name), str(node.ip), node.client_port))
            self._clusterregistry_client = ClusterRegistry(str(self.guid), arakoon_cluster_name, arakoon_node_configs)
        self._frozen = True
