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
from ovs.constants.storagedriver import VOLDRV_DTL_MANUAL_MODE, VOLDRV_DTL_ASYNC
from ovs.constants.vpool import STATUS_DELETING, STATUS_EXTENDING, STATUS_FAILURE, STATUS_INSTALLING, STATUS_RUNNING, STATUS_SHRINKING
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Dynamic, Property
from ovs_extensions.constants.vpools import MDS_CONFIG_PATH
from ovs.extensions.generic.configuration import Configuration, NotFoundException
from ovs.extensions.storageserver.storagedriver import ClusterRegistryClient, StorageDriverClient, ObjectRegistryClient, StorageDriverConfiguration,\
    LocalStorageRouterClient
from ovs.extensions.storageserver.storagedriverconfig import StorageDriverConfig

class VPool(DataObject):
    """
    The VPool class represents a vPool. A vPool is a Virtual Storage Pool, a Filesystem, used to
    deploy vDisks. a vPool can span multiple Storage Drivers and connects to a single Storage BackendType.
    """
    STATUSES = DataObject.enumerator('Status', [STATUS_RUNNING, STATUS_SHRINKING, STATUS_INSTALLING, STATUS_FAILURE, STATUS_DELETING, STATUS_EXTENDING])
    CACHES = DataObject.enumerator('Cache', {'BLOCK': 'block',
                                             'FRAGMENT': 'fragment'})

    __properties = [Property('name', str, unique=True, indexed=True, doc='Name of the vPool'),
                    Property('description', str, mandatory=False, doc='Description of the vPool'),
                    Property('login', str, mandatory=False, doc='Login/Username for the Storage BackendType.'),
                    Property('password', str, mandatory=False, doc='Password for the Storage BackendType.'),
                    Property('connection', str, mandatory=False, doc='Connection (IP, URL, Domain name, Zone, ...) for the Storage BackendType.'),
                    Property('metadata', dict, mandatory=False, doc='Metadata for the backends, as used by the Storage Drivers.'),
                    Property('rdma_enabled', bool, default=False, doc='Has the vpool been configured to use RDMA for DTL transport, which is only possible if all storagerouters are RDMA capable'),
                    Property('status', STATUSES.keys(), doc='Status of the vPool'),
                    Property('metadata_store_bits', int, mandatory=False, doc='StorageDrivers deployed for this vPool will make use of this amount of metadata store bits')]
    __relations = []
    __dynamics = [Dynamic('configuration', dict, 3600),
                  Dynamic('statistics', dict, 4),
                  Dynamic('identifier', str, 120),
                  Dynamic('extensible', tuple, 60),
                  Dynamic('volume_potentials', dict, 60)]
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
            self._clusterregistry_client = ClusterRegistryClient.load(self)
        self._frozen = True

    def _configuration(self):
        """
        VPool configuration
        """
        if not self.storagedrivers or not self.storagedrivers[0].storagerouter:
            return {}

        storagedriver_config = StorageDriverConfiguration(self.guid, self.storagedrivers[0].storagedriver_id).configuration # type: StorageDriverConfig

        file_system = storagedriver_config.filesystem_config
        volume_manager = storagedriver_config.volume_manager_config

        dtl_host = file_system.fs_dtl_host
        dtl_mode = file_system.fs_dtl_mode or  VOLDRV_DTL_ASYNC
        dtl_config_mode = file_system.fs_dtl_config_mode
        dtl_transport = storagedriver_config.dtl_config.dtl_transport
        cluster_size = volume_manager.default_cluster_size / 1024
        tlog_multiplier = volume_manager.number_of_scos_in_tlog
        non_disposable_sco_factor = volume_manager.non_disposable_scos_factor
        sco_multiplier = storagedriver_config.vrouter_config.vrouter_sco_multiplier

        sco_size = sco_multiplier * cluster_size / 1024  # SCO size is in MiB ==> SCO multiplier * cluster size (4 KiB by default)
        write_buffer = tlog_multiplier * sco_size * non_disposable_sco_factor
        dtl_enabled = not (dtl_config_mode == VOLDRV_DTL_MANUAL_MODE and dtl_host == '')

        try:
            mds_config = Configuration.get(MDS_CONFIG_PATH.format(self.guid))
        except NotFoundException:
            mds_config = {}

        return {'sco_size': sco_size,
                'dtl_mode': StorageDriverClient.REVERSE_DTL_MODE_MAP[dtl_mode] if dtl_enabled is True else 'no_sync',
                'mds_config': mds_config,
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
        for storagedriver in self.storagedrivers:
            for key, value in storagedriver.fetch_statistics().iteritems():
                if isinstance(value, dict):
                    if key not in statistics:
                        statistics[key] = {}
                        for subkey, subvalue in value.iteritems():
                            if subkey not in statistics[key]:
                                statistics[key][subkey] = 0
                            statistics[key][subkey] += subvalue
                else:
                    if key not in statistics:
                        statistics[key] = 0
                    statistics[key] += value
        statistics['timestamp'] = time.time()
        VDisk.calculate_delta(self._key, dynamic, statistics)
        return statistics

    def _identifier(self):
        """
        An identifier of this vPool in its current configuration state
        """
        return '{0}_{1}'.format(self.guid, '_'.join(self.storagedrivers_guids))

    def _extensible(self):
        """
        Verifies whether this vPool can be extended or not
        """
        reasons = []
        if self.status != VPool.STATUSES.RUNNING:
            reasons.append('non_running')
        if self.metadata_store_bits is None:
            reasons.append('voldrv_missing_info')
        return len(reasons) == 0, reasons

    def _volume_potentials(self):
        # type: () -> Dict[str, int]
        """
        Get an overview of all volume potentials for every Storagedriver in this vpool
        A possible -1 can be returned for the volume potential which indicates that the potential could not be retrieved
        :return: The overview with the volume potential
        :rtype: dict
        """
        volume_potentials = {}
        for storagedriver in self.storagedrivers:
            volume_potential = -1
            try:
                std_config = StorageDriverConfiguration(storagedriver.vpool_guid, storagedriver.storagedriver_id)
                client = LocalStorageRouterClient(std_config.remote_path)
                volume_potential = client.volume_potential(str(storagedriver.storagedriver_id))
            except Exception:
                self._logger.exception('Unable to retrieve configuration for storagedriver {0}'.format(storagedriver.storagedriver_id))
            volume_potentials[storagedriver.storagerouter.guid] = volume_potential
        return volume_potentials
