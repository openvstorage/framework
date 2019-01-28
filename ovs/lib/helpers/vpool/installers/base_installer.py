# Copyright (C) 2019 iNuron NV
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


import re
import logging
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.vpoollist import VPoolList
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.storageserver.storagedriver import StorageDriverClient
from ovs.extensions.storageserver.storagedriver import ClusterNodeConfig


class VPoolInstallerBase(object):
    """
    This container class provides some basic information that is shared across vpool installers for create and extend, but also shrinking of vpools
    """

    _logger = logging.getLogger(__name__)

    def __init__(self, name):
        """
        Initialize a vPool container class instance containing information about:
            - vPool information on which a new StorageDriver is going to be deployed, eg: global vPool configurations, vPool name, ...
            - Information about caching behavior
            - Information about which ALBA Backends to use as main Backend, fragment cache Backend, block cache Backend
            - Connection information about how to reach the ALBA Backends via the API
            - StorageDriver configuration settings
            - The storage IP address
        """
        if not re.match(pattern=ExtensionsToolbox.regex_vpool, string=name):
            raise ValueError('Incorrect vPool name provided')

        self.name = name
        self.vpool = VPoolList.get_vpool_by_name(vpool_name=name)
        self.sd_installer = None
        self.sr_installer = None
        self.storagedriver_amount = 0 if self.vpool is None else len(self.vpool.storagedrivers)

    def validate(self, storagerouter=None, storagedriver=None):
        """
        Perform some validations before creating or extending a vPool
        :param storagerouter: StorageRouter on which the vPool will be created or extended
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :param storagedriver: When passing a StorageDriver, perform validations when shrinking a vPool
        :type storagedriver: ovs.dal.hybrids.storagedriver.StorageDriver
        :raises ValueError: If extending a vPool which status is not RUNNING
                RuntimeError: If this vPool's configuration does not meet the requirements
                              If the vPool has already been extended on the specified StorageRouter
        :return: None
        :rtype: NoneType
        """
        if self.vpool is not None:
            if self.vpool.status != VPool.STATUSES.RUNNING:
                raise ValueError('vPool should be in {0} status'.format(VPool.STATUSES.RUNNING))

            ExtensionsToolbox.verify_required_params(actual_params=self.vpool.configuration,
                                                     required_params={'sco_size': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.keys()),
                                                                      'dtl_mode': (str, StorageDriverClient.VPOOL_DTL_MODE_MAP.keys()),
                                                                      'write_buffer': (float, None),
                                                                      'dtl_transport': (str, StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP.keys()),
                                                                      'tlog_multiplier': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.values())})

    def configure_cluster_registry(self, exclude=None, apply_on=None, allow_raise=False):
        """
        Retrieve the cluster node configurations for the StorageDrivers related to the vPool without the excluded StorageDrivers
        :param exclude: List of StorageDrivers to exclude from the node configurations
        :type exclude: list
        :param apply_on: Apply the updated cluster configurations on these StorageDrivers (or all but current if none provided)
        :type apply_on: list[ovs.dal.hybrids.storagedriver.StorageDriver]
        :param allow_raise: Allow the function to raise an exception instead of returning True when an exception occurred (Defaults to False)
        :type allow_raise: bool
        :raises Exception: When allow_raises is True and and updating the configuration would have failed
        :return: A boolean indication whether something failed
        :rtype: bool
        """
        if exclude is None:
            exclude = []
        if apply_on is None:
            apply_on = []
        try:
            node_configs = []
            for sd in self.vpool.storagedrivers:
                if sd in exclude:
                    continue
                sd.invalidate_dynamics('cluster_node_config')
                node_configs.append(ClusterNodeConfig(**sd.cluster_node_config))

            self.vpool.clusterregistry_client.set_node_configs(node_configs)
            for sd in apply_on or self.vpool.storagedrivers:
                if sd == self.sd_installer.storagedriver:
                    continue
                self._logger.info('Applying cluster node config for StorageDriver {0}'.format(sd.storagedriver_id))
                self.vpool.storagedriver_client.update_cluster_node_configs(str(sd.storagedriver_id), req_timeout_secs=10)
            return False
        except Exception:
            self._logger.exception('Updating the cluster node configurations failed')
            if allow_raise is True:
                raise
            return True

    def update_status(self, status):
        """
        Update the status of the vPool
        :param status: Status to set on the vPool
        :type status: ovs.dal.hybrids.vpool.VPool.STATUSES
        :raises ValueError: If unsupported status has been provided
        :return: None
        :rtype: NoneType
        """
        if status not in VPool.STATUSES:
            raise ValueError('Allowed statuses are: {0}'.format(', '.join(VPool.STATUSES)))

        self.vpool.status = status
        self.vpool.save()

