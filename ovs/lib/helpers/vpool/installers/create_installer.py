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


from ovs.dal.hybrids.vpool import VPool
from ovs_extensions.constants.vpools import MDS_CONFIG_PATH
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.lib.helpers.vpool.installers.installer import VPoolInstaller


class CreateInstaller(VPoolInstaller):
    """"
    VPool create specific installer instance
    """

    def __init__(self, name):
        super(CreateInstaller, self).__init__(name)
        self.is_new = True

    def ensure_exists(self, mds_config_params, *args, **kwargs):
        """
        Ensure that the VPool DAL object exists.
        :param mds_config_params: MDS config params provided by the API call
        :return: None
        """
        self.create(rdma_enabled=self.sd_installer.rdma_enabled)
        self.configure_mds(mds_config_params)

    def configure_mds(self, config):
        """
        Configure the global MDS settings for this vPool
        :param config: MDS configuration settings (Can contain amount of tlogs to wait for during MDS checkup, MDS safety and the maximum load for an MDS)
        :type config: dict
        :raises RuntimeError: If specified safety not between 1 and 5
                              If specified amount of tlogs is less than 1
                              If specified maximum load is less than 10%
        :return: None
        :rtype: NoneType
        """
        if self.vpool is None:
            raise RuntimeError('Cannot configure MDS settings when no vPool has been created yet')

        ExtensionsToolbox.verify_required_params(verify_keys=True,
                                                 actual_params=config,
                                                 required_params={'mds_tlogs': (int, {'min': 1}, False),
                                                                  'mds_safety': (int, {'min': 1, 'max': 5}, False),
                                                                  'mds_maxload': (int, {'min': 10}, False)})

        # Don't set a default value here, because we need to know whether these values have been specifically set or were set at None
        self.mds_tlogs = config.get('mds_tlogs')
        self.mds_safety = config.get('mds_safety')
        self.mds_maxload = config.get('mds_maxload')
        Configuration.set(key=MDS_CONFIG_PATH.format(self.vpool.guid),
                          value={'mds_tlogs': self.mds_tlogs or 100,
                                 'mds_safety': self.mds_safety or 3,
                                 'mds_maxload': self.mds_maxload or 75})

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
        try:
            super(CreateInstaller, self).configure_cluster_registry(exclude, apply_on, allow_raise)
        except:
            self.revert_vpool(status=VPool.STATUSES.RUNNING)
            raise
