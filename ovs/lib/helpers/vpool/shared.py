# Copyright (C) 2017 iNuron NV
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
from ovs_extensions.constants.framework import REMOTE_CONFIG_BACKEND, REMOTE_CONFIG_BACKEND_INI
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig

class VPoolShared(object):
    """
    Collection of generic methods that can be shared for VPools
    This module takes care of some circular dependencies
    """

    @staticmethod
    def _retrieve_remote_alba_arakoon_config(alba_backend_guid, ovs_client):
        """
        Retrieve the ALBA Arakoon configuration
        WARNING: YOU DO NOT BELONG HERE, PLEASE MOVE TO YOUR OWN PLUGIN
        :param alba_backend_guid: Guid of the ALBA Backend
        :type alba_backend_guid: str
        :param ovs_client: OVS client object
        :type ovs_client: OVSClient
        :return: Arakoon configuration information
        :rtype: dict
        """
        task_id = ovs_client.get('/alba/backends/{0}/get_config_metadata'.format(alba_backend_guid))
        successful, arakoon_config = ovs_client.wait_for_task(task_id, timeout=300)
        if successful is False:
            raise RuntimeError('Could not load metadata from environment {0}'.format(ovs_client.ip))
        return arakoon_config

    @staticmethod
    def retrieve_local_alba_arakoon_config(alba_backend_guid, as_ini=False):
        """
        Retrieves the local ALBA Arakoon configuration.
        WARNING: YOU DO NOT BELONG HERE, PLEASE MOVE TO YOUR OWN PLUGIN
        :param alba_backend_guid: Guid of the ALBA Backend
        :type alba_backend_guid: str
        :return: Arakoon configuration information
        :rtype: dict
        """
        cfg =  Configuration.get(REMOTE_CONFIG_BACKEND.format(alba_backend_guid), default=None)
        if as_ini:
            cfg = ArakoonClusterConfig.convert_config_to(cfg, return_type='INI')
        return cfg

    @staticmethod
    def sync_alba_arakoon_config(alba_backend_guid, ovs_client):
        """
        Compares the remote and local config. Updates the local config if needed. Guarantees the latest greatest config
        WARNING: YOU DO NOT BELONG HERE, PLEASE MOVE TO YOUR OWN PLUGIN
        :param alba_backend_guid: Guid of the ALBA Backend
        :type alba_backend_guid: str
        :param ovs_client: OVS client object
        :type ovs_client: OVSClient
        :return: Arakoon configuration information
        :rtype: dict
        """
        remote_config = VPoolShared._retrieve_remote_alba_arakoon_config(alba_backend_guid, ovs_client)
        current_config = Configuration.get(REMOTE_CONFIG_BACKEND.format(alba_backend_guid), default=None)

        if current_config != remote_config:
            Configuration.set(REMOTE_CONFIG_BACKEND.format(alba_backend_guid), remote_config)

            ini_config = ArakoonClusterConfig.convert_config_to(config=remote_config, return_type='INI')
            Configuration.set(REMOTE_CONFIG_BACKEND_INI.format(alba_backend_guid), ini_config, raw=True)
        return remote_config

