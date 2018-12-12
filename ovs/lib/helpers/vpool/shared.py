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
from ovs.dal.lists.vpoollist import VPoolList
from ovs_extensions.constants.framework import REMOTE_CONFIG_BACKEND_INI, REMOTE_CONFIG_BACKEND_CONFIG, REMOTE_CONFIG_BACKEND_BASE
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration


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
        # type: (str, Optional[bool]) -> dict
        """
        Retrieves the local ALBA Arakoon configuration.
        WARNING: YOU DO NOT BELONG HERE, PLEASE MOVE TO YOUR OWN PLUGIN
        :param alba_backend_guid: Guid of the ALBA Backend
        :type alba_backend_guid: str
        :param as_ini: Save the config as an INI file for alba
        :type as_ini: bool
        :return: Arakoon configuration information
        :rtype: dict
        """
        cfg = Configuration.get(REMOTE_CONFIG_BACKEND_CONFIG.format(alba_backend_guid), default=None)
        if as_ini:
            cfg = ArakoonClusterConfig.convert_config_to(cfg, return_type='INI')
        return cfg

    @classmethod
    def sync_alba_arakoon_config(cls, alba_backend_guid, ovs_client):
        # type: (str, OVSClient) -> None
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
        #todo: Fix leak of backends not bein removed

        remote_config = cls._retrieve_remote_alba_arakoon_config(alba_backend_guid, ovs_client)
        current_config = Configuration.get(REMOTE_CONFIG_BACKEND_CONFIG.format(alba_backend_guid), default=None)
        if current_config != remote_config:
            transaction_id = Configuration.begin_transaction()
            Configuration.set(key=REMOTE_CONFIG_BACKEND_CONFIG.format(alba_backend_guid), value=remote_config, transaction=transaction_id)

            ini_config = ArakoonClusterConfig.convert_config_to(config=remote_config, return_type='INI')
            Configuration.set(key=REMOTE_CONFIG_BACKEND_INI.format(alba_backend_guid), value=ini_config, raw=True, transaction=transaction_id)
            Configuration.apply_transaction(transaction_id)


    @classmethod
    def calculate_abm_configs_in_use(cls):
        """
        Iterate over all vPools in the cluster and check which local or remote backend configs are still in use.
        :return:
        """
        present_remote_configs = dict([(key, REMOTE_CONFIG_BACKEND_CONFIG.format(key)) for key in list(Configuration.list(REMOTE_CONFIG_BACKEND_BASE))])
        in_use = set()
        cache_types = [StorageDriverConfiguration.CACHE_FRAGMENT, StorageDriverConfiguration.CACHE_BLOCK]

        for vpool in VPoolList.get_vpools():
            print vpool.name
            proxy_config_template = '/ovs/vpools/{0}/proxies/{{0}}/config/main'.format(vpool.guid)
            for std in vpool.storagedrivers:
                for proxy in std.alba_proxies:
                    cfg = Configuration.get(proxy_config_template.format(proxy.guid), default=None)
                    cfg_mgr_url = cfg['albamgr_cfg_url']  # type:str

                    if cfg_mgr_url not in present_remote_configs.values():
                        print cfg_mgr_url
                        in_use.add(Configuration.extract_key_from_path(cfg_mgr_url))
                    # Extract all albamgr_cfg_urls
                    for cache_type in cache_types:
                        cache_cfg = cfg.get(cache_type)
                        if len(cache_cfg) == 1:  # No backend caching for this type
                            continue
                        else:
                            cache_cfg_url = cache_cfg[1].get('albamgr_cfg_url')  # type:str
                            if cache_cfg_url not in present_remote_configs.values():
                                in_use.add(Configuration.extract_key_from_path(cache_cfg_url))
        return in_use