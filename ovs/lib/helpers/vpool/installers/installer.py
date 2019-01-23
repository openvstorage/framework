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

"""
VpoolInstaller class responsible for adding/removing vPools
"""

import copy
import json
from ovs_extensions.constants.vpools import VPOOL_BASE_PATH
from ovs.dal.hybrids.vpool import VPool
from ovs_extensions.api.client import OVSClient
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
from ovs.lib.helpers.vpool.shared import VPoolShared
from ovs.lib.helpers.vpool.installers.base_installer import VPoolInstallerBase


class VPoolInstaller(VPoolInstallerBase):
    """
    Class used to create/remove a vPool
    This class will be responsible for
        - __init__: Validations whether the specified configurations are valid
        - create: Creation of a vPool pure model-wise
        - configure_mds: Configure the global MDS settings for the vPool
        - validate: Validate whether a vPool with specified name can be created
        - update_status: Update the status of the vPool (eg: INSTALLING, RUNNING, FAILURE)
        - revert_vpool: If anything goes wrong during creation/extension we revert the vPool to previous clean state
        - refresh_metadata: Refresh the vPool's metadata (arakoon info, backend info, ...)
        - configure_cluster_registry: Configure the cluster registry
        - calculate_read_preferences: Retrieve the read preferences
    """

    def __init__(self, name):
        super(VPoolInstaller, self).__init__(name)

        self.is_new = True if self.vpool is None else False

        self.mds_tlogs = None
        self.mds_safety = None
        self.mds_maxload = None
        self.mds_services = []

        self.complete_backend_info = {}  # Used to store the Backend information retrieved via the API in a dict, because used in several places

    def create(self, **kwargs):
        """
        Create a new vPool instance
        :raises RuntimeError:
         - If a vPool has already been found with the name specified in the constructor
         - If no StorageDriverInstaller has been linked
        :return: None
        :rtype: NoneType
        """
        if self.vpool is not None:
            raise RuntimeError('vPool with name {0} has already been created'.format(self.vpool.name))
        if self.sd_installer is None:
            raise RuntimeError('Connection information to the Backend for this vPool is unknown')

        connection_info = self.sd_installer.connection_info
        self.vpool = VPool()
        self.vpool.name = self.name
        self.vpool.login = connection_info['client_id']
        self.vpool.status = VPool.STATUSES.INSTALLING
        self.vpool.password = connection_info['client_secret']
        self.vpool.metadata = {}
        self.vpool.connection = '{0}:{1}'.format(connection_info['host'], connection_info['port'])
        self.vpool.description = self.name
        self.vpool.rdma_enabled = kwargs.get('rdma_enabled', False)
        self.vpool.metadata_store_bits = 5
        self.vpool.save()

    def revert_vpool(self, status):
        """
        Remove the vPool being created or revert the vPool being extended
        :param status: Status to put the vPool in
        :type status: ovs.dal.hybrids.vpool.VPool.STATUSES
        :return: None
        :rtype: NoneType
        """
        self.vpool.status = status
        self.vpool.save()

        if status == VPool.STATUSES.RUNNING:
            if self.sr_installer is not None:
                try:
                    self.sr_installer.root_client.dir_delete(directories=self.sr_installer.created_dirs)
                except Exception:
                    self._logger.warning('Failed to clean up following directories: {0}'.format(', '.join(self.sr_installer.created_dirs)))

            if self.sd_installer is not None and self.sd_installer.storagedriver is not None:
                for sdp in self.sd_installer.storagedriver.partitions:
                    sdp.delete()
                for proxy in self.sd_installer.storagedriver.alba_proxies:
                    proxy.delete()
                self.sd_installer.storagedriver.delete()
            if len(self.vpool.storagedrivers) == 0:
                self.vpool.delete()
                if Configuration.dir_exists(key=VPOOL_BASE_PATH.format(self.vpool.guid)):
                    Configuration.delete(key=VPOOL_BASE_PATH.format(self.vpool.guid))
        elif status == VPool.STATUSES.FAILURE:
            # In case of failure status the cluster registry settings have already been adapted, so revert
            self.configure_cluster_registry(exclude=[self.sd_installer.storagedriver])

    def _refresh_backend_metadata(self, backend_info, connection_info):
        """
        Returns data about the backend. Used to store all required data in the metadata property of a vPool
        :param backend_info: Information about the backend (should contain the alba_backend_guid and the preset_name)
        :param connection_info: Information about the cluster to find the backend on (host, port, client_id, client_secret)
        :return: Filled backend info
        Structure:
        {
            'name': <ALBA Backend name>,
            'preset': <preset name>,
            'scaling': LOCAL|GLOBAL,
            'policies': <policies>,
            'sco_size': <sco size>,
            'frag_size': <fragment cache size>,
            'total_size': <total ALBA Backend size>,
            'backend_guid': <Backend guid>,
            'alba_backend_guid': <ALBA Backend guid>,
            'connection_info': {
                'host': <ip>,
                'port': <port>,
                'local': <bool indicating local ALBA backend>,
                'client_id': <client_id>,
                'client_secret': <client_secret>
            }
        }
        :rtype: dict
        """
        # Validation
        if self.is_new is True and self.sd_installer is None:
            raise RuntimeError('A StorageDriver installer is required when working with a new vPool')
        ExtensionsToolbox.verify_required_params(actual_params=backend_info,
                                                 required_params={'alba_backend_guid': (str, None),
                                                                  'preset': (str, None)})
        ovs_client = OVSClient.get_instance(connection_info=connection_info, cache_store=VolatileFactory.get_client())

        new_backend_info = copy.deepcopy(backend_info)
        preset_name = backend_info['preset']
        alba_backend_guid = backend_info['alba_backend_guid']
        VPoolShared.sync_alba_arakoon_config(alba_backend_guid=alba_backend_guid, ovs_client=ovs_client)
        arakoon_config = VPoolShared.retrieve_local_alba_arakoon_config(alba_backend_guid=alba_backend_guid)

        # Requesting the remote stack for re-use in calculate read preference
        backend_dict = ovs_client.get('/alba/backends/{0}/'.format(alba_backend_guid), params={'contents': 'name,usages,presets,backend,remote_stack'})
        self.complete_backend_info[alba_backend_guid] = backend_dict

        preset_info = dict((preset['name'], preset) for preset in backend_dict['presets'])
        if preset_name not in preset_info:
            raise RuntimeError('Given preset {0} is not available in backend {1}'.format(preset_name, backend_dict['name']))

        policies = []
        for policy_info in preset_info[preset_name]['policies']:
            policy = json.loads('[{0}]'.format(policy_info.strip('()')))
            policies.append(policy)
        # Get the sco_size
        if self.is_new is True:
            sco_size = self.sd_installer.sco_size * 1024.0 ** 2
        else:
            sco_size = self.vpool.configuration['sco_size'] * 1024.0 ** 2

        new_backend_info.update({'name': backend_dict['name'],
                                 'preset': preset_name,
                                 'scaling': backend_dict['scaling'],
                                 'policies': policies,
                                 'sco_size': sco_size,
                                 'frag_size': float(preset_info[preset_name]['fragment_size']),
                                 'total_size': float(backend_dict['usages']['size']),
                                 'backend_guid': backend_dict['backend_guid'],
                                 'alba_backend_guid': alba_backend_guid,
                                 'connection_info': connection_info,
                                 'arakoon_config': arakoon_config})  # It is still preferable to keep this in the metadata, as this way, envs can be rebuilt upon failure

        return new_backend_info

    def refresh_metadata(self):
        """
        Refreshes the metadata for a current vPool
        Metadata structure:
            {
                'backend': {
                    'backend_info': {
                        'name': <ALBA Backend name>,
                        'preset': <preset name>,
                        'scaling': LOCAL|GLOBAL,
                        'policies': <policies>,
                        'sco_size': <sco size>,
                        'frag_size': <fragment cache size>,
                        'total_size': <total ALBA Backend size>,
                        'backend_guid': <Backend guid>,
                        'arakoon_config': <arakoon_config>,
                        'alba_backend_guid': <ALBA Backend guid>,
                        'connection_info': {
                            'host': <ip>,
                            'port': <port>,
                            'local': <bool indicating local ALBA backend>,
                            'client_id': <client_id>,
                            'client_secret': <client_secret>
                        }
                    }
                },
                'caching_info': {
                    <storagerouter_guid>: {
                        'block_cache': {
                            'read': True|False,
                            'write': True|False,
                            'quota': <quota>,
                            'is_backend': True|False,
                            'backend_info': {                # Backend info only filled out when 'is_backend' is True for block cache
                                Data is identical to {'backend': 'backend_info': { Data } }
                            }
                        },
                        'fragment_cache': {
                            'read': True|False,
                            'write': True|False,
                            'quota': <quota>,
                            'is_backend': True|False,
                            'backend_info': {                # Backend info only filled out when 'is_backend' is True for fragment cache
                                Data is identical to {'backend': 'backend_info': { Data } }
                            }
                        }
                    },
                    ...  Additional section per StorageRouter on which the vPool has been extended
                }
            }
        :return: None
        :rtype: NoneType
        """
        if self.sr_installer is None or self.sd_installer is None:
            raise RuntimeError('No StorageRouterInstaller or StorageDriverInstaller instance found')

        # Create caching info object for current StorageRouter
        sr_guid = self.sr_installer.storagerouter.guid
        caching_info = {}
        for cache_type in [StorageDriverConfiguration.CACHE_BLOCK, StorageDriverConfiguration.CACHE_FRAGMENT]:
            cache_info = {'read': getattr(self.sd_installer, '{0}_on_read'.format(cache_type)),
                          'write': getattr(self.sd_installer, '{0}_on_write'.format(cache_type)),
                          'quota': getattr(self.sd_installer, '{0}_quota'.format(cache_type))}
            # Check for backend information
            is_backend = getattr(self.sd_installer, '{0}_backend_info'.format(cache_type)) is not None
            cache_info['is_backend'] = is_backend
            if is_backend is True:
                # Fill in the backend data
                cache_info['backend_info'] = self._refresh_backend_metadata(backend_info=getattr(self.sd_installer, '{0}_backend_info'.format(cache_type)),
                                                                            connection_info=getattr(self.sd_installer, '{0}_connection_info'.format(cache_type)))
            caching_info[cache_type] = cache_info
        # caching_info = {StorageDriverConfiguration.CACHE_BLOCK: {'read': self.sd_installer.block_cache_on_read,
        #                                                          'write': self.sd_installer.block_cache_on_write,
        #                                                          'quota': self.sd_installer.block_cache_quota,
        #                                                          'is_backend': self.sd_installer.block_cache_backend_info is not None},
        #                 StorageDriverConfiguration.CACHE_FRAGMENT: {'read': self.sd_installer.fragment_cache_on_read,
        #                                                             'write': self.sd_installer.fragment_cache_on_write,
        #                                                             'quota': self.sd_installer.fragment_cache_quota,
        #                                                             'is_backend': self.sd_installer.fragment_cache_backend_info is not None}}
        if self.is_new is False:
            new_metadata = copy.deepcopy(self.vpool.metadata)
            # Refresh the Arakoon information and backend metadata for StorageRouters which are already present in the metadata
            for storagerouter_guid, caching_data in new_metadata['caching_info'].iteritems():
                for cache_type, cache_type_data in caching_data.iteritems():
                    if cache_type_data['is_backend'] is True:
                        cache_type_data['backend_info'].update(self._refresh_backend_metadata(cache_type_data['backend_info'],
                                                                                              cache_type_data['backend_info']['connection_info']))
            # Add new StorageRouter to the caching information
            new_metadata['caching_info'][sr_guid] = caching_info
        else:
            # Create new metadata object for new vPool
            new_backend_info = self._refresh_backend_metadata(self.sd_installer.backend_info, self.sd_installer.connection_info)
            new_metadata = {'backend': {'backend_info': new_backend_info},
                            'caching_info': {sr_guid: caching_info}}

        self.vpool.metadata = new_metadata
        self._logger.debug('Refreshed metadata : {0}'.format(new_metadata))
        self.vpool.save()

    def calculate_read_preferences(self):
        """
        Calculates the read preferences to be used by the ALBA proxy services
        :return: List with all read preferences
        :rtype: list
        """
        backends_to_check = {}
        # Use vpool metadata as the source of data
        if self.vpool is None:
            raise RuntimeError('No vPool has been associated with this installer')
        if not self.vpool.metadata:
            self.refresh_metadata()
        local_backend_info = self.vpool.metadata['backend']['backend_info']
        local_connection_info = local_backend_info['connection_info']
        local_alba_backend_guid = local_backend_info['alba_backend_guid']
        if local_connection_info.get('local') is True and local_backend_info['scaling'] == 'GLOBAL' and local_alba_backend_guid in self.complete_backend_info:
            backends_to_check[local_alba_backend_guid] = self.complete_backend_info[local_alba_backend_guid]

        for sr_guid, caching_info in self.vpool.metadata['caching_info'].iteritems():
            for cache_type, cache_type_data in caching_info.iteritems():
                if cache_type_data['is_backend'] is False:
                    continue
                backend_info = cache_type_data['backend_info']
                alba_backend_guid = backend_info['alba_backend_guid']
                if backend_info['connection_info'].get('local') is True and backend_info['scaling'] == 'GLOBAL' and alba_backend_guid in self.complete_backend_info:
                    backends_to_check[alba_backend_guid] = self.complete_backend_info[alba_backend_guid]

        read_preferences = []
        for backend_dict in backends_to_check.itervalues():
            for node_id, value in backend_dict['remote_stack'].iteritems():
                if value.get('domain') is not None and value['domain']['guid'] in self.sr_installer.storagerouter.regular_domains:
                    read_preferences.append(node_id)
        return read_preferences

    def ensure_exists(self, *args, **kwargs):
        """
        This abstract implementation should be implemented in its two inheriting classes: createInstaller and extendInstaller.
        :return:
        """
        raise NotImplementedError('Should be implemented in Extend or Create VPool installer')