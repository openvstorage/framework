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
VPoolController class responsible for making changes to existing vPools
VpoolInstaller class responsible for adding/removing vPools
"""

import re
import copy
import json
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.vpoollist import VPoolList
from ovs_extensions.api.client import OVSClient
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.sshclient import SSHClient
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.storagedriver import ClusterNodeConfig, StorageDriverClient, StorageDriverConfiguration
from ovs.lib.helpers.decorators import log
from ovs.lib.helpers.decorators import ovs_task


class VPoolInstaller(object):
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
    _logger = Logger('lib')

    def __init__(self, name):
        """
        Initialize a StorageDriverInstaller class instance containing information about:
            - vPool information on which a new StorageDriver is going to be deployed, eg: global vPool configurations, vPool name, ...
            - Information about caching behavior
            - Information about which ALBA Backends to use as main Backend, fragment cache Backend, block cache Backend
            - Connection information about how to reach the ALBA Backends via the API
            - StorageDriver configuration settings
            - The storage IP address
        """
        if not re.match(pattern=name, string=ExtensionsToolbox.regex_vpool):
            raise ValueError('Incorrect vPool name provided')

        self.name = name
        self.vpool = VPoolList.get_vpool_by_name(vpool_name=name)
        self.is_new = True if self.vpool is None else False
        self.mds_tlogs = None
        self.mds_safety = None
        self.mds_maxload = None
        self.sd_installer = None
        self.sr_installer = None
        self.connection_info = None
        self.complete_backend_info = {}  # Used to store the Backend information retrieved via the API in a dict, because used in several places

    def create(self, **kwargs):
        """
        Create a new vPool instance
        :raises RuntimeError: If a vPool has already been found with the name specified in the constructor
        :return: None
        :rtype: NoneType
        """
        if self.vpool is not None:
            raise RuntimeError('vPool with name {0} has already been created'.format(self.vpool.name))
        if self.connection_info is None:
            raise RuntimeError('Connection information to the Backend for this vPool is unknown')

        self.vpool = VPool()
        self.vpool.name = self.name
        self.vpool.login = self.connection_info['client_id']
        self.vpool.status = VPool.STATUSES.INSTALLING
        self.vpool.password = self.connection_info['client_secret']
        self.vpool.metadata = {}
        self.vpool.connection = '{0}:{1}'.format(self.connection_info['host'], self.connection_info['port'])
        self.vpool.description = self.name
        self.vpool.rdma_enabled = kwargs.get('rdma_enabled', False)
        self.vpool.metadata_store_bits = 5
        self.vpool.save()

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
        Configuration.set(key='/ovs/vpools/{0}/mds_config'.format(self.vpool.guid),
                          value={'mds_tlogs': self.mds_tlogs or 100,
                                 'mds_safety': self.mds_safety or 3,
                                 'mds_maxload': self.mds_maxload or 75})

    def validate(self, storagerouter):
        """
        Perform some validations before creating or extending a vPool
        :param storagerouter: StorageRouter on which the vPool will be created or extended
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
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

            for vpool_storagedriver in self.vpool.storagedrivers:
                if vpool_storagedriver.storagerouter_guid == storagerouter.guid:
                    raise RuntimeError('A StorageDriver is already linked to this StorageRouter for vPool {0}'.format(self.vpool.name))

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
                if Configuration.dir_exists(key='/ovs/vpools/{0}'.format(self.vpool.guid)):
                    Configuration.delete(key='/ovs/vpools/{0}'.format(self.vpool.guid))
        elif status == VPool.STATUSES.FAILURE:
            # In case of failure status the cluster registry settings have already been adapted, so revert
            self.configure_cluster_registry(exclude=[self.sd_installer.storagedriver])

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
        def _refresh_arakoon_metadata(client, info):
            return {'arakoon_config': VPoolController.retrieve_alba_arakoon_config(alba_backend_guid=info['alba_backend_guid'],
                                                                                   ovs_client=client)}

        def _refresh_backend_metadata(client, info):
            preset_name = info['preset']
            alba_backend_guid = info['alba_backend_guid']
            backend_dict = client.get(api='/alba/backends/{0}/'.format(alba_backend_guid), params={'contents': 'name,usages,presets,backend,remote_stack'})  # Remote stack is used in calculate_read_preferences
            preset_info = dict((preset['name'], preset) for preset in backend_dict['presets'])
            if preset_name not in preset_info:
                raise RuntimeError('Given preset {0} is not available for ALBA Backend {1}'.format(preset_name, backend_dict['name']))

            policies = []
            for policy_info in preset_info[preset_name]['policies']:
                policy = json.loads('[{0}]'.format(policy_info.strip('()')))
                policies.append(policy)

            self.complete_backend_info[alba_backend_guid] = backend_dict
            return {'name': backend_dict['name'],
                    'scaling': backend_dict['scaling'],
                    'policies': policies,
                    'frag_size': float(preset_info[preset_name]['fragment_size']),
                    'total_size': float(backend_dict['usages']['size']),
                    'backend_guid': backend_dict['backend_guid']}


        if self.sr_installer is None or self.sd_installer is None:
            raise RuntimeError('No StorageRouterInstaller or StorageDriverInstaller instance found')

        # Create caching info object for current StorageRouter
        sr_guid = self.sr_installer.storagerouter.guid
        caching_info = {StorageDriverConfiguration.CACHE_BLOCK: {'read': self.sd_installer.block_cache_on_read,
                                                                 'write': self.sd_installer.block_cache_on_write,
                                                                 'quota': self.sd_installer.block_cache_quota,
                                                                 'is_backend': self.sd_installer.block_cache_backend_info is not None},
                        StorageDriverConfiguration.CACHE_FRAGMENT: {'read': self.sd_installer.fragment_cache_on_read,
                                                                    'write': self.sd_installer.fragment_cache_on_write,
                                                                    'quota': self.sd_installer.fragment_cache_quota,
                                                                    'is_backend': self.sd_installer.fragment_cache_backend_info is not None}}
        if self.is_new is False:
            new_metadata = copy.deepcopy(self.vpool.metadata)

            # Refresh the arakoon information and backend metadata for StorageRouters which are already present in the metadata
            for storagerouter_guid, caching_data in new_metadata['caching_info'].iteritems():
                for cache_type, cache_type_data in caching_data.iteritems():
                    if cache_type_data['is_backend'] is True:
                        ovs_client = OVSClient.get_instance(connection_info=cache_type_data['backend_info']['connection_info'], cache_store=VolatileFactory.get_client())
                        cache_type_data['backend_info'].update(_refresh_backend_metadata(client=ovs_client, info=cache_type_data['backend_info']))
                        cache_type_data['backend_info'].update(_refresh_backend_metadata(client=ovs_client, info=cache_type_data['backend_info']))
            # Add new StorageRouter to the caching information
            new_metadata['caching_info'][sr_guid] = caching_info
        else:
            # Create new metadata object for new vPool
            new_metadata = {'backend': {'backend_info': {self.sd_installer.backend_info}},
                            'caching_info': {sr_guid: caching_info}}

        # Add arakoon information and backend metadata to the new caching information for current StorageRouter
        if self.sd_installer.block_cache_backend_info is not None:
            backend_info = self.sd_installer.block_cache_backend_info
            connection_info = self.sd_installer.block_cache_connection_info
            ovs_client = OVSClient.get_instance(connection_info=connection_info, cache_store=VolatileFactory.get_client())
            new_metadata['caching_info'][sr_guid][StorageDriverConfiguration.CACHE_BLOCK]['backend_info'] = backend_info
            new_metadata['caching_info'][sr_guid][StorageDriverConfiguration.CACHE_BLOCK]['backend_info'].update({'connection_info': connection_info})
            new_metadata['caching_info'][sr_guid][StorageDriverConfiguration.CACHE_BLOCK]['backend_info'].update(_refresh_backend_metadata(client=ovs_client, info=backend_info))
            new_metadata['caching_info'][sr_guid][StorageDriverConfiguration.CACHE_BLOCK]['backend_info'].update(_refresh_arakoon_metadata(client=ovs_client, info=backend_info))
        if self.sd_installer.fragment_cache_backend_info is not None:
            backend_info = self.sd_installer.fragment_cache_backend_info
            connection_info = self.sd_installer.fragment_cache_connection_info
            ovs_client = OVSClient.get_instance(connection_info=connection_info, cache_store=VolatileFactory.get_client())
            new_metadata['caching_info'][sr_guid][StorageDriverConfiguration.CACHE_FRAGMENT]['backend_info'] = backend_info
            new_metadata['caching_info'][sr_guid][StorageDriverConfiguration.CACHE_FRAGMENT]['backend_info'].update({'connection_info': connection_info})
            new_metadata['caching_info'][sr_guid][StorageDriverConfiguration.CACHE_FRAGMENT]['backend_info'].update(_refresh_backend_metadata(client=ovs_client, info=backend_info))
            new_metadata['caching_info'][sr_guid][StorageDriverConfiguration.CACHE_FRAGMENT]['backend_info'].update(_refresh_arakoon_metadata(client=ovs_client, info=backend_info))

        self.vpool.metadata = new_metadata
        self.vpool.save()

    def configure_cluster_registry(self, exclude=list()):
        """
        Retrieve the cluster node configurations for the StorageDrivers related to the vPool without the excluded StorageDrivers
        :param exclude: List of StorageDrivers to exclude from the node configurations
        :type exclude: list
        :return: List of ClusterNodeConfig objects
        :rtype: list
        """
        node_configs = []
        for sd in self.vpool.storagedrivers:
            if sd in exclude:
                continue
            sd.invalidate_dynamics('cluster_node_config')
            node_configs.append(ClusterNodeConfig(**sd.cluster_node_config))

        self.vpool.clusterregistry_client.set_node_configs(node_configs)
        for sd in self.vpool.storagedrivers:
            if sd == self.sd_installer.storagedriver:
                continue
            self.vpool.storagedriver_client.update_cluster_node_configs(str(sd.storagedriver_id), req_timeout_secs=10)

    def calculate_read_preferences(self):
        """
        Calculates the read preferences to be used by the ALBA proxy services
        :return: List with all read preferences
        :rtype: list
        """
        backends_to_check = {}
        local_backend_info = self.sd_installer.backend_info
        local_alba_backend_guid = local_backend_info['alba_backend_guid']
        if local_backend_info['connection_info'].get('local') is True and local_backend_info['scaling'] == 'GLOBAL' and local_alba_backend_guid in self.complete_backend_info:
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

class VPoolController(object):
    """
    Contains all BLL related to VPools
    """
    _logger = Logger('lib')

    @staticmethod
    @ovs_task(name='ovs.vpool.up_and_running')
    @log('VOLUMEDRIVER_TASK')
    def up_and_running(storagedriver_id):
        """
        Volumedriver informs us that the service is completely started. Post-start events can be executed
        :param storagedriver_id: ID of the storagedriver
        """
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        if storagedriver is None:
            raise RuntimeError('A Storage Driver with id {0} could not be found.'.format(storagedriver_id))
        storagedriver.startup_counter += 1
        storagedriver.save()

    # noinspection PyTypeChecker
    @staticmethod
    @ovs_task(name='ovs.storagerouter.create_hprm_config_files')
    def create_hprm_config_files(vpool_guid, local_storagerouter_guid, parameters):
        """
        Create the required configuration files to be able to make use of HPRM (aka PRACC)
        This configuration will be zipped and made available for download
        :param vpool_guid: The guid of the VPool for which a HPRM manager needs to be deployed
        :type vpool_guid: str
        :param local_storagerouter_guid: The guid of the StorageRouter the API was requested on
        :type local_storagerouter_guid: str
        :param parameters: Additional information required for the HPRM configuration files
        :type parameters: dict
        :return: Name of the zipfile containing the configuration files
        :rtype: str
        """
        # Validations
        required_params = {'port': (int, {'min': 1, 'max': 65535}),
                           'identifier': (str, ExtensionsToolbox.regex_vpool)}
        ExtensionsToolbox.verify_required_params(actual_params=parameters,
                                                 required_params=required_params)
        vpool = VPool(vpool_guid)
        identifier = parameters['identifier']
        config_path = None
        local_storagerouter = StorageRouter(local_storagerouter_guid)
        for sd in vpool.storagedrivers:
            if len(sd.alba_proxies) == 0:
                raise ValueError('No ALBA proxies configured for vPool {0} on StorageRouter {1}'.format(vpool.name,
                                                                                                        sd.storagerouter.name))
            config_path = '/ovs/vpools/{0}/proxies/{1}/config/{{0}}'.format(vpool.guid, sd.alba_proxies[0].guid)

        if config_path is None:
            raise ValueError('vPool {0} has not been extended any StorageRouter'.format(vpool.name))
        proxy_cfg = Configuration.get(key=config_path.format('main'))

        cache_info = {}
        arakoons = {}
        cache_types = VPool.CACHES.values()
        if not any(ctype in parameters for ctype in cache_types):
            raise ValueError('At least one cache type should be passed: {0}'.format(', '.join(cache_types)))
        for ctype in cache_types:
            if ctype not in parameters:
                continue
            required_dict = {'read': (bool, None),
                             'write': (bool, None)}
            required_params.update({ctype: (dict, required_dict)})
            ExtensionsToolbox.verify_required_params(actual_params=parameters, required_params=required_params)
            read = parameters[ctype]['read']
            write = parameters[ctype]['write']
            if read is False and write is False:
                cache_info[ctype] = ['none']
                continue
            path = parameters[ctype].get('path')
            if path is not None:
                path = path.strip()
                if not path or path.endswith('/.') or '..' in path or '/./' in path:
                    raise ValueError('Invalid path specified')
                required_dict.update({'path': (str, None),
                                      'size': (int, {'min': 1, 'max': 10 * 1024})})
                ExtensionsToolbox.verify_required_params(actual_params=parameters, required_params=required_params)
                while '//' in path:
                    path = path.replace('//', '/')
                cache_info[ctype] = ['local', {'path': path,
                                               'max_size': parameters[ctype]['size'] * 1024 ** 3,
                                               'cache_on_read': read,
                                               'cache_on_write': write}]
            else:
                required_dict.update({'backend_info': (dict, {'preset': (str, ExtensionsToolbox.regex_preset),
                                                              'alba_backend_guid': (str, ExtensionsToolbox.regex_guid),
                                                              'alba_backend_name': (str, ExtensionsToolbox.regex_backend)}),
                                      'connection_info': (dict, {'host': (str, ExtensionsToolbox.regex_ip, False),
                                                                 'port': (int, {'min': 1, 'max': 65535}, False),
                                                                 'client_id': (str, ExtensionsToolbox.regex_guid, False),
                                                                 'client_secret': (str, None, False)})})
                ExtensionsToolbox.verify_required_params(actual_params=parameters, required_params=required_params)
                connection_info = parameters[ctype]['connection_info']
                if connection_info['host']:  # Remote Backend for accelerated Backend
                    alba_backend_guid = parameters[ctype]['backend_info']['alba_backend_guid']
                    ovs_client = OVSClient.get_instance(connection_info=connection_info)
                    arakoon_config = VPoolController.retrieve_alba_arakoon_config(alba_backend_guid=alba_backend_guid,
                                                                                  ovs_client=ovs_client)
                    arakoons[ctype] = ArakoonClusterConfig.convert_config_to(arakoon_config, return_type='INI')
                else:  # Local Backend for accelerated Backend
                    alba_backend_name = parameters[ctype]['backend_info']['alba_backend_name']
                    if Configuration.exists(key='/ovs/arakoon/{0}-abm/config'.format(alba_backend_name),
                                            raw=True) is False:
                        raise ValueError('Arakoon cluster for ALBA Backend {0} could not be retrieved'.format(alba_backend_name))
                    arakoons[ctype] = Configuration.get(key='/ovs/arakoon/{0}-abm/config'.format(alba_backend_name),
                                                        raw=True)
                cache_info[ctype] = ['alba', {'albamgr_cfg_url': '/etc/hprm/{0}/{1}_cache_arakoon.ini'.format(identifier, ctype),
                                              'bucket_strategy': ['1-to-1', {'prefix': vpool.guid,
                                                                             'preset': parameters[ctype]['backend_info']['preset']}],
                                              'manifest_cache_size': proxy_cfg['manifest_cache_size'],
                                              'cache_on_read': read,
                                              'cache_on_write': write}]

        tgz_name = 'hprm_config_files_{0}_{1}.tgz'.format(identifier, vpool.name)
        config = {'ips': ['127.0.0.1'],
                  'port': parameters['port'],
                  'pracc': {'uds_path': '/var/run/hprm/{0}/uds_path'.format(identifier),
                            'max_clients': 1000,
                            'max_read_buf_size': 64 * 1024,  # Buffer size for incoming requests (in bytes)
                            'thread_pool_size': 64},  # Amount of threads
                  'transport': 'tcp',
                  'log_level': 'info',
                  'read_preference': proxy_cfg['read_preference'],
                  'albamgr_cfg_url': '/etc/hprm/{0}/arakoon.ini'.format(identifier),
                  'manifest_cache_size': proxy_cfg['manifest_cache_size']}
        file_contents_map = {}
        for ctype in cache_types:
            if ctype in cache_info:
                config['{0}_cache'.format(ctype)] = cache_info[ctype]
            if ctype in arakoons:
                file_contents_map['/opt/OpenvStorage/config/{0}/{1}_cache_arakoon.ini'.format(identifier, ctype)] = arakoons[ctype]
        file_contents_map.update({'/opt/OpenvStorage/config/{0}/config.json'.format(identifier): json.dumps(config, indent=4),
                                  '/opt/OpenvStorage/config/{0}/arakoon.ini'.format(identifier): Configuration.get(key=config_path.format('abm'), raw=True)})

        local_client = SSHClient(endpoint=local_storagerouter)
        local_client.dir_create(directories='/opt/OpenvStorage/config/{0}'.format(identifier))
        local_client.dir_create(directories='/opt/OpenvStorage/webapps/frontend/downloads')
        for file_name, contents in file_contents_map.iteritems():
            local_client.file_write(contents=contents, filename=file_name)
        local_client.run(command=['tar', '--transform', 's#^config/{0}#{0}#'.format(identifier),
                                  '-czf', '/opt/OpenvStorage/webapps/frontend/downloads/{0}'.format(tgz_name),
                                  'config/{0}'.format(identifier)])
        local_client.dir_delete(directories='/opt/OpenvStorage/config/{0}'.format(identifier))
        return tgz_name

    @staticmethod
    def retrieve_alba_arakoon_config(alba_backend_guid, ovs_client):
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
