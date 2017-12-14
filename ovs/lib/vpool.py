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
import time
from subprocess import CalledProcessError
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vdisklist import VDiskList
from ovs.dal.lists.vpoollist import VPoolList
from ovs_extensions.api.client import OVSClient
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig, ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.storagedriver import ClusterNodeConfig, LocalStorageRouterClient, StorageDriverClient, StorageDriverConfiguration
from ovs.lib.disk import DiskController
from ovs.lib.helpers.decorators import log
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.storagedriver import StorageDriverController, StorageDriverInstaller
from ovs.lib.storagerouter import StorageRouterController, StorageRouterInstaller
from ovs.lib.vdisk import VDiskController


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
    _service_manager = ServiceFactory.get_manager()

    @classmethod
    @ovs_task(name='ovs.storagerouter.add_vpool')
    def add_vpool(cls, parameters):
        """
        Add a vPool to the machine this task is running on
        :param parameters: Parameters for vPool creation
        :type parameters: dict
        :return: None
        :rtype: NoneType
        """
        # VALIDATIONS
        if not isinstance(parameters, dict):
            raise ValueError('Parameters passed to create a vPool should be of type dict')

        # Check StorageRouter existence
        storagerouter = StorageRouterList.get_by_ip(ip=parameters.get('storagerouter_ip'))
        if storagerouter is None:
            raise RuntimeError('Could not find StorageRouter')

        # Validate requested vPool configurations
        vp_installer = VPoolInstaller(name=parameters.get('vpool_name'))
        vp_installer.validate(storagerouter=storagerouter)

        # Validate requested StorageDriver configurations
        cls._logger.info('vPool {0}: Validating StorageDriver configurations'.format(vp_installer.name))
        sd_installer = StorageDriverInstaller(storage_ip=parameters.get('storage_ip'),
                                              vp_installer=vp_installer,
                                              caching_info=parameters.get('caching_info'),
                                              backend_info={'main': parameters.get('backend_info'),
                                                            StorageDriverConfiguration.CACHE_BLOCK: parameters.get('backend_info_bc'),
                                                            StorageDriverConfiguration.CACHE_FRAGMENT: parameters.get('backend_info_fc')},
                                              connection_info={'main': parameters.get('connection_info'),
                                                               StorageDriverConfiguration.CACHE_BLOCK: parameters.get('connection_info_bc'),
                                                               StorageDriverConfiguration.CACHE_FRAGMENT: parameters.get('connection_info_fc')},
                                              sd_configuration=parameters.get('config_params'))

        partitions_mutex = volatile_mutex('add_vpool_partitions_{0}'.format(storagerouter.guid))
        try:
            # VPOOL CREATION
            # Create the vPool as soon as possible in the process to be displayed in the GUI (INSTALLING/EXTENDING state)
            if vp_installer.is_new is True:
                vp_installer.create(rdma_enabled=sd_installer.rdma_enabled)
                vp_installer.configure_mds(config=parameters.get('mds_config_params'))
            else:
                vp_installer.update_status(status=VPool.STATUSES.EXTENDING)

            # ADDITIONAL VALIDATIONS
            # Check StorageRouter connectivity
            cls._logger.info('vPool {0}: Validating StorageRouter connectivity'.format(vp_installer.name))
            linked_storagerouters = [storagerouter]
            if vp_installer.is_new is False:
                linked_storagerouters += [sd.storagerouter for sd in vp_installer.vpool.storagedrivers]

            ip_client_map = {}
            offline_nodes = []
            for sr in linked_storagerouters:
                try:
                    ip_client_map[sr.ip] = {'ovs': SSHClient(endpoint=sr.ip, username='ovs'),
                                            'root': SSHClient(endpoint=sr.ip, username='root')}
                except UnableToConnectException:
                    if sr == storagerouter:
                        raise RuntimeError('Node on which the vPool is being {0} is not reachable'.format('created' if vp_installer.is_new is True else 'extended'))
                    offline_nodes.append(sr)  # We currently want to allow offline nodes while setting up or extend a vPool

            sr_installer = StorageRouterInstaller(root_client=ip_client_map[storagerouter.ip]['root'],
                                                  sd_installer=sd_installer,
                                                  vp_installer=vp_installer,
                                                  storagerouter=storagerouter)
            sd_installer.sr_installer = sr_installer
            vp_installer.sr_installer = sr_installer
            vp_installer.sd_installer = sd_installer

            # When 2 or more jobs simultaneously run on the same StorageRouter, we need to check and create the StorageDriver partitions in locked context
            partitions_mutex.acquire(wait=60)
            sr_installer.partition_info = StorageRouterController.get_partition_info(storagerouter_guid=storagerouter.guid)
            sr_installer.validate_vpool_extendable()
            sr_installer.validate_global_write_buffer(requested_size=parameters.get('writecache_size', 0))
            sr_installer.validate_local_cache_size(requested_proxies=parameters.get('parallelism', {}).get('proxies', 2))

            # MODEL STORAGEDRIVER AND PARTITION JUNCTIONS
            sd_installer.create()
            sd_installer.create_partitions()
        except Exception:
            cls._logger.exception('Something went wrong during the validation or modeling of vPool {0} on StorageRouter {1}'.format(vp_installer.name, storagerouter.name))
            vp_installer.revert_vpool(status=VPool.STATUSES.RUNNING)
            raise
        finally:
            partitions_mutex.release()

        cls._logger.info('vPool {0}: Refreshing metadata'.format(vp_installer.name))
        try:
            vp_installer.refresh_metadata()
        except Exception:
            # At this point still nothing irreversible has changed, so revert to RUNNING
            cls._logger.exception('vPool {0}: Refreshing metadata failed'.format(vp_installer.name))
            vp_installer.revert_vpool(status=VPool.STATUSES.RUNNING)
            raise

        # Arakoon setup
        counter = 0
        while counter < 300:
            try:
                if StorageDriverController.manual_voldrv_arakoon_checkup() is True:
                    break
            except Exception:
                cls._logger.exception('Arakoon checkup for voldrv cluster failed')
                vp_installer.revert_vpool(status=VPool.STATUSES.RUNNING)
                raise
            counter += 1
            time.sleep(1)
            if counter == 300:
                vp_installer.revert_vpool(status=VPool.STATUSES.RUNNING)
                raise RuntimeError('Arakoon checkup for the StorageDriver cluster could not be started')

        # Cluster registry
        vp_installer.configure_cluster_registry()
        try:
            vp_installer.configure_cluster_registry()
        except:
            cls._logger.exception('vPool {0}: Cluster registry configuration failed'.format(vp_installer.name))
            if vp_installer.is_new is True:
                vp_installer.revert_vpool(status=VPool.STATUSES.RUNNING)
            else:
                vp_installer.revert_vpool(status=VPool.STATUSES.FAILURE)
            raise

        # Configurations
        try:
            # Configure regular proxies and scrub proxies
            sd_installer.setup_proxy_configs()

            # Configure the StorageDriver service
            sd_installer.configure_storagedriver_service()

            DiskController.sync_with_reality(storagerouter.guid)
            MDSServiceController.prepare_mds_service(storagerouter=storagerouter, vpool=vp_installer.vpool)

            # Update the MDS safety if changed via API (vpool.configuration will be available at this point also for the newly added StorageDriver)
            vp_installer.vpool.invalidate_dynamics('configuration')
            if vp_installer.mds_safety is not None and vp_installer.vpool.configuration['mds_config']['mds_safety'] != vp_installer.mds_safety:
                Configuration.set(key='/ovs/vpools/{0}/mds_config|mds_safety'.format(vp_installer.vpool.guid), value=vp_installer.mds_safety)
        except:
            # From here on out we don't want to revert the vPool anymore, since it might break stuff even more, instead we just put it in FAILURE
            cls._logger.exception('vPool {0}: Configuration failed'.format(vp_installer.name))
            vp_installer.update_status(status=VPool.STATUSES.FAILURE)
            raise

        # Create and start watcher volumedriver, DTL, proxies and StorageDriver services
        try:
            sd_installer.start_services()
        except Exception:
            cls._logger.exception('vPool {0}: Creating and starting all services failed'.format(vp_installer.name))
            vp_installer.update_status(status=VPool.STATUSES.FAILURE)
            raise

        # Post creation/extension checkups
        try:
            mds_config_set = MDSServiceController.get_mds_storagedriver_config_set(vpool=vp_installer.vpool, offline_nodes=offline_nodes)
            for sr, clients in ip_client_map.iteritems():
                for current_storagedriver in [sd for sd in sr.storagedrivers if sd.vpool_guid == vp_installer.vpool.guid]:
                    storagedriver_config = StorageDriverConfiguration(vpool_guid=vp_installer.vpool.guid, storagedriver_id=current_storagedriver.storagedriver_id)
                    if storagedriver_config.config_missing is False:
                        # Filesystem section in StorageDriver configuration are all parameters used for vDisks created directly on the filesystem
                        # So when a vDisk gets created on the filesystem, these MDSes will be assigned to them
                        storagedriver_config.configure_filesystem(fs_metadata_backend_mds_nodes=mds_config_set[sr.guid])
                        storagedriver_config.save(client=clients['ovs'])

            # Everything's reconfigured, refresh new cluster configuration
            for current_storagedriver in vp_installer.vpool.storagedrivers:
                if current_storagedriver.storagerouter.ip not in ip_client_map:
                    continue
                vp_installer.vpool.storagedriver_client.update_cluster_node_configs(str(current_storagedriver.storagedriver_id), req_timeout_secs=10)
        except Exception:
            cls._logger.exception('vPool {0}: Updating the MDS node configuration or cluster node config failed'.format(vp_installer.name))
            vp_installer.update_status(status=VPool.STATUSES.FAILURE)
            raise

        # When a node is offline, we can run into errors, but also when 1 or more volumes are not running
        # Scheduled tasks below, so don't really care whether they succeed or not
        try:
            VDiskController.dtl_checkup(vpool_guid=vp_installer.vpool.guid, ensure_single_timeout=600)
        except:
            pass
        for vdisk in vp_installer.vpool.vdisks:
            try:
                MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid)
            except:
                pass
        vp_installer.update_status(status=VPool.STATUSES.RUNNING)
        cls._logger.info('Add vPool {0} ended successfully'.format(vp_installer.name))

    @classmethod
    @ovs_task(name='ovs.storagerouter.remove_storagedriver')
    def remove_storagedriver(cls, storagedriver_guid, offline_storage_router_guids=list()):
        """
        Removes a StorageDriver (if its the last StorageDriver for a vPool, the vPool is removed as well)
        :param storagedriver_guid: Guid of the StorageDriver to remove
        :type storagedriver_guid: str
        :param offline_storage_router_guids: Guids of StorageRouters which are offline and will be removed from cluster.
                                             WHETHER VPOOL WILL BE DELETED DEPENDS ON THIS
        :type offline_storage_router_guids: list
        :return: None
        :rtype: NoneType
        """
        storage_driver = StorageDriver(storagedriver_guid)
        cls._logger.info('StorageDriver {0} - Deleting StorageDriver {1}'.format(storage_driver.guid, storage_driver.name))

        #############
        # Validations
        vpool = storage_driver.vpool
        if vpool.status != VPool.STATUSES.RUNNING:
            raise ValueError('VPool should be in {0} status'.format(VPool.STATUSES.RUNNING))

        # Sync with reality to have a clear vision of vDisks
        VDiskController.sync_with_reality(storage_driver.vpool_guid)
        storage_driver.invalidate_dynamics('vdisks_guids')
        if len(storage_driver.vdisks_guids) > 0:
            raise RuntimeError('There are still vDisks served from the given StorageDriver')

        storage_router = storage_driver.storagerouter
        mds_services_to_remove = [mds_service for mds_service in vpool.mds_services if mds_service.service.storagerouter_guid == storage_router.guid]
        for mds_service in mds_services_to_remove:
            if len(mds_service.storagedriver_partitions) == 0 or mds_service.storagedriver_partitions[0].storagedriver is None:
                raise RuntimeError('Failed to retrieve the linked StorageDriver to this MDS Service {0}'.format(mds_service.service.name))

        cls._logger.info('StorageDriver {0} - Checking availability of related StorageRouters'.format(storage_driver.guid, storage_driver.name))
        client = None
        errors_found = False
        storage_drivers_left = False
        storage_router_online = True
        available_storage_drivers = []
        for sd in vpool.storagedrivers:
            sr = sd.storagerouter
            if sr != storage_router:
                storage_drivers_left = True
            try:
                temp_client = SSHClient(sr, username='root')
                if sr.guid in offline_storage_router_guids:
                    raise Exception('StorageRouter "{0}" passed as "offline StorageRouter" appears to be reachable'.format(sr.name))
                if sr == storage_router:
                    mtpt_pids = temp_client.run("lsof -t +D '/mnt/{0}' || true".format(vpool.name.replace(r"'", r"'\''")), allow_insecure=True).splitlines()
                    if len(mtpt_pids) > 0:
                        raise RuntimeError('vPool cannot be deleted. Following processes keep the vPool mount point occupied: {0}'.format(', '.join(mtpt_pids)))
                with remote(temp_client.ip, [LocalStorageRouterClient]) as rem:
                    sd_key = '/ovs/vpools/{0}/hosts/{1}/config'.format(vpool.guid, sd.storagedriver_id)
                    if Configuration.exists(sd_key) is True:
                        try:
                            path = Configuration.get_configuration_path(sd_key)
                            lsrc = rem.LocalStorageRouterClient(path)
                            lsrc.server_revision()  # 'Cheap' call to verify whether volumedriver is responsive
                            cls._logger.info('StorageDriver {0} - Available StorageDriver for migration - {1}'.format(storage_driver.guid, sd.name))
                            available_storage_drivers.append(sd)
                        except Exception as ex:
                            if 'ClusterNotReachableException' not in str(ex):
                                raise
                client = temp_client
                cls._logger.info('StorageDriver {0} - StorageRouter {1} with IP {2} is online'.format(storage_driver.guid, sr.name, sr.ip))
            except UnableToConnectException:
                if sr == storage_router or sr.guid in offline_storage_router_guids:
                    cls._logger.warning('StorageDriver {0} - StorageRouter {1} with IP {2} is offline'.format(storage_driver.guid, sr.name, sr.ip))
                    if sr == storage_router:
                        storage_router_online = False
                else:
                    raise RuntimeError('Not all StorageRouters are reachable')

        if client is None:
            raise RuntimeError('Could not find any responsive node in the cluster')

        ###############
        # Start removal
        if storage_drivers_left is True:
            vpool.status = VPool.STATUSES.SHRINKING
        else:
            vpool.status = VPool.STATUSES.DELETING
        vpool.save()

        available_sr_names = [sd.storagerouter.name for sd in available_storage_drivers]
        unavailable_sr_names = [sd.storagerouter.name for sd in vpool.storagedrivers if sd not in available_storage_drivers]
        cls._logger.info('StorageDriver {0} - StorageRouters on which an available StorageDriver runs: {1}'.format(storage_driver.guid, ', '.join(available_sr_names)))
        if unavailable_sr_names:
            cls._logger.warning('StorageDriver {0} - StorageRouters on which a StorageDriver is unavailable: {1}'.format(storage_driver.guid, ', '.join(unavailable_sr_names)))

        # Remove stale vDisks
        voldrv_vdisks = [entry.object_id() for entry in vpool.objectregistry_client.get_all_registrations()]
        voldrv_vdisk_guids = VDiskList.get_in_volume_ids(voldrv_vdisks).guids
        for vdisk_guid in set(vpool.vdisks_guids).difference(set(voldrv_vdisk_guids)):
            cls._logger.warning('vDisk with guid {0} does no longer exist on any StorageDriver linked to vPool {1}, deleting...'.format(vdisk_guid, vpool.name))
            VDiskController.clean_vdisk_from_model(vdisk=VDisk(vdisk_guid))

        # Un-configure or reconfigure the MDSes
        cls._logger.info('StorageDriver {0} - Reconfiguring MDSes'.format(storage_driver.guid))
        vdisks = []
        for mds in mds_services_to_remove:
            for junction in mds.vdisks:
                vdisk = junction.vdisk
                if vdisk in vdisks:
                    continue
                vdisks.append(vdisk)
                vdisk.invalidate_dynamics(['info', 'storagedriver_id'])
                if vdisk.storagedriver_id:
                    try:
                        cls._logger.debug('StorageDriver {0} - vDisk {1} {2} - Ensuring MDS safety'.format(storage_driver.guid, vdisk.guid, vdisk.name))
                        MDSServiceController.ensure_safety(vdisk_guid=vdisk.guid,
                                                           excluded_storagerouter_guids=[storage_router.guid] + offline_storage_router_guids)
                    except Exception:
                        cls._logger.exception('StorageDriver {0} - vDisk {1} {2} - Ensuring MDS safety failed'.format(storage_driver.guid, vdisk.guid, vdisk.name))

        # Validate that all MDSes on current StorageRouter have been moved away
        # Ensure safety does not always throw an error, that's why we perform this check here instead of in the Exception clause of above code
        vdisks = []
        for mds in mds_services_to_remove:
            for junction in mds.vdisks:
                vdisk = junction.vdisk
                if vdisk in vdisks:
                    continue
                vdisks.append(vdisk)
                cls._logger.critical('StorageDriver {0} - vDisk {1} {2} - MDS Services have not been migrated away'.format(storage_driver.guid, vdisk.guid, vdisk.name))
        if len(vdisks) > 0:
            # Put back in RUNNING, so it can be used again. Errors keep on displaying in GUI now anyway
            vpool.status = VPool.STATUSES.RUNNING
            vpool.save()
            raise RuntimeError('Not all MDS Services have been successfully migrated away')

        # Disable and stop DTL, voldrv and albaproxy services
        if storage_router_online is True:
            dtl_service = 'dtl_{0}'.format(vpool.name)
            voldrv_service = 'volumedriver_{0}'.format(vpool.name)
            client = SSHClient(storage_router, username='root')

            for service in [voldrv_service, dtl_service]:
                try:
                    if cls._service_manager.has_service(service, client=client):
                        cls._logger.debug('StorageDriver {0} - Stopping service {1}'.format(storage_driver.guid, service))
                        cls._service_manager.stop_service(service, client=client)
                        cls._logger.debug('StorageDriver {0} - Removing service {1}'.format(storage_driver.guid, service))
                        cls._service_manager.remove_service(service, client=client)
                except Exception:
                    cls._logger.exception('StorageDriver {0} - Disabling/stopping service {1} failed'.format(storage_driver.guid, service))
                    errors_found = True

            sd_config_key = '/ovs/vpools/{0}/hosts/{1}/config'.format(vpool.guid, storage_driver.storagedriver_id)
            if storage_drivers_left is False and Configuration.exists(sd_config_key):
                try:
                    for proxy in storage_driver.alba_proxies:
                        if cls._service_manager.has_service(proxy.service.name, client=client):
                            cls._logger.debug('StorageDriver {0} - Starting proxy {1}'.format(storage_driver.guid, proxy.service.name))
                            cls._service_manager.start_service(proxy.service.name, client=client)
                            tries = 10
                            running = False
                            port = proxy.service.ports[0]
                            while running is False and tries > 0:
                                cls._logger.debug('StorageDriver {0} - Waiting for the proxy {1} to start up'.format(storage_driver.guid, proxy.service.name))
                                tries -= 1
                                time.sleep(10 - tries)
                                try:
                                    client.run(['alba', 'proxy-statistics', '--host', storage_driver.storage_ip, '--port', str(port)])
                                    running = True
                                except CalledProcessError as ex:
                                    cls._logger.error('StorageDriver {0} - Fetching alba proxy-statistics failed with error (but ignoring): {1}'.format(storage_driver.guid, ex))
                            if running is False:
                                raise RuntimeError('Alba proxy {0} failed to start'.format(proxy.service.name))
                            cls._logger.debug('StorageDriver {0} - Alba proxy {0} running'.format(storage_driver.guid, proxy.service.name))

                    cls._logger.debug('StorageDriver {0} - Destroying filesystem and erasing node configs'.format(storage_driver.guid))
                    with remote(client.ip, [LocalStorageRouterClient], username='root') as rem:
                        path = Configuration.get_configuration_path(sd_config_key)
                        storagedriver_client = rem.LocalStorageRouterClient(path)
                        try:
                            storagedriver_client.destroy_filesystem()
                        except RuntimeError as rte:
                            # If backend has already been deleted, we cannot delete the filesystem anymore --> storage leak!!!
                            if 'MasterLookupResult.Error' not in rte.message:
                                raise

                    # noinspection PyArgumentList
                    vpool.clusterregistry_client.erase_node_configs()
                except RuntimeError:
                    cls._logger.exception('StorageDriver {0} - Destroying filesystem and erasing node configs failed'.format(storage_driver.guid))
                    errors_found = True

            for proxy in storage_driver.alba_proxies:
                service_name = proxy.service.name
                try:
                    if cls._service_manager.has_service(service_name, client=client):
                        cls._logger.debug('StorageDriver {0} - Stopping service {1}'.format(storage_driver.guid, service_name))
                        cls._service_manager.stop_service(service_name, client=client)
                        cls._logger.debug('StorageDriver {0} - Removing service {1}'.format(storage_driver.guid, service_name))
                        cls._service_manager.remove_service(service_name, client=client)
                except Exception:
                    cls._logger.exception('StorageDriver {0} - Disabling/stopping service {1} failed'.format(storage_driver.guid, service_name))
                    errors_found = True

        # Reconfigure cluster node configs
        if storage_drivers_left is True:
            try:
                cls._logger.info('StorageDriver {0} - Reconfiguring cluster node configs'.format(storage_driver.guid))
                node_configs = []
                for sd in vpool.storagedrivers:
                    if sd != storage_driver:
                        sd.invalidate_dynamics(['cluster_node_config'])
                        config = sd.cluster_node_config
                        if storage_driver.storagedriver_id in config['node_distance_map']:
                            del config['node_distance_map'][storage_driver.storagedriver_id]
                        node_configs.append(ClusterNodeConfig(**config))
                cls._logger.debug('StorageDriver {0} - Node configs - \n{1}'.format(storage_driver.guid, '\n'.join([str(config) for config in node_configs])))
                vpool.clusterregistry_client.set_node_configs(node_configs)
                for sd in available_storage_drivers:
                    if sd != storage_driver:
                        cls._logger.debug('StorageDriver {0} - StorageDriver {1} {2} - Updating cluster node configs'.format(storage_driver.guid, sd.guid, sd.name))
                        vpool.storagedriver_client.update_cluster_node_configs(str(sd.storagedriver_id), req_timeout_secs=10)
            except Exception:
                cls._logger.exception('StorageDriver {0} - Reconfiguring cluster node configs failed'.format(storage_driver.guid))
                errors_found = True

        # Removing MDS services
        cls._logger.info('StorageDriver {0} - Removing MDS services'.format(storage_driver.guid))
        for mds_service in mds_services_to_remove:
            # All MDSServiceVDisk object should have been deleted above
            try:
                cls._logger.debug('StorageDriver {0} - Remove MDS service (number {1}) for StorageRouter with IP {2}'.format(storage_driver.guid, mds_service.number, storage_router.ip))
                MDSServiceController.remove_mds_service(mds_service=mds_service,
                                                        reconfigure=False,
                                                        allow_offline=not storage_router_online)
            except Exception:
                cls._logger.exception('StorageDriver {0} - Removing MDS service failed'.format(storage_driver.guid))
                errors_found = True

        # Clean up directories and files
        dirs_to_remove = [storage_driver.mountpoint]
        for sd_partition in storage_driver.partitions[:]:
            dirs_to_remove.append(sd_partition.path)
            sd_partition.delete()

        for proxy in storage_driver.alba_proxies:
            config_tree = '/ovs/vpools/{0}/proxies/{1}'.format(vpool.guid, proxy.guid)
            Configuration.delete(config_tree)

        if storage_router_online is True:
            # Cleanup directories/files
            cls._logger.info('StorageDriver {0} - Deleting vPool related directories and files'.format(storage_driver.guid))
            try:
                mountpoints = StorageRouterController.get_mountpoints(client)
                for dir_name in dirs_to_remove:
                    if dir_name and client.dir_exists(dir_name) and dir_name not in mountpoints and dir_name != '/':
                        client.dir_delete(dir_name)
            except Exception:
                cls._logger.exception('StorageDriver {0} - Failed to retrieve mount point information or delete directories'.format(storage_driver.guid))
                cls._logger.warning('StorageDriver {0} - Following directories should be checked why deletion was prevented: {1}'.format(storage_driver.guid, ', '.join(dirs_to_remove)))
                errors_found = True

            cls._logger.debug('StorageDriver {0} - Synchronizing disks with reality'.format(storage_driver.guid))
            try:
                DiskController.sync_with_reality(storage_router.guid)
            except Exception:
                cls._logger.exception('StorageDriver {0} - Synchronizing disks with reality failed'.format(storage_driver.guid))
                errors_found = True

        Configuration.delete('/ovs/vpools/{0}/hosts/{1}'.format(vpool.guid, storage_driver.storagedriver_id))

        # Model cleanup
        cls._logger.info('StorageDriver {0} - Cleaning up model'.format(storage_driver.guid))
        for proxy in storage_driver.alba_proxies:
            cls._logger.debug('StorageDriver {0} - Removing alba proxy service {1} from model'.format(storage_driver.guid, proxy.service.name))
            service = proxy.service
            proxy.delete()
            service.delete()

        sd_can_be_deleted = True
        if storage_drivers_left is False:
            for relation in ['mds_services', 'storagedrivers', 'vdisks']:
                expected_amount = 1 if relation == 'storagedrivers' else 0
                if len(getattr(vpool, relation)) > expected_amount:
                    sd_can_be_deleted = False
                    break
        else:
            metadata_key = 'backend_aa_{0}'.format(storage_router.guid)
            if metadata_key in vpool.metadata:
                vpool.metadata.pop(metadata_key)
                vpool.save()
            metadata_key = 'backend_bc_{0}'.format(storage_router.guid)
            if metadata_key in vpool.metadata:
                vpool.metadata.pop(metadata_key)
                vpool.save()
            cls._logger.debug('StorageDriver {0} - Checking DTL for all vDisks in vPool {1} with guid {2}'.format(storage_driver.guid, vpool.name, vpool.guid))
            try:
                VDiskController.dtl_checkup(vpool_guid=vpool.guid, ensure_single_timeout=600)
            except Exception:
                cls._logger.exception('StorageDriver {0} - DTL checkup failed for vPool {1} with guid {2}'.format(storage_driver.guid, vpool.name, vpool.guid))

        if sd_can_be_deleted is True:
            storage_driver.delete()
            if storage_drivers_left is False:
                cls._logger.info('StorageDriver {0} - Removing vPool from model'.format(storage_driver.guid))
                vpool.delete()
                Configuration.delete('/ovs/vpools/{0}'.format(vpool.guid))
        else:
            try:
                vpool.delete()  # Try to delete the vPool to invoke a proper stacktrace to see why it can't be deleted
            except Exception:
                errors_found = True
                cls._logger.exception('StorageDriver {0} - Cleaning up vPool from the model failed'.format(storage_driver.guid))

        cls._logger.info('StorageDriver {0} - Running MDS checkup'.format(storage_driver.guid))
        try:
            MDSServiceController.mds_checkup()
        except Exception:
            cls._logger.exception('StorageDriver {0} - MDS checkup failed'.format(storage_driver.guid))

        if errors_found is True:
            if storage_drivers_left is True:
                vpool.status = VPool.STATUSES.FAILURE
                vpool.save()
            raise RuntimeError('1 or more errors occurred while trying to remove the StorageDriver. Please check the logs for more information')
        if storage_drivers_left is True:
            vpool.status = VPool.STATUSES.RUNNING
            vpool.save()
        cls._logger.info('StorageDriver {0} - Deleted StorageDriver {1}'.format(storage_driver.guid, storage_driver.name))
        if len(VPoolList.get_vpools()) == 0:
            cluster_name = ArakoonInstaller.get_cluster_name('voldrv')
            if ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)['internal'] is True:
                cls._logger.debug('StorageDriver {0} - Removing Arakoon cluster {1}'.format(storage_driver.guid, cluster_name))
                try:
                    installer = ArakoonInstaller(cluster_name=cluster_name)
                    installer.load()
                    installer.delete_cluster()
                except Exception:
                    cls._logger.exception('StorageDriver {0} - Delete voldrv Arakoon cluster failed'.format(storage_driver.guid))
                service_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON)
                service_name = ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name)
                for service in list(service_type.services):
                    if service.name == service_name:
                        service.delete()

        if len(storage_router.storagedrivers) == 0 and storage_router_online is True:  # ensure client is initialized for StorageRouter
            try:
                if cls._service_manager.has_service(ServiceFactory.SERVICE_WATCHER_VOLDRV, client=client):
                    cls._service_manager.stop_service(ServiceFactory.SERVICE_WATCHER_VOLDRV, client=client)
                    cls._service_manager.remove_service(ServiceFactory.SERVICE_WATCHER_VOLDRV, client=client)
            except Exception:
                cls._logger.exception('StorageDriver {0} - {1} deletion failed'.format(storage_driver.guid, ServiceFactory.SERVICE_WATCHER_VOLDRV))

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
