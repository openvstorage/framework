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
StorageDriverInstaller class responsible for adding and removing StorageDrivers
"""

import re
import copy
import json
import time
from subprocess import CalledProcessError
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.j_albaproxy import AlbaProxy
from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
from ovs.dal.hybrids.service import Service
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs_extensions.constants.framework import REMOTE_CONFIG_BACKEND_INI
from ovs_extensions.constants.vpools import PROXY_PATH, GENERIC_SCRUB, HOSTS_PATH, HOSTS_CONFIG_PATH, PROXY_CONFIG_MAIN
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.system import System
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.packages.packagefactory import PackageFactory
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storageserver.storagedriver import LocalStorageRouterClient, StorageDriverClient, StorageDriverConfiguration
from ovs.lib.storagedriver import StorageDriverController
from ovs.lib.helpers.vpool.shared import VPoolShared

class StorageDriverInstaller(object):
    """
    Class used to create/remove a StorageDriver to/from a StorageRouter
    This class will be responsible for
        - __init__: Validations whether the specified configurations are valid
        - create: Creation of a StorageDriver pure model-wise
        - create_partitions: Create StorageDriverPartition junctions in the model
        - setup_proxy_configs: Create the configurations for the proxy services and put them in the configuration management
        - configure_storagedriver_service: Make all the necessary configurations to the StorageDriverConfiguration for this StorageDriver
        - start_services: Start the services required for a healthy StorageDriver
        - stop_services: Stop the services related to a StorageDriver
    """
    SERVICE_TEMPLATE_SD = 'ovs-volumedriver'
    SERVICE_TEMPLATE_DTL = 'ovs-dtl'
    SERVICE_TEMPLATE_PROXY = 'ovs-albaproxy'

    _logger = Logger('lib')

    def __init__(self, vp_installer, configurations=None, storagedriver=None):
        """
        Initialize a StorageDriverInstaller class instance containing information about:
            - vPool information on which a new StorageDriver is going to be deployed, eg: global vPool configurations, vPool name, ...
            - Information about caching behavior
            - Information about which ALBA Backends to use as main Backend, fragment cache Backend, block cache Backend
            - Connection information about how to reach the ALBA Backends via the API
            - StorageDriver configuration settings
            - The storage IP address
        """
        if (configurations is None and storagedriver is None) or (configurations is not None and storagedriver is not None):
            raise RuntimeError('Configurations and storagedriver are mutual exclusive options')

        self.sd_service = 'ovs-volumedriver_{0}'.format(vp_installer.name)
        self.dtl_service = 'ovs-dtl_{0}'.format(vp_installer.name)
        self.sr_installer = None
        self.vp_installer = vp_installer
        self.storagedriver = storagedriver
        self.service_manager = ServiceFactory.get_manager()

        # Validations
        if configurations is not None:
            storage_ip = configurations.get('storage_ip')
            caching_info = configurations.get('caching_info')
            backend_info = configurations.get('backend_info')
            connection_info = configurations.get('connection_info')
            sd_configuration = configurations.get('sd_configuration')

            if not re.match(pattern=ExtensionsToolbox.regex_ip, string=storage_ip):
                raise ValueError('Incorrect storage IP provided')

            ExtensionsToolbox.verify_required_params(actual_params=caching_info,
                                                     required_params={'cache_quota_bc': (int, None, False),
                                                                      'cache_quota_fc': (int, None, False),
                                                                      'block_cache_on_read': (bool, None),
                                                                      'block_cache_on_write': (bool, None),
                                                                      'fragment_cache_on_read': (bool, None),
                                                                      'fragment_cache_on_write': (bool, None)})

            ExtensionsToolbox.verify_required_params(actual_params=sd_configuration,
                                                     required_params={'advanced': (dict, {'number_of_scos_in_tlog': (int, {'min': 4, 'max': 20}),
                                                                                          'non_disposable_scos_factor': (float, {'min': 1.5, 'max': 20})},
                                                                                   False),
                                                                      'dtl_mode': (str, StorageDriverClient.VPOOL_DTL_MODE_MAP.keys()),
                                                                      'sco_size': (int, StorageDriverClient.TLOG_MULTIPLIER_MAP.keys()),
                                                                      'cluster_size': (int, StorageDriverClient.CLUSTER_SIZES),
                                                                      'write_buffer': (int, {'min': 128, 'max': 10240}),  # Volume write buffer
                                                                      'dtl_transport': (str, StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP.keys())})

            for section, backend_information in backend_info.iteritems():
                if section == 'main' or backend_information is not None:  # For the main section we require the backend info to be filled out
                    ExtensionsToolbox.verify_required_params(actual_params=backend_information,
                                                             required_params={'preset': (str, ExtensionsToolbox.regex_preset),
                                                                              'alba_backend_guid': (str, ExtensionsToolbox.regex_guid)})
                    if backend_information is not None:  # For block and fragment cache we only need connection information when backend info has been passed
                        ExtensionsToolbox.verify_required_params(actual_params=connection_info[section],
                                                                 required_params={'host': (str, ExtensionsToolbox.regex_ip),
                                                                                  'port': (int, {'min': 1, 'max': 65535}),
                                                                                  'client_id': (str, None),
                                                                                  'client_secret': (str, None),
                                                                                  'local': (bool, None, False)})

            # General configurations
            self.storage_ip = storage_ip
            self.write_caches = []
            self.backend_info = backend_info['main']
            self.cache_size_local = None
            self.connection_info = connection_info['main']
            self.storagedriver_partition_dtl = None
            self.storagedriver_partition_tlogs = None
            self.storagedriver_partitions_caches = []
            self.storagedriver_partition_metadata = None
            self.storagedriver_partition_file_driver = None

            # StorageDriver configurations
            self.dtl_mode = sd_configuration['dtl_mode']
            self.sco_size = sd_configuration['sco_size']
            self.cluster_size = sd_configuration['cluster_size']
            self.write_buffer = sd_configuration['write_buffer']
            self.rdma_enabled = sd_configuration['dtl_transport'] == StorageDriverClient.FRAMEWORK_DTL_TRANSPORT_RSOCKET
            self.dtl_transport = sd_configuration['dtl_transport']
            self.tlog_multiplier = StorageDriverClient.TLOG_MULTIPLIER_MAP[self.sco_size]

            # Block cache behavior configurations
            self.block_cache_quota = caching_info.get('cache_quota_bc')
            self.block_cache_on_read = caching_info['block_cache_on_read']
            self.block_cache_on_write = caching_info['block_cache_on_write']
            self.block_cache_backend_info = backend_info[StorageDriverConfiguration.CACHE_BLOCK]
            self.block_cache_connection_info = connection_info[StorageDriverConfiguration.CACHE_BLOCK]
            self.block_cache_local = self.block_cache_backend_info is None and (self.block_cache_on_read is True or self.block_cache_on_write is True)

            # Fragment cache behavior configurations
            self.fragment_cache_quota = caching_info.get('cache_quota_fc')
            self.fragment_cache_on_read = caching_info['fragment_cache_on_read']
            self.fragment_cache_on_write = caching_info['fragment_cache_on_write']
            self.fragment_cache_backend_info = backend_info[StorageDriverConfiguration.CACHE_FRAGMENT]
            self.fragment_cache_connection_info = connection_info[StorageDriverConfiguration.CACHE_FRAGMENT]
            self.fragment_cache_local = self.fragment_cache_backend_info is None and (self.fragment_cache_on_read is True or self.fragment_cache_on_write is True)

            # Additional validations
            if (self.sco_size == 128 and self.write_buffer < 256) or not (128 <= self.write_buffer <= 10240):
                raise RuntimeError('Incorrect StorageDriver configuration settings specified')

            alba_backend_guid_main = self.backend_info['alba_backend_guid']
            if self.block_cache_backend_info is not None and alba_backend_guid_main == self.block_cache_backend_info['alba_backend_guid']:
                raise RuntimeError('Backend and block cache backend cannot be the same')
            if self.fragment_cache_backend_info is not None and alba_backend_guid_main == self.fragment_cache_backend_info['alba_backend_guid']:
                raise RuntimeError('Backend and fragment cache backend cannot be the same')

            if self.vp_installer.is_new is False:
                if alba_backend_guid_main != self.vp_installer.vpool.metadata['backend']['backend_info']['alba_backend_guid']:
                    raise RuntimeError('Incorrect ALBA Backend guid specified')

                current_vpool_configuration = self.vp_installer.vpool.configuration
                for key, value in sd_configuration.iteritems():
                    current_value = current_vpool_configuration.get(key)
                    if value != current_value:
                        raise RuntimeError('Specified StorageDriver config "{0}" with value {1} does not match the expected value {2}'.format(key, value, current_value))

            # Add some additional required information
            self.backend_info['sco_size'] = self.sco_size * 1024.0 ** 2
            if self.block_cache_backend_info is not None:
                self.block_cache_backend_info['sco_size'] = self.sco_size * 1024.0 ** 2
            if self.fragment_cache_backend_info is not None:
                self.fragment_cache_backend_info['sco_size'] = self.sco_size * 1024.0 ** 2

        # Cross reference
        self.vp_installer.sd_installer = self

    def create(self):
        """
        Prepares a new Storagedriver for a given vPool and Storagerouter
        :return: None
        :rtype: NoneType
        """
        if self.sr_installer is None:
            raise RuntimeError('No StorageRouterInstaller instance found')

        machine_id = System.get_my_machine_id(client=self.sr_installer.root_client)
        port_range = Configuration.get('/ovs/framework/hosts/{0}/ports|storagedriver'.format(machine_id))
        storagerouter = self.sr_installer.storagerouter
        with volatile_mutex('add_vpool_get_free_ports_{0}'.format(machine_id), wait=30):
            model_ports_in_use = []
            for sd in StorageDriverList.get_storagedrivers():
                if sd.storagerouter_guid == storagerouter.guid:
                    model_ports_in_use += sd.ports.values()
                    for proxy in sd.alba_proxies:
                        model_ports_in_use.append(proxy.service.ports[0])
            ports = System.get_free_ports(selected_range=port_range, exclude=model_ports_in_use, amount=4 + self.sr_installer.requested_proxies, client=self.sr_installer.root_client)

            vpool = self.vp_installer.vpool
            vrouter_id = '{0}{1}'.format(vpool.name, machine_id)
            storagedriver = StorageDriver()
            storagedriver.name = vrouter_id.replace('_', ' ')
            storagedriver.ports = {'management': ports[0],
                                   'xmlrpc': ports[1],
                                   'dtl': ports[2],
                                   'edge': ports[3]}
            storagedriver.vpool = vpool
            storagedriver.cluster_ip = Configuration.get('/ovs/framework/hosts/{0}/ip'.format(machine_id))
            storagedriver.storage_ip = self.storage_ip
            storagedriver.mountpoint = '/mnt/{0}'.format(vpool.name)
            storagedriver.description = storagedriver.name
            storagedriver.storagerouter = storagerouter
            storagedriver.storagedriver_id = vrouter_id
            storagedriver.save()

            # ALBA Proxies
            proxy_service_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ALBA_PROXY)
            for proxy_id in xrange(self.sr_installer.requested_proxies):
                service = Service()
                service.storagerouter = storagerouter
                service.ports = [ports[4 + proxy_id]]
                service.name = 'albaproxy_{0}_{1}'.format(vpool.name, proxy_id)
                service.type = proxy_service_type
                service.save()
                alba_proxy = AlbaProxy()
                alba_proxy.service = service
                alba_proxy.storagedriver = storagedriver
                alba_proxy.save()
        self.storagedriver = storagedriver

    def create_partitions(self):
        """
        Configure all partitions for a StorageDriver (junctions between a StorageDriver and a DiskPartition)
        :raises: ValueError: - When calculating the cache sizes went wrong
        :return: Dict with information about the created items
        :rtype: dict
        """
        if self.storagedriver is None:
            raise RuntimeError('A StorageDriver needs to be created first')
        if self.sr_installer is None:
            raise RuntimeError('No StorageRouterInstaller instance found')

        # Assign WRITE / Fragment cache
        for writecache_info in self.sr_installer.write_partitions:
            available = writecache_info['available']
            partition = DiskPartition(writecache_info['guid'])
            proportion = available * 100.0 / self.sr_installer.global_write_buffer_available_size
            size_to_be_used = proportion * self.sr_installer.global_write_buffer_requested_size / 100
            write_cache_percentage = 0.98
            if self.sr_installer.requested_local_proxies > 0 and partition == self.sr_installer.largest_write_partition:  # At least 1 local proxy has been requested either for fragment or block cache
                self.cache_size_local = int(size_to_be_used * 0.10)  # Bytes
                write_cache_percentage = 0.88
                for _ in xrange(self.sr_installer.requested_proxies):
                    storagedriver_partition_cache = StorageDriverController.add_storagedriverpartition(storagedriver=self.storagedriver,
                                                                                                       partition_info={'size': None,
                                                                                                                       'role': DiskPartition.ROLES.WRITE,
                                                                                                                       'sub_role': StorageDriverPartition.SUBROLE.FCACHE,
                                                                                                                       'partition': partition})
                    self.sr_installer.created_dirs.append(storagedriver_partition_cache.path)
                    if self.block_cache_local is True:
                        self.sr_installer.created_dirs.append('{0}/bc'.format(storagedriver_partition_cache.path))
                    if self.fragment_cache_local is True:
                        self.sr_installer.created_dirs.append('{0}/fc'.format(storagedriver_partition_cache.path))
                    self.storagedriver_partitions_caches.append(storagedriver_partition_cache)

            w_size = int(size_to_be_used * write_cache_percentage / 1024 / 4096) * 4096
            storagedriver_partition_write = StorageDriverController.add_storagedriverpartition(storagedriver=self.storagedriver,
                                                                                               partition_info={'size': long(size_to_be_used),
                                                                                                               'role': DiskPartition.ROLES.WRITE,
                                                                                                               'sub_role': StorageDriverPartition.SUBROLE.SCO,
                                                                                                               'partition': partition})
            self.write_caches.append({'path': storagedriver_partition_write.path,
                                      'size': '{0}KiB'.format(w_size)})
            self.sr_installer.created_dirs.append(storagedriver_partition_write.path)
            if self.sr_installer.smallest_write_partition_size in [0, None] or (w_size * 1024) < self.sr_installer.smallest_write_partition_size:
                self.sr_installer.smallest_write_partition_size = w_size * 1024

        # Verify cache size
        if self.cache_size_local is None and (self.block_cache_local is True or self.fragment_cache_local is True):
            raise ValueError('Something went wrong trying to calculate the cache sizes')

        # Assign FD partition
        self.storagedriver_partition_file_driver = StorageDriverController.add_storagedriverpartition(storagedriver=self.storagedriver,
                                                                                                      partition_info={'size': None,
                                                                                                                      'role': DiskPartition.ROLES.WRITE,
                                                                                                                      'sub_role': StorageDriverPartition.SUBROLE.FD,
                                                                                                                      'partition': self.sr_installer.largest_write_partition})
        self.sr_installer.created_dirs.append(self.storagedriver_partition_file_driver.path)

        # Assign DB partition
        db_info = self.sr_installer.partition_info[DiskPartition.ROLES.DB][0]
        self.storagedriver_partition_tlogs = StorageDriverController.add_storagedriverpartition(storagedriver=self.storagedriver,
                                                                                                partition_info={'size': None,
                                                                                                                'role': DiskPartition.ROLES.DB,
                                                                                                                'sub_role': StorageDriverPartition.SUBROLE.TLOG,
                                                                                                                'partition': DiskPartition(db_info['guid'])})
        self.storagedriver_partition_metadata = StorageDriverController.add_storagedriverpartition(storagedriver=self.storagedriver,
                                                                                                   partition_info={'size': None,
                                                                                                                   'role': DiskPartition.ROLES.DB,
                                                                                                                   'sub_role': StorageDriverPartition.SUBROLE.MD,
                                                                                                                   'partition': DiskPartition(db_info['guid'])})
        self.sr_installer.created_dirs.append(self.storagedriver_partition_tlogs.path)
        self.sr_installer.created_dirs.append(self.storagedriver_partition_metadata.path)

        # Assign DTL
        dtl_info = self.sr_installer.partition_info[DiskPartition.ROLES.DTL][0]
        self.storagedriver_partition_dtl = StorageDriverController.add_storagedriverpartition(storagedriver=self.storagedriver,
                                                                                              partition_info={'size': None,
                                                                                                              'role': DiskPartition.ROLES.DTL,
                                                                                                              'partition': DiskPartition(dtl_info['guid'])})
        self.sr_installer.created_dirs.append(self.storagedriver_partition_dtl.path)
        self.sr_installer.created_dirs.append(self.storagedriver.mountpoint)

        # Create the directories
        self.sr_installer.root_client.dir_create(directories=self.sr_installer.created_dirs)

    def setup_proxy_configs(self):
        """
        Sets up the proxies their configuration data in the configuration management
        :return: None
        :rtype: NoneType
        """
        def _generate_proxy_cache_config(cache_settings, cache_type, proxy_index):
            if cache_settings['read'] is False and cache_settings['write'] is False:
                return ['none']
            if cache_settings['is_backend'] is True:
                alba_backend_guid = vpool.metadata['caching_info'][self.storagedriver.storagerouter_guid][cache_type]['backend_info']['alba_backend_guid']

                return ['alba', {'cache_on_read': cache_settings['read'],
                                 'cache_on_write': cache_settings['write'],
                                 'albamgr_cfg_url': Configuration.get_configuration_path(REMOTE_CONFIG_BACKEND_INI.format(alba_backend_guid)),
                                 'bucket_strategy': ['1-to-1', {'prefix': vpool.guid,
                                                                'preset': cache_settings['backend_info']['preset']}],
                                 'manifest_cache_size': manifest_cache_size}]

            if cache_type == StorageDriverConfiguration.CACHE_BLOCK:
                path = '{0}/bc'.format(self.storagedriver_partitions_caches[proxy_index].path)
            else:
                path = '{0}/fc'.format(self.storagedriver_partitions_caches[proxy_index].path)
            return ['local', {'path': path,
                              'max_size': self.cache_size_local / self.sr_installer.requested_local_proxies,
                              'cache_on_read': cache_settings['read'],
                              'cache_on_write': cache_settings['write']}]

        def _generate_scrub_proxy_cache_config(cache_settings, main_proxy_cache_config):
            scrub_cache_info = ['none']
            if cache_settings['is_backend'] is True and cache_settings['write'] is True:
                scrub_cache_info = copy.deepcopy(main_proxy_cache_config)
                scrub_cache_info[1]['cache_on_read'] = False
            return scrub_cache_info

        def _generate_proxy_config(proxy_type, proxy_service):
            alba_backend_guid = vpool.metadata['backend']['backend_info']['alba_backend_guid']

            proxy_config = {'log_level': 'info',
                            'port': proxy_service.service.ports[0] if proxy_type == 'main' else 0,
                            'ips': [self.storagedriver.storage_ip] if proxy_type == 'main' else ['127.0.0.1'],
                            'manifest_cache_size': manifest_cache_size,
                            'fragment_cache': fragment_cache_main_proxy if proxy_type == 'main' else fragment_cache_scrub_proxy,
                            'transport': 'tcp',
                            'read_preference': read_preferences,
                            'albamgr_cfg_url': Configuration.get_configuration_path(REMOTE_CONFIG_BACKEND_INI.format(alba_backend_guid))}
            if self.sr_installer.block_cache_supported:
                proxy_config['block_cache'] = block_cache_main_proxy if proxy_type == 'main' else block_cache_scrub_proxy
            return proxy_config

        vpool = self.vp_installer.vpool
        read_preferences = self.vp_installer.calculate_read_preferences()
        manifest_cache_size = 500 * 1024 ** 2
        block_cache_settings = vpool.metadata['caching_info'][self.storagedriver.storagerouter_guid][StorageDriverConfiguration.CACHE_BLOCK]
        fragment_cache_settings = vpool.metadata['caching_info'][self.storagedriver.storagerouter_guid][StorageDriverConfiguration.CACHE_FRAGMENT]

        # Obtain all arakoon configurations for each Backend (main, block cache, fragment cache)
        arakoon_data = {'abm': VPoolShared.retrieve_local_alba_arakoon_config(vpool.metadata['backend']['backend_info']['alba_backend_guid'])}
        if block_cache_settings['is_backend'] is True:
            arakoon_data['abm_bc'] = block_cache_settings['backend_info']['arakoon_config']

        if fragment_cache_settings['is_backend'] is True:
            arakoon_data['abm_aa'] = fragment_cache_settings['backend_info']['arakoon_config']

        for proxy_id, alba_proxy in enumerate(self.storagedriver.alba_proxies):
            # Generate cache information for main proxy
            block_cache_main_proxy = _generate_proxy_cache_config(cache_type=StorageDriverConfiguration.CACHE_BLOCK, cache_settings=block_cache_settings, proxy_index=proxy_id)
            fragment_cache_main_proxy = _generate_proxy_cache_config(cache_type=StorageDriverConfiguration.CACHE_FRAGMENT, cache_settings=fragment_cache_settings, proxy_index=proxy_id)

            # Generate cache information for scrub proxy
            block_cache_scrub_proxy = _generate_scrub_proxy_cache_config(cache_settings=block_cache_settings, main_proxy_cache_config=block_cache_main_proxy)
            fragment_cache_scrub_proxy = _generate_scrub_proxy_cache_config(cache_settings=fragment_cache_settings, main_proxy_cache_config=fragment_cache_main_proxy)

            # Generate complete main and proxy configuration
            main_proxy_config = _generate_proxy_config(proxy_type='main', proxy_service=alba_proxy)
            scrub_proxy_config = _generate_proxy_config(proxy_type='scrub', proxy_service=alba_proxy)

            # Add configurations to configuration management
            Configuration.set(PROXY_CONFIG_MAIN.format(vpool.guid, alba_proxy.guid), json.dumps(main_proxy_config, indent=4))
            Configuration.set(GENERIC_SCRUB.format(vpool.guid), json.dumps(scrub_proxy_config, indent=4))

    def configure_storagedriver_service(self):
        """
        Configure the StorageDriver service
        :return: None
        :rtype: NoneType
        """
        def _generate_queue_urls():
            mq_user = Configuration.get('/ovs/framework/messagequeue|user')
            mq_protocol = Configuration.get('/ovs/framework/messagequeue|protocol')
            mq_password = Configuration.get('/ovs/framework/messagequeue|password')
            return [{'amqp_uri': '{0}://{1}:{2}@{3}:5672'.format(mq_protocol, mq_user, mq_password, sr.ip)} for sr in StorageRouterList.get_masters()]

        def _generate_config_file_system():
            config = {'fs_dtl_host': '',
                      'fs_enable_shm_interface': 0,
                      'fs_enable_network_interface': 1,
                      'fs_metadata_backend_arakoon_cluster_nodes': [],
                      'fs_metadata_backend_mds_nodes': [],
                      'fs_metadata_backend_type': 'MDS',
                      'fs_virtual_disk_format': 'raw',
                      'fs_raw_disk_suffix': '.raw',
                      'fs_file_event_rules': [{'fs_file_event_rule_calls': ['Rename'],
                                               'fs_file_event_rule_path_regex': '.*'}]}
            if self.dtl_mode == StorageDriverClient.FRAMEWORK_DTL_NO_SYNC:
                config['fs_dtl_config_mode'] = StorageDriverClient.VOLDRV_DTL_MANUAL_MODE
            else:
                config['fs_dtl_mode'] = StorageDriverClient.VPOOL_DTL_MODE_MAP[self.dtl_mode]
                config['fs_dtl_config_mode'] = StorageDriverClient.VOLDRV_DTL_AUTOMATIC_MODE
            return config

        def _generate_config_backend_connection_manager():
            config = {'backend_type': 'MULTI',
                      'backend_interface_retries_on_error': 5,
                      'backend_interface_retry_interval_secs': 1,
                      'backend_interface_retry_backoff_multiplier': 2.0}
            for index, proxy in enumerate(sorted(self.storagedriver.alba_proxies, key=lambda k: k.service.ports[0])):
                config[str(index)] = {'alba_connection_host': self.storagedriver.storage_ip,
                                      'alba_connection_port': proxy.service.ports[0],
                                      'alba_connection_preset': vpool.metadata['backend']['backend_info']['preset'],
                                      'alba_connection_timeout': 30,
                                      'alba_connection_use_rora': True,
                                      'alba_connection_transport': 'TCP',
                                      'alba_connection_rora_manifest_cache_capacity': 25000,
                                      'alba_connection_asd_connection_pool_capacity': 10,
                                      'alba_connection_rora_timeout_msecs': 50,
                                      'backend_type': 'ALBA'}
            return config

        if self.sr_installer is None:
            raise RuntimeError('No StorageRouterInstaller instance found')
        if len(self.write_caches) == 0:
            raise RuntimeError('The StorageDriverPartition junctions have not been created yet')

        vpool = self.vp_installer.vpool
        gap_configuration = StorageDriverController.calculate_trigger_and_backoff_gap(cache_size=self.sr_installer.smallest_write_partition_size)
        arakoon_cluster_name = str(Configuration.get('/ovs/framework/arakoon_clusters|voldrv'))
        arakoon_nodes = [{'host': node.ip,
                          'port': node.client_port,
                          'node_id': node.name} for node in ArakoonClusterConfig(cluster_id=arakoon_cluster_name).nodes]

        storagedriver_config = StorageDriverConfiguration(vpool.guid, self.storagedriver.storagedriver_id)
        storagedriver_config.configure_scocache(scocache_mount_points=self.write_caches,
                                                trigger_gap=ExtensionsToolbox.convert_byte_size_to_human_readable(size=gap_configuration['trigger']),
                                                backoff_gap=ExtensionsToolbox.convert_byte_size_to_human_readable(size=gap_configuration['backoff']))
        storagedriver_config.configure_file_driver(fd_cache_path=self.storagedriver_partition_file_driver.path,
                                                   fd_extent_cache_capacity=1024,
                                                   fd_namespace='fd-{0}-{1}'.format(vpool.name, vpool.guid))
        storagedriver_config.configure_volume_router(vrouter_id=self.storagedriver.storagedriver_id,
                                                     vrouter_redirect_timeout_ms=120000,
                                                     vrouter_keepalive_time_secs=15,
                                                     vrouter_keepalive_interval_secs=5,
                                                     vrouter_keepalive_retries=2,
                                                     vrouter_routing_retries=10,
                                                     vrouter_volume_read_threshold=0,
                                                     vrouter_volume_write_threshold=0,
                                                     vrouter_file_read_threshold=0,
                                                     vrouter_file_write_threshold=0,
                                                     vrouter_min_workers=4,
                                                     vrouter_max_workers=16,
                                                     vrouter_sco_multiplier=self.sco_size * 1024 / self.cluster_size,
                                                     vrouter_backend_sync_timeout_ms=60000,
                                                     vrouter_migrate_timeout_ms=60000,
                                                     vrouter_use_fencing=True)
        storagedriver_config.configure_volume_manager(tlog_path=self.storagedriver_partition_tlogs.path,
                                                      metadata_path=self.storagedriver_partition_metadata.path,
                                                      clean_interval=1,
                                                      dtl_throttle_usecs=4000,
                                                      default_cluster_size=self.cluster_size * 1024,
                                                      number_of_scos_in_tlog=self.tlog_multiplier,
                                                      non_disposable_scos_factor=float(self.write_buffer) / self.tlog_multiplier / self.sco_size)
        storagedriver_config.configure_event_publisher(events_amqp_routing_key=Configuration.get('/ovs/framework/messagequeue|queues.storagedriver'),
                                                       events_amqp_uris=_generate_queue_urls())
        storagedriver_config.configure_volume_registry(vregistry_arakoon_cluster_id=arakoon_cluster_name,
                                                       vregistry_arakoon_cluster_nodes=arakoon_nodes)
        storagedriver_config.configure_network_interface(network_max_neighbour_distance=StorageDriver.DISTANCES.FAR - 1)
        storagedriver_config.configure_threadpool_component(num_threads=16)
        storagedriver_config.configure_volume_router_cluster(vrouter_cluster_id=vpool.guid)
        storagedriver_config.configure_distributed_lock_store(dls_type='Arakoon',
                                                              dls_arakoon_cluster_id=arakoon_cluster_name,
                                                              dls_arakoon_cluster_nodes=arakoon_nodes)
        storagedriver_config.configure_content_addressed_cache(serialize_read_cache=False,
                                                               read_cache_serialization_path=[])
        storagedriver_config.configure_distributed_transaction_log(dtl_path=self.storagedriver_partition_dtl.path,  # Not used, but required
                                                                   dtl_transport=StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP[self.dtl_transport])

        storagedriver_config.configure_filesystem(**_generate_config_file_system())
        storagedriver_config.configure_backend_connection_manager(**_generate_config_backend_connection_manager())

        storagedriver_config.save(client=self.sr_installer.root_client)

    def start_services(self):
        """
        Start all services related to the Storagedriver
        :return: None
        :rtype: NoneType
        """
        if self.sr_installer is None:
            raise RuntimeError('No StorageRouterInstaller instance found')

        vpool = self.vp_installer.vpool
        root_client = self.sr_installer.root_client
        storagerouter = self.sr_installer.storagerouter
        alba_pkg_name, alba_version_cmd = PackageFactory.get_package_and_version_cmd_for(component=PackageFactory.COMP_ALBA)
        voldrv_pkg_name, voldrv_version_cmd = PackageFactory.get_package_and_version_cmd_for(component=PackageFactory.COMP_SD)

        # Add/start watcher volumedriver service
        if not self.service_manager.has_service(name=ServiceFactory.SERVICE_WATCHER_VOLDRV, client=root_client):
            self.service_manager.add_service(name=ServiceFactory.SERVICE_WATCHER_VOLDRV, client=root_client)
            self.service_manager.start_service(name=ServiceFactory.SERVICE_WATCHER_VOLDRV, client=root_client)

        # Add/start DTL service
        self.service_manager.add_service(name=self.SERVICE_TEMPLATE_DTL,
                                         params={'DTL_PATH': self.storagedriver_partition_dtl.path,
                                                 'DTL_ADDRESS': self.storagedriver.storage_ip,
                                                 'DTL_PORT': str(self.storagedriver.ports['dtl']),
                                                 'DTL_TRANSPORT': StorageDriverClient.VPOOL_DTL_TRANSPORT_MAP[self.dtl_transport],
                                                 'LOG_SINK': Logger.get_sink_path('storagedriver-dtl_{0}'.format(self.storagedriver.storagedriver_id)),
                                                 'VOLDRV_PKG_NAME': voldrv_pkg_name,
                                                 'VOLDRV_VERSION_CMD': voldrv_version_cmd},
                                         client=root_client,
                                         target_name=self.dtl_service)
        self.service_manager.start_service(name=self.dtl_service, client=root_client)

        # Add/start ALBA proxy services
        for proxy in self.storagedriver.alba_proxies:
            alba_proxy_service = 'ovs-{0}'.format(proxy.service.name)
            self.service_manager.add_service(name=self.SERVICE_TEMPLATE_PROXY,
                                             params={'VPOOL_NAME': vpool.name,
                                                     'LOG_SINK': Logger.get_sink_path(proxy.service.name),
                                                     'CONFIG_PATH': Configuration.get_configuration_path(PROXY_CONFIG_MAIN.format(vpool.guid, proxy.guid)),
                                                     'ALBA_PKG_NAME': alba_pkg_name,
                                                     'ALBA_VERSION_CMD': alba_version_cmd},
                                             client=root_client,
                                             target_name=alba_proxy_service)
            self.service_manager.start_service(name=alba_proxy_service, client=root_client)

        # Add/start StorageDriver service
        self.service_manager.add_service(name=self.SERVICE_TEMPLATE_SD,
                                         params={'KILL_TIMEOUT': '30',
                                                 'VPOOL_NAME': vpool.name,
                                                 'VPOOL_MOUNTPOINT': self.storagedriver.mountpoint,
                                                 'CONFIG_PATH': StorageDriverConfiguration(vpool_guid=vpool.guid, storagedriver_id=self.storagedriver.storagedriver_id).remote_path,
                                                 'OVS_UID': root_client.run(['id', '-u', 'ovs']).strip(),
                                                 'OVS_GID': root_client.run(['id', '-g', 'ovs']).strip(),
                                                 'LOG_SINK': Logger.get_sink_path('storagedriver_{0}'.format(self.storagedriver.storagedriver_id)),
                                                 'VOLDRV_PKG_NAME': voldrv_pkg_name,
                                                 'VOLDRV_VERSION_CMD': voldrv_version_cmd,
                                                 'METADATASTORE_BITS': 5},
                                         client=root_client,
                                         target_name=self.sd_service)

        current_startup_counter = self.storagedriver.startup_counter
        self.service_manager.start_service(name=self.sd_service, client=root_client)

        tries = 60
        while self.storagedriver.startup_counter == current_startup_counter and tries > 0:
            self._logger.debug('Waiting for the StorageDriver to start up for vPool {0} on StorageRouter {1} ...'.format(vpool.name, storagerouter.name))
            if self.service_manager.get_service_status(name=self.sd_service, client=root_client) != 'active':
                raise RuntimeError('StorageDriver service failed to start (service not running)')
            tries -= 1
            time.sleep(60 - tries)
            self.storagedriver.discard()
        if self.storagedriver.startup_counter == current_startup_counter:
            raise RuntimeError('StorageDriver service failed to start (got no event)')
        self._logger.debug('StorageDriver running')

    def stop_services(self):
        """
        Stop all services related to the Storagedriver
        :return: A boolean indicating whether something went wrong
        :rtype: bool
        """
        if self.sr_installer is None:
            raise RuntimeError('No StorageRouterInstaller instance found')

        root_client = self.sr_installer.root_client
        errors_found = False

        for service in [self.sd_service, self.dtl_service]:
            try:
                if self.service_manager.has_service(name=service, client=root_client):
                    self._logger.debug('StorageDriver {0} - Stopping service {1}'.format(self.storagedriver.guid, service))
                    self.service_manager.stop_service(name=service, client=root_client)
                    self._logger.debug('StorageDriver {0} - Removing service {1}'.format(self.storagedriver.guid, service))
                    self.service_manager.remove_service(name=service, client=root_client)
            except Exception:
                self._logger.exception('StorageDriver {0} - Disabling/stopping service {1} failed'.format(self.storagedriver.guid, service))
                errors_found = True

        sd_config_key = HOSTS_CONFIG_PATH.format(self.vp_installer.vpool.guid, self.storagedriver.storagedriver_id)
        if self.vp_installer.storagedriver_amount <= 1 and Configuration.exists(sd_config_key):
            try:
                for proxy in self.storagedriver.alba_proxies:
                    if self.service_manager.has_service(name=proxy.service.name, client=root_client):
                        self._logger.debug('StorageDriver {0} - Starting proxy {1}'.format(self.storagedriver.guid, proxy.service.name))
                        self.service_manager.start_service(name=proxy.service.name, client=root_client)
                        tries = 10
                        running = False
                        port = proxy.service.ports[0]
                        while running is False and tries > 0:
                            self._logger.debug('StorageDriver {0} - Waiting for the proxy {1} to start up'.format(self.storagedriver.guid, proxy.service.name))
                            tries -= 1
                            time.sleep(10 - tries)
                            try:
                                root_client.run(['alba', 'proxy-statistics', '--host', self.storagedriver.storage_ip, '--port', str(port)])
                                running = True
                            except CalledProcessError as ex:
                                self._logger.error('StorageDriver {0} - Fetching alba proxy-statistics failed with error (but ignoring): {1}'.format(self.storagedriver.guid, ex))
                        if running is False:
                            raise RuntimeError('Alba proxy {0} failed to start'.format(proxy.service.name))
                        self._logger.debug('StorageDriver {0} - Alba proxy {0} running'.format(self.storagedriver.guid, proxy.service.name))

                self._logger.debug('StorageDriver {0} - Destroying filesystem and erasing node configs'.format(self.storagedriver.guid))
                with remote(root_client.ip, [LocalStorageRouterClient], username='root') as rem:
                    path = Configuration.get_configuration_path(sd_config_key)
                    storagedriver_client = rem.LocalStorageRouterClient(path)
                    try:
                        storagedriver_client.destroy_filesystem()
                    except RuntimeError as rte:
                        # If backend has already been deleted, we cannot delete the filesystem anymore --> storage leak!!!
                        if 'MasterLookupResult.Error' not in rte.message:
                            raise

                self.vp_installer.vpool.clusterregistry_client.erase_node_configs()
            except RuntimeError:
                self._logger.exception('StorageDriver {0} - Destroying filesystem and erasing node configs failed'.format(self.storagedriver.guid))
                errors_found = True

        for proxy in self.storagedriver.alba_proxies:
            service_name = proxy.service.name
            try:
                if self.service_manager.has_service(name=service_name, client=root_client):
                    self._logger.debug('StorageDriver {0} - Stopping service {1}'.format(self.storagedriver.guid, service_name))
                    self.service_manager.stop_service(name=service_name, client=root_client)
                    self._logger.debug('StorageDriver {0} - Removing service {1}'.format(self.storagedriver.guid, service_name))
                    self.service_manager.remove_service(name=service_name, client=root_client)
            except Exception:
                self._logger.exception('StorageDriver {0} - Disabling/stopping service {1} failed'.format(self.storagedriver.guid, service_name))
                errors_found = True

        return errors_found

    def clean_config_management(self):
        """
        Remove the configuration management entries related to a StorageDriver removal
        :return: A boolean indicating whether something went wrong
        :rtype: bool
        """
        try:
            for proxy in self.storagedriver.alba_proxies:
                Configuration.delete(PROXY_PATH.format(self.vp_installer.vpool.guid), proxy.guid)
            Configuration.delete(HOSTS_PATH.format(self.vp_installer.vpool.guid, self.storagedriver.storagedriver_id))
            return False
        except Exception:
            self._logger.exception('Cleaning configuration management failed')
            return True

    def clean_directories(self, mountpoints):
        """
        Remove the directories from the filesystem when removing a StorageDriver
        :param mountpoints: The mountpoints on the StorageRouter of the StorageDriver being removed
        :type mountpoints: list
        :return: A boolean indicating whether something went wrong
        :rtype: bool
        """
        self._logger.info('Deleting vPool related directories and files')
        dirs_to_remove = [self.storagedriver.mountpoint] + [sd_partition.path for sd_partition in self.storagedriver.partitions]
        try:
            for dir_name in dirs_to_remove:
                if dir_name and self.sr_installer.root_client.dir_exists(dir_name) and dir_name not in mountpoints and dir_name != '/':
                    self.sr_installer.root_client.dir_delete(dir_name)
            return False
        except Exception:
            self._logger.exception('StorageDriver {0} - Failed to retrieve mount point information or delete directories'.format(self.storagedriver.guid))
            self._logger.warning('StorageDriver {0} - Following directories should be checked why deletion was prevented: {1}'.format(self.storagedriver.guid, ', '.join(dirs_to_remove)))
            return True

    def clean_model(self):
        """
        Clean up the model after removing a StorageDriver
        :return: A boolean indicating whether something went wrong
        :rtype: bool
        """
        self._logger.info('Cleaning up model')
        try:
            for sd_partition in self.storagedriver.partitions[:]:
                sd_partition.delete()
            for proxy in self.storagedriver.alba_proxies:
                service = proxy.service
                proxy.delete()
                service.delete()

            sd_can_be_deleted = True
            if self.vp_installer.storagedriver_amount <= 1:
                for relation in ['mds_services', 'storagedrivers', 'vdisks']:
                    expected_amount = 1 if relation == 'storagedrivers' else 0
                    if len(getattr(self.vp_installer.vpool, relation)) > expected_amount:
                        sd_can_be_deleted = False
                        break

            if sd_can_be_deleted is True:
                self.storagedriver.delete()
            return False
        except Exception:
            self._logger.exception('Cleaning up the model failed')
            return True
