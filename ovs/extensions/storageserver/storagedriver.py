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
Wrapper class for the storagedriver client of the voldrv team
"""
import os
import copy
import json
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.generic.remote import remote
from ovs.log.log_handler import LogHandler
from volumedriver.storagerouter import storagerouterclient
from volumedriver.storagerouter.storagerouterclient import ClusterContact, DTLMode, LocalStorageRouterClient, MDSNodeConfig, ReadCacheBehaviour, ReadCacheMode, Statistics, VolumeInfo
if os.environ.get('RUNNING_UNITTESTS') == 'True':
    from ovs.extensions.storageserver.tests.mockups import MockStorageRouterClient as SRClient
    from ovs.extensions.storageserver.tests.mockups import MockMetadataServerClient as MDSClient
else:
    from volumedriver.storagerouter.storagerouterclient import MDSClient
    from volumedriver.storagerouter.storagerouterclient import StorageRouterClient as SRClient

client_vpool_cache = {}
mdsclient_service_cache = {}


# noinspection PyArgumentList
class StorageDriverClient(object):
    """
    Client to access storagedriver client
    """
    storagerouterclient.Logger.setupLogging(LogHandler.load_path('storagerouterclient'))
    # noinspection PyArgumentList
    storagerouterclient.Logger.enableLogging()

    VOLDRV_DTL_SYNC = 'Synchronous'
    VOLDRV_DTL_ASYNC = 'Asynchronous'
    VOLDRV_NO_CACHE = 'NoCache'
    VOLDRV_CACHE_ON_READ = 'CacheOnRead'
    VOLDRV_CONTENT_BASED = 'ContentBased'
    VOLDRV_CACHE_ON_WRITE = 'CacheOnWrite'
    VOLDRV_LOCATION_BASED = 'LocationBased'
    VOLDRV_DTL_MANUAL_MODE = 'Manual'
    VOLDRV_DTL_AUTOMATIC_MODE = 'Automatic'
    VOLDRV_DTL_TRANSPORT_TCP = 'TCP'
    VOLDRV_DTL_TRANSPORT_RSOCKET = 'RSocket'

    FRAMEWORK_DTL_SYNC = 'sync'
    FRAMEWORK_DTL_ASYNC = 'a_sync'
    FRAMEWORK_DTL_NO_SYNC = 'no_sync'
    FRAMEWORK_NO_CACHE = 'none'
    FRAMEWORK_CACHE_ON_READ = 'on_read'
    FRAMEWORK_CONTENT_BASED = 'dedupe'
    FRAMEWORK_CACHE_ON_WRITE = 'on_write'
    FRAMEWORK_LOCATION_BASED = 'non_dedupe'
    FRAMEWORK_DTL_TRANSPORT_TCP = 'tcp'
    FRAMEWORK_DTL_TRANSPORT_RSOCKET = 'rdma'

    METADATA_CACHE_PAGE_SIZE = 256 * 24
    DEFAULT_METADATA_CACHE_SIZE = 8192 * METADATA_CACHE_PAGE_SIZE

    VDISK_CACHE_MAP = {FRAMEWORK_NO_CACHE: ReadCacheBehaviour.NO_CACHE,
                       FRAMEWORK_CACHE_ON_READ: ReadCacheBehaviour.CACHE_ON_READ,
                       FRAMEWORK_CACHE_ON_WRITE: ReadCacheBehaviour.CACHE_ON_WRITE}
    VPOOL_CACHE_MAP = {FRAMEWORK_NO_CACHE: VOLDRV_NO_CACHE,
                       FRAMEWORK_CACHE_ON_READ: VOLDRV_CACHE_ON_READ,
                       FRAMEWORK_CACHE_ON_WRITE: VOLDRV_CACHE_ON_WRITE}
    VDISK_DEDUPE_MAP = {FRAMEWORK_CONTENT_BASED: ReadCacheMode.CONTENT_BASED,
                        FRAMEWORK_LOCATION_BASED: ReadCacheMode.LOCATION_BASED}
    VPOOL_DEDUPE_MAP = {FRAMEWORK_CONTENT_BASED: VOLDRV_CONTENT_BASED,
                        FRAMEWORK_LOCATION_BASED: VOLDRV_LOCATION_BASED}
    VDISK_DTL_MODE_MAP = {FRAMEWORK_DTL_SYNC: DTLMode.SYNCHRONOUS,
                          FRAMEWORK_DTL_ASYNC: DTLMode.ASYNCHRONOUS,
                          FRAMEWORK_DTL_NO_SYNC: None}
    VPOOL_DTL_MODE_MAP = {FRAMEWORK_DTL_SYNC: VOLDRV_DTL_SYNC,
                          FRAMEWORK_DTL_ASYNC: VOLDRV_DTL_ASYNC,
                          FRAMEWORK_DTL_NO_SYNC: None}
    VPOOL_DTL_TRANSPORT_MAP = {FRAMEWORK_DTL_TRANSPORT_TCP: VOLDRV_DTL_TRANSPORT_TCP,
                               FRAMEWORK_DTL_TRANSPORT_RSOCKET: VOLDRV_DTL_TRANSPORT_RSOCKET}
    REVERSE_CACHE_MAP = {VOLDRV_NO_CACHE: FRAMEWORK_NO_CACHE,
                         VOLDRV_CACHE_ON_READ: FRAMEWORK_CACHE_ON_READ,
                         VOLDRV_CACHE_ON_WRITE: FRAMEWORK_CACHE_ON_WRITE,
                         ReadCacheBehaviour.NO_CACHE: FRAMEWORK_NO_CACHE,
                         ReadCacheBehaviour.CACHE_ON_READ: FRAMEWORK_CACHE_ON_READ,
                         ReadCacheBehaviour.CACHE_ON_WRITE: FRAMEWORK_CACHE_ON_WRITE}
    REVERSE_DEDUPE_MAP = {VOLDRV_CONTENT_BASED: FRAMEWORK_CONTENT_BASED,
                          VOLDRV_LOCATION_BASED: FRAMEWORK_LOCATION_BASED,
                          ReadCacheMode.CONTENT_BASED: FRAMEWORK_CONTENT_BASED,
                          ReadCacheMode.LOCATION_BASED: FRAMEWORK_LOCATION_BASED}
    REVERSE_DTL_MODE_MAP = {VOLDRV_DTL_SYNC: FRAMEWORK_DTL_SYNC,
                            VOLDRV_DTL_ASYNC: FRAMEWORK_DTL_ASYNC,
                            DTLMode.SYNCHRONOUS: FRAMEWORK_DTL_SYNC,
                            DTLMode.ASYNCHRONOUS: FRAMEWORK_DTL_ASYNC}
    REVERSE_DTL_TRANSPORT_MAP = {VOLDRV_DTL_TRANSPORT_TCP: FRAMEWORK_DTL_TRANSPORT_TCP,
                                 VOLDRV_DTL_TRANSPORT_RSOCKET: FRAMEWORK_DTL_TRANSPORT_RSOCKET}
    CLUSTER_SIZES = [4, 8, 16, 32, 64]
    TLOG_MULTIPLIER_MAP = {4: 16,
                           8: 8,
                           16: 4,
                           32: 2,
                           64: 1,
                           128: 1}

    DTL_STATUS = {'': 0,
                  'ok_sync': 10,
                  'ok_standalone': 20,
                  'catch_up': 30,
                  'checkup_required': 30,
                  'degraded': 40}
    EMPTY_STATISTICS = staticmethod(lambda: Statistics())
    EMPTY_INFO = staticmethod(lambda: VolumeInfo())
    STAT_SUMS = {'operations': ['write_operations', 'read_operations'],
                 'cache_hits': ['sco_cache_hits', 'cluster_cache_hits'],
                 'cache_misses': ['sco_cache_misses'],
                 '4k_operations': ['4k_read_operations', '4k_write_operations'],
                 'data_transferred': ['data_written', 'data_read']}
    STAT_KEYS = ['backend_data_read', 'backend_data_written', 'backend_read_operations', 'backend_write_operations',
                 'cluster_cache_hits', 'cluster_cache_misses', 'data_read', 'data_written', 'metadata_store_hits',
                 'metadata_store_misses', 'read_operations', 'sco_cache_hits', 'sco_cache_misses', 'write_operations',
                 '4k_read_operations', '4k_write_operations', 'stored']
    STAT_KEYS.extend(STAT_SUMS.keys())

    def __init__(self):
        """
        Dummy init method
        """
        pass

    @staticmethod
    def load(vpool):
        """
        Initializes the wrapper given a vpool name for which it finds the corresponding Storage Driver
        Loads and returns the client
        :param vpool: vPool for which the StorageRouterClient needs to be loaded
        """
        key = vpool.identifier
        if key not in client_vpool_cache:
            cluster_contacts = []
            for storagedriver in vpool.storagedrivers[:3]:
                cluster_contacts.append(ClusterContact(str(storagedriver.cluster_ip), storagedriver.ports['xmlrpc']))
            client = SRClient(str(vpool.guid), cluster_contacts)
            client_vpool_cache[key] = client
        return client_vpool_cache[key]


class MetadataServerClient(object):
    """
    Builds a MDSClient
    """
    _logger = LogHandler.get('extensions', name='storagedriver')
    storagerouterclient.Logger.setupLogging(LogHandler.load_path('storagerouterclient'))
    # noinspection PyArgumentList
    storagerouterclient.Logger.enableLogging()

    def __init__(self):
        """
        Dummy init method
        """
        pass

    @staticmethod
    def load(service):
        """
        Loads a MDSClient
        :param service: Service for which the MDSClient needs to be loaded
        """
        if service.storagerouter is None:
            raise ValueError('MDS service {0} does not have a Storage Router linked to it'.format(service.name))

        key = service.guid
        if key not in mdsclient_service_cache:
            try:
                # noinspection PyArgumentList
                client = MDSClient(MDSNodeConfig(address=str(service.storagerouter.ip), port=service.ports[0]))
                mdsclient_service_cache[key] = client
            except RuntimeError as ex:
                MetadataServerClient._logger.error('Error loading MDSClient on {0}: {1}'.format(service.storagerouter.ip, ex))
                return None
        return mdsclient_service_cache[key]


class StorageDriverConfiguration(object):
    """
    StorageDriver configuration class
    """

    # The below dictionary is GENERATED by the storagedriver
    # - Specific test generating the output: ConfigurationTest.print_framework_parameter_dict
    # DO NOT MAKE MANUAL CHANGES HERE

    parameters = {
        # hg branch: dev
        # hg revision: d6c3c04c6e6a938bec87f84c8566a7b5e825e723
        # buildTime: Thu Jun  2 07:51:33 UTC 2016
        'metadataserver': {
            'backend_connection_manager': {
                'optional': ['backend_connection_pool_capacity', 'backend_interface_retries_on_error', 'backend_interface_retry_interval_secs', 'backend_interface_retry_backoff_multiplier', 'backend_type', 's3_connection_host', 's3_connection_port', 's3_connection_username', 's3_connection_password', 's3_connection_verbose_logging', 's3_connection_use_ssl', 's3_connection_ssl_verify_host', 's3_connection_ssl_cert_file', 's3_connection_flavour', 'alba_connection_host', 'alba_connection_port', 'alba_connection_timeout', 'alba_connection_preset', 'alba_connection_transport', ],
                'mandatory': ['local_connection_path', ]
            },
            'metadata_server': {
                'optional': ['mds_db_type', 'mds_cached_pages', 'mds_poll_secs', 'mds_timeout_secs', 'mds_threads', 'mds_nodes', ],
                'mandatory': []
            },
        },
        'storagedriver': {
            'backend_connection_manager': {
                'optional': ['backend_connection_pool_capacity', 'backend_interface_retries_on_error', 'backend_interface_retry_interval_secs', 'backend_interface_retry_backoff_multiplier', 'backend_type', 's3_connection_host', 's3_connection_port', 's3_connection_username', 's3_connection_password', 's3_connection_verbose_logging', 's3_connection_use_ssl', 's3_connection_ssl_verify_host', 's3_connection_ssl_cert_file', 's3_connection_flavour', 'alba_connection_host', 'alba_connection_port', 'alba_connection_timeout', 'alba_connection_preset', 'alba_connection_transport', ],
                'mandatory': ['local_connection_path', ]
            },
            'backend_garbage_collector': {
                'optional': ['bgc_threads', ],
                'mandatory': []
            },
            'content_addressed_cache': {
                'optional': ['serialize_read_cache', 'clustercache_mount_points', ],
                'mandatory': ['read_cache_serialization_path', ]
            },
            'event_publisher': {
                'optional': ['events_amqp_uris', 'events_amqp_exchange', 'events_amqp_routing_key', ],
                'mandatory': []
            },
            'distributed_lock_store': {
                'optional': ['dls_type', 'dls_arakoon_timeout_ms', 'dls_arakoon_cluster_id', 'dls_arakoon_cluster_nodes', ],
                'mandatory': []
            },
            'distributed_transaction_log': {
                'optional': ['dtl_transport', ],
                'mandatory': ['dtl_path', ]
            },
            'file_driver': {
                'optional': ['fd_extent_cache_capacity', ],
                'mandatory': ['fd_cache_path', 'fd_namespace', ]
            },
            'filesystem': {
                'optional': ['fs_ignore_sync', 'fs_raw_disk_suffix', 'fs_max_open_files', 'fs_file_event_rules', 'fs_metadata_backend_type', 'fs_metadata_backend_arakoon_cluster_id', 'fs_metadata_backend_arakoon_cluster_nodes', 'fs_metadata_backend_mds_nodes', 'fs_metadata_backend_mds_apply_relocations_to_slaves', 'fs_metadata_backend_mds_timeout_secs', 'fs_cache_dentries', 'fs_dtl_config_mode', 'fs_dtl_host', 'fs_dtl_port', 'fs_dtl_mode', 'fs_enable_shm_interface', 'fs_enable_network_interface', ],
                'mandatory': ['fs_virtual_disk_format', ]
            },
            'metadata_server': {
                'optional': ['mds_db_type', 'mds_cached_pages', 'mds_poll_secs', 'mds_timeout_secs', 'mds_threads', 'mds_nodes', ],
                'mandatory': []
            },
            'network_interface': {
                'optional': ['network_uri', 'network_snd_rcv_queue_depth', ],
                'mandatory': []
            },
            'scocache': {
                'optional': [],
                'mandatory': ['trigger_gap', 'backoff_gap', 'scocache_mount_points', ]
            },
            'scrub_manager': {
                'optional': ['scrub_manager_interval', 'scrub_manager_sync_wait_secs', ],
                'mandatory': []
            },
            'shm_interface': {
                'optional': ['shm_region_size', ],
                'mandatory': []
            },
            'stats_collector': {
                'optional': ['stats_collector_interval_secs', 'stats_collector_destination', ],
                'mandatory': []
            },
            'threadpool_component': {
                'optional': ['num_threads', ],
                'mandatory': []
            },
            'volume_manager': {
                'optional': ['open_scos_per_volume', 'dtl_throttle_usecs', 'dtl_queue_depth', 'dtl_write_trigger', 'sap_persist_interval', 'dtl_check_interval_in_seconds', 'read_cache_default_behaviour', 'read_cache_default_mode', 'required_tlog_freespace', 'required_meta_freespace', 'freespace_check_interval', 'number_of_scos_in_tlog', 'non_disposable_scos_factor', 'default_cluster_size', 'metadata_cache_capacity', 'debug_metadata_path', 'arakoon_metadata_sequence_size', ],
                'mandatory': ['metadata_path', 'tlog_path', 'clean_interval', ]
            },
            'volume_registry': {
                'optional': ['vregistry_arakoon_timeout_ms', ],
                'mandatory': ['vregistry_arakoon_cluster_id', 'vregistry_arakoon_cluster_nodes', ]
            },
            'volume_router': {
                'optional': ['vrouter_local_io_sleep_before_retry_usecs', 'vrouter_local_io_retries', 'vrouter_check_local_volume_potential_period', 'vrouter_volume_read_threshold', 'vrouter_volume_write_threshold', 'vrouter_file_read_threshold', 'vrouter_file_write_threshold', 'vrouter_redirect_timeout_ms', 'vrouter_backend_sync_timeout_ms', 'vrouter_migrate_timeout_ms', 'vrouter_redirect_retries', 'vrouter_sco_multiplier', 'vrouter_routing_retries', 'vrouter_min_workers', 'vrouter_max_workers', 'vrouter_registry_cache_capacity', ],
                'mandatory': ['vrouter_id', ]
            },
            'volume_router_cluster': {
                'optional': [],
                'mandatory': ['vrouter_cluster_id', ]
            },
        },
    }

    def __init__(self, config_type, vpool_guid, storagedriver_id):
        """
        Initializes the class
        """

        def make_configure(sct):
            """
            section closure
            :param sct: Section to create configure function for
            """
            return lambda **kwargs: self._add(sct, **kwargs)

        if config_type != 'storagedriver':
            raise RuntimeError('Invalid configuration type. Allowed: storagedriver')

        storagerouterclient.Logger.setupLogging(LogHandler.load_path('storagerouterclient'))
        # noinspection PyArgumentList
        storagerouterclient.Logger.enableLogging()

        self._logger = LogHandler.get('extensions', name='storagedriver')
        self.config_type = config_type
        self.configuration = {}
        self.path = '/ovs/vpools/{0}/hosts/{1}/config/{{0}}'.format(vpool_guid, storagedriver_id)
        self.remote_path = 'etcd://127.0.0.1:2379{0}'.format(self.path.format('')).strip('/')
        self.is_new = True
        self.dirty_entries = []
        self.params = copy.deepcopy(StorageDriverConfiguration.parameters)  # Never use parameters directly
        # Fix some manual "I know what I'm doing" overrides
        backend_connection_manager = 'backend_connection_manager'
        self.params[self.config_type][backend_connection_manager]['optional'].append('s3_connection_strict_consistency')
        # Generate configure_* methods
        for section in self.params[self.config_type]:
            setattr(self, 'configure_{0}'.format(section), make_configure(section))

    def load(self):
        """
        Loads the configuration from a given file, optionally a remote one
        """
        self.configuration = {}
        if EtcdConfiguration.dir_exists(self.path.format('')):
            self.is_new = False
            for key in self.params[self.config_type]:
                if EtcdConfiguration.exists(self.path.format(key)):
                    self.configuration[key] = json.loads(EtcdConfiguration.get(self.path.format(key), raw=True))
        else:
            self._logger.debug('Could not find config {0}, a new one will be created'.format(self.path.format('')))
        self.dirty_entries = []

    def save(self, client=None, reload_config=True):
        """
        Saves the configuration to a given file, optionally a remote one
        :param client: If provided, save remote configuration
        :param reload_config: Reload the running Storage Driver configuration
        """
        self._validate()
        for key in self.configuration:
            contents = json.dumps(self.configuration[key], indent=4)
            EtcdConfiguration.set(self.path.format(key), contents, raw=True)
        if self.config_type == 'storagedriver' and reload_config is True:
            if len(self.dirty_entries) > 0:
                if client is None:
                    self._logger.info('Applying local storagedriver configuration changes')
                    changes = LocalStorageRouterClient(self.remote_path).update_configuration(self.remote_path)
                else:
                    self._logger.info('Applying storagedriver configuration changes on {0}'.format(client.ip))
                    with remote(client.ip, [LocalStorageRouterClient]) as rem:
                        changes = copy.deepcopy(rem.LocalStorageRouterClient(self.remote_path).update_configuration(self.remote_path))
                for change in changes:
                    if change['param_name'] not in self.dirty_entries:
                        raise RuntimeError('Unexpected configuration change: {0}'.format(change['param_name']))
                    self._logger.info('Changed {0} from "{1}" to "{2}"'.format(change['param_name'], change['old_value'], change['new_value']))
                    self.dirty_entries.remove(change['param_name'])
                self._logger.info('Changes applied')
                if len(self.dirty_entries) > 0:
                    self._logger.warning('Following changes were not applied: {0}'.format(', '.join(self.dirty_entries)))
            else:
                self._logger.debug('No need to apply changes, nothing changed')
        self.is_new = False
        self.dirty_entries = []

    def clean(self):
        """
        Cleans the loaded configuration, removing all obsolete parameters
        """
        for section, entries in self.params[self.config_type].iteritems():
            if section in self.configuration:
                section_configuration = copy.deepcopy(self.configuration[section])
                for param in section_configuration:
                    if param not in entries['mandatory'] and param not in entries['optional']:
                        del self.configuration[section][param]

    def _validate(self):
        """
        Validates the loaded configuration against the mandatory and optional parameters
        """
        # Fix some manual "I know what I'm doing" overrides
        backend_connection_manager = 'backend_connection_manager'
        backend_type = 'backend_type'
        if self.configuration.get(backend_connection_manager, {}).get(backend_type, '') != 'LOCAL':
            if 'local_connection_path' in self.params[self.config_type][backend_connection_manager]['mandatory']:
                self.params[self.config_type][backend_connection_manager]['mandatory'].remove('local_connection_path')
                self.params[self.config_type][backend_connection_manager]['optional'].append('local_connection_path')
        # Validation
        errors = []
        for section, entries in self.params[self.config_type].iteritems():
            if section not in self.configuration:
                if len(entries['mandatory']) > 0:
                    errors.append('Section {0} was not found'.format(section))
            else:
                for param in entries['mandatory']:
                    if param not in self.configuration[section]:
                        errors.append('Key {0} -> {1} missing'.format(section, param))
                for param in self.configuration[section]:
                    if param not in entries['mandatory'] and param not in entries['optional']:
                        errors.append('Key {0} -> {1} is obsolete/invalid'.format(section, param))
        if errors:
            raise RuntimeError('Invalid configuration:\n  {0}'.format('\n  '.join(errors)))

    def _add(self, section, **kwargs):
        """
        Configures a section
        """
        sparams = self.params[self.config_type][section]
        errors = []
        for item in kwargs:
            if item not in sparams['mandatory'] and item not in sparams['optional']:
                errors.append(item)
        if errors:
            raise RuntimeError('Invalid parameters:\n  {0}'.format('\n  '.join(errors)))
        for item, value in kwargs.iteritems():
            if section not in self.configuration:
                self.configuration[section] = {}
            if item not in self.configuration[section] or self.configuration[section][item] != value:
                self.dirty_entries.append(item)
            self.configuration[section][item] = value
