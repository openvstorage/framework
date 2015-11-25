# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Wrapper class for the storagedriver client of the voldrv team
"""

import os
import json
import copy
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.remote import Remote
from ovs.log.logHandler import LogHandler
from volumedriver.storagerouter import storagerouterclient
from volumedriver.storagerouter.storagerouterclient import ClusterContact
from volumedriver.storagerouter.storagerouterclient import LocalStorageRouterClient as LSRClient
from volumedriver.storagerouter.storagerouterclient import MDSClient
from volumedriver.storagerouter.storagerouterclient import MDSNodeConfig
from volumedriver.storagerouter.storagerouterclient import ReadCacheBehaviour
from volumedriver.storagerouter.storagerouterclient import ReadCacheMode
from volumedriver.storagerouter.storagerouterclient import Statistics
from volumedriver.storagerouter.storagerouterclient import StorageRouterClient as SRClient
from volumedriver.storagerouter.storagerouterclient import VolumeInfo


logger = LogHandler.get('extensions', name='storagedriver')
storagerouterclient.Logger.setupLogging(LogHandler.load_path('storagerouterclient'))
# noinspection PyArgumentList
storagerouterclient.Logger.enableLogging()

client_vpool_cache = {}
client_storagedriver_cache = {}
mdsclient_service_cache = {}


class StorageDriverClient(object):
    """
    Client to access storagedriver client
    """
    VOLDRV_DTL_SYNC = ''
    VOLDRV_DTL_ASYNC = ''
    VOLDRV_DTL_NOSYNC = ''
    VOLDRV_NO_CACHE = 'NoCache'
    VOLDRV_CACHE_ON_READ = 'CacheOnRead'
    VOLDRV_CONTENT_BASED = 'ContentBased'
    VOLDRV_CACHE_ON_WRITE = 'CacheOnWrite'
    VOLDRV_LOCATION_BASED = 'LocationBased'
    VOLDRV_DTL_TRANSPORT_TCP = 'TCP'
    VOLDRV_DTL_TRANSPORT_RSOCKET = 'RSocket'

    FRAMEWORK_DTL_SYNC = 'sync'
    FRAMEWORK_DTL_ASYNC = 'async'
    FRAMEWORK_DTL_NOSYNC = 'no_sync'
    FRAMEWORK_NO_CACHE = 'none'
    FRAMEWORK_CACHE_ON_READ = 'on_read'
    FRAMEWORK_CONTENT_BASED = 'dedupe'
    FRAMEWORK_CACHE_ON_WRITE = 'on_write'
    FRAMEWORK_LOCATION_BASED = 'non_dedupe'
    FRAMEWORK_DTL_TRANSPORT_TCP = 'tcp'
    FRAMEWORK_DTL_TRANSPORT_RSOCKET = 'rdma'

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
    VDISK_DTL_MODE_MAP = {FRAMEWORK_DTL_SYNC: '',
                          FRAMEWORK_DTL_ASYNC: '',
                          FRAMEWORK_DTL_NOSYNC: ''}
    VPOOL_DTL_MODE_MAP = {FRAMEWORK_DTL_SYNC: VOLDRV_DTL_SYNC,
                          FRAMEWORK_DTL_ASYNC: VOLDRV_DTL_ASYNC,
                          FRAMEWORK_DTL_NOSYNC: VOLDRV_DTL_NOSYNC}
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
                            VOLDRV_DTL_NOSYNC: FRAMEWORK_DTL_NOSYNC,
                            '': FRAMEWORK_DTL_SYNC,
                            '': FRAMEWORK_DTL_ASYNC,
                            '': FRAMEWORK_DTL_NOSYNC}
    REVERSE_DTL_TRANSPORT_MAP = {VOLDRV_DTL_TRANSPORT_TCP: FRAMEWORK_DTL_TRANSPORT_TCP,
                                 VOLDRV_DTL_TRANSPORT_RSOCKET: FRAMEWORK_DTL_TRANSPORT_RSOCKET}
    TLOG_MULTIPLIER_MAP = {4: 16,
                           8: 8,
                           16: 4,
                           32: 2,
                           64: 1,
                           128: 1}

    DTL_STATUS = {'': 0,
                  'ok_standalone': 10,
                  'ok_sync': 10,
                  'catch_up': 20,
                  'degraded': 30}
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
                 '4k_read_operations', '4k_write_operations']
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

        key = '{0}_{1}'.format(vpool.guid, '_'.join(guid for guid in vpool.storagedrivers_guids))
        if key not in client_vpool_cache:
            cluster_contacts = []
            for storagedriver in vpool.storagedrivers[:3]:
                cluster_contacts.append(ClusterContact(str(storagedriver.cluster_ip), storagedriver.ports[1]))
            client = SRClient(str(vpool.guid), cluster_contacts)
            client_vpool_cache[key] = client
        return client_vpool_cache[key]


class MetadataServerClient(object):
    """
    Builds a MDSClient
    """

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

        key = service.guid
        if key not in mdsclient_service_cache:
            try:
                # noinspection PyArgumentList
                client = MDSClient(MDSNodeConfig(address=str(service.storagerouter.ip), port=service.ports[0]))
                mdsclient_service_cache[key] = client
            except RuntimeError as ex:
                logger.error('Error loading MDSClient on {0}: {1}'.format(service.storagerouter.ip, ex))
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
        # hg revision: 63d8c887a77f44365f8258a78caf889cbf5fd2bc
        # buildTime: Mon Oct 12 08:55:42 UTC 2015
        'metadataserver': {
            'backend_connection_manager': {
                'optional': ['backend_connection_pool_capacity', 'backend_type', 's3_connection_host', 's3_connection_port', 's3_connection_username', 's3_connection_password', 's3_connection_verbose_logging', 's3_connection_use_ssl', 's3_connection_ssl_verify_host', 's3_connection_ssl_cert_file', 's3_connection_flavour', 'alba_connection_host', 'alba_connection_port', 'alba_connection_timeout', 'alba_connection_preset', ],
                'mandatory': ['local_connection_path', ]
            },
            'metadata_server': {
                'optional': ['mds_db_type', 'mds_cached_pages', 'mds_poll_secs', 'mds_timeout_secs', 'mds_threads', 'mds_nodes', ],
                'mandatory': []
            },
        },
        'storagedriver': {
            'backend_connection_manager': {
                'optional': ['backend_connection_pool_capacity', 'backend_type', 's3_connection_host', 's3_connection_port', 's3_connection_username', 's3_connection_password', 's3_connection_verbose_logging', 's3_connection_use_ssl', 's3_connection_ssl_verify_host', 's3_connection_ssl_cert_file', 's3_connection_flavour', 'alba_connection_host', 'alba_connection_port', 'alba_connection_timeout', 'alba_connection_preset', ],
                'mandatory': ['local_connection_path', ]
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
            'failovercache': {
                'optional': ['failovercache_transport', ],
                'mandatory': ['failovercache_path', ]
            },
            'file_driver': {
                'optional': ['fd_extent_cache_capacity', ],
                'mandatory': ['fd_cache_path', 'fd_namespace', ]
            },
            'filesystem': {
                'optional': ['fs_ignore_sync', 'fs_raw_disk_suffix', 'fs_max_open_files', 'fs_file_event_rules', 'fs_metadata_backend_type', 'fs_metadata_backend_arakoon_cluster_id', 'fs_metadata_backend_arakoon_cluster_nodes', 'fs_metadata_backend_mds_nodes', 'fs_metadata_backend_mds_apply_relocations_to_slaves', 'fs_cache_dentries', 'fs_dtl_config_mode', 'fs_dtl_host', 'fs_dtl_port', 'fs_dtl_mode', ],
                'mandatory': ['fs_virtual_disk_format', ]
            },
            'metadata_server': {
                'optional': ['mds_db_type', 'mds_cached_pages', 'mds_poll_secs', 'mds_timeout_secs', 'mds_threads', 'mds_nodes', ],
                'mandatory': []
            },
            'scocache': {
                'optional': [],
                'mandatory': ['trigger_gap', 'backoff_gap', 'scocache_mount_points', ]
            },
            'threadpool_component': {
                'optional': ['num_threads', ],
                'mandatory': []
            },
            'volume_manager': {
                'optional': ['open_scos_per_volume', 'foc_throttle_usecs', 'foc_queue_depth', 'foc_write_trigger', 'sap_persist_interval', 'failovercache_check_interval_in_seconds', 'read_cache_default_behaviour', 'read_cache_default_mode', 'required_tlog_freespace', 'required_meta_freespace', 'freespace_check_interval', 'number_of_scos_in_tlog', 'non_disposable_scos_factor', 'debug_metadata_path', 'arakoon_metadata_sequence_size', ],
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

    def __init__(self, config_type, vpool_name, number=None):
        """
        Initializes the class
        """

        def make_configure(sct):
            """
            section closure
            :param sct: Section to create configure function for
            """
            return lambda **kwargs: self._add(sct, **kwargs)

        if config_type not in ['storagedriver', 'metadataserver']:
            raise RuntimeError('Invalid configuration type. Allowed: storagedriver, metadataserver')
        self.config_type = config_type
        self.vpool_name = vpool_name
        self.configuration = {}
        self.is_new = True
        self.dirty_entries = []
        self.number = number
        self.params = copy.deepcopy(StorageDriverConfiguration.parameters)  # Never use parameters directly
        self.base_path = '{0}/storagedriver/{1}'.format(Configuration.get('ovs.core.cfgdir'), self.config_type)
        if self.number is None:
            self.path = '{0}/{1}.json'.format(self.base_path, self.vpool_name)
        else:
            self.path = '{0}/{1}_{2}.json'.format(self.base_path, self.vpool_name, self.number)
        # Fix some manual "I know what I'm doing" overrides
        backend_connection_manager = 'backend_connection_manager'
        self.params[self.config_type][backend_connection_manager]['optional'].append('s3_connection_strict_consistency')
        # Generate configure_* methods
        for section in self.params[self.config_type]:
            setattr(self, 'configure_{0}'.format(section), make_configure(section))

    def load(self, client=None):
        """
        Loads the configuration from a given file, optionally a remote one
        :param client: If provided, load remote configuration
        """
        contents = '{}'
        if client is None:
            if os.path.isfile(self.path):
                logger.debug('Loading file {0}'.format(self.path))
                with open(self.path, 'r') as config_file:
                    contents = config_file.read()
                    self.is_new = False
            else:
                logger.debug('Could not find file {0}, a new one will be created'.format(self.path))
        else:
            if client.file_exists(self.path):
                logger.debug('Loading file {0}'.format(self.path))
                contents = client.file_read(self.path)
                self.is_new = False
            else:
                logger.debug('Could not find file {0}, a new one will be created'.format(self.path))
        self.dirty_entries = []
        self.configuration = json.loads(contents)

    def save(self, client=None, reload_config=True):
        """
        Saves the configuration to a given file, optionally a remote one
        :param client: If provided, save remote configuration
        :param reload_config: Reload the running Storage Driver configuration
        """
        self._validate()
        contents = json.dumps(self.configuration, indent=2)
        if client is None:
            if not os.path.exists(self.base_path):
                os.makedirs(self.base_path)
            with open(self.path, 'w') as config_file:
                config_file.write(contents)
        else:
            client.dir_create(self.base_path)
            client.file_write(self.path, contents)
        if self.config_type == 'storagedriver' and reload_config is True:
            if len(self.dirty_entries) > 0:
                if client is None:
                    logger.info('Applying local storagedriver configuration changes')
                    changes = LSRClient(self.path).update_configuration(self.path)
                else:
                    logger.info('Applying storagedriver configuration changes on {0}'.format(client.ip))
                    with Remote(client.ip, [LSRClient]) as remote:
                        changes = copy.deepcopy(remote.LocalStorageRouterClient(self.path).update_configuration(self.path))
                for change in changes:
                    if change['param_name'] not in self.dirty_entries:
                        raise RuntimeError('Unexpected configuration change: {0}'.format(change['param_name']))
                    logger.info('Changed {0} from "{1}" to "{2}"'.format(change['param_name'], change['old_value'], change['new_value']))
                    self.dirty_entries.remove(change['param_name'])
                logger.info('Changes applied')
                if len(self.dirty_entries) > 0:
                    logger.warning('Following changes were not applied: {0}'.format(', '.join(self.dirty_entries)))
            else:
                logger.debug('No need to apply changes, nothing changed')
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

    @staticmethod
    def build_filesystem_by_hypervisor(hypervisor_type):
        """
        Builds a filesystem configuration dict, based on a given hypervisor
        :param hypervisor_type: Hypervisor type for which to build a filesystem
        """
        if hypervisor_type == 'VMWARE':
            return {'fs_virtual_disk_format': 'vmdk',
                    'fs_file_event_rules': [{'fs_file_event_rule_calls': ['Mknod', 'Unlink', 'Rename'],
                                             'fs_file_event_rule_path_regex': '.*.vmx'},
                                            {'fs_file_event_rule_calls': ['Rename'],
                                             'fs_file_event_rule_path_regex': '.*.vmx~'}]}
        if hypervisor_type == 'KVM':
            return {'fs_virtual_disk_format': 'raw',
                    'fs_raw_disk_suffix': '.raw',
                    'fs_file_event_rules': [{'fs_file_event_rule_calls': ['Mknod', 'Unlink', 'Rename', 'Write'],
                                             'fs_file_event_rule_path_regex': '(?!vmcasts)(.*.xml)'}]}
        return {}

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


class GaneshaConfiguration:

    def __init__(self):
        self._config_corefile = os.path.join(Configuration.get('ovs.core.cfgdir'), 'templates', 'ganesha-core.conf')
        self._config_exportfile = os.path.join(Configuration.get('ovs.core.cfgdir'), 'templates', 'ganesha-export.conf')

    def generate_config(self, target_file, params):
        with open(self._config_corefile, 'r') as core_config_file:
            config = core_config_file.read()
        with open(self._config_exportfile, 'r') as export_section_file:
            config += export_section_file.read()

        for key, value in params.iteritems():
            print 'replacing {0} by {1}'.format(key, value)
            config = config.replace(key, value)

        with open(target_file, 'wb') as config_out:
            config_out.write(config)
