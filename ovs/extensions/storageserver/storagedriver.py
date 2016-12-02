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
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonClusterConfig
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.remote import remote
from ovs.extensions.generic.toolbox import Toolbox
from ovs.log.log_handler import LogHandler
from volumedriver.storagerouter import storagerouterclient
from volumedriver.storagerouter.storagerouterclient import \
    ClusterContact, ClusterNodeConfig, \
    DTLConfig, DTLConfigMode, DTLMode, \
    MDSMetaDataBackendConfig,  MDSNodeConfig, \
    ObjectNotFoundException as SRCObjectNotFoundException, \
    ReadCacheBehaviour, ReadCacheMode, \
    Role, Statistics, VolumeInfo
if os.environ.get('RUNNING_UNITTESTS') == 'True':
    from ovs.extensions.storageserver.tests.mockups import \
        ArakoonNodeConfig, ClusterRegistry, LocalStorageRouterClient, \
        MDSClient, ObjectRegistryClient as ORClient, StorageRouterClient
else:
    from volumedriver.storagerouter.storagerouterclient import \
        ArakoonNodeConfig, ClusterRegistry, LocalStorageRouterClient, \
        MDSClient, ObjectRegistryClient as ORClient, StorageRouterClient

client_vpool_cache = {}
oclient_vpool_cache = {}
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
    VOLDRV_DTL_MANUAL_MODE = 'Manual'
    VOLDRV_DTL_AUTOMATIC_MODE = 'Automatic'
    VOLDRV_DTL_TRANSPORT_TCP = 'TCP'
    VOLDRV_DTL_TRANSPORT_RSOCKET = 'RSocket'

    FRAMEWORK_DTL_SYNC = 'sync'
    FRAMEWORK_DTL_ASYNC = 'a_sync'
    FRAMEWORK_DTL_NO_SYNC = 'no_sync'
    FRAMEWORK_DTL_TRANSPORT_TCP = 'tcp'
    FRAMEWORK_DTL_TRANSPORT_RSOCKET = 'rdma'

    METADATA_CACHE_PAGE_SIZE = 256 * 24
    DEFAULT_METADATA_CACHE_SIZE = 8192 * METADATA_CACHE_PAGE_SIZE

    VDISK_DTL_MODE_MAP = {FRAMEWORK_DTL_SYNC: DTLMode.SYNCHRONOUS,
                          FRAMEWORK_DTL_ASYNC: DTLMode.ASYNCHRONOUS,
                          FRAMEWORK_DTL_NO_SYNC: None}
    VPOOL_DTL_MODE_MAP = {FRAMEWORK_DTL_SYNC: VOLDRV_DTL_SYNC,
                          FRAMEWORK_DTL_ASYNC: VOLDRV_DTL_ASYNC,
                          FRAMEWORK_DTL_NO_SYNC: None}
    VPOOL_DTL_TRANSPORT_MAP = {FRAMEWORK_DTL_TRANSPORT_TCP: VOLDRV_DTL_TRANSPORT_TCP,
                               FRAMEWORK_DTL_TRANSPORT_RSOCKET: VOLDRV_DTL_TRANSPORT_RSOCKET}
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
    def load(vpool, excluded_storagedrivers=None):
        """
        Initializes the wrapper for a given vpool
        :param vpool: vPool for which the StorageRouterClient needs to be loaded
        :type vpool: vPool
        :param excluded_storagedrivers: A list of storagedrivers that cannot be used as a client
        :type excluded_storagedrivers: list or None
        """
        if excluded_storagedrivers is None:
            excluded_storagedrivers = []
        key = vpool.identifier
        if key not in client_vpool_cache:
            cluster_contacts = []
            for storagedriver in vpool.storagedrivers[:3]:
                if storagedriver not in excluded_storagedrivers:
                    cluster_contacts.append(ClusterContact(str(storagedriver.cluster_ip), storagedriver.ports['xmlrpc']))
            client = StorageRouterClient(str(vpool.guid), cluster_contacts)
            client_vpool_cache[key] = client
        return client_vpool_cache[key]


class ObjectRegistryClient(object):
    """
    Client to access the object registry
    """
    def __init__(self):
        """
        Dummy init method
        """
        pass

    @staticmethod
    def load(vpool):
        """
        Initializes the wrapper for a given vpool
        :param vpool: vPool for which the ObjectRegistryClient needs to be loaded
        """
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            return ORClient(str(vpool.guid), None, None)

        key = vpool.identifier
        if key not in oclient_vpool_cache:
            arakoon_node_configs = []
            arakoon_cluster_name = str(Configuration.get('/ovs/framework/arakoon_clusters|voldrv'))
            config = ArakoonClusterConfig(cluster_id=arakoon_cluster_name, filesystem=False)
            config.load_config()
            for node in config.nodes:
                arakoon_node_configs.append(ArakoonNodeConfig(str(node.name), str(node.ip), node.client_port))
            client = ORClient(str(vpool.guid), str(arakoon_cluster_name), arakoon_node_configs)
            oclient_vpool_cache[key] = client
        return oclient_vpool_cache[key]


class MetadataServerClient(object):
    """
    Builds a MDSClient
    """
    _logger = LogHandler.get('extensions', name='storagedriver')
    storagerouterclient.Logger.setupLogging(LogHandler.load_path('storagerouterclient'))
    # noinspection PyArgumentList
    storagerouterclient.Logger.enableLogging()

    MDS_ROLE = type('MDSRole', (), {'MASTER': Role.Master,
                                    'SLAVE': Role.Slave})

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

    def __init__(self, config_type, vpool_guid, storagedriver_id):
        """
        Initializes the class
        """
        if config_type != 'storagedriver':
            raise RuntimeError('Invalid configuration type. Allowed: storagedriver')

        storagerouterclient.Logger.setupLogging(LogHandler.load_path('storagerouterclient'))
        # noinspection PyArgumentList
        storagerouterclient.Logger.enableLogging()

        self._logger = LogHandler.get('extensions', name='storagedriver')
        self.config_type = config_type
        self.configuration = {}
        self.key = '/ovs/vpools/{0}/hosts/{1}/config'.format(vpool_guid, storagedriver_id)
        self.remote_path = Configuration.get_configuration_path(self.key).strip('/')
        self.is_new = True
        self.dirty_entries = []

    def load(self):
        """
        Loads the configuration from a given file, optionally a remote one
        """
        self.configuration = {}
        if Configuration.exists(self.key):
            self.is_new = False
            self.configuration = json.loads(Configuration.get(self.key, raw=True))
        else:
            self._logger.debug('Could not find config {0}, a new one will be created'.format(self.key))
        self.dirty_entries = []

    def save(self, client=None, reload_config=True):
        """
        Saves the configuration to a given file, optionally a remote one
        :param client: If provided, save remote configuration
        :param reload_config: Reload the running Storage Driver configuration
        """
        Configuration.set(self.key, json.dumps(self.configuration, indent=4), raw=True)
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

    def __getattr__(self, item):
        if item.startswith('configure_'):
            section = Toolbox.remove_prefix(item, 'configure_')
            return lambda **kwargs: self._add(section, **kwargs)

    def _add(self, section, **kwargs):
        """
        Configures a section
        """
        for item, value in kwargs.iteritems():
            if section not in self.configuration:
                self.configuration[section] = {}
            if item not in self.configuration[section] or self.configuration[section][item] != value:
                self.dirty_entries.append(item)
            self.configuration[section][item] = value
