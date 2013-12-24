# license see http://www.openvstorage.com/licenses/opensource/
"""
Wrapper class for the storagerouterclient of the voldrv team
"""

from volumedriver.storagerouter import storagerouterclient
from ovs.plugin.provider.configuration import Configuration
import json
import os


class VolumeStorageRouterClient(object):
    """
    Client to access storagerouterclient
    """

    STATISTICS_KEYS = ['cluster_cache_hits',
                       'backend_write_operations',
                       'backend_data_read',
                       'metadata_store_hits',
                       'data_written',
                       'data_read',
                       'write_time',
                       'metadata_store_misses',
                       'backend_data_written',
                       'sco_cache_misses',
                       'backend_read_operations',
                       'sco_cache_hits',
                       'write_operations',
                       'cluster_cache_misses',
                       'read_operations']
    FOC_STATUS = {'ok_standalone': 10,
                  'ok_sync': 10,
                  'catch_up': 20,
                  'degraded': 30}

    def __init__(self):
        """
        Initializes the wrapper given a configfile for the RPC communication
        """
        self._host = Configuration.get('ovs.grid.ip')
        self._port = int(Configuration.get('volumedriver.filesystem.xmlrpc.port'))

    def load(self):
        """
        Loads and returns the client
        """
        return storagerouterclient.StorageRouterClient(self._host, self._port)

class VolumeStorageRouterConfiguration(object):
    """
    VolumeStorageRouter configuration class
    """
    def __init__(self, storagerouter):
        self._config_specfile = os.path.join(Configuration.get('ovs.core.cfgdir'),'specs', 'volumedriverfs.json')
        self._config_file = os.path.join(Configuration.get('ovs.core.cfgdir'), '{}.json'.format(storagerouter))
        self._config_tmpfile = os.path.join(Configuration.get('ovs.core.cfgdir'), '{}.json.tmp'.format(storagerouter))

    def load_config(self):
        if os.path.exists(self._config_file) and not os.path.getsize(self._config_file) == 0:
            self._config_readfile_handler = open(self._config_file, 'r')
            self._config_file_handler = open(self._config_tmpfile, 'w')
            self._config_file_content = json.load(self._config_readfile_handler)
            self._config_readfile_handler.close()
        else:
            self._config_file_handler = open(self._config_file, 'w')
            self._config_specfile_handler = open(self._config_specfile, 'r')
            self._config_file_content = json.load(self._config_specfile_handler)
            self._config_specfile_handler.close()

    def write_config(self):
        json.dump(self._config_file_content, self._config_file_handler, indent=2)
        self._config_file_handler.close()
        if os.path.exists(self._config_tmpfile):
            os.rename(self._config_tmpfile, self._config_file)

    def add_cache(self):
        pass

    def configure_backend(self, backend_config):
        self.load_config()
        if not backend_config:
            raise ValueError('No backend config specified, unable to configure volumestoragerouter')
        for key,value in backend_config.iteritems():
            self._config_file_content['backend_connection_manager'][key] = value
        self.write_config()

    def configure_readcache(self, readcaches, rspath):
        """
        Configures volume storage router content address cache
        @param readcache_config: list of readcache configuration dictionaries
        """
        self.load_config()
        self._config_file_content['content_addressed_cache']['clustercache_mount_points'] = readcaches
        self._config_file_content['content_addressed_cache']['read_cache_serialization_path'] = rspath
        self.write_config()

    def configure_volumemanager(self, volumemanager_config):
        """
        Configures volume storage router volume manager
        @param volumemanager_config: dictionary with key/value pairs
        """
        self.load_config()
        for key,value in volumemanager_config.iteritems():
            self._config_file_content['volume_manager'][key] = value
        self.write_config()

    def configure_scocache(self, scocaches, trigger_gap, backoff_gap):
        """
        Configures volume storage router scocaches
        @param scocaches: list of scocache dictionaries
        @param trigger_gap: string to be set as trigger_gap value
        @param backoff_gap: string to be set as backoff gap value
        """
        self.load_config()
        self._config_file_content['scocache']['scocache_mount_points'] = scocaches
        self._config_file_content['scocache']['trigger_gap'] = trigger_gap
        self._config_file_content['scocache']['backoff_gap'] = backoff_gap
        self.write_config()

    def configure_failovercache(self, failovercache):
        """
        Configures volume storage router failover cache path
        @param failovercache: path to the failover cache directory
        """
        self.load_config()
        self._config_file_content.update({'failovercache': {'failovercache_path': failovercache }})
        self.write_config()

    def configure_filesystem(self, filesystem_config):
        """
        Configures volume storage router filesystem properties
        @param filesystem_config: dictionary with key/value pairs
        """
        self.load_config()
        for key,value in filesystem_config.iteritems():
            self._config_file_content['filesystem'][key] = value
        self.write_config()

    def configure_volumerouter(self, vrouter_cluster, vrouter_config):
        """
        Configures volume storage router
        @param vrouter_config: dictionary of key/value pairs
        """
        self.load_config()
        for key,value in vrouter_config.iteritems():
            if not key in ('host', 'xmlrpc_port'):
                self._config_file_content['volume_router'][key] = value
        # Configure the vrouter arakoon with empty values in order to use tokyo cabinet
        self._config_file_content['volume_router']['vrouter_arakoon_cluster_id'] = ''
        self._config_file_content['volume_router']['vrouter_arakoon_cluster_nodes'] = []
        if not 'volume_router_cluster' in self._config_file_content:
            self._config_file_content['volume_router_cluster'] = {}
        self._config_file_content['volume_router_cluster'].update({'vrouter_cluster_id': vrouter_cluster})
        if 'vrouter_cluster_nodes' in self._config_file_content['volume_router_cluster']:
            for node in self._config_file_content['volume_router_cluster']['vrouter_cluster_nodes']:
                if node['vrouter_id'] == vrouter_config['vrouter_id'] or \
                node['host'] == '127.0.0.1':
                    self._config_file_content['volume_router_cluster']['vrouter_cluster_nodes'].remove(node)
                    break
        else:
            self._config_file_content['volume_router_cluster']['vrouter_cluster_nodes'] = []
        new_node = {'vrouter_id': vrouter_config['vrouter_id'],
                    'host': vrouter_config['host'],
                    'message_port': int(vrouter_config['xmlrpc_port'])-1,
                    'xmlrpc_port': vrouter_config['xmlrpc_port'],
                    'failovercache_port': int(vrouter_config['xmlrpc_port'])+1}
        self._config_file_content['volume_router_cluster']['vrouter_cluster_nodes'].append(new_node)
        self.write_config()

    def configure_arakoon_cluster(self, arakoon_cluster_id, arakoon_nodes):
        """
        Configures volume storage router arakoon cluster
        @param arakoon_cluster_id: name of the arakoon cluster
        @param arakoon_nodes: dictionary of arakoon nodes in this cluster
        """
        self.load_config()
        if not 'volume_registry' in self._config_file_content:
            self._config_file_content['volume_registry'] = {}
        self._config_file_content['volume_registry']['vregistry_arakoon_cluster_id'] = arakoon_cluster_id
        self._config_file_content['volume_registry']['vregistry_arakoon_cluster_nodes'] = []
        self._config_file_content['filesystem']['fs_arakoon_cluster_id'] = arakoon_cluster_id
        for node_id,node_config in arakoon_nodes.iteritems():
            node_dict = {'node_id' : node_id, 'host' : node_config[0][0], 'port' : node_config[1]}
            self._config_file_content['volume_registry']['vregistry_arakoon_cluster_nodes'].append(node_dict)
        self.write_config()

    def configure_event_publisher(self, queue_config):
        """
        Configures volume storage router event publisher
        @param queue_config: dictionary of with queue configuration key/value
        """
        self.load_config()
        if not "event_publisher" in self._config_file_content:
            self._config_file_content["event_publisher"] = {}
        for key,value in queue_config.iteritems():
            self._config_file_content["event_publisher"][key] = value
        self.write_config()
