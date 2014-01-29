# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Wrapper class for the storagerouterclient of the voldrv team
"""

from volumedriver.storagerouter import storagerouterclient
from ovs.plugin.provider.configuration import Configuration
import json
import os

vsr_cache = {}


class VolumeStorageRouterClient(object):
    """
    Client to access storagerouterclient
    """

    FOC_STATUS = {'': 0,
                  'ok_standalone': 10,
                  'ok_sync': 10,
                  'catch_up': 20,
                  'degraded': 30}

    def __init__(self):
        """
        Init method
        """
        self._host = None
        self._port = None
        self.empty_statistics = lambda: storagerouterclient.Statistics()
        self.empty_info = lambda: storagerouterclient.VolumeInfo()

    def load(self, vpool=None, vsr=None):
        """
        Initializes the wrapper given a vpool name for which it finds the corresponding vsr
        Loads and returns the client
        """

        if vpool is None and vsr is None:
            raise RuntimeError('One of the parameters vpool or vsr needs to be passed')
        if vpool is not None and vsr is not None:
            raise RuntimeError('Only one of the parameters vpool or vsr needs to be passed')

        if vpool is not None:
            if vpool.guid in vsr_cache:
                return vsr_cache[vpool.guid]
            if len(vpool.vsrs) > 0:
                vsr = vpool.vsrs[0]
            else:
                raise ValueError('Cannot find vsr for vpool {0}'.format(vpool.guid))
        self._host = vsr.ip
        self._port = vsr.port
        client = storagerouterclient.StorageRouterClient(str(self._host), int(self._port))
        vsr_cache[vsr.vpool_guid] = client
        return client


class VolumeStorageRouterConfiguration(object):
    """
    VolumeStorageRouter configuration class
    """
    def __init__(self, storagerouter):
        self._config_specfile = os.path.join(Configuration.get('ovs.core.cfgdir'), 'specs', 'volumedriverfs.json')
        self._config_file = os.path.join(Configuration.get('ovs.core.cfgdir'), '{}.json'.format(storagerouter))
        self._config_tmpfile = os.path.join(Configuration.get('ovs.core.cfgdir'), '{}.json.tmp'.format(storagerouter))
        self._config_readfile_handler = None
        self._config_file_handler = None
        self._config_specfile_handler = None
        self._config_file_content = None

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
        for key, value in backend_config.iteritems():
            self._config_file_content['backend_connection_manager'][key] = value
        self.write_config()

    def configure_readcache(self, readcaches, rspath):
        """
        Configures volume storage router content address cache
        @param readcaches: list of readcache configuration dictionaries
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
        for key, value in volumemanager_config.iteritems():
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
        self._config_file_content.update({'failovercache': {'failovercache_path': failovercache}})
        self.write_config()

    def configure_filesystem(self, filesystem_config):
        """
        Configures volume storage router filesystem properties
        @param filesystem_config: dictionary with key/value pairs
        """
        self.load_config()
        for key, value in filesystem_config.iteritems():
            self._config_file_content['filesystem'][key] = value
        self.write_config()

    def configure_volumerouter(self, vrouter_cluster, vrouter_config, update_cluster=True):
        """
        Configures volume storage router
        @param vrouter_config: dictionary of key/value pairs
        """
        self.load_config()
        for key, value in vrouter_config.iteritems():
            if not key in ('host', 'xmlrpc_port'):
                self._config_file_content['volume_router'][key] = value
        # Configure the vrouter arakoon with empty values in order to use tokyo cabinet
        self._config_file_content['volume_router']['vrouter_arakoon_cluster_id'] = ''
        self._config_file_content['volume_router']['vrouter_arakoon_cluster_nodes'] = []
        if update_cluster:
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
                        'message_port': int(vrouter_config['xmlrpc_port']) - 1,
                        'xmlrpc_port': vrouter_config['xmlrpc_port'],
                        'failovercache_port': int(vrouter_config['xmlrpc_port']) + 1}
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
        for node_id, node_config in arakoon_nodes.iteritems():
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
        for key, value in queue_config.iteritems():
            self._config_file_content["event_publisher"][key] = value
        self.write_config()
