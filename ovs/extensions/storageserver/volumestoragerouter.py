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

from volumedriver.storagerouter.storagerouterclient import StorageRouterClient, ClusterContact, Statistics, VolumeInfo
from ovs.plugin.provider.configuration import Configuration
from ovs.plugin.provider.net import Net
import json
import os

client_vpool_cache = {}
client_vsr_cache = {}

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
        self.empty_statistics = lambda: Statistics()
        self.empty_info = lambda: VolumeInfo()
        self.stat_counters = ['backend_data_read', 'backend_data_written',
                              'backend_read_operations', 'backend_write_operations',
                              'cluster_cache_hits', 'cluster_cache_misses', 'data_read',
                              'data_written', 'metadata_store_hits', 'metadata_store_misses',
                              'read_operations', 'sco_cache_hits', 'sco_cache_misses',
                              'write_operations']
        self.stat_sums = {'operations': ['write_operations', 'read_operations'],
                          'cache_hits': ['sco_cache_hits', 'cluster_cache_hits'],
                          'data_transferred': ['data_written', 'data_read']}
        self.stat_keys = self.stat_counters + self.stat_sums.keys()

    def load(self, vpool=None, vsr=None):
        """
        Initializes the wrapper given a vpool name for which it finds the corresponding vsr
        Loads and returns the client
        """

        if vpool is None and vsr is None:
            raise RuntimeError('One of the parameters vpool or vsr needs to be passed')
        if vpool is not None and vsr is not None:
            raise RuntimeError('Only one of the parameters vpool or vsr needs to be passed')

        if vpool is None:
            if vsr.guid not in client_vsr_cache:
                client = StorageRouterClient(str(vsr.vpool.name), [ClusterContact(str(vsr.cluster_ip), vsr.port)])
                client_vsr_cache[vsr.guid] = client
            return client_vsr_cache[vsr.guid]

        if vpool.guid not in client_vpool_cache:
            cluster_contacts = []
            for vsr in vpool.vsrs:
                cluster_contacts.append(ClusterContact(str(vsr.cluster_ip), vsr.port))
            client = StorageRouterClient(str(vpool.name), cluster_contacts)
            client_vpool_cache[vpool.guid] = client
        return client_vpool_cache[vpool.guid]


class VolumeStorageRouterConfiguration(object):
    """
    VolumeStorageRouter configuration class
    """
    def __init__(self, vpool_name):
        self._vpool = vpool_name
        self._config_specfile = os.path.join(Configuration.get('ovs.core.cfgdir'), 'specs', 'volumedriverfs.json')
        if not os.path.exists(os.path.join(Configuration.get('ovs.core.cfgdir'), 'voldrv_vpools')):
            os.makedirs(os.path.join(Configuration.get('ovs.core.cfgdir'), 'voldrv_vpools'))
        self._config_file = os.path.join(Configuration.get('ovs.core.cfgdir'), 'voldrv_vpools', '{}.json'.format(vpool_name))
        self._config_tmpfile = os.path.join(Configuration.get('ovs.core.cfgdir'), 'voldrv_vpools', '{}.json.tmp'.format(vpool_name))
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

    def configure_volumerouter(self, vrouter_cluster, vrouter_config):
        """
        Configures volume storage router
        @param vrouter_config: dictionary of key/value pairs
        """
        nics = Net.getNics()
        nics.remove('lo')
        mac_addresses = sorted(map(lambda n: Net.getMacAddress(n).replace(':', ''), nics))
        unique_machine_id = mac_addresses[0]
        self.load_config()
        if vrouter_config['vrouter_id'] == '{}{}'.format(self._vpool, unique_machine_id):
            for key, value in vrouter_config.iteritems():
                self._config_file_content['volume_router'][key] = value
        # Configure the vrouter arakoon with empty values in order to use tokyo cabinet
        self._config_file_content['volume_router']['vrouter_arakoon_cluster_id'] = ''
        self._config_file_content['volume_router']['vrouter_arakoon_cluster_nodes'] = []
        if not 'volume_router_cluster' in self._config_file_content:
            self._config_file_content['volume_router_cluster'] = {}
        self._config_file_content['volume_router_cluster'].update({'vrouter_cluster_id': vrouter_cluster})
        self.write_config()

    def configure_arakoon_cluster(self, arakoon_cluster_id):
        """
        Configures volume storage router arakoon cluster
        @param arakoon_cluster_id: name of the arakoon cluster
        @param arakoon_nodes: dictionary of arakoon nodes in this cluster
        """
        self.load_config()
        if not 'volume_registry' in self._config_file_content:
            self._config_file_content['volume_registry'] = {}
        self._config_file_content['volume_registry']['vregistry_arakoon_cluster_id'] = arakoon_cluster_id
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
