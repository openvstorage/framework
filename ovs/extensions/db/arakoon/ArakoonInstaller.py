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

from ConfigParser import ConfigParser
from ovs.extensions.generic.sshclient import SSHClient
from ovs.lib.setup import System
from StringIO import StringIO
from subprocess import check_output

import os


class ClusterNode(object):
    """
    cluster node config parameters
    """
    def __init__(self, name=None, ip=None, client_port=None, messaging_port=None):
        self.name = name
        self.ip = ip
        self.client_port = client_port
        self.messaging_port = messaging_port


class ClusterConfig():
    """
    contains cluster config parameters"
    """
    def __init__(self, base_dir, cluster_name, log_level, plugins=None):
        self.base_dir = base_dir
        self.cluster_name = cluster_name
        self.log_level = log_level
        self.log_dir = "/var/log/arakoon/" + cluster_name
        self.home_dir = "/".join([self.base_dir, 'arakoon', cluster_name])
        self.tlog_dir = "/".join([self.base_dir, 'tlogs', cluster_name])
        if plugins is None:
            self.plugins = []
        else:
            self.plugins = plugins
        self.nodes = []
        self.fsync = True

    def set_base_dir(self, base_dir):
        self.home_dir = base_dir + '/arakoon/' + self.cluster_name
        self.tlog_dir = base_dir + '/tlogs/' + self.cluster_name
        self.base_dir = base_dir

    def set_cluster_name(self, cluster_name):
        self.log_dir = "/var/log/arakoon/" + cluster_name
        self.home_dir = "/".join([self.base_dir, 'arakoon', cluster_name])
        self.tlog_dir = "/".join([self.base_dir, 'tlogs', cluster_name])
        self.cluster_name = cluster_name


class ArakoonInstaller():
    """
    class to dynamically install/(re)configure arakoon cluster
    """

    ARAKOON_LIB = '/opt/alba/lib'
    ARAKOON_BIN = '/opt/alba/bin/arakoon'
    ARAKOON_PLUGIN_DIR = '/opt/alba/plugins'
    ARAKOON_CONFIG_DIR = '/opt/OpenvStorage/config/arakoon'
    ARAKOON_BASE_DIR = '/mnt/db'
    ABM_PLUGIN = 'albamgr_plugin.cmxs'
    NSM_PLUGIN = 'nsm_host_plugin.cmxs'

    def __init__(self, password=None):
        self.master_password = password
        self.node_id = System.get_my_machine_id()
        self.config = None

    def get_config_file(self, suffix=None):
        config_dir = '/'.join([self.ARAKOON_CONFIG_DIR, self.config.cluster_name])
        filename = '/'.join([config_dir, self.config.cluster_name + suffix])
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)
        return filename

    def create_config(self, base_dir, cluster_name, ip, client_port, messaging_port, plugins=None):
        """
        Creates initial config object causing this host to be master
        :param base_dir: mountpoint of arakoon database(s)
        :param cluster_name: unique name for this arakoon cluster used in paths
        :param ip: ip on which service should listen
        :param client_port:
        :param messaging_port:
        :param plugins: optional arakoon plugins
        :return:
        """

        if plugins is None:
            plugins = list()

        client = SSHClient.load(ip)
        node_name = System.get_my_machine_id(client)
        self.clear_config()
        cm = ClusterNode(node_name, ip, client_port, messaging_port)
        self.config = ClusterConfig(base_dir, cluster_name, 'info', plugins)
        self.config.nodes.append(cm)

    def load_config_from(self, base_dir, cluster_name, master_ip, master_password=None):
        """
        Reads actual config from master node
        Assumes this node is up-to-date and is considered valid
        :param base_dir: base_dir should be identical across multiple nodes
        """

        client = SSHClient.load(master_ip, master_password)
        cfg_file = client.file_read(self.ARAKOON_CONFIG_DIR + '/{0}/{0}.cfg'.format(cluster_name))
        cfg = ConfigParser()
        cfg.readfp(StringIO(cfg_file))

        nodes = cfg.sections()
        nodes.pop(nodes.index('global'))

        # validate config
        if not nodes:
            raise ValueError('Expected at least one node in cfg file')

        for node_id in nodes:
            node = dict(cfg.items(node_id))
            if base_dir not in node['home']:
                raise ValueError('base_dir {0} incorrect: not part of home_dir: {1}'.format(base_dir, node['home']))
            if base_dir not in node['tlog_dir']:
                raise ValueError('base_dir {0} incorrect: not part of tlog_dir: {1}'.format(base_dir, node['tlog_dir']))

        master_node = nodes.pop()
        master_config = dict(cfg.items(master_node))
        self.create_config(base_dir, cluster_name, master_node, master_config['ip'],
                           master_config['client_port'], master_config['messaging_port'])

        for node_id in nodes:
            node = dict(cfg.items(node_id))
            ClusterNode(node_id, node['ip'], node['client_port'], node['messaging_port'])

    def clone_config_from(self, base_dir, cluster_name, master_ip, new_cluster_name, master_password=None):
        self.load_config_from(base_dir, cluster_name, master_ip, master_password)
        self.config.set_cluster_name(new_cluster_name)
        self.generate_config()
        self.generate_client_config()
        self.generate_local_nodes_config()
        self.upload_config_for(new_cluster_name)

    def upload_config_for(self, cluster_name):
        if self.config.cluster_name != cluster_name:
            raise RuntimeError('Configuration is not setup for: {0} '.format(cluster_name))

        client_ips = System.get_my_ips()
        cluster_ips = list()
        for node in self.config.nodes:
            cluster_ips.append(node.ip)

        print client_ips
        print cluster_ips

        self.generate_config()
        self.generate_client_config()
        self.generate_local_nodes_config()

        for config_suffix in ['.cfg', '_client.cfg', '_local_nodes.cfg']:
            # upload _client.cfg file to all ovs nodes
            config_file = self.get_config_file(config_suffix)
            print config_file
            for ip in cluster_ips:
                if ip in client_ips:
                    continue
                client = SSHClient.load(ip)
                client.dir_ensure(os.path.dirname(config_file))
                client.file_upload(config_file, config_file)

    def clear_config(self):
        self.config = None

    def get_config(self):
        return self.config

    def add_node_to_config(self, node_id, ip, client_port, messaging_port):
        node = ClusterNode(node_id, ip, client_port, messaging_port)
        self.config.nodes.append(node)

    def get_nr_of_free_ports_for_config(self, nr):
        if len(self.config.nodes) == 0:
            raise RuntimeError('Configuration should be present before requesting new ports')
        ports_in_use = set()

        saved_current_config = self.config
        saved_ip = self.config.nodes[0].ip
        print 'saved ip:' + str(saved_ip)

        client = SSHClient.load(saved_ip)
        arakoon_cluster_names = System.get_arakoon_cluster_names(client)
        for name in arakoon_cluster_names:
            self.load_config_from(saved_current_config.base_dir, name, saved_ip)
            for node in self.config.nodes:
                print node
                ports_in_use.add(int(node.client_port))
                ports_in_use.add(int(node.messaging_port))

        self.config = saved_current_config

        ports_in_use = list(ports_in_use)
        return System.get_free_ports(min(ports_in_use), ports_in_use, nr)

    def generate_config(self):
        config_file = self.get_config_file('.cfg')
        contents = ConfigParser()
        contents.add_section('global')
        contents.set('global', 'cluster_id', self.config.cluster_name)
        contents.set('global', 'cluster', '')
        for node in self.config.nodes:
            contents.add_section(node.name)
            contents.set(node.name, 'name', node.name)
            contents.set(node.name, 'ip', node.ip)
            contents.set(node.name, 'client_port', node.client_port)
            contents.set(node.name, 'messaging_port', node.messaging_port)
            contents.set(node.name, 'log_level', self.config.log_level)
            contents.set(node.name, 'log_dir', self.config.log_dir)
            contents.set(node.name, 'home', self.config.home_dir)
            contents.set(node.name, 'tlog_dir', self.config.tlog_dir)
            contents.set(node.name, 'fsync', str(self.config.fsync).lower())
            contents.set('global', 'cluster', ','.join([contents.get('global', 'cluster'), node.name]))
        contents.set('global', 'cluster', self.node_id)
        with open(config_file, 'wb') as f:
            contents.write(f)

    def generate_client_config(self):
        config_file = self.get_config_file('_client.cfg')
        contents = ConfigParser()
        contents.add_section('global')
        contents.set('global', 'cluster_id', self.config.cluster_name)
        contents.set('global', 'cluster', '')

        for node in self.config.nodes:
            contents.add_section(node.name)
            contents.set(node.name, 'name', node.name)
            contents.set(node.name, 'ip', node.ip)
            contents.set(node.name, 'client_port', node.client_port)
            contents.set('global', 'cluster', ','.join([contents.get('global', 'cluster'), node.name]))
        contents.set('global', 'cluster', self.node_id)
        with open(config_file, 'wb') as f:
            contents.write(f)

    def generate_local_nodes_config(self):
        config_file = self.get_config_file('_local_nodes.cfg')

        contents = ConfigParser()
        contents.add_section('global')
        contents.set('global', 'cluster', self.node_id)
        with open(config_file, 'wb') as f:
            contents.write(f)

    def generate_configs(self):
        self.generate_config()
        self.generate_client_config()
        self.generate_local_nodes_config()

    def create_dir_structure(self, cluster_name):
        cmd = """
mkdir -p {0}/arakoon/{1}
mkdir -p {0}/tlogs/{1}
mkdir -p /var/log/arakoon/{1}
""".format(ArakoonInstaller.ARAKOON_BASE_DIR, cluster_name)
        check_output(cmd, shell=True).strip()

    @staticmethod
    def create_cluster(base_dir, cluster_name, ip, client_port, messaging_port, plugins=None):
        ai = ArakoonInstaller()
        ai.clear_config()
        ai.create_config(base_dir, cluster_name, ip, client_port, messaging_port, plugins)
        ai.generate_configs()
        ai.create_dir_structure(cluster_name)

    @staticmethod
    def clone_cluster(ip, src_name, tgt_name):
        ai = ArakoonInstaller()
        ai.clone_config_from('/mnt/db', src_name, ip, tgt_name)
        ai.upload_config_for(tgt_name)
