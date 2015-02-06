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

from ConfigParser import RawConfigParser
from ovs.extensions.generic.sshclient import SSHClient
from ovs.lib.setup import System
from StringIO import StringIO

import os
import tempfile


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
        self.target_ip = '127.0.0.1'
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

    def set_cluster_name(self, cluster_name, exclude_ports):
        self.log_dir = "/var/log/arakoon/" + cluster_name
        self.home_dir = "/".join([self.base_dir, 'arakoon', cluster_name])
        self.tlog_dir = "/".join([self.base_dir, 'tlogs', cluster_name])
        self.cluster_name = cluster_name
        ports_used = set()
        for node in self.nodes:
            ports_used.add(node.client_port)
            ports_used.add(node.messaging_port)
        print "ports used: {0}".format(ports_used)
        exclude_ports.extend(list(ports_used))
        print "excluded ports: {0}".format(exclude_ports)
        free_ports = System.get_free_ports(min(ports_used), exclude_ports, 2)
        print "new ports: {0}".format(free_ports)
        for node in self.nodes:
            node.client_port = free_ports[0]
            node.messaging_port = free_ports[1]


class ArakoonInstaller():
    """
    class to dynamically install/(re)configure arakoon cluster
    """

    ARAKOON_LIB = '/opt/alba/lib'
    ARAKOON_BIN = '/usr/bin/arakoon'
    ARAKOON_PLUGIN_DIR = '/opt/alba/plugins'
    ARAKOON_CONFIG_DIR = '/opt/OpenvStorage/config/arakoon'
    ARAKOON_BASE_DIR = '/mnt/db'
    ABM_PLUGIN = 'albamgr_plugin.cmxs'
    NSM_PLUGIN = 'nsm_host_plugin.cmxs'

    ARAKOON_UPSTART = """
description "Arakoon upstart"

start on (local-filesystems and started networking)
stop on runlevel [016]

kill timeout 60
respawn
respawn limit 10 5
console log
setuid root
setgid root

env PYTHONPATH=/opt/OpenvStorage
env LD_LIBRARY_PATH=/opt/alba/lib
chdir /opt/OpenvStorage

exec /usr/bin/python2 /opt/OpenvStorage/ovs/extensions/db/arakoon/ArakoonManagement.py --start --cluster {0}
"""

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

    def create_config(self, base_dir, cluster_name, ip, client_port, messaging_port, exclude_ports=None, plugins=None):
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
        if exclude_ports is None:
            exclude_ports = list()

        if plugins is None:
            plugins = list()

        client = SSHClient.load(ip)
        node_name = System.get_my_machine_id(client)
        self.clear_config()
        free_ports = System.get_free_ports(min([client_port, messaging_port]), exclude_ports, 2)
        cm = ClusterNode(node_name, ip, free_ports[0], free_ports[1])
        self.config = ClusterConfig(base_dir, cluster_name, 'info', plugins)
        self.config.nodes.append(cm)
        self.config.target_ip = ip
        return free_ports

    def load_config_from(self, base_dir, cluster_name, master_ip, master_password=None):
        """
        Reads actual config from master node
        Assumes this node is up-to-date and is considered valid
        :param base_dir: base_dir should be identical across multiple nodes
        """

        client = SSHClient.load(master_ip, master_password)
        cfg_file = client.file_read(self.ARAKOON_CONFIG_DIR + '/{0}/{0}.cfg'.format(cluster_name))
        cfg = RawConfigParser()
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
        self.create_config(base_dir, cluster_name, master_config['ip'],
                           master_config['client_port'], master_config['messaging_port'])

        for node_id in nodes:
            node = dict(cfg.items(node_id))
            ClusterNode(node_id, node['ip'], node['client_port'], node['messaging_port'])

    def clone_config_from(self, base_dir, cluster_name, master_ip, new_cluster_name, exclude_ports=None, master_password=None):
        self.load_config_from(base_dir, cluster_name, master_ip, master_password)
        if exclude_ports is None:
            exclude_ports = list()
        self.config.set_cluster_name(new_cluster_name, exclude_ports)
        self.generate_configs()
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

        config_files = list()
        for config_suffix in ['.cfg', '_client.cfg', '_local_nodes.cfg']:
            config_files.append(self.get_config_file(config_suffix))
        config_files.append('/etc/init/ovs-arakoon_{0}.conf'.format(cluster_name))
        for config_file in config_files:
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

    def generate_config(self, client=None):
        (temp_handle, temp_filename) = tempfile.mkstemp()
        config_file = self.get_config_file('.cfg')
        contents = RawConfigParser()
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
        if client is None:
            with open(config_file, 'wb') as f:
                contents.write(f)
        else:
            with open(temp_filename, 'wb') as f:
                contents.write(f)
            client.file_upload(config_file, temp_filename)
        os.remove(temp_filename)

    def generate_client_config(self, client=None):
        (temp_handle, temp_filename) = tempfile.mkstemp()
        config_file = self.get_config_file('_client.cfg')
        contents = RawConfigParser()
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
        if client is None:
            with open(config_file, 'wb') as f:
                contents.write(f)
        else:
            with open(temp_filename, 'wb') as f:
                contents.write(f)
            client.file_upload(config_file, temp_filename)
        os.remove(temp_filename)

    def generate_local_nodes_config(self, client=None):
        (temp_handle, temp_filename) = tempfile.mkstemp()
        config_file = self.get_config_file('_local_nodes.cfg')

        contents = RawConfigParser()
        contents.add_section('global')
        contents.set('global', 'cluster', self.node_id)
        if client is None:
            with open(config_file, 'wb') as f:
                contents.write(f)
        else:
            with open(temp_filename, 'wb') as f:
                contents.write(f)
            client.file_upload(config_file, temp_filename)
        os.remove(temp_filename)

    def generate_upstart_config(self, client=None):
        (temp_handle, temp_filename) = tempfile.mkstemp()
        config_file = '/etc/init/ovs-arakoon-{0}.conf'.format(self.config.cluster_name)
        contents = ArakoonInstaller.ARAKOON_UPSTART.format(self.config.cluster_name)

        if client is None:
            with open(config_file, 'wb') as f:
                f.write(contents)
        else:
            with open(temp_filename, 'wb') as f:
                f.write(contents)
            client.file_upload(config_file, temp_filename)
        os.remove(temp_filename)

    def create_dir_structure(self, cluster_name, client=None):
        cmd = """
sudo mkdir -p {0}/arakoon/{1}
sudo mkdir -p {0}/tlogs/{1}
sudo mkdir -p /var/log/arakoon/{1}
""".format(ArakoonInstaller.ARAKOON_BASE_DIR, cluster_name)
        System.run(cmd, client)

    def generate_configs(self, client=None):
        self.generate_config(client)
        self.generate_client_config(client)
        self.generate_local_nodes_config(client)
        self.generate_upstart_config(client)

    @staticmethod
    def create_cluster(cluster_name, ip, base_dir='/mnt/db', client_port=8870, messaging_port=8871, exclude_ports=None, plugins=None):
        ai = ArakoonInstaller()
        ai.clear_config()
        if exclude_ports is None:
            exclude_ports = list()
        ports_used = ai.create_config(base_dir, cluster_name, ip, client_port, messaging_port, exclude_ports, plugins)
        client = SSHClient.load(ip)
        ai.generate_configs(client)
        ai.create_dir_structure(cluster_name, client)
        return {'client_port': ports_used[0],
                'messaging_port': ports_used[1]}

    @staticmethod
    def clone_cluster(ip, src_name, tgt_name, exclude_ports=None):
        if exclude_ports is None:
            exclude_ports = list()
        ai = ArakoonInstaller()
        ai.clone_config_from('/mnt/db', src_name, ip, tgt_name, exclude_ports)
        client = SSHClient.load(ip)
        ai.create_dir_structure(tgt_name, client)
        ai.upload_config_for(tgt_name)

    @staticmethod
    def get_client_config_from(ip, cluster_name):
        ai = ArakoonInstaller()
        ai.load_config_from('/mnt/db', cluster_name, ip)
        contents = RawConfigParser()
        contents.add_section('global')
        contents.set('global', 'cluster_id', ai.config.cluster_name)
        contents.set('global', 'cluster', '')

        for node in ai.config.nodes:
            contents.add_section(node.name)
            contents.set(node.name, 'name', node.name)
            contents.set(node.name, 'ip', node.ip)
            contents.set(node.name, 'client_port', node.client_port)
            contents.set(node.name, 'messaging_port', node.client_port)
            contents.set('global', 'cluster', ','.join([contents.get('global', 'cluster'), node.name]))
        contents.set('global', 'cluster', ai.node_id)
        return contents