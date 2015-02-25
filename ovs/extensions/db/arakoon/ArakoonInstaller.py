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

import logging
from ConfigParser import RawConfigParser
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System
from StringIO import StringIO

import os
import tempfile
import time


class ClusterNode(object):
    """
    cluster node config parameters
    """
    def __init__(self, name=None, ip=None, client_port=None, messaging_port=None):
        self.name = name
        self.ip = ip
        self.client_port = client_port
        self.messaging_port = messaging_port

    def __hash__(self):
        """
        Defines a hashing equivalent for a given ClusterNode
        """
        return hash('{0}_{1}_{2}_{3}'.format(self.name, self.ip, self.client_port, self.messaging_port))

    def __eq__(self, other):
        """
        Checks whether two objects are the same.
        """
        if not isinstance(other, ClusterNode):
            return False
        return self.__hash__() == other.__hash__()

    def __ne__(self, other):
        """
        Checks whether to objects are not the same.
        """
        if not isinstance(other, ClusterNode):
            return True
        return not self.__eq__(other)


class ClusterConfig():
    """
    contains cluster config parameters
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
            self.plugins = ""
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
    ARAKOON_BIN = '/usr/bin/arakoon'
    ARAKOON_CONFIG_DIR = '/opt/OpenvStorage/config/arakoon'
    ARAKOON_CONFIG_FILE = '/opt/OpenvStorage/config/arakoon/{0}/{0}.cfg'

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
{0}
chdir /opt/OpenvStorage

exec /usr/bin/python2 /opt/OpenvStorage/ovs/extensions/db/arakoon/ArakoonManagement.py --start --cluster {1}
"""

    def __init__(self):
        self.config = None

    def get_config_file(self, suffix=None):
        config_dir = '/'.join([ArakoonInstaller.ARAKOON_CONFIG_DIR, self.config.cluster_name])
        filename = '/'.join([config_dir, self.config.cluster_name + suffix])
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)
        return filename

    def create_config(self, cluster_name, ip, client_port, messaging_port, plugins=None):
        """
        Creates initial config object causing this host to be master
        :param cluster_name: unique name for this arakoon cluster used in paths
        :param ip: ip on which service should listen
        :param client_port:
        :param messaging_port:
        :param plugins: optional arakoon plugins
        :return:
        """

        client = SSHClient.load(ip)
        node_name = System.get_my_machine_id(client)
        base_dir = System.read_remote_config(client, 'ovs.core.db.arakoon.location')
        self.clear_config()
        self.config = ClusterConfig(base_dir, cluster_name, 'info', plugins)
        self.config.nodes.append(ClusterNode(node_name, ip, client_port, messaging_port))
        self.config.target_ip = ip

    @staticmethod
    def get_config_from(cluster_name, master_ip, master_password=None):
        """
        Gets a config object representation for the cluster on master
        """
        client = SSHClient.load(master_ip, master_password)
        cfg_file = client.file_read(ArakoonInstaller.ARAKOON_CONFIG_FILE.format(cluster_name))
        cfg = RawConfigParser()
        cfg.readfp(StringIO(cfg_file))
        return cfg

    def load_config_from(self, cluster_name, master_ip):
        """
        Reads actual config from master node
        Assumes this node is up-to-date and is considered valid
        :param base_dir: base_dir should be identical across multiple nodes
        """
        cfg = ArakoonInstaller.get_config_from(cluster_name, master_ip)

        global_section = dict(cfg.items('global'))
        nodes = cfg.sections()
        nodes.remove('global')

        # validate config
        if not nodes:
            raise ValueError('Expected at least one node in cfg file')

        first = True
        for node_id in nodes:
            node_config = dict(cfg.items(node_id))
            if first is True:
                self.create_config(cluster_name, node_config['ip'],
                                   node_config['client_port'], node_config['messaging_port'],
                                   plugins=global_section['plugins'])
                first = False
            else:
                self.add_node_to_config(node_id, node_config['ip'],
                                        node_config['client_port'], node_config['messaging_port'])

    def upload_config_for(self, cluster_name):
        if self.config.cluster_name != cluster_name:
            raise RuntimeError('Configuration is not setup for: {0} '.format(cluster_name))

        cluster_ips = list()
        for node in self.config.nodes:
            cluster_ips.append(node.ip)

        for ip in cluster_ips:
            client = SSHClient.load(ip)
            self.generate_config(client)
            self.generate_upstart_config(client)

    def clear_config(self):
        self.config = None

    def get_config(self):
        return self.config

    def add_node_to_config(self, node_id, ip, client_port, messaging_port):
        node = ClusterNode(node_id, ip, client_port, messaging_port)
        self.config.nodes.append(node)

    def remove_node_from_config(self, node_id):
        for node in self.config.nodes:
            if node.name == node_id:
                self.config.nodes.remove(node)
                break

    def generate_config(self, client=None):
        (temp_handle, temp_filename) = tempfile.mkstemp()
        config_file = self.get_config_file('.cfg')
        contents = RawConfigParser()
        contents.add_section('global')
        contents.set('global', 'cluster_id', self.config.cluster_name)
        contents.set('global', 'cluster', '')
        contents.set('global', 'plugins', self.config.plugins)
        for node in self.config.nodes:
            if not contents.has_section(node.name):
                contents.add_section(node.name)
            contents.set(node.name, 'name', node.name)
            contents.set(node.name, 'ip', node.ip)
            contents.set(node.name, 'client_port', node.client_port)
            contents.set(node.name, 'messaging_port', node.messaging_port)
            contents.set(node.name, 'tlog_compression', 'snappy')
            contents.set(node.name, 'log_level', self.config.log_level)
            contents.set(node.name, 'log_dir', self.config.log_dir)
            contents.set(node.name, 'home', self.config.home_dir)
            contents.set(node.name, 'tlog_dir', self.config.tlog_dir)
            contents.set(node.name, 'fsync', str(self.config.fsync).lower())
            if contents.get('global', 'cluster'):
                contents.set('global', 'cluster', ','.join([contents.get('global', 'cluster'), node.name]))
            else:
                contents.set('global', 'cluster', node.name)
        if client is None:
            with open(config_file, 'wb') as f:
                contents.write(f)
        else:
            with open(temp_filename, 'wb') as f:
                contents.write(f)
            client.dir_ensure(os.path.dirname(config_file))
            client.file_upload(config_file, temp_filename)
            os.remove(temp_filename)

    def generate_upstart_config(self, client=None):
        (temp_handle, temp_filename) = tempfile.mkstemp()
        config_file = '/etc/init/ovs-arakoon-{0}.conf'.format(self.config.cluster_name)
        ld_config = 'env LD_LIBRARY_PATH=/usr/lib/alba'
        contents = ArakoonInstaller.ARAKOON_UPSTART.format(ld_config, self.config.cluster_name)

        if client is None:
            with open(config_file, 'wb') as f:
                f.write(contents)
        else:
            with open(temp_filename, 'wb') as f:
                f.write(contents)
            client.dir_ensure(os.path.dirname(config_file))
            client.file_upload(config_file, temp_filename)
            os.remove(temp_filename)

    def create_dir_structure(self, client=None, cluster_name=None):
        if cluster_name is None:
            cluster_name = self.config.cluster_name
        cmd = """
mkdir -p {0}/arakoon/{1}
mkdir -p {0}/tlogs/{1}
mkdir -p /var/log/arakoon/{1}
""".format(self.config.base_dir, cluster_name)
        System.run(cmd, client)

    def delete_dir_structure(self, client=None, cluster_name=None):
        if cluster_name is None:
            cluster_name = self.config.cluster_name
        cmd = """
rm -rf {0}/arakoon/{1}
rm -rf {0}/tlogs/{1}
rm -rf /var/log/arakoon/{1}
""".format(self.config.base_dir, cluster_name)
        System.run(cmd, client)

    def generate_configs(self, client=None):
        self.generate_config(client)
        self.generate_upstart_config(client)

    @staticmethod
    def create_cluster(cluster_name, ip, exclude_ports, plugins=None):
        ai = ArakoonInstaller()
        ai.clear_config()
        client = SSHClient.load(ip)
        port_range = System.read_remote_config(client, 'ovs.ports.arakoon')
        free_ports = System.get_free_ports(port_range, exclude_ports, 2, client)
        ai.create_config(cluster_name, ip, free_ports[0], free_ports[1], plugins)
        ai.generate_configs(client)
        ai.create_dir_structure(client)
        return {'client_port': free_ports[0],
                'messaging_port': free_ports[1]}

    @staticmethod
    def start(cluster_name, ip):
        client = SSHClient.load(ip)
        cmd = """
from ovs.plugin.provider.service import Service
print Service.start_service('arakoon-{0}')
""".format(cluster_name)
        System.exec_remote_python(client, cmd)

    @staticmethod
    def stop(cluster_name, ip):
        client = SSHClient.load(ip)
        cmd = """
from ovs.plugin.provider.service import Service
print Service.stop_service('arakoon-{0}')
""".format(cluster_name)
        System.exec_remote_python(client, cmd)

    @staticmethod
    def status(cluster_name, ip):
        client = SSHClient.load(ip)
        cmd = """
from ovs.plugin.provider.service import Service
print Service.get_service_status('arakoon-{0}')
""".format(cluster_name)
        System.exec_remote_python(client, cmd)

    @staticmethod
    def catchup_cluster_node(cluster_name, ip):
        client = SSHClient.load(ip)
        cmd = """
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagementEx
cluster = ArakoonManagementEx().getCluster('arakoon-{0}')
cluster.catchup_node()
""".format(cluster_name)
        System.exec_remote_python(client, cmd)

    @staticmethod
    def extend_cluster(src_ip, tgt_ip, cluster_name, exclude_ports):
        ai = ArakoonInstaller()
        ai.load_config_from(cluster_name, src_ip)
        client = SSHClient.load(tgt_ip)
        tgt_id = System.get_my_machine_id(client)
        port_range = System.read_remote_config(client, 'ovs.ports.arakoon')
        free_ports = System.get_free_ports(port_range, exclude_ports, 2, client)
        ai.create_dir_structure(client)
        ai.add_node_to_config(tgt_id, tgt_ip, free_ports[0], free_ports[1])
        ai.upload_config_for(cluster_name)
        return {'client_port': free_ports[0],
                'messaging_port': free_ports[1]}

    @staticmethod
    def shrink_cluster(remaining_node_ip, deleted_node_ip, cluster_name):
        ai = ArakoonInstaller()
        ai.load_config_from(cluster_name, remaining_node_ip)
        client = SSHClient.load(deleted_node_ip)
        deleted_node_id = System.get_my_machine_id(client)
        ai.delete_dir_structure(client)
        ai.remove_node_from_config(deleted_node_id)
        ai.upload_config_for(cluster_name)

    @staticmethod
    def deploy_config(from_ip, to_ip, cluster_name):
        ai = ArakoonInstaller()
        ai.load_config_from(cluster_name, from_ip)
        client = SSHClient.load(to_ip)
        ai.generate_config(client)

    @staticmethod
    def wait_for_cluster(cluster_name):
        """
        Waits for an Arakoon cluster to be available (by sending a nop)
        """
        from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagementEx
        from ovs.extensions.db.arakoon.arakoon.ArakoonExceptions import ArakoonSockReadNoBytes

        last_exception = None
        tries = 3
        while tries > 0:
            try:
                cluster_object = ArakoonManagementEx().getCluster(str(cluster_name))
                client = cluster_object.getClient()
                client.nop()
                return True
            except ArakoonSockReadNoBytes as exception:
                last_exception = exception
                tries -= 1
                time.sleep(1)
        raise last_exception

    @staticmethod
    def restart_cluster_add(cluster_name, current_ips, new_ip):
        """
        Execute a (re)start sequence after adding a new node to a cluster.
        """
        # Make sure all nodes are correctly (re)started
        loglevel = logging.root.manager.disable  # Workaround for disabling Arakoon logging
        logging.disable('WARNING')
        ArakoonInstaller.catchup_cluster_node(cluster_name, new_ip)
        threshold = 2 if new_ip in current_ips else 1
        for ip in current_ips:
            if ip == new_ip:
                continue
            ArakoonInstaller.stop(cluster_name, ip)
            ArakoonInstaller.start(cluster_name, ip)
            if len(current_ips) > threshold:  # A two node cluster needs all nodes running
                ArakoonInstaller.wait_for_cluster(cluster_name)
        ArakoonInstaller.start(cluster_name, new_ip)
        ArakoonInstaller.wait_for_cluster(cluster_name)
        logging.disable(loglevel)  # Restore workaround

    @staticmethod
    def restart_cluster_remove(cluster_name, remaining_ips):
        """
        Execute a restart sequence after removing a node from a cluster
        """
        loglevel = logging.root.manager.disable  # Workaround for disabling Arakoon logging
        logging.disable('WARNING')
        for ip in remaining_ips:
            ArakoonInstaller.stop(cluster_name, ip)
            ArakoonInstaller.start(cluster_name, ip)
            if len(remaining_ips) > 2:  # A two node cluster needs all nodes running
                ArakoonInstaller.wait_for_cluster(cluster_name)
        logging.disable(loglevel)  # Restore workaround
