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
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagementEx
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.remote import Remote
from ovs.extensions.generic.system import System
from ovs.plugin.provider.service import Service
from StringIO import StringIO

import os
import tempfile
import time


class ArakoonNodeConfig(object):
    """
    cluster node config parameters
    """
    def __init__(self, name, ip, client_port, messaging_port, log_dir, home, tlog_dir):
        """
        Initializes a new Config entry for a single Node
        """
        self.name = name
        self.ip = ip
        self.client_port = client_port
        self.messaging_port = messaging_port
        self.tlog_compression = 'snappy'
        self.log_level = 'info'
        self.log_dir = log_dir
        self.home = home
        self.tlog_dir = tlog_dir
        self.fsync = True

    def __hash__(self):
        """
        Defines a hashing equivalent for a given ArakoonNodeConfig
        """
        return hash(self.name)

    def __eq__(self, other):
        """
        Checks whether two objects are the same.
        """
        if not isinstance(other, ArakoonNodeConfig):
            return False
        return self.__hash__() == other.__hash__()

    def __ne__(self, other):
        """
        Checks whether two objects are not the same.
        """
        if not isinstance(other, ArakoonNodeConfig):
            return True
        return not self.__eq__(other)


class ArakoonClusterConfig():
    """
    contains cluster config parameters
    """
    ARAKOON_CONFIG_DIR = '/opt/OpenvStorage/config/arakoon/{0}'
    ARAKOON_CONFIG_FILE = '/opt/OpenvStorage/config/arakoon/{0}/{0}.cfg'

    def __init__(self, cluster_id, plugins=None):
        """
        Initializes an empty Cluster Config
        """
        self.cluster_id = cluster_id
        self._dir = ArakoonClusterConfig.ARAKOON_CONFIG_DIR.format(self.cluster_id)
        self._filename = ArakoonClusterConfig.ARAKOON_CONFIG_FILE.format(self.cluster_id)
        self.nodes = []
        self._plugins = []
        if isinstance(plugins, list):
            self._plugins = plugins
        elif isinstance(plugins, basestring):
            self._plugins.append(plugins)

    def load_config(self, client):
        """
        Reads a configuration from reality
        """
        contents = client.file_read(self._filename)
        parser = RawConfigParser()
        parser.readfp(StringIO(contents))

        if parser.has_option('global', 'plugins'):
            self._plugins = [plugin.strip() for plugin in parser.get('global', 'plugins').split(',')]
        for node in parser.get('global', 'cluster').split(','):
            node = node.strip()
            self.nodes.append(ArakoonNodeConfig(name=node,
                                                ip=parser.get(node, 'ip'),
                                                client_port=parser.get(node, 'client_port'),
                                                messaging_port=parser.get(node, 'messaging_port'),
                                                log_dir=parser.get(node, 'log_dir'),
                                                home=parser.get(node, 'home'),
                                                tlog_dir=parser.get(node, 'tlog_dir')))

    def export(self):
        """
        Exports the current configuration to a python dict
        """
        data = {'global': {'cluster_id': self.cluster_id,
                           'cluster': ','.join(sorted(node.name for node in self.nodes)),
                           'plugins': ','.join(sorted(self._plugins))}}
        for node in self.nodes:
            data[node.name] = {'name': node.name,
                               'ip': node.ip,
                               'client_port': node.client_port,
                               'messaging_port': node.messaging_port,
                               'tlog_compression': node.tlog_compression,
                               'log_level': node.log_level,
                               'log_dir': node.log_dir,
                               'home': node.home,
                               'tlog_dir': node.tlog_dir,
                               'fsync': 'true' if node.fsync else 'false'}
        return data

    def write_config(self, client):
        """
        Writes the configuration down to in the format expected by Arakoon
        """
        (temp_handle, temp_filename) = tempfile.mkstemp()
        contents = RawConfigParser()
        data = self.export()
        for section in data:
            contents.add_section(section)
            for item in data[section]:
                contents.set(section, item, data[section][item])
        with open(temp_filename, 'wb') as config_file:
            contents.write(config_file)
        client.dir_ensure(self._dir, recursive=True)
        client.file_upload(self._filename, temp_filename)
        os.remove(temp_filename)

    def delete_config(self, client):
        """
        Deletes a configuration file
        """
        client.run('rm -rf {0}'.format(self._dir))


class ArakoonInstaller():
    """
    class to dynamically install/(re)configure arakoon cluster
    """
    ARAKOON_LOG_DIR = '/var/log/arakoon/{0}'
    ARAKOON_HOME_DIR = '{0}/arakoon/{1}'
    ARAKOON_TLOG_DIR = '{0}/tlogs/{1}'
    ARAKOON_CONFIG_DIR = '/opt/OpenvStorage/config/arakoon'
    ARAKOON_CONFIG_FILE = '/opt/OpenvStorage/config/arakoon/{0}/{0}.cfg'

    ARAKOON_UPSTART_FILE = '/etc/init/ovs-arakoon-{0}.conf'
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
chdir /opt/OpenvStorage

exec /usr/bin/python2 /opt/OpenvStorage/ovs/extensions/db/arakoon/ArakoonManagement.py --start --cluster {0}
"""

    def __init__(self):
        """
        ArakoonInstaller should not be instantiated
        """
        raise RuntimeError('ArakoonInstaller is a complete static helper class')

    @staticmethod
    def create_cluster(cluster_name, ip, exclude_ports, plugins=None):
        """
        Creates a cluster
        """
        client = SSHClient.load(ip)
        base_dir = System.read_remote_config(client, 'ovs.core.db.arakoon.location').rstrip('/')
        port_range = System.read_remote_config(client, 'ovs.ports.arakoon')
        ports = System.get_free_ports(port_range, exclude_ports, 2, client)
        node_name = System.get_my_machine_id(client)

        config = ArakoonClusterConfig(cluster_name, plugins)
        config.nodes.append(ArakoonNodeConfig(name=node_name,
                                              ip=ip,
                                              client_port=ports[0],
                                              messaging_port=ports[1],
                                              log_dir=ArakoonInstaller.ARAKOON_LOG_DIR.format(cluster_name),
                                              home=ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, cluster_name),
                                              tlog_dir=ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, cluster_name)))
        ArakoonInstaller._deploy(config)
        return {'client_port': ports[0],
                'messaging_port': ports[1]}

    @staticmethod
    def delete_cluster(cluster_name, ip):
        """
        Deletes a complete cluster
        """
        config = ArakoonClusterConfig(cluster_name)
        config.load_config(SSHClient.load(ip))
        ArakoonInstaller._destroy(config)

    @staticmethod
    def extend_cluster(master_ip, new_ip, cluster_name, exclude_ports):
        """
        Extends a cluster to a given new node
        """
        client = SSHClient.load(master_ip)
        config = ArakoonClusterConfig(cluster_name)
        config.load_config(client)

        client = SSHClient.load(new_ip)
        base_dir = System.read_remote_config(client, 'ovs.core.db.arakoon.location').rstrip('/')
        port_range = System.read_remote_config(client, 'ovs.ports.arakoon')
        ports = System.get_free_ports(port_range, exclude_ports, 2, client)
        node_name = System.get_my_machine_id(client)

        config.nodes.append(ArakoonNodeConfig(name=node_name,
                                              ip=new_ip,
                                              client_port=ports[0],
                                              messaging_port=ports[1],
                                              log_dir=ArakoonInstaller.ARAKOON_LOG_DIR.format(cluster_name),
                                              home=ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, cluster_name),
                                              tlog_dir=ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, cluster_name)))
        ArakoonInstaller._deploy(config)
        return {'client_port': ports[0],
                'messaging_port': ports[1]}

    @staticmethod
    def shrink_cluster(remaining_node_ip, deleted_node_ip, cluster_name):
        """
        Removes a node from a cluster, the old node will become a slave
        """
        client = SSHClient.load(remaining_node_ip)
        config = ArakoonClusterConfig(cluster_name)
        config.load_config(client)

        for node in config.nodes[:]:
            if node.ip == deleted_node_ip:
                config.nodes.remove(node)
                ArakoonInstaller._destroy_node(config, node)
        ArakoonInstaller._deploy(config)
        ArakoonInstaller.deploy_to_slave(remaining_node_ip, deleted_node_ip, cluster_name)

    @staticmethod
    def _destroy(config):
        """
        Cleans up a complete cluster (remove services, directories and configuration files)
        """
        for node in config.nodes:
            ArakoonInstaller._destroy_node(config, node)

    @staticmethod
    def _destroy_node(config, node):
        """
        Cleans up a single node (remove services, directories and configuration files)
        """
        ArakoonInstaller._clean_services(config.cluster_id, node)
        ArakoonInstaller._remove_directories(node)
        ArakoonInstaller._remove_configs(config, node)

    @staticmethod
    def _deploy(config):
        """
        Deploys a complete cluster: Distributing the configuration files, creating directories and services
        """
        ArakoonInstaller._distribute_configs(config)
        ArakoonInstaller._create_directories(config)
        ArakoonInstaller._create_services(config)

    @staticmethod
    def _create_services(config):
        """
        Creates services for/on all nodes in the config
        """
        for node in config.nodes:
            (temp_handle, temp_filename) = tempfile.mkstemp()
            config_file = ArakoonInstaller.ARAKOON_UPSTART_FILE.format(config.cluster_id)
            contents = ArakoonInstaller.ARAKOON_UPSTART.format(config.cluster_id)
            client = SSHClient.load(node.ip)
            with open(temp_filename, 'wb') as f:
                f.write(contents)
            client.dir_ensure(os.path.dirname(config_file), recursive=True)
            client.file_upload(config_file, temp_filename)
            os.remove(temp_filename)

    @staticmethod
    def _clean_services(cluster_name, node):
        """
        Removes services for a cluster on a given node
        """
        ArakoonInstaller.stop(cluster_name, node.ip)
        ArakoonInstaller.remove(cluster_name, node.ip)

    @staticmethod
    def _create_directories(config):
        """
        Creates directories on all nodes for a given config
        """
        for node in config.nodes:
            client = SSHClient.load(node.ip)
            for directory in [node.log_dir, node.tlog_dir, node.home]:
                client.run('mkdir -p {0}'.format(directory))

    @staticmethod
    def _remove_directories(node):
        """
        Cleans all directories on a given node
        """
        client = SSHClient.load(node.ip)
        for directory in [node.log_dir, node.tlog_dir, node.home]:
            client.run('rm -rf {0}'.format(directory))

    @staticmethod
    def _distribute_configs(config):
        """
        Distributes a configuration file to all its nodes
        """
        for ip in [node.ip for node in config.nodes]:
            client = SSHClient.load(ip)
            config.write_config(client)

    @staticmethod
    def _remove_configs(config, node):
        """
        Removes a configuration file from a node
        """
        client = SSHClient.load(node.ip)
        config.delete_config(client)

    @staticmethod
    def start(cluster_name, ip):
        """
        Starts an arakoon cluster
        """
        client = SSHClient.load(ip)
        cmd = """
from ovs.plugin.provider.service import Service
if Service.get_service_status('arakoon-{0}') is False:
    Service.start_service('arakoon-{0}')
""".format(cluster_name)
        System.exec_remote_python(client, cmd)

    @staticmethod
    def stop(cluster_name, ip):
        """
        Stops an arakoon service
        """
        client = SSHClient.load(ip)
        cmd = """
from ovs.plugin.provider.service import Service
if Service.get_service_status('arakoon-{0}') is True:
    Service.stop_service('arakoon-{0}')
""".format(cluster_name)
        System.exec_remote_python(client, cmd)

    @staticmethod
    def remove(cluster_name, ip):
        """
        Removes an arakoon service
        """
        client = SSHClient.load(ip)
        cmd = """
from ovs.plugin.provider.service import Service
print Service.remove_service('', 'arakoon-{0}')
""".format(cluster_name)
        System.exec_remote_python(client, cmd)

    @staticmethod
    def catchup_cluster_node(cluster_name, ip):
        """
        Executes a catchup
        """
        client = SSHClient.load(ip)
        cmd = """
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagementEx
cluster = ArakoonManagementEx().getCluster('{0}')
cluster.catchup_node()
""".format(cluster_name)
        System.exec_remote_python(client, cmd)

    @staticmethod
    def deploy_to_slave(master_ip, slave_ip, cluster_name):
        """
        Deploys the configuration file to a slave
        """
        client = SSHClient.load(master_ip)
        config = ArakoonClusterConfig(cluster_name)
        config.load_config(client)
        client = SSHClient.load(slave_ip)
        config.write_config(client)

    @staticmethod
    def remove_from_slave(master_ip, slave_ip, cluster_name):
        """
        Removes everything related to a given cluster from the slave
        """
        client = SSHClient.load(master_ip)
        config = ArakoonClusterConfig(cluster_name)
        config.load_config(client)
        client = SSHClient.load(slave_ip)
        config.delete_config(client)

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
