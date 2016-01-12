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

import os
import time
import tempfile
from ConfigParser import RawConfigParser
from ovs.extensions.generic.remote import Remote
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.services.service import ServiceManager
from StringIO import StringIO
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', name='arakoon_installer')
logger.logger.propagate = False


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
        self.client_port = int(client_port)
        self.messaging_port = int(messaging_port)
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


class ArakoonClusterConfig(object):
    """
    contains cluster config parameters
    """
    ETCD_CONFIG_KEY = '/ovs/arakoon/{0}/config'

    def __init__(self, cluster_id, plugins=None):
        """
        Initializes an empty Cluster Config
        """
        self.cluster_id = cluster_id
        self._extra_globals = {'__tainted_tlog_entries_per_file': 5000}
        self.nodes = []
        self._plugins = []
        if isinstance(plugins, list):
            self._plugins = plugins
        elif isinstance(plugins, basestring):
            self._plugins.append(plugins)

    def load_config(self):
        """
        Reads a configuration from reality
        """
        contents = EtcdConfiguration.get(ArakoonClusterConfig.ETCD_CONFIG_KEY.format(self.cluster_id), raw=True)
        parser = RawConfigParser()
        parser.readfp(StringIO(contents))

        self.nodes = []
        self._extra_globals = {}
        for key in parser.options('global'):
            if key == 'plugins':
                self._plugins = [plugin.strip() for plugin in parser.get('global', 'plugins').split(',')]
            elif key in ['cluster_id', 'cluster']:
                pass  # Ignore these
            else:
                self._extra_globals[key] = parser.get('global', key)
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
        for key, value in self._extra_globals.iteritems():
            data['global'][key] = value
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

    def write_config(self):
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
        with open(temp_filename, 'r') as the_file:
            EtcdConfiguration.set(ArakoonClusterConfig.ETCD_CONFIG_KEY.format(self.cluster_id), the_file.read(), raw=True)
        os.remove(temp_filename)

    def delete_config(self):
        """
        Deletes a configuration file
        """
        EtcdConfiguration.delete(ArakoonClusterConfig.ETCD_CONFIG_KEY.format(self.cluster_id), raw=True)


class ArakoonInstaller(object):
    """
    class to dynamically install/(re)configure arakoon cluster
    """
    ARAKOON_LOG_DIR = '/var/log/arakoon/{0}'
    ARAKOON_BASE_DIR = '{0}/arakoon'
    ARAKOON_HOME_DIR = '{0}/arakoon/{1}/db'
    ARAKOON_TLOG_DIR = '{0}/arakoon/{1}/tlogs'
    ARAKOON_CATCHUP_COMMAND = 'arakoon --node {0} -config {1} -catchup-only'
    ETCD_CONFIG_ROOT = '/ovs/arakoon'
    ETCD_CONFIG_KEY = '/ovs/arakoon/{0}/config'

    def __init__(self):
        """
        ArakoonInstaller should not be instantiated
        """
        raise RuntimeError('ArakoonInstaller is a complete static helper class')

    @staticmethod
    def archive_existing_arakoon_data(ip, directory, top_dir, cluster_name):
        """
        Copy existing arakoon data, when setting up a new arakoon cluster, to the side
        :param ip: IP on which to check for existing data
        :param directory: Directory to check for existence
        :param top_dir: Top directory
        :param cluster_name: Name of arakoon cluster
        :return: None
        """
        new_client = SSHClient(ip)
        logger.debug('archive - check if {0} exists'.format(directory))
        if new_client.dir_exists(directory):
            logger.debug('archive - from {0}'.format(directory))
            archive_dir = '/'.join([top_dir, 'archive', cluster_name])
            if new_client.dir_exists(archive_dir + '/' + os.path.basename(directory)):
                logger.debug('archive - from existing archive {0}'.format(archive_dir))
                timestamp = time.strftime('%Y%m%d%H%M%S', time.gmtime())
                new_archive_dir = archive_dir + '-' + timestamp
                new_client.dir_create(new_archive_dir)
                new_client.run('mv {0} {1}'.format(archive_dir, new_archive_dir))
                logger.debug('archive - to new {0}'.format(new_archive_dir))
            logger.debug('create archive dir: {0}'.format(archive_dir))
            new_client.dir_create(archive_dir)
            logger.debug('archive from {0} to {1}'.format(directory, archive_dir))
            if cluster_name == os.path.basename(directory) and new_client.dir_list(directory):
                new_client.run('mv {0}/* {1}'.format(directory, archive_dir))
            else:
                new_client.run('mv {0} {1}'.format(directory, archive_dir))

    @staticmethod
    def create_cluster(cluster_name, ip, base_dir, plugins=None, locked=True):
        """
        Creates a cluster
        :param locked: Indicates whether the create should run in a locked context (e.g. to prevent port conflicts)
        :param plugins: Plugins that should be added to the configuration file
        :param base_dir: Base directory that should contain the data and tlogs
        :param ip: IP address of the first node of the new cluster
        :param cluster_name: Name of the cluster
        """
        logger.debug('Creating cluster {0} on {1}'.format(cluster_name, ip))
        base_dir = base_dir.rstrip('/')

        client = SSHClient(ip)
        node_name = System.get_my_machine_id(client)

        home_dir = ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, cluster_name)
        log_dir = ArakoonInstaller.ARAKOON_LOG_DIR.format(cluster_name)
        tlog_dir = ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, cluster_name)

        ArakoonInstaller.archive_existing_arakoon_data(ip, home_dir, ArakoonInstaller.ARAKOON_BASE_DIR.format(base_dir),
                                                       cluster_name)
        ArakoonInstaller.archive_existing_arakoon_data(ip, log_dir, ArakoonInstaller.ARAKOON_LOG_DIR.format(''),
                                                       cluster_name)
        ArakoonInstaller.archive_existing_arakoon_data(ip, tlog_dir, ArakoonInstaller.ARAKOON_BASE_DIR.format(base_dir),
                                                       cluster_name)
        port_mutex = None
        try:
            if locked is True:
                from ovs.extensions.generic.volatilemutex import VolatileMutex
                port_mutex = VolatileMutex('arakoon_install_ports_{0}'.format(ip))
                port_mutex.acquire(wait=60)
            ports = ArakoonInstaller._get_free_ports(client)
            config = ArakoonClusterConfig(cluster_name, plugins)
            config.nodes.append(ArakoonNodeConfig(name=node_name,
                                                  ip=ip,
                                                  client_port=ports[0],
                                                  messaging_port=ports[1],
                                                  log_dir=log_dir,
                                                  home=home_dir,
                                                  tlog_dir=tlog_dir))
            ArakoonInstaller._deploy(config)
        finally:
            if port_mutex is not None:
                port_mutex.release()

        logger.debug('Creating cluster {0} on {1} completed'.format(cluster_name, ip))
        return {'client_port': ports[0],
                'messaging_port': ports[1]}

    @staticmethod
    def delete_cluster(cluster_name, ip):
        """
        Deletes a complete cluster
        :param ip: IP address of the last node of a cluster
        :param cluster_name: Name of the cluster to remove
        """
        logger.debug('Deleting cluster {0} on {1}'.format(cluster_name, ip))
        config = ArakoonClusterConfig(cluster_name)
        config.load_config()

        # Cleans up a complete cluster (remove services, directories and configuration files)
        for node in config.nodes:
            ArakoonInstaller._destroy_node(config, node)
        logger.debug('Deleting cluster {0} on {1} completed'.format(cluster_name, ip))

    @staticmethod
    def extend_cluster(master_ip, new_ip, cluster_name, base_dir):
        """
        Extends a cluster to a given new node
        :param base_dir: Base directory that will hold the db and tlogs
        :param cluster_name: Name of the cluster to be extended
        :param new_ip: IP address of the node to be added
        :param master_ip: IP of one of the already existing nodes
        """
        logger.debug('Extending cluster {0} from {1} to {2}'.format(cluster_name, master_ip, new_ip))
        base_dir = base_dir.rstrip('/')
        from ovs.extensions.generic.volatilemutex import VolatileMutex
        port_mutex = VolatileMutex('arakoon_install_ports_{0}'.format(new_ip))

        config = ArakoonClusterConfig(cluster_name)
        config.load_config()

        client = SSHClient(new_ip)
        node_name = System.get_my_machine_id(client)

        home_dir = ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, cluster_name)
        log_dir = ArakoonInstaller.ARAKOON_LOG_DIR.format(cluster_name)
        tlog_dir = ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, cluster_name)

        ArakoonInstaller.archive_existing_arakoon_data(new_ip, home_dir,
                                                       ArakoonInstaller.ARAKOON_BASE_DIR.format(base_dir), cluster_name)
        ArakoonInstaller.archive_existing_arakoon_data(new_ip, log_dir,
                                                       ArakoonInstaller.ARAKOON_LOG_DIR.format(''), cluster_name)
        ArakoonInstaller.archive_existing_arakoon_data(new_ip, tlog_dir,
                                                       ArakoonInstaller.ARAKOON_BASE_DIR.format(base_dir), cluster_name)

        try:
            port_mutex.acquire(wait=60)
            ports = ArakoonInstaller._get_free_ports(client)
            if node_name not in [node.name for node in config.nodes]:
                config.nodes.append(ArakoonNodeConfig(name=node_name,
                                                      ip=new_ip,
                                                      client_port=ports[0],
                                                      messaging_port=ports[1],
                                                      log_dir=log_dir,
                                                      home=home_dir,
                                                      tlog_dir=tlog_dir))
            ArakoonInstaller._deploy(config)
        finally:
            port_mutex.release()

        logger.debug('Extending cluster {0} from {1} to {2} completed'.format(cluster_name, master_ip, new_ip))
        return {'client_port': ports[0],
                'messaging_port': ports[1]}

    @staticmethod
    def shrink_cluster(remaining_node_ip, deleted_node_ip, cluster_name, offline_nodes=None):
        """
        Removes a node from a cluster, the old node will become a slave
        :param cluster_name: The name of the cluster to shrink
        :param deleted_node_ip: The ip of the node that should be deleted
        :param remaining_node_ip: The ip of a remaining node
        :param offline_nodes: Storage Routers which are offline
        """
        logger.debug('Shrinking cluster {0} from {1}'.format(cluster_name, deleted_node_ip))
        config = ArakoonClusterConfig(cluster_name)
        config.load_config()

        if offline_nodes is None:
            offline_nodes = []

        for node in config.nodes[:]:
            if node.ip == deleted_node_ip:
                config.nodes.remove(node)
                if node.ip not in offline_nodes:
                    ArakoonInstaller._destroy_node(config, node)
        ArakoonInstaller._deploy(config, offline_nodes)
        logger.debug('Shrinking cluster {0} from {1} completed'.format(cluster_name, deleted_node_ip))

    @staticmethod
    def deploy_cluster(cluster_name, node_ip):
        """
        (Re)deploys a given cluster
        :param cluster_name: Name of the cluster to (re)deploy
        :param node_ip: IP address of one of the cluster's nodes
        """
        logger.debug('(Re)deploying cluster {0} from {1}'.format(cluster_name, node_ip))
        config = ArakoonClusterConfig(cluster_name)
        config.load_config()
        ArakoonInstaller._deploy(config)

    @staticmethod
    def _get_free_ports(client):
        node_name = System.get_my_machine_id(client)
        clusters = []
        exclude_ports = []
        if EtcdConfiguration.dir_exists(ArakoonInstaller.ETCD_CONFIG_ROOT):
            for cluster_name in EtcdConfiguration.list(ArakoonInstaller.ETCD_CONFIG_ROOT):
                try:
                    config = ArakoonClusterConfig(cluster_name)
                    config.load_config()
                    for node in config.nodes:
                        if node.name == node_name:
                            clusters.append(cluster_name)
                            exclude_ports.append(node.client_port)
                            exclude_ports.append(node.messaging_port)
                except:
                    logger.error('  Could not load port information of cluster {0}'.format(cluster_name))
        ports = System.get_free_ports(EtcdConfiguration.get('/ovs/framework/hosts/{0}/ports|arakoon'.format(node_name)), exclude_ports, 2, client)
        logger.debug('  Loaded free ports {0} based on existing clusters {1}'.format(ports, clusters))
        return ports

    @staticmethod
    def _destroy_node(config, node):
        """
        Cleans up a single node (remove services, directories and configuration files)
        """
        logger.debug('Destroy node {0} in cluster {1}'.format(node.ip, config.cluster_id))

        # Removes services for a cluster on a given node
        ovs_client = SSHClient(node.ip)
        root_client = SSHClient(node.ip, username='root')
        ArakoonInstaller.stop(config.cluster_id, client=root_client)
        ArakoonInstaller.remove(config.cluster_id, client=root_client)

        # Cleans all directories on a given node
        for directory in [node.log_dir, node.tlog_dir, node.home]:
            root_client.dir_delete([directory])

        # Removes a configuration file from a node
        config.delete_config(ovs_client)
        logger.debug('Destroy node {0} in cluster {1} completed'.format(node.ip, config.cluster_id))

    @staticmethod
    def _deploy(config, offline_nodes=None):
        """
        Deploys a complete cluster: Distributing the configuration files, creating directories and services
        """
        logger.debug('Deploying cluster {0}'.format(config.cluster_id))
        if offline_nodes is None:
            offline_nodes = []
        for node in config.nodes:
            if node.ip in offline_nodes:
                continue
            logger.debug('  Deploying cluster {0} on {1}'.format(config.cluster_id, node.ip))
            root_client = SSHClient(node.ip, username='root')

            # Distributes a configuration file to all its nodes
            config.write_config()

            # Create dirs as root because mountpoint /mnt/cache1 is typically owned by root
            abs_paths = [node.log_dir, node.tlog_dir, node.home]
            root_client.dir_create(abs_paths)
            root_client.dir_chmod(abs_paths, 0755, recursive=True)
            root_client.dir_chown(abs_paths, 'ovs', 'ovs', recursive=True)

            # Creates services for/on all nodes in the config
            base_name = 'ovs-arakoon'
            target_name = 'ovs-arakoon-{0}'.format(config.cluster_id)
            ServiceManager.add_service(base_name, root_client,
                                       params={'CLUSTER': config.cluster_id,
                                               'NODE_ID': node.name,
                                               'CONFIG_FILE': '/tmp/arakoon-{0}.ini'.format(config.cluster_id)},
                                       target_name=target_name)
            logger.debug('  Deploying cluster {0} on {1} completed'.format(config.cluster_id, node.ip))

    @staticmethod
    def start(cluster_name, client):
        """
        Starts an arakoon cluster
        :param client: Client on which to start the service
        :param cluster_name: The name of the cluster service to start
        """
        if ServiceManager.has_service('arakoon-{0}'.format(cluster_name), client=client) is True and \
                ServiceManager.get_service_status('arakoon-{0}'.format(cluster_name), client=client) is False:
            ServiceManager.start_service('arakoon-{0}'.format(cluster_name), client=client)

    @staticmethod
    def stop(cluster_name, client):
        """
        Stops an arakoon service
        :param client: Client on which to stop the service
        :param cluster_name: The name of the cluster service to stop
        """
        if ServiceManager.has_service('arakoon-{0}'.format(cluster_name), client=client) is True and \
                ServiceManager.get_service_status('arakoon-{0}'.format(cluster_name), client=client) is True:
            ServiceManager.stop_service('arakoon-{0}'.format(cluster_name), client=client)

    @staticmethod
    def remove(cluster_name, client):
        """
        Removes an arakoon service
        :param client: Client on which to remove the service
        :param cluster_name: The name of the cluster service to remove
        """
        if ServiceManager.has_service('arakoon-{0}'.format(cluster_name), client=client) is True:
            ServiceManager.remove_service('arakoon-{0}'.format(cluster_name), client=client)

    @staticmethod
    def wait_for_cluster(cluster_name, sshclient):
        """
        Waits for an Arakoon cluster to be available (by sending a nop)
        :param sshclient: Client on which to wait for the cluster
        :param cluster_name: Name of the cluster to wait on
        """
        logger.debug('Waiting for cluster {0}'.format(cluster_name))
        from ovs.extensions.storage.persistentfactory import PersistentFactory
        with Remote(sshclient.ip, [PersistentFactory], 'ovs') as remote:
            client = remote.PersistentFactory.get_client()
            client.nop()
            logger.debug('Waiting for cluster {0}: available'.format(cluster_name))
            return True

    @staticmethod
    def restart_cluster(cluster_name, master_ip):
        """
        Execute a restart sequence (Executed after arakoon and/or alba package upgrade)
        :param master_ip: IP of one of the cluster nodes
        :param cluster_name: Name of the cluster to restart
        """
        logger.debug('Restart sequence for {0} via {1}'.format(cluster_name, master_ip))

        config = ArakoonClusterConfig(cluster_name)
        config.load_config()

        all_clients = [SSHClient(node.ip, username='root') for node in config.nodes]
        if len(config.nodes) <= 2:
            logger.debug('  Insufficient nodes in cluster {0}. Full restart'.format(cluster_name))
            for function in [ArakoonInstaller.stop, ArakoonInstaller.start]:
                for client in all_clients:
                    function(cluster_name, client)
            ArakoonInstaller.wait_for_cluster(cluster_name, all_clients[0])
        else:
            logger.debug('  Sufficient nodes in cluster {0}. Sequential restart'.format(cluster_name))
            for client in all_clients:
                ArakoonInstaller.stop(cluster_name, client)
                ArakoonInstaller.start(cluster_name, client)
                logger.debug('  Restarted node {0} on cluster {1}'.format(client.ip, cluster_name))
                ArakoonInstaller.wait_for_cluster(cluster_name, client)
        logger.debug('Restart sequence for {0} via {1} completed'.format(cluster_name, master_ip))

    @staticmethod
    def restart_cluster_add(cluster_name, current_ips, new_ip):
        """
        Execute a (re)start sequence after adding a new node to a cluster.
        :param new_ip: IP of the newly added node
        :param current_ips: IPs of the previous nodes
        :param cluster_name: Name of the cluster to restart
        """
        logger.debug('Restart sequence (add) for {0}'.format(cluster_name))
        logger.debug('Current ips: {0}'.format(', '.join(current_ips)))
        logger.debug('New ip: {0}'.format(new_ip))

        client = SSHClient(new_ip)
        config = ArakoonClusterConfig(cluster_name)
        config.load_config()

        if len(config.nodes) > 1:
            logger.debug('Catching up new node {0} for cluster {1}'.format(new_ip, cluster_name))
            node_name = [node.name for node in config.nodes if node.ip == new_ip][0]
            (temp_handle, temp_filename) = tempfile.mkstemp()
            with open(temp_filename, 'wb') as config_file:
                config_file.write(EtcdConfiguration.get(ArakoonInstaller.ETCD_CONFIG_KEY.format(cluster_name), raw=True))
            client.run(ArakoonInstaller.ARAKOON_CATCHUP_COMMAND.format(node_name, temp_filename))
            os.remove(temp_filename)
            logger.debug('Catching up new node {0} for cluster {1} completed'.format(new_ip, cluster_name))

        threshold = 2 if new_ip in current_ips else 1
        for ip in current_ips:
            if ip == new_ip:
                continue
            current_client = SSHClient(ip, username='root')
            ArakoonInstaller.stop(cluster_name, client=current_client)
            ArakoonInstaller.start(cluster_name, client=current_client)
            logger.debug('  Restarted node {0} for cluster {1}'.format(current_client.ip, cluster_name))
            if len(current_ips) > threshold:  # A two node cluster needs all nodes running
                ArakoonInstaller.wait_for_cluster(cluster_name, current_client)
        client = SSHClient(new_ip, username='root')
        ArakoonInstaller.start(cluster_name, client=client)
        ArakoonInstaller.wait_for_cluster(cluster_name, client)
        logger.debug('Started node {0} for cluster {1}'.format(new_ip, cluster_name))

    @staticmethod
    def restart_cluster_remove(cluster_name, remaining_ips):
        """
        Execute a restart sequence after removing a node from a cluster
        :param remaining_ips: IPs of the remaining nodes after shrink
        :param cluster_name: Name of the cluster to restart
        """
        logger.debug('Restart sequence (remove) for {0}'.format(cluster_name))
        logger.debug('Remaining ips: {0}'.format(', '.join(remaining_ips)))
        for ip in remaining_ips:
            client = SSHClient(ip, username='root')
            ArakoonInstaller.stop(cluster_name, client=client)
            ArakoonInstaller.start(cluster_name, client=client)
            logger.debug('  Restarted node {0} for cluster {1}'.format(client.ip, cluster_name))
            if len(remaining_ips) > 2:  # A two node cluster needs all nodes running
                ArakoonInstaller.wait_for_cluster(cluster_name, client)
        logger.debug('Restart sequence (remove) for {0} completed'.format(cluster_name))
