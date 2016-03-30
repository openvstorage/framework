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
ArakoonNodeConfig class
ArakoonClusterConfig class
ArakoonInstaller class
"""

import os
import time
from ConfigParser import RawConfigParser
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.generic.remote import Remote
from ovs.extensions.generic.sshclient import CalledProcessError, SSHClient
from ovs.extensions.generic.system import System
from ovs.extensions.services.service import ServiceManager
from ovs.log.logHandler import LogHandler
from StringIO import StringIO

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
        self._extra_globals = {'tlog_max_entries': 5000}
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
        contents = RawConfigParser()
        data = self.export()
        for section in data:
            contents.add_section(section)
            for item in data[section]:
                contents.set(section, item, data[section][item])
        config_io = StringIO()
        contents.write(config_io)
        EtcdConfiguration.set(ArakoonClusterConfig.ETCD_CONFIG_KEY.format(self.cluster_id), config_io.getvalue(), raw=True)

    def delete_config(self):
        """
        Deletes a configuration file
        """
        key = ArakoonClusterConfig.ETCD_CONFIG_KEY.format(self.cluster_id)
        if EtcdConfiguration.exists(key, raw=True):
            EtcdConfiguration.delete(key, raw=True)


class ArakoonClusterMetadata(object):
    """
    Contains cluster metadata parameters
    """
    ETCD_METADATA_KEY = '/ovs/arakoon/{0}/metadata'

    def __init__(self, cluster_id):
        """
        Initializes an empty Cluster Config
        """
        self.in_use = False
        self.internal = True
        self.cluster_id = cluster_id
        self.cluster_type = None

    def load_metadata(self):
        """
        Reads the metadata for an arakoon cluster from reality
        :return: None
        """
        key = ArakoonClusterMetadata.ETCD_METADATA_KEY.format(self.cluster_id)
        if not EtcdConfiguration.exists(key):
            return

        metadata = EtcdConfiguration.get(key)
        if not isinstance(metadata, dict):
            raise ValueError('Metadata should be a dictionary')

        for key in ['in_use', 'internal', 'type']:
            if key not in metadata:
                raise ValueError('Not all required metadata keys are present for arakoon cluster {0}'.format(self.cluster_id))
            value = metadata[key]
            if key == 'in_use':
                if not isinstance(value, bool):
                    raise ValueError('"in_use" should be of type "bool"')
                self.in_use = value
            elif key == 'internal':
                if not isinstance(value, bool):
                    raise ValueError('"internal" should be of type "bool"')
                self.internal = value
            else:
                if value not in ServiceType.ARAKOON_CLUSTER_TYPES:
                    raise ValueError('Unsupported arakoon cluster type {0} found\nPlease choose from {1}'.format(value, ', '.join(ServiceType.ARAKOON_CLUSTER_TYPES)))
                self.cluster_type = value

    def write(self):
        """
        Write the metadata to Etcd
        :return: None
        """
        if self.cluster_type is None or self.cluster_type == '':
            raise ValueError('Cluster type must be defined before being able to store the cluster metadata information')

        etcd_key = ArakoonClusterMetadata.ETCD_METADATA_KEY.format(self.cluster_id)
        EtcdConfiguration.set(key=etcd_key, value={'type': self.cluster_type,
                                                   'in_use': self.in_use,
                                                   'internal': self.internal})


class ArakoonInstaller(object):
    """
    class to dynamically install/(re)configure arakoon cluster
    """
    ARAKOON_LOG_DIR = '/var/log/arakoon/{0}'
    ARAKOON_BASE_DIR = '{0}/arakoon'
    ARAKOON_HOME_DIR = '{0}/arakoon/{1}/db'
    ARAKOON_TLOG_DIR = '{0}/arakoon/{1}/tlogs'
    ARAKOON_CATCHUP_COMMAND = 'arakoon --node {0} -config {1} -catchup-only'
    ABM_PLUGIN = 'albamgr_plugin'
    NSM_PLUGIN = 'nsm_host_plugin'
    ARAKOON_PLUGIN_DIR = '/usr/lib/alba'
    ETCD_CONFIG_ROOT = '/ovs/arakoon'
    ETCD_CONFIG_KEY = ETCD_CONFIG_ROOT + '/{0}/config'
    ETCD_CONFIG_PATH = 'etcd://127.0.0.1:2379' + ETCD_CONFIG_KEY
    SSHCLIENT_USER = 'ovs'
    ARAKOON_START_PORT = 26400

    def __init__(self):
        """
        ArakoonInstaller should not be instantiated
        """
        raise RuntimeError('ArakoonInstaller is a complete static helper class')

    @staticmethod
    def clean_leftover_arakoon_data(ip, directories):
        """
        Delete existing arakoon data or copy to the side
        Directories should be a dict with key the absolute paths and value a boolean indicating archive or delete
        eg: {'/var/log/arakoon/ovsdb': True,                     --> Files under this directory will be archived
             '/opt/OpenvStorage/db/arakoon/ovsdb/tlogs': False}  --> Files under this directory will be deleted
        :param ip: IP on which to check for existing data
        :type ip: str

        :param directories: Directories to archive or delete
        :type directories: dictionary

        :return: None
        """
        root_client = SSHClient(ip, username='root')

        # Verify whether all files to be archived have been released properly
        open_file_errors = []
        logger.debug('Cleanup old arakoon - Checking open files')
        dirs_with_files = {}
        for directory, archive in directories.iteritems():
            logger.debug('Cleaning old arakoon - Checking directory {0}'.format(directory))
            if root_client.dir_exists(directory):
                logger.debug('Cleaning old arakoon - Directory {0} exists'.format(directory))
                file_names = root_client.file_list(directory, abs_path=True, recursive=True)
                if len(file_names) > 0:
                    logger.debug('Cleaning old arakoon - Files found in directory {0}'.format(directory))
                    dirs_with_files[directory] = {'files': file_names,
                                                  'archive': archive}
                for file_name in file_names:
                    try:
                        open_files = root_client.run('lsof {0}'.format(file_name))
                        if open_files != '':
                            open_file_errors.append('Open file {0} detected in directory {1}'.format(os.path.basename(file_name), directory))
                    except CalledProcessError:
                        continue

        if len(open_file_errors) > 0:
            raise RuntimeError('\n - ' + '\n - '.join(open_file_errors))

        for directory, info in dirs_with_files.iteritems():
            if info['archive'] is True:
                # Create zipped tar
                logger.debug('Cleanup old arakoon - Start archiving directory {0}'.format(directory))
                archive_dir = '{0}/archive'.format(directory)
                if not root_client.dir_exists(archive_dir):
                    logger.debug('Cleanup old arakoon - Creating archive directory {0}'.format(archive_dir))
                    root_client.dir_create(archive_dir)

                logger.debug('Cleanup old arakoon - Creating tar file')
                tar_name = '{0}/{1}.tgz'.format(archive_dir, int(time.time()))
                root_client.run('cd {0}; tar -cz -f {1} --exclude "archive" *'.format(directory, tar_name))

            logger.debug('Cleanup old arakoon - Removing old files from {0}'.format(directory))
            root_client.file_delete(info['files'])

    @staticmethod
    def create_cluster(cluster_name, cluster_type, ip, base_dir, locked=True, internal=True):
        """
        Always creates a cluster but marks it's usage according to the internal flag
        :param cluster_name: Name of the cluster
        :param cluster_type: Type of the cluster (See ServiceType.ARAKOON_CLUSTER_TYPES)
        :param ip: IP address of the first node of the new cluster
        :param base_dir: Base directory that should contain the data and tlogs
        :param locked: Indicates whether the create should run in a locked context (e.g. to prevent port conflicts)
        :param internal: Is cluster internally managed by OVS
        """
        if cluster_type not in ServiceType.ARAKOON_CLUSTER_TYPES:
            raise ValueError('Cluster type {0} is not supported. Please choose from {1}'.format(cluster_type, ', '.join(ServiceType.ARAKOON_CLUSTER_TYPES)))

        plugins = []
        if cluster_type == ServiceType.ARAKOON_CLUSTER_TYPES.NSM:
            plugins = [ArakoonInstaller.NSM_PLUGIN]
        if cluster_type == ServiceType.ARAKOON_CLUSTER_TYPES.ABM:
            plugins = [ArakoonInstaller.ABM_PLUGIN]

        logger.debug('Creating cluster {0} on {1}'.format(cluster_name, ip))
        base_dir = base_dir.rstrip('/')

        EtcdConfiguration.set('/ovs/framework/stores', {'persistent': 'pyrakoon',
                                                        'volatile': 'memcache'})
        EtcdConfiguration.create_dir('/ovs/arakoon')

        client = SSHClient(ip, username=ArakoonInstaller.SSHCLIENT_USER)
        if ArakoonInstaller.is_running(cluster_name, client):
            logger.info('Arakoon service running for cluster {0}'.format(cluster_name))
            config = ArakoonClusterConfig(cluster_name, plugins)
            config.load_config()
            for node in config.nodes:
                if node.ip == ip:
                    return {'client_port': node.client_port,
                            'messaging_port': node.messaging_port}

        node_name = System.get_my_machine_id(client)

        home_dir = ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, cluster_name)
        log_dir = ArakoonInstaller.ARAKOON_LOG_DIR.format(cluster_name)
        tlog_dir = ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, cluster_name)
        ArakoonInstaller.clean_leftover_arakoon_data(ip, {log_dir: True,
                                                          home_dir: False,
                                                          tlog_dir: False})

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

            data_dir = '' if base_dir == '/' else base_dir
            for plugin in plugins:
                cmd = 'ln -s {0}/{1}.cmxs {2}/arakoon/{3}/db'.format(ArakoonInstaller.ARAKOON_PLUGIN_DIR, plugin,
                                                                     data_dir, cluster_name)
                client.run(cmd)

            metadata = ArakoonClusterMetadata(cluster_id=cluster_name)
            metadata.cluster_type = cluster_type.upper()
            metadata.internal = internal
            metadata.write()
        finally:
            if port_mutex is not None:
                port_mutex.release()

        logger.debug('Creating cluster {0} on {1} completed'.format(cluster_name, ip))
        return {'client_port': ports[0],
                'messaging_port': ports[1]}

    @staticmethod
    def claim_cluster(cluster_name, cluster_type):
        """
        Claim a cluster and mark it in use
        :param cluster_name: Name of the cluster to claim
        :param cluster_type: Type of cluster to claim: ['FWK','SD','ABM','NSM']
        :return: Cluster name
        """

        internal = ArakoonInstaller.is_internal(cluster_type=cluster_type)
        # if internal is True and cluster_name is None:
        #     raise RuntimeError('Cluster name required when marking internal cluster')

        available_cluster = None
        for cluster in ArakoonInstaller.get_arakoon_metadata_by_cluster_type(cluster_type=cluster_type, in_use=False):
            if cluster.cluster_id == cluster_name:
                available_cluster = cluster
                break

        if available_cluster is None:
            raise RuntimeError('No available internal clusters found for type: {0}'.format(cluster_type))

        if available_cluster.in_use is True:
            raise RuntimeError('Cluster is already marked as in use: {0}'.format(cluster_name))

        available_cluster.in_use = True
        available_cluster.write()

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
        EtcdConfiguration.delete('{0}/{1}'.format(ArakoonInstaller.ETCD_CONFIG_ROOT, cluster_name), raw=True)
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

        client = SSHClient(new_ip, username=ArakoonInstaller.SSHCLIENT_USER)
        node_name = System.get_my_machine_id(client)

        home_dir = ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, cluster_name)
        log_dir = ArakoonInstaller.ARAKOON_LOG_DIR.format(cluster_name)
        tlog_dir = ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, cluster_name)
        ArakoonInstaller.clean_leftover_arakoon_data(new_ip, {log_dir: True,
                                                              home_dir: False,
                                                              tlog_dir: False})

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
    def shrink_cluster(deleted_node_ip, cluster_name, offline_nodes=None):
        """
        Removes a node from a cluster, the old node will become a slave
        :param cluster_name: The name of the cluster to shrink
        :param deleted_node_ip: The ip of the node that should be deleted
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
    def get_arakoon_metadata_by_cluster_type(cluster_type, in_use):
        """
        Retrieve cluster information for an unused cluster based on its type
        :param cluster_type: Type of arakoon cluster (See ServiceType.ARAKOON_CLUSTER_TYPES)
        :param in_use: Return clusters which are already in use or not
        :return: List of ArakoonClusterMetadata objects
        """
        clusters = []
        cluster_type = cluster_type.upper()
        if cluster_type not in ServiceType.ARAKOON_CLUSTER_TYPES:
            raise ValueError('Unsupported arakoon cluster type provided. Please choose from {0}'.format(', '.join(ServiceType.ARAKOON_CLUSTER_TYPES)))
        if not EtcdConfiguration.exists('/ovs/arakoon', raw=True):
            return clusters

        for cluster_name in EtcdConfiguration.list('/ovs/arakoon'):
            arakoon_metadata = ArakoonClusterMetadata(cluster_id=cluster_name)
            arakoon_metadata.load_metadata()
            if arakoon_metadata.cluster_type == cluster_type and arakoon_metadata.in_use is in_use:
                clusters.append(arakoon_metadata)
                if in_use is False:
                    break  # We only need 1 unused cluster (at a time)
        return clusters

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
        key = '/ovs/framework/hosts/{0}/ports|arakoon'.format(node_name)
        if EtcdConfiguration.exists(key):
            ports = System.get_free_ports(EtcdConfiguration.get(key), exclude_ports, 2, client)
        else:
            ports = System.get_free_ports([ArakoonInstaller.ARAKOON_START_PORT], exclude_ports, 2, client)
        logger.debug('  Loaded free ports {0} based on existing clusters {1}'.format(ports, clusters))
        return ports

    @staticmethod
    def _destroy_node(config, node):
        """
        Cleans up a single node (remove services, directories and configuration files)
        """
        logger.debug('Destroy node {0} in cluster {1}'.format(node.ip, config.cluster_id))

        # Removes services for a cluster on a given node
        root_client = SSHClient(node.ip, username='root')
        ArakoonInstaller.stop(config.cluster_id, client=root_client)
        ArakoonInstaller.remove(config.cluster_id, client=root_client)

        # Cleans all directories on a given node
        for directory in [node.log_dir, node.tlog_dir, node.home]:
            root_client.dir_delete([directory])

        # Removes a configuration file from a node
        config.delete_config()
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
            if not root_client.dir_exists(abs_paths):
                root_client.dir_create(abs_paths)
                root_client.dir_chmod(abs_paths, 0755, recursive=True)
                root_client.dir_chown(abs_paths, 'ovs', 'ovs', recursive=True)

            # Creates services for/on all nodes in the config
            base_name = 'ovs-arakoon'
            target_name = 'ovs-arakoon-{0}'.format(config.cluster_id)
            ServiceManager.add_service(base_name, root_client,
                                       params={'CLUSTER': config.cluster_id,
                                               'NODE_ID': node.name,
                                               'CONFIG_PATH': ArakoonInstaller.ETCD_CONFIG_PATH.format(config.cluster_id)},
                                       target_name=target_name)
            logger.debug('  Deploying cluster {0} on {1} completed'.format(config.cluster_id, node.ip))

    @staticmethod
    def start(cluster_name, client):
        """
        Starts an arakoon cluster
        :param client: Client on which to start the service
        :param cluster_name: The name of the cluster service to start
        """
        if ServiceManager.has_service('arakoon-{0}'.format(cluster_name), client=client) is True:
            ServiceManager.start_service('arakoon-{0}'.format(cluster_name), client=client)

    @staticmethod
    def stop(cluster_name, client):
        """
        Stops an arakoon service
        :param client: Client on which to stop the service
        :param cluster_name: The name of the cluster service to stop
        """
        if ServiceManager.has_service('arakoon-{0}'.format(cluster_name), client=client) is True:
            ServiceManager.stop_service('arakoon-{0}'.format(cluster_name), client=client)

    @staticmethod
    def is_running(cluster_name, client):
        """
        Checks if arakoon service is running
        :param client: Client on which to stop the service
        :param cluster_name: The name of the cluster service to stop
        """
        if ServiceManager.has_service('arakoon-{0}'.format(cluster_name), client=client):
            return ServiceManager.get_service_status('arakoon-{0}'.format(cluster_name), client=client)
        return False

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

        client = SSHClient(new_ip, username=ArakoonInstaller.SSHCLIENT_USER)
        if ArakoonInstaller.is_running(cluster_name, client):
            logger.info('Arakoon service for {0} is already running'.format(cluster_name))
            return
        config = ArakoonClusterConfig(cluster_name)
        config.load_config()

        if len(config.nodes) > 1:
            logger.debug('Catching up new node {0} for cluster {1}'.format(new_ip, cluster_name))
            node_name = [node.name for node in config.nodes if node.ip == new_ip][0]
            config_path = ArakoonInstaller.ETCD_CONFIG_PATH.format(cluster_name)
            client.run(ArakoonInstaller.ARAKOON_CATCHUP_COMMAND.format(node_name, config_path))
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

    @staticmethod
    def is_internal(cluster_type):
        """
        Checks if an arakoon cluster is internally managed, if no clusters are found, internally managed is assumed
        Any cluster found that is externally managed for this cluster_type -> return False
        :param cluster_type: Type of cluster to claim: ['FWK','SD','ABM','NSM']
        :return: True|False
        """

        metadata_in_use = ArakoonInstaller.get_arakoon_metadata_by_cluster_type(cluster_type=cluster_type,
                                                                                in_use=True)
        metadata_not_in_use = ArakoonInstaller.get_arakoon_metadata_by_cluster_type(cluster_type=cluster_type,
                                                                                    in_use=False)
        for metadata in metadata_in_use + metadata_not_in_use:
            if metadata.cluster_type == cluster_type and metadata.internal is False:
                return False
        return True
