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
ArakoonNodeConfig class
ArakoonClusterConfig class
ArakoonInstaller class
"""

import os
import json
from ConfigParser import RawConfigParser
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.sshclient import CalledProcessError, SSHClient
from ovs.extensions.generic.system import System
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.services.service import ServiceManager
from ovs.log.log_handler import LogHandler
from StringIO import StringIO


class ArakoonNodeConfig(object):
    """
    cluster node config parameters
    """
    def __init__(self, name, ip, client_port, messaging_port, log_sinks, crash_log_sinks, home, tlog_dir):
        """
        Initializes a new Config entry for a single Node
        """
        self.name = name
        self.ip = ip
        self.client_port = int(client_port)
        self.messaging_port = int(messaging_port)
        self.tlog_compression = 'snappy'
        self.log_level = 'info'
        self.log_sinks = log_sinks
        self.crash_log_sinks = crash_log_sinks
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
    CONFIG_KEY = '/ovs/arakoon/{0}/config'
    CONFIG_FILE = '/opt/OpenvStorage/config/arakoon_{0}.ini'

    def __init__(self, cluster_id, filesystem, plugins=None):
        """
        Initializes an empty Cluster Config
        """
        self.filesystem = filesystem
        self.cluster_id = cluster_id
        self._extra_globals = {'tlog_max_entries': 5000}
        self.nodes = []
        self._plugins = []
        if isinstance(plugins, list):
            self._plugins = plugins
        elif isinstance(plugins, basestring):
            self._plugins.append(plugins)

    @property
    def config_path(self):
        """
        Retrieve the configuration path
        :return: Configuration path
        """
        if self.filesystem is False:
            return ArakoonClusterConfig.CONFIG_KEY.format(self.cluster_id)
        return ArakoonClusterConfig.CONFIG_FILE.format(self.cluster_id)

    def _load_client(self, ip):
        if self.filesystem is True:
            if ip is None:
                raise RuntimeError('An IP should be passed for filesystem configuration')
            return SSHClient(ip, username=ArakoonInstaller.SSHCLIENT_USER)

    def load_config(self, ip=None):
        """
        Reads a configuration from reality
        """
        if self.filesystem is False:
            contents = Configuration.get(self.config_path, raw=True)
        else:
            client = self._load_client(ip)
            contents = client.file_read(self.config_path)
        self.read_config(contents)

    def read_config(self, contents):
        """
        Constructs a configuration object from config contents
        :param contents: Raw .ini contents
        """
        parser = RawConfigParser()
        parser.readfp(StringIO(contents))
        self.nodes = []
        self._extra_globals = {}
        for key in parser.options('global'):
            if key == 'plugins':
                self._plugins = [plugin.strip() for plugin in parser.get('global', 'plugins').split(',')]
            elif key == 'cluster_id':
                self.cluster_id = parser.get('global', 'cluster_id')
            elif key == 'cluster':
                pass  # Ignore these
            else:
                self._extra_globals[key] = parser.get('global', key)
        for node in parser.get('global', 'cluster').split(','):
            node = node.strip()
            self.nodes.append(ArakoonNodeConfig(name=node,
                                                ip=parser.get(node, 'ip'),
                                                client_port=parser.get(node, 'client_port'),
                                                messaging_port=parser.get(node, 'messaging_port'),
                                                log_sinks=parser.get(node, 'log_sinks'),
                                                crash_log_sinks=parser.get(node, 'crash_log_sinks'),
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
                               'log_sinks': node.log_sinks,
                               'crash_log_sinks': node.crash_log_sinks,
                               'home': node.home,
                               'tlog_dir': node.tlog_dir,
                               'fsync': 'true' if node.fsync else 'false'}
        return data

    def export_ini(self):
        """
        Exports the current configuration to an ini file format
        """
        contents = RawConfigParser()
        data = self.export()
        sections = data.keys()
        sections.remove('global')
        for section in ['global'] + sorted(sections):
            contents.add_section(section)
            for item in sorted(data[section]):
                contents.set(section, item, data[section][item])
        config_io = StringIO()
        contents.write(config_io)
        return str(config_io.getvalue())

    def write_config(self, ip=None):
        """
        Writes the configuration down to in the format expected by Arakoon
        """
        contents = self.export_ini()
        if self.filesystem is False:
            Configuration.set(self.config_path, contents, raw=True)
        else:
            client = self._load_client(ip)
            client.file_write(self.config_path, contents)

    def delete_config(self, ip=None):
        """
        Deletes a configuration file
        """
        if self.filesystem is False:
            key = self.config_path
            if Configuration.exists(key, raw=True):
                Configuration.delete(key, raw=True)
        else:
            client = self._load_client(ip)
            client.file_delete(self.config_path)


class ArakoonInstaller(object):
    """
    class to dynamically install/(re)configure arakoon cluster
    """
    ARAKOON_BASE_DIR = '{0}/arakoon'
    ARAKOON_HOME_DIR = '{0}/arakoon/{1}/db'
    ARAKOON_TLOG_DIR = '{0}/arakoon/{1}/tlogs'
    CONFIG_ROOT = '/ovs/arakoon'
    CONFIG_KEY = CONFIG_ROOT + '/{0}/config'
    SSHCLIENT_USER = 'ovs'
    METADATA_KEY = '__ovs_metadata'
    INTERNAL_CONFIG_KEY = '__ovs_config'

    _logger = LogHandler.get('extensions', name='arakoon_installer')
    _logger.logger.propagate = False

    def __init__(self):
        """
        ArakoonInstaller should not be instantiated
        """
        raise RuntimeError('ArakoonInstaller is a complete static helper class')

    @staticmethod
    def clean_leftover_arakoon_data(ip, directories):
        """
        Delete existing arakoon data
        :param ip: IP on which to check for existing data
        :type ip: str

        :param directories: Directories to delete
        :type directories: list

        :return: None
        """
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            return

        root_client = SSHClient(ip, username='root')

        # Verify whether all files to be archived have been released properly
        open_file_errors = []
        ArakoonInstaller._logger.debug('Cleanup old arakoon - Checking open files')
        dirs_with_files = {}
        for directory in directories:
            ArakoonInstaller._logger.debug('Cleaning old arakoon - Checking directory {0}'.format(directory))
            if root_client.dir_exists(directory):
                ArakoonInstaller._logger.debug('Cleaning old arakoon - Directory {0} exists'.format(directory))
                file_names = root_client.file_list(directory, abs_path=True, recursive=True)
                if len(file_names) > 0:
                    ArakoonInstaller._logger.debug('Cleaning old arakoon - Files found in directory {0}'.format(directory))
                    dirs_with_files[directory] = file_names
                for file_name in file_names:
                    try:
                        open_files = root_client.run(['lsof', file_name])
                        if open_files != '':
                            open_file_errors.append('Open file {0} detected in directory {1}'.format(os.path.basename(file_name), directory))
                    except CalledProcessError:
                        continue

        if len(open_file_errors) > 0:
            raise RuntimeError('\n - ' + '\n - '.join(open_file_errors))

        for directory, info in dirs_with_files.iteritems():
            ArakoonInstaller._logger.debug('Cleanup old arakoon - Removing old files from {0}'.format(directory))
            root_client.file_delete(info)

    @staticmethod
    def create_cluster(cluster_name, cluster_type, ip, base_dir, plugins=None, locked=True, internal=True, filesystem=False, ports=None):
        """
        Always creates a cluster but marks it's usage according to the internal flag
        :param cluster_name: Name of the cluster
        :type cluster_name: str
        :param cluster_type: Type of the cluster (See ServiceType.ARAKOON_CLUSTER_TYPES)
        :type cluster_type: str
        :param ip: IP address of the first node of the new cluster
        :type ip: str
        :param base_dir: Base directory that should contain the data and tlogs
        :type base_dir: str
        :param plugins: Plugins that should be added to the configuration file
        :type plugins: list
        :param locked: Indicates whether the create should run in a locked context (e.g. to prevent port conflicts)
        :type locked: bool
        :param internal: Is cluster internally managed by OVS
        :type internal: bool
        :param filesystem: Indicates whether the configuration should be on the filesystem or in a configuration cluster
        :type filesystem: bool
        :param ports: A list of ports to be used for this cluster's node
        :type ports: list
        :return: Ports used by arakoon cluster
        :rtype: dict
        """
        if cluster_type not in ServiceType.ARAKOON_CLUSTER_TYPES:
            raise ValueError('Cluster type {0} is not supported. Please choose from {1}'.format(cluster_type, ', '.join(ServiceType.ARAKOON_CLUSTER_TYPES)))

        client = SSHClient(ip, username=ArakoonInstaller.SSHCLIENT_USER)
        if filesystem is True:
            exists = client.file_exists(ArakoonClusterConfig.CONFIG_FILE.format(cluster_name))
        else:
            exists = Configuration.dir_exists('/ovs/arakoon/{0}'.format(cluster_name))
        if exists is True:
            raise ValueError('An Arakoon cluster with name "{0}" already exists'.format(cluster_name))

        ArakoonInstaller._logger.debug('Creating cluster {0} on {1}'.format(cluster_name, ip))

        node_name = System.get_my_machine_id(client)
        base_dir = base_dir.rstrip('/')
        home_dir = ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, cluster_name)
        tlog_dir = ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, cluster_name)
        ArakoonInstaller.clean_leftover_arakoon_data(ip, [home_dir, tlog_dir])

        port_mutex = None
        try:
            if locked is True:
                from ovs.extensions.generic.volatilemutex import volatile_mutex
                port_mutex = volatile_mutex('arakoon_install_ports_{0}'.format(ip))
                port_mutex.acquire(wait=60)
            if ports is None:
                ports = ArakoonInstaller._get_free_ports(client)
            config = ArakoonClusterConfig(cluster_name, filesystem, plugins)
            config.nodes.append(ArakoonNodeConfig(name=node_name,
                                                  ip=ip,
                                                  client_port=ports[0],
                                                  messaging_port=ports[1],
                                                  log_sinks=LogHandler.get_sink_path('arakoon_server'),
                                                  crash_log_sinks=LogHandler.get_sink_path('arakoon_server_crash'),
                                                  home=home_dir,
                                                  tlog_dir=tlog_dir))
            metadata = {'internal': internal,
                        'cluster_name': cluster_name,
                        'cluster_type': cluster_type.upper(),
                        'in_use': False}
            ArakoonInstaller._deploy(config, filesystem=filesystem)
        finally:
            if port_mutex is not None:
                port_mutex.release()

        ArakoonInstaller._logger.debug('Creating cluster {0} on {1} completed'.format(cluster_name, ip))
        return {'metadata': metadata,
                'client_port': ports[0],
                'messaging_port': ports[1]}

    @staticmethod
    def delete_cluster(cluster_name, ip, filesystem=False):
        """
        Deletes a complete cluster
        :param cluster_name: Name of the cluster to remove
        :type cluster_name: str
        :param ip: IP address of the last node of a cluster
        :type ip: str
        :param filesystem: Indicates whether the configuration should be on the filesystem or in a configuration cluster
        :type filesystem: bool
        :return: None
        """
        ArakoonInstaller._logger.debug('Deleting cluster {0} on {1}'.format(cluster_name, ip))
        config = ArakoonClusterConfig(cluster_name, filesystem)
        config.load_config(ip)

        # Cleans up a complete cluster (remove services, directories and configuration files)
        for node in config.nodes:
            ArakoonInstaller._destroy_node(config, node)
            config.delete_config(ip)

        ArakoonInstaller._logger.debug('Deleting cluster {0} on {1} completed'.format(cluster_name, ip))

    @staticmethod
    def extend_cluster(master_ip, new_ip, cluster_name, base_dir, locked=True, filesystem=False, ports=None):
        """
        Extends a cluster to a given new node
        :param master_ip: IP of one of the already existing nodes
        :type master_ip: str
        :param new_ip: IP address of the node to be added
        :type new_ip: str
        :param cluster_name: Name of the cluster to be extended
        :type cluster_name: str
        :param base_dir: Base directory that will hold the db and tlogs
        :type base_dir: str
        :param locked: Indicates whether the extend should run in a locked context (e.g. to prevent port conflicts)
        :type locked: bool
        :param filesystem: Indicates whether the configuration should be on the filesystem or in a configuration cluster
        :type filesystem: bool
        :param ports: A list of ports to be used for this cluster's node
        :type ports: list
        :return: Ports used by arakoon cluster
        :rtype: dict
        """
        ArakoonInstaller._logger.debug('Extending cluster {0} from {1} to {2}'.format(cluster_name, master_ip, new_ip))
        base_dir = base_dir.rstrip('/')

        config = ArakoonClusterConfig(cluster_name, filesystem)
        config.load_config(master_ip)

        client = SSHClient(new_ip, username=ArakoonInstaller.SSHCLIENT_USER)
        node_name = System.get_my_machine_id(client)

        home_dir = ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, cluster_name)
        tlog_dir = ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, cluster_name)
        ArakoonInstaller.clean_leftover_arakoon_data(new_ip, [home_dir, tlog_dir])

        port_mutex = None
        try:
            if locked is True:
                from ovs.extensions.generic.volatilemutex import volatile_mutex
                port_mutex = volatile_mutex('arakoon_install_ports_{0}'.format(new_ip))
                port_mutex.acquire(wait=60)
            if ports is None:
                ports = ArakoonInstaller._get_free_ports(client)
            if node_name not in [node.name for node in config.nodes]:
                config.nodes.append(ArakoonNodeConfig(name=node_name,
                                                      ip=new_ip,
                                                      client_port=ports[0],
                                                      messaging_port=ports[1],
                                                      log_sinks=LogHandler.get_sink_path('arakoon_server'),
                                                      crash_log_sinks=LogHandler.get_sink_path('arakoon_server_crash'),
                                                      home=home_dir,
                                                      tlog_dir=tlog_dir))
            ArakoonInstaller._deploy(config, filesystem=filesystem)
        finally:
            if port_mutex is not None:
                port_mutex.release()

        ArakoonInstaller._logger.debug('Extending cluster {0} from {1} to {2} completed'.format(cluster_name, master_ip, new_ip))
        return {'client_port': ports[0],
                'messaging_port': ports[1],
                'ips': [node.ip for node in config.nodes]}

    @staticmethod
    def shrink_cluster(deleted_node_ip, remaining_node_ip, cluster_name, offline_nodes=None, filesystem=False):
        """
        Removes a node from a cluster, the old node will become a slave
        :param deleted_node_ip: The ip of the node that should be deleted
        :type deleted_node_ip: str
        :param remaining_node_ip: The ip of one of the remaining nodes
        :type remaining_node_ip: str
        :param cluster_name: The name of the cluster to shrink
        :type cluster_name: str
        :param offline_nodes: Storage Routers which are offline
        :type offline_nodes: list
        :param filesystem: Indicates whether the configuration should be on the filesystem or in a configuration cluster
        :type filesystem: bool
        :return: None
        """
        ArakoonInstaller._logger.debug('Shrinking cluster {0} from {1}'.format(cluster_name, deleted_node_ip))
        config = ArakoonClusterConfig(cluster_name, filesystem)
        config.load_config(remaining_node_ip)

        if offline_nodes is None:
            offline_nodes = []

        for node in config.nodes[:]:
            if node.ip == deleted_node_ip:
                config.nodes.remove(node)
                if node.ip not in offline_nodes:
                    ArakoonInstaller._destroy_node(config, node)
                    if filesystem is True:
                        config.delete_config(node.ip)
        ArakoonInstaller._deploy(config, filesystem=filesystem, offline_nodes=offline_nodes)
        restart_ips = [node.ip for node in config.nodes if node.ip != deleted_node_ip and node.ip not in offline_nodes]

        ArakoonInstaller._logger.debug('Shrinking cluster {0} from {1} completed'.format(cluster_name, deleted_node_ip))
        return restart_ips

    @staticmethod
    def deploy_cluster(cluster_name, node_ip, filesystem=False):
        """
        (Re)deploys a given cluster
        :param cluster_name: Name of the cluster to (re)deploy
        :type cluster_name: str
        :param node_ip: IP address of one of the cluster's nodes
        :type node_ip: str
        :param filesystem: Indicates whether the configuration should be on the filesystem or in a configuration cluster
        :type filesystem: bool
        :return: None
        """
        ArakoonInstaller._logger.debug('(Re)deploying cluster {0} from {1}'.format(cluster_name, node_ip))
        config = ArakoonClusterConfig(cluster_name, filesystem)
        config.load_config(node_ip)
        ArakoonInstaller._deploy(config, filesystem=filesystem)

    @staticmethod
    def get_unused_arakoon_metadata_and_claim(cluster_type, locked=True):
        """
        Retrieve arakoon cluster information based on its type
        :param cluster_type: Type of arakoon cluster (See ServiceType.ARAKOON_CLUSTER_TYPES)
        :type cluster_type: str
        :param locked: Execute this in a locked context
        :type locked: bool
        :return: Metadata of the arakoon cluster
        :rtype: dict
        """
        cluster_type = cluster_type.upper()
        if cluster_type not in ServiceType.ARAKOON_CLUSTER_TYPES:
            raise ValueError('Unsupported arakoon cluster type provided. Please choose from {0}'.format(', '.join(ServiceType.ARAKOON_CLUSTER_TYPES)))
        if not Configuration.dir_exists(ArakoonInstaller.CONFIG_ROOT):
            return None

        mutex = volatile_mutex('claim_arakoon_metadata', wait=10)
        try:
            if locked is True:
                mutex.acquire()

            for cluster_name in Configuration.list(ArakoonInstaller.CONFIG_ROOT):
                config = ArakoonClusterConfig(cluster_id=cluster_name, filesystem=False)
                config.load_config()
                arakoon_client = ArakoonInstaller.build_client(config)
                if arakoon_client.exists(ArakoonInstaller.METADATA_KEY):
                    metadata = json.loads(arakoon_client.get(ArakoonInstaller.METADATA_KEY))
                    if metadata['cluster_type'] == cluster_type and metadata['in_use'] is False and metadata['internal'] is False:
                        metadata['in_use'] = True
                        arakoon_client.set(ArakoonInstaller.METADATA_KEY, json.dumps(metadata, indent=4))
                        return metadata
        finally:
            if locked is True:
                mutex.release()

    @staticmethod
    def get_arakoon_metadata_by_cluster_name(cluster_name, filesystem=False, ip=None):
        """
        Retrieve arakoon cluster information based on its name
        :param cluster_name: Name of the arakoon cluster
        :type cluster_name: str
        :param filesystem: Indicates whether the configuration should be on the filesystem or in a configuration cluster
        :type filesystem: bool
        :param ip: The ip address of one of the nodes containing the configuration file, if on filesystem
        :type ip: str
        :return: Arakoon cluster metadata information
        :rtype: dict
        """
        config = ArakoonClusterConfig(cluster_name, filesystem)
        config.load_config(ip)
        arakoon_client = ArakoonInstaller.build_client(config)
        return json.loads(arakoon_client.get(ArakoonInstaller.METADATA_KEY))

    @staticmethod
    def _get_free_ports(client):
        node_name = System.get_my_machine_id(client)
        clusters = []
        exclude_ports = []
        if Configuration.dir_exists(ArakoonInstaller.CONFIG_ROOT):
            for cluster_name in Configuration.list(ArakoonInstaller.CONFIG_ROOT):
                config = ArakoonClusterConfig(cluster_name, False)
                config.load_config()
                for node in config.nodes:
                    if node.name == node_name:
                        clusters.append(cluster_name)
                        exclude_ports.append(node.client_port)
                        exclude_ports.append(node.messaging_port)

        ports = System.get_free_ports(Configuration.get('/ovs/framework/hosts/{0}/ports|arakoon'.format(node_name)), exclude_ports, 2, client)
        ArakoonInstaller._logger.debug('  Loaded free ports {0} based on existing clusters {1}'.format(ports, clusters))
        return ports

    @staticmethod
    def _destroy_node(config, node):
        """
        Cleans up a single node (remove services, directories and configuration files)
        """
        ArakoonInstaller._logger.debug('Destroy node {0} in cluster {1}'.format(node.ip, config.cluster_id))

        # Removes services for a cluster on a given node
        root_client = SSHClient(node.ip, username='root')
        ArakoonInstaller.stop(config.cluster_id, client=root_client)
        ArakoonInstaller.remove(config.cluster_id, client=root_client)

        # Cleans all directories on a given node
        abs_paths = {node.tlog_dir, node.home}  # That's a set
        if node.log_sinks.startswith('/'):
            abs_paths.add(os.path.dirname(os.path.abspath(node.log_sinks)))
        if node.crash_log_sinks.startswith('/'):
            abs_paths.add(os.path.dirname(os.path.abspath(node.crash_log_sinks)))
        root_client.dir_delete(list(abs_paths))
        ArakoonInstaller._logger.debug('Destroy node {0} in cluster {1} completed'.format(node.ip, config.cluster_id))

    @staticmethod
    def _deploy(config, filesystem, offline_nodes=None):
        """
        Deploys a complete cluster: Distributing the configuration files, creating directories and services
        """
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            if filesystem is True:
                raise NotImplementedError('At this moment, there is no support for unittesting filesystem backend Arakoon clusters')

        ArakoonInstaller._logger.debug('Deploying cluster {0}'.format(config.cluster_id))
        if offline_nodes is None:
            offline_nodes = []
        for node in config.nodes:
            if node.ip in offline_nodes:
                continue
            ArakoonInstaller._logger.debug('  Deploying cluster {0} on {1}'.format(config.cluster_id, node.ip))
            root_client = SSHClient(node.ip, username='root')

            # Distributes a configuration file to all its nodes
            config.write_config(node.ip)

            # Create dirs as root because mountpoint /mnt/cache1 is typically owned by root
            abs_paths = {node.tlog_dir, node.home}  # That's a set
            if node.log_sinks.startswith('/'):
                abs_paths.add(os.path.dirname(os.path.abspath(node.log_sinks)))
            if node.crash_log_sinks.startswith('/'):
                abs_paths.add(os.path.dirname(os.path.abspath(node.crash_log_sinks)))
            abs_paths = list(abs_paths)
            root_client.dir_create(abs_paths)
            root_client.dir_chmod(abs_paths, 0755, recursive=True)
            root_client.dir_chown(abs_paths, 'ovs', 'ovs', recursive=True)

            # Creates services for/on all nodes in the config
            if config.filesystem is True:
                config_path = config.config_path
            else:
                config_path = Configuration.get_configuration_path(config.config_path)
            base_name = 'ovs-arakoon'
            target_name = 'ovs-arakoon-{0}'.format(config.cluster_id)
            ServiceManager.add_service(base_name, root_client,
                                       params={'CLUSTER': config.cluster_id,
                                               'NODE_ID': node.name,
                                               'CONFIG_PATH': config_path,
                                               'STARTUP_DEPENDENCY': 'started ovs-watcher-config' if filesystem is False else '(local-filesystems and started networking)'},
                                       target_name=target_name)
            ArakoonInstaller._logger.debug('  Deploying cluster {0} on {1} completed'.format(config.cluster_id, node.ip))

    @staticmethod
    def start(cluster_name, client):
        """
        Starts an arakoon cluster
        :param cluster_name: The name of the cluster service to start
        :type cluster_name: str

        :param client: Client on which to start the service
        :type client: SSHClient

        :return: None
        """
        if ServiceManager.has_service('arakoon-{0}'.format(cluster_name), client=client) is True:
            ServiceManager.start_service('arakoon-{0}'.format(cluster_name), client=client)

    @staticmethod
    def stop(cluster_name, client):
        """
        Stops an arakoon service
        :param cluster_name: The name of the cluster service to stop
        :type cluster_name: str

        :param client: Client on which to stop the service
        :type client: SSHClient

        :return: None
        """
        if ServiceManager.has_service('arakoon-{0}'.format(cluster_name), client=client) is True:
            ServiceManager.stop_service('arakoon-{0}'.format(cluster_name), client=client)

    @staticmethod
    def is_running(cluster_name, client):
        """
        Checks if arakoon service is running
        :param cluster_name: The name of the cluster service to check
        :type cluster_name: str

        :param client: Client on which to check the service
        :type client: SSHClient

        :return: None
        """
        if ServiceManager.has_service('arakoon-{0}'.format(cluster_name), client=client):
            return ServiceManager.get_service_status('arakoon-{0}'.format(cluster_name), client=client)[0]
        return False

    @staticmethod
    def remove(cluster_name, client):
        """
        Removes an arakoon service
        :param cluster_name: The name of the cluster service to remove
        :type cluster_name: str

        :param client: Client on which to remove the service
        :type client: SSHClient

        :return: None
        """
        if ServiceManager.has_service('arakoon-{0}'.format(cluster_name), client=client) is True:
            ServiceManager.remove_service('arakoon-{0}'.format(cluster_name), client=client)

    @staticmethod
    def wait_for_cluster(cluster_name, node_ip, filesystem=False):
        """
        Waits for an Arakoon cluster to be available (by sending a nop)
        :param cluster_name: Name of the cluster to wait on
        :type cluster_name: str
        :param node_ip: IP address of one of the cluster's nodes
        :type node_ip: str
        :param filesystem: Indicates whether the configuration should be on the filesystem or in a configuration cluster
        :type filesystem: bool
        :return: True
        :rtype: Boolean
        """
        ArakoonInstaller._logger.debug('Waiting for cluster {0}'.format(cluster_name))
        config = ArakoonClusterConfig(cluster_name, filesystem)
        config.load_config(node_ip)
        arakoon_client = ArakoonInstaller.build_client(config)
        arakoon_client.nop()
        ArakoonInstaller._logger.debug('Waiting for cluster {0}: available'.format(cluster_name))
        return True

    @staticmethod
    def start_cluster(cluster_name, master_ip, filesystem):
        """
        Execute a start sequence (only makes sense for a fresh cluster)
        :param cluster_name: Name of the cluster to start
        :type cluster_name: str
        :param master_ip: IP of one of the cluster nodes
        :type master_ip: str
        :param filesystem: Indicates whether the configuration should be on the filesystem or in a configuration cluster
        :type filesystem: bool
        :return: None
        """
        config = ArakoonClusterConfig(cluster_name, filesystem)
        config.load_config(master_ip)
        root_clients = [SSHClient(node.ip, username='root') for node in config.nodes]
        for client in root_clients:
            ArakoonInstaller.start(cluster_name, client)
        ArakoonInstaller.wait_for_cluster(cluster_name, master_ip, filesystem)
        arakoon_client = ArakoonInstaller.build_client(config)
        arakoon_client.set(ArakoonInstaller.INTERNAL_CONFIG_KEY, config.export_ini())

    @staticmethod
    def restart_cluster(cluster_name, master_ip, filesystem):
        """
        Execute a restart sequence (Executed after arakoon and/or alba package upgrade)
        :param cluster_name: Name of the cluster to restart
        :type cluster_name: str
        :param master_ip: IP of one of the cluster nodes
        :type master_ip: str
        :param filesystem: Indicates whether the configuration should be on the filesystem or in a configuration cluster
        :type filesystem: bool
        :return: None
        """
        ArakoonInstaller._logger.debug('Restart sequence for {0} via {1}'.format(cluster_name, master_ip))

        config = ArakoonClusterConfig(cluster_name, filesystem)
        config.load_config(master_ip)
        arakoon_client = ArakoonInstaller.build_client(config)

        root_client = SSHClient(master_ip, username='root')
        all_clients = [SSHClient(node.ip, username='root') for node in config.nodes]

        if len(config.nodes) <= 2:
            ArakoonInstaller._logger.debug('  Insufficient nodes in cluster {0}. Full restart'.format(cluster_name))
            for function in [ArakoonInstaller.stop, ArakoonInstaller.start]:
                for client in all_clients:
                    function(cluster_name, client)
            ArakoonInstaller.wait_for_cluster(cluster_name, master_ip, filesystem)
        else:
            ArakoonInstaller._logger.debug('  Sufficient nodes in cluster {0}. Sequential restart'.format(cluster_name))
            for client in all_clients:
                ArakoonInstaller.stop(cluster_name, client)
                ArakoonInstaller.start(cluster_name, client)
                ArakoonInstaller._logger.debug('  Restarted node {0} on cluster {1}'.format(client.ip, cluster_name))
                ArakoonInstaller.wait_for_cluster(cluster_name, master_ip, filesystem)
        ArakoonInstaller.start(cluster_name, root_client)
        ArakoonInstaller.wait_for_cluster(cluster_name, master_ip, filesystem)
        arakoon_client.set(ArakoonInstaller.INTERNAL_CONFIG_KEY, config.export_ini())
        ArakoonInstaller._logger.debug('Restart sequence for {0} via {1} completed'.format(cluster_name, master_ip))

    @staticmethod
    def restart_cluster_add(cluster_name, current_ips, new_ip, filesystem):
        """
        Execute a (re)start sequence after adding a new node to a cluster.
        :param cluster_name: Name of the cluster to restart
        :type cluster_name: str
        :param current_ips: IPs of the previous nodes
        :type current_ips: list
        :param new_ip: IP of the newly added node
        :type new_ip: str
        :param filesystem: Indicates whether the configuration should be on the filesystem or in a configuration cluster
        :type filesystem: bool
        :return: None
        """
        ArakoonInstaller._logger.debug('Restart sequence (add) for {0}'.format(cluster_name))
        ArakoonInstaller._logger.debug('Current ips: {0}'.format(', '.join(current_ips)))
        ArakoonInstaller._logger.debug('New ip: {0}'.format(new_ip))

        client = SSHClient(new_ip, username=ArakoonInstaller.SSHCLIENT_USER)
        if ArakoonInstaller.is_running(cluster_name, client):
            ArakoonInstaller._logger.info('Arakoon service for {0} is already running'.format(cluster_name))
            return
        config = ArakoonClusterConfig(cluster_name, filesystem)
        config.load_config(new_ip)
        arakoon_client = ArakoonInstaller.build_client(config)

        if len(config.nodes) > 1:
            ArakoonInstaller._logger.debug('Catching up new node {0} for cluster {1}'.format(new_ip, cluster_name))
            node_name = [node.name for node in config.nodes if node.ip == new_ip][0]
            if filesystem is True:
                config_path = config.config_path
            else:
                config_path = Configuration.get_configuration_path(config.config_path)
            client.run(['arakoon', '--node', node_name, '-config', config_path, '-catchup-only'])
            ArakoonInstaller._logger.debug('Catching up new node {0} for cluster {1} completed'.format(new_ip, cluster_name))

        threshold = 2 if new_ip in current_ips else 1
        for ip in current_ips:
            if ip == new_ip:
                continue
            current_client = SSHClient(ip, username='root')
            ArakoonInstaller.stop(cluster_name, client=current_client)
            ArakoonInstaller.start(cluster_name, client=current_client)
            ArakoonInstaller._logger.debug('  Restarted node {0} for cluster {1}'.format(current_client.ip, cluster_name))
            if len(current_ips) > threshold:  # A two node cluster needs all nodes running
                ArakoonInstaller.wait_for_cluster(cluster_name, ip, filesystem)
        client = SSHClient(new_ip, username='root')
        ArakoonInstaller.start(cluster_name, client=client)
        ArakoonInstaller.wait_for_cluster(cluster_name, new_ip, filesystem)
        arakoon_client.set(ArakoonInstaller.INTERNAL_CONFIG_KEY, config.export_ini())
        ArakoonInstaller._logger.debug('Started node {0} for cluster {1}'.format(new_ip, cluster_name))

    @staticmethod
    def restart_cluster_remove(cluster_name, remaining_ips, filesystem):
        """
        Execute a restart sequence after removing a node from a cluster
        :param cluster_name: Name of the cluster to restart
        :type cluster_name: str
        :param remaining_ips: IPs of the remaining nodes after shrink
        :type remaining_ips: list
        :param filesystem: Indicates whether the configuration should be on the filesystem or in a configuration cluster
        :type filesystem: bool
        :return: None
        """
        ArakoonInstaller._logger.debug('Restart sequence (remove) for {0}'.format(cluster_name))
        ArakoonInstaller._logger.debug('Remaining ips: {0}'.format(', '.join(remaining_ips)))
        for ip in remaining_ips:
            client = SSHClient(ip, username='root')
            ArakoonInstaller.stop(cluster_name, client=client)
            ArakoonInstaller.start(cluster_name, client=client)
            ArakoonInstaller._logger.debug('  Restarted node {0} for cluster {1}'.format(client.ip, cluster_name))
            if len(remaining_ips) > 2:  # A two node cluster needs all nodes running
                ArakoonInstaller.wait_for_cluster(cluster_name, remaining_ips[0], filesystem)
        ArakoonInstaller.wait_for_cluster(cluster_name, remaining_ips[0], filesystem)
        config = ArakoonClusterConfig(cluster_name, filesystem)
        config.load_config(remaining_ips[0])
        arakoon_client = ArakoonInstaller.build_client(config)
        arakoon_client.set(ArakoonInstaller.INTERNAL_CONFIG_KEY, config.export_ini())
        ArakoonInstaller._logger.debug('Restart sequence (remove) for {0} completed'.format(cluster_name))

    @staticmethod
    def claim_cluster(cluster_name, master_ip, filesystem, metadata=None):
        """
        Claims the cluster
        :param cluster_name: Name of the cluster to restart
        :type cluster_name: str
        :param master_ip: IP of one of the cluster nodes
        :type master_ip: str
        :param filesystem: Indicates whether the configuration should be on the filesystem or in a configuration cluster
        :type filesystem: bool
        :param metadata: Metadata if not yet in the cluster
        :type metadata: dict
        """
        config = ArakoonClusterConfig(cluster_name, filesystem)
        config.load_config(master_ip)
        arakoon_client = ArakoonInstaller.build_client(config)
        if metadata is None:
            metadata = json.loads(arakoon_client.get(ArakoonInstaller.METADATA_KEY))
        metadata['in_use'] = True
        arakoon_client.set(ArakoonInstaller.METADATA_KEY, json.dumps(metadata, indent=4))
        arakoon_client.set(ArakoonInstaller.INTERNAL_CONFIG_KEY, config.export_ini())

    @staticmethod
    def unclaim_cluster(cluster_name, master_ip, filesystem, metadata=None):
        """
        Un-claims the cluster
        :param cluster_name: Name of the cluster to restart
        :type cluster_name: str
        :param master_ip: IP of one of the cluster nodes
        :type master_ip: str
        :param filesystem: Indicates whether the configuration should be on the filesystem or in a configuration cluster
        :type filesystem: bool
        :param metadata: Metadata if not yet in the cluster
        :type metadata: dict
        """
        config = ArakoonClusterConfig(cluster_name, filesystem)
        config.load_config(master_ip)
        arakoon_client = ArakoonInstaller.build_client(config=config)
        if metadata is None:
            metadata = json.loads(arakoon_client.get(ArakoonInstaller.METADATA_KEY))
        metadata['in_use'] = False
        arakoon_client.set(ArakoonInstaller.METADATA_KEY, json.dumps(metadata, indent=4))

    @staticmethod
    def build_client(config):
        """
        Build the ArakoonClient object with all configured nodes in the cluster
        :param config: Configuration on which to base the client
        :type config: ArakoonClientConfig
        :return: The newly generated PyrakoonClient
        :rtype: PyrakoonClient
        """
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            from ovs.extensions.db.arakoon.tests.client import MockPyrakoonClient
            return MockPyrakoonClient(config.cluster_id, None)

        from ovs.extensions.db.arakoon.pyrakoon.client import PyrakoonClient
        nodes = {}
        for node in config.nodes:
            nodes[node.name] = ([node.ip], node.client_port)
        return PyrakoonClient(config.cluster_id, nodes)
