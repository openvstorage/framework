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
    CONFIG_ROOT = '/ovs/arakoon'
    CONFIG_KEY = CONFIG_ROOT + '/{0}/config'
    CONFIG_FILE = '/opt/OpenvStorage/config/arakoon_{0}.ini'

    def __init__(self, cluster_id, load_config=True, source_ip=None, plugins=None):
        """
        Initializes an empty Cluster Config
        """
        self._plugins = []
        self._extra_globals = {'tlog_max_entries': 5000}
        if isinstance(plugins, list):
            self._plugins = plugins
        elif isinstance(plugins, basestring):
            self._plugins.append(plugins)

        self.nodes = []
        self.source_ip = source_ip
        self.cluster_id = cluster_id
        if self.source_ip is None:
            self.internal_config_path = ArakoonClusterConfig.CONFIG_KEY.format(cluster_id)
            self.external_config_path = Configuration.get_configuration_path(self.internal_config_path)
        else:
            self.internal_config_path = ArakoonClusterConfig.CONFIG_FILE.format(cluster_id)
            self.external_config_path = self.internal_config_path

        if load_config is True:
            if self.source_ip is None:
                contents = Configuration.get(self.internal_config_path, raw=True)
            else:
                client = self.load_client(self.source_ip)
                contents = client.file_read(self.internal_config_path)
            self.read_config(contents)

    def load_client(self, ip):
        """
        Create an SSHClient instance to the IP provided
        :param ip: IP for the SSHClient
        :type ip: str
        :return: An SSHClient instance
        :rtype: ovs.extensions.generic.sshclient.SSHClient
        """
        if self.source_ip is not None:
            if ip is None:
                raise RuntimeError('An IP should be passed for filesystem configuration')
            return SSHClient(ip, username=ArakoonInstaller.SSHCLIENT_USER)

    def read_config(self, contents):
        """
        Constructs a configuration object from config contents
        :param contents: Raw .ini contents
        :type contents: str
        :return: None
        :rtype: NoneType
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
        :return: Data available in the Arakoon configuration
        :rtype: dict
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
        :return: Arakoon configuration in string format
        :rtype: str
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
        if self.source_ip is None:
            Configuration.set(self.internal_config_path, contents, raw=True)
        else:
            client = self.load_client(ip)
            client.file_write(self.internal_config_path, contents)

    def delete_config(self, ip=None):
        """
        Deletes a configuration file
        :return: None
        :rtype: NoneType
        """
        if self.source_ip is None:
            key = self.internal_config_path
            if Configuration.exists(key, raw=True):
                Configuration.delete(key, raw=True)
        else:
            client = self.load_client(ip)
            client.file_delete(self.internal_config_path)

    @staticmethod
    def get_cluster_name(internal_name):
        """
        Retrieve the name of the cluster
        :param internal_name: Name as known by the framework
        :type internal_name: str
        :return: Name known by user
        :rtype: str
        """
        config_key = '/ovs/framework/arakoon_clusters'
        if Configuration.exists(config_key):
            cluster_info = Configuration.get(config_key)
            if internal_name in cluster_info:
                return cluster_info[internal_name]
        if internal_name not in ['ovsdb', 'voldrv']:
            return internal_name


class ArakoonInstaller(object):
    """
    Class to dynamically install/(re)configure Arakoon cluster
    """
    ARAKOON_HOME_DIR = '{0}/arakoon/{1}/db'
    ARAKOON_TLOG_DIR = '{0}/arakoon/{1}/tlogs'
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
        Delete existing Arakoon data
        :param ip: IP on which to check for existing data
        :type ip: str
        :param directories: Directories to delete
        :type directories: list
        :return: None
        :rtype: NoneType
        """
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            return

        root_client = SSHClient(ip, username='root')

        # Verify whether all files to be archived have been released properly
        open_file_errors = []
        ArakoonInstaller._logger.debug('Cleanup old Arakoon - Checking open files')
        dirs_with_files = {}
        for directory in directories:
            ArakoonInstaller._logger.debug('Cleaning old Arakoon - Checking directory {0}'.format(directory))
            if root_client.dir_exists(directory):
                ArakoonInstaller._logger.debug('Cleaning old Arakoon - Directory {0} exists'.format(directory))
                file_names = root_client.file_list(directory, abs_path=True, recursive=True)
                if len(file_names) > 0:
                    ArakoonInstaller._logger.debug('Cleaning old Arakoon - Files found in directory {0}'.format(directory))
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
            ArakoonInstaller._logger.debug('Cleanup old Arakoon - Removing old files from {0}'.format(directory))
            root_client.file_delete(info)

    @staticmethod
    def create_cluster(cluster_name, cluster_type, ip, base_dir, plugins=None, locked=True, internal=True, port_range=None):
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
        :type plugins: dict
        :param locked: Indicates whether the create should run in a locked context (e.g. to prevent port conflicts)
        :type locked: bool
        :param internal: Is cluster internally managed by OVS
        :type internal: bool
        :param port_range: Range of ports which should be used for the Arakoon processes (2 available ports in the range will be selected) eg: [26400, 26499]
        :type port_range: list
        :return: Ports used by the cluster, metadata of the cluster and metadata of the service
        :rtype: dict
        """
        if cluster_type not in ServiceType.ARAKOON_CLUSTER_TYPES:
            raise ValueError('Cluster type {0} is not supported. Please choose from {1}'.format(cluster_type, ', '.join(sorted(ServiceType.ARAKOON_CLUSTER_TYPES))))
        if plugins is not None and not isinstance(plugins, dict):
            raise ValueError('Plugins should be a dict')

        client = SSHClient(endpoint=ip, username=ArakoonInstaller.SSHCLIENT_USER)
        filesystem = cluster_type == ServiceType.ARAKOON_CLUSTER_TYPES.CFG
        if filesystem is True:
            exists = client.file_exists(ArakoonClusterConfig.CONFIG_FILE.format(cluster_name))
        else:
            exists = Configuration.dir_exists('/ovs/arakoon/{0}'.format(cluster_name))
        if exists is True:
            raise ValueError('An Arakoon cluster with name "{0}" already exists'.format(cluster_name))

        ArakoonInstaller._logger.debug('Creating cluster {0} of type {1} on {2}'.format(cluster_name, cluster_type, ip))

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

            if filesystem is True:
                if port_range is None:
                    port_range = [26400]
                ports = System.get_free_ports(selected_range=port_range, nr=2, client=client)
            else:
                ports = ArakoonInstaller._get_free_ports(client=client, port_range=port_range)
            config = ArakoonClusterConfig(cluster_id=cluster_name,
                                          source_ip=ip if filesystem is True else None,
                                          load_config=False,
                                          plugins=plugins.keys() if plugins is not None else None)
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
                        'cluster_type': cluster_type,
                        'in_use': False}
            service_metadata = ArakoonInstaller._deploy(config=config,
                                                        plugins=plugins.values() if plugins is not None else None,
                                                        delay_service_registration=filesystem)[ip]
        finally:
            if port_mutex is not None:
                port_mutex.release()

        ArakoonInstaller._logger.debug('Creating cluster {0} of type {1} on {2} completed'.format(cluster_name, cluster_type, ip))
        return {'ports': [ports[0], ports[1]],  # Client port, messaging port
                'metadata': metadata,
                'service_metadata': service_metadata}

    @staticmethod
    def delete_cluster(cluster_name, ip=None):
        """
        Deletes a complete cluster
        :param cluster_name: Name of the cluster to remove
        :type cluster_name: str
        :param ip: IP of one of the already existing nodes (Only required for filesystem Arakoons)
        :type ip: str
        :return: None
        :rtype: NoneType
        """
        cluster_type = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name, ip=ip)['cluster_type']
        filesystem, ip = ArakoonInstaller._is_filesystem_cluster(cluster_type=cluster_type, ip=ip)

        ArakoonInstaller._logger.debug('Deleting cluster {0} of type {1}'.format(cluster_name, cluster_type))
        config = ArakoonClusterConfig(cluster_id=cluster_name, source_ip=ip)
        service_name = ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name)
        for node in config.nodes:
            try:
                ServiceManager.unregister_service(service_name=service_name, node_name=node.name)
            except:
                ArakoonInstaller._logger.exception('Un-registering service {0} on {1} failed'.format(service_name, node.ip))

        # Cleans up a complete cluster (remove services, directories and configuration files)
        for node in config.nodes:
            ArakoonInstaller._destroy_node(cluster_name=cluster_name,
                                           node=node,
                                           delay_unregistration=filesystem)
            config.delete_config(ip=ip)
        ArakoonInstaller._logger.debug('Deleting cluster {0} of type {1} completed'.format(cluster_name, cluster_type))

    @staticmethod
    def extend_cluster(cluster_name, new_ip, base_dir, plugins=None, locked=True, ip=None, port_range=None):
        """
        Extends a cluster to a given new node
        :param cluster_name: Name of the cluster to be extended
        :type cluster_name: str
        :param new_ip: IP address of the node to be added
        :type new_ip: str
        :param base_dir: Base directory that should contain the data and tlogs
        :type base_dir: str
        :param plugins: Plugins that should be added to the configuration file
        :type plugins: dict
        :param locked: Indicates whether the extend should run in a locked context (e.g. to prevent port conflicts)
        :type locked: bool
        :param ip: IP of one of the already existing nodes (Only required for filesystem Arakoons)
        :type ip: str
        :param port_range: Range of ports which should be used for the Arakoon processes (2 available ports in the range will be selected) eg: [26400, 26499]
        :type port_range: list
        :return: Ports used by the cluster, IPs on which the cluster is extended and metadata for the service
        :rtype: dict
        """
        if plugins is not None and not isinstance(plugins, dict):
            raise ValueError('Plugins should be a dict')

        cluster_type = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name, ip=ip)['cluster_type']
        filesystem, ip = ArakoonInstaller._is_filesystem_cluster(cluster_type=cluster_type, ip=ip)

        ArakoonInstaller._logger.debug('Extending cluster {0} of type {1} to {2}'.format(cluster_name, cluster_type, new_ip))
        config = ArakoonClusterConfig(cluster_id=cluster_name,
                                      source_ip=ip,
                                      plugins=plugins.keys() if plugins is not None else None)
        client = SSHClient(endpoint=new_ip, username=ArakoonInstaller.SSHCLIENT_USER)
        base_dir = base_dir.rstrip('/')
        home_dir = ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, cluster_name)
        tlog_dir = ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, cluster_name)
        node_name = System.get_my_machine_id(client=client)
        ArakoonInstaller.clean_leftover_arakoon_data(ip=new_ip, directories=[home_dir, tlog_dir])

        port_mutex = None
        try:
            if locked is True:
                from ovs.extensions.generic.volatilemutex import volatile_mutex
                port_mutex = volatile_mutex('arakoon_install_ports_{0}'.format(new_ip))
                port_mutex.acquire(wait=60)

            if filesystem is True:
                if port_range is None:
                    port_range = [26400]
                ports = System.get_free_ports(selected_range=port_range, nr=2, client=client)
            else:
                ports = ArakoonInstaller._get_free_ports(client=client, port_range=port_range)

            if node_name not in [node.name for node in config.nodes]:
                config.nodes.append(ArakoonNodeConfig(name=node_name,
                                                      ip=new_ip,
                                                      client_port=ports[0],
                                                      messaging_port=ports[1],
                                                      log_sinks=LogHandler.get_sink_path('arakoon_server'),
                                                      crash_log_sinks=LogHandler.get_sink_path('arakoon_server_crash'),
                                                      home=home_dir,
                                                      tlog_dir=tlog_dir))
            service_metadata = ArakoonInstaller._deploy(config=config,
                                                        plugins=plugins.values() if plugins is not None else None,
                                                        delay_service_registration=filesystem)[new_ip]
        finally:
            if port_mutex is not None:
                port_mutex.release()

        ArakoonInstaller._logger.debug('Extending cluster {0} of type {1} to {2} completed'.format(cluster_name, cluster_type, new_ip))
        return {'ips': [node.ip for node in config.nodes],
                'ports': [ports[0], ports[1]],  # Client port, messaging port
                'service_metadata': service_metadata}

    @staticmethod
    def shrink_cluster(cluster_name, ip, remaining_ip=None, offline_nodes=None):
        """
        Removes a node from a cluster, the old node will become a slave
        :param cluster_name: The name of the cluster to shrink
        :type cluster_name: str
        :param ip: The IP of the node that should be removed from the cluster
        :type ip: str
        :param remaining_ip: An IP of 1 of the remaining nodes in the cluster (Only required for filesystem Arakoons)
        :type remaining_ip: str
        :param offline_nodes: Storage Routers which are offline
        :type offline_nodes: list
        :return: IPs of the remaining nodes in the cluster
        :rtype: list
        """
        if offline_nodes is None:
            offline_nodes = []

        cluster_type = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name, ip=remaining_ip)['cluster_type']
        filesystem, remaining_ip = ArakoonInstaller._is_filesystem_cluster(cluster_type=cluster_type, ip=remaining_ip)
        if remaining_ip is not None and remaining_ip in offline_nodes:
            raise ValueError('The specified remaining IP must be the IP of an online node')

        # Shrink the cluster
        ArakoonInstaller._logger.debug('Shrinking cluster {0} of type {1} from {2}'.format(cluster_name, cluster_type, ip))
        config = ArakoonClusterConfig(cluster_id=cluster_name, source_ip=remaining_ip)
        restart_ips = []
        removal_node = None
        for node in config.nodes[:]:
            if node.ip == ip:
                config.nodes.remove(node)
                removal_node = node
                if node.ip not in offline_nodes:
                    ArakoonInstaller._destroy_node(cluster_name=cluster_name,
                                                   node=node,
                                                   delay_unregistration=filesystem)
                    if filesystem is True:
                        config.delete_config(node.ip)
            elif node.ip not in offline_nodes:
                restart_ips.append(node.ip)

        ArakoonInstaller._deploy(config=config,
                                 offline_nodes=offline_nodes,
                                 delay_service_registration=filesystem)
        ArakoonInstaller._logger.debug('Shrinking cluster {0} of type {1} from {2} completed'.format(cluster_name, cluster_type, ip))

        # Recreate the config
        config = ArakoonClusterConfig(cluster_id=cluster_name, source_ip=remaining_ip)

        # Restart remaining nodes
        ArakoonInstaller._logger.debug('Restart sequence (remove) for {0}'.format(cluster_name))
        for restart_ip in restart_ips:
            ArakoonInstaller._logger.debug('  Restarting IP: {0}'.format(restart_ip))
            client = SSHClient(endpoint=restart_ip, username='root')
            ArakoonInstaller.stop(cluster_name=cluster_name, client=client)
            ArakoonInstaller.start(cluster_name=cluster_name, client=client)
            ArakoonInstaller._logger.debug('  Restarted node {0} for cluster {1}'.format(client.ip, cluster_name))
            if len(restart_ips) > 2:  # A two node cluster needs all nodes running
                ArakoonInstaller._wait_for_cluster(config=config)
        arakoon_client = ArakoonInstaller._wait_for_cluster(config=config)
        arakoon_client.set(ArakoonInstaller.INTERNAL_CONFIG_KEY, config.export_ini())

        if removal_node is not None:
            ServiceManager.unregister_service(node_name=removal_node.name,
                                              service_name=ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name))
        ArakoonInstaller._logger.debug('Restart sequence (remove) for {0} completed'.format(cluster_name))
        return restart_ips

    @staticmethod
    def get_unused_arakoon_metadata_and_claim(cluster_type, cluster_name=None):
        """
        Retrieve cluster information based on its type
        :param cluster_type: Type of the cluster (See ServiceType.ARAKOON_CLUSTER_TYPES)
        :type cluster_type: str
        :param cluster_name: Name of the cluster to claim
        :type cluster_name: str
        :return: Metadata of the cluster
        :rtype: dict
        """
        if cluster_type not in ServiceType.ARAKOON_CLUSTER_TYPES:
            raise ValueError('Unsupported Arakoon cluster type provided. Please choose from {0}'.format(', '.join(sorted(ServiceType.ARAKOON_CLUSTER_TYPES))))
        if not Configuration.dir_exists(ArakoonClusterConfig.CONFIG_ROOT):
            return None

        mutex = volatile_mutex('claim_arakoon_metadata', wait=10)
        locked = cluster_type not in [ServiceType.ARAKOON_CLUSTER_TYPES.CFG, ServiceType.ARAKOON_CLUSTER_TYPES.FWK]
        try:
            if locked is True:
                mutex.acquire()

            for cl_name in Configuration.list(ArakoonClusterConfig.CONFIG_ROOT):
                if cluster_name is not None and cl_name != cluster_name:
                    continue
                config = ArakoonClusterConfig(cluster_id=cl_name)
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
    def get_unused_arakoon_clusters(cluster_type):
        """
        Retrieve all unclaimed clusters of type <cluster_type>
        :param cluster_type: Type of the cluster (See ServiceType.ARAKOON_CLUSTER_TYPES w/o type CFG, since this is not available in the configuration management)
        :type cluster_type: str
        :return: All unclaimed clusters of specified type
        :rtype: list
        """
        clusters = []
        if not Configuration.dir_exists(ArakoonClusterConfig.CONFIG_ROOT):
            return clusters

        supported_types = ServiceType.ARAKOON_CLUSTER_TYPES.keys()
        supported_types.remove(ServiceType.ARAKOON_CLUSTER_TYPES.CFG)
        if cluster_type not in supported_types:
            raise ValueError('Unsupported Arakoon cluster type provided. Please choose from {0}'.format(', '.join(sorted(supported_types))))

        for cluster_name in Configuration.list(ArakoonClusterConfig.CONFIG_ROOT):
            metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)
            if metadata['cluster_type'] == cluster_type and metadata['in_use'] is False:
                clusters.append(metadata)
        return clusters

    @staticmethod
    def get_arakoon_metadata_by_cluster_name(cluster_name, ip=None):
        """
        Retrieve cluster information based on its name
        :param cluster_name: Name of the cluster
        :type cluster_name: str
        :param ip: The IP address of one of the nodes containing the configuration file (Only required for filesystem Arakoons)
        :type ip: str
        :return: Cluster metadata information
        :rtype: dict
        """
        config = ArakoonClusterConfig(cluster_id=cluster_name, source_ip=ip)
        arakoon_client = ArakoonInstaller.build_client(config)
        return json.loads(arakoon_client.get(ArakoonInstaller.METADATA_KEY))

    @staticmethod
    def _get_free_ports(client, port_range=None):
        node_name = System.get_my_machine_id(client)
        clusters = []
        exclude_ports = []
        if Configuration.dir_exists(ArakoonClusterConfig.CONFIG_ROOT):
            for cluster_name in Configuration.list(ArakoonClusterConfig.CONFIG_ROOT):
                config = ArakoonClusterConfig(cluster_id=cluster_name)
                for node in config.nodes:
                    if node.name == node_name:
                        clusters.append(cluster_name)
                        exclude_ports.append(node.client_port)
                        exclude_ports.append(node.messaging_port)

        if port_range is None:
            port_range = Configuration.get('/ovs/framework/hosts/{0}/ports|arakoon'.format(node_name))
        ports = System.get_free_ports(selected_range=port_range, exclude=exclude_ports, nr=2, client=client)
        ArakoonInstaller._logger.debug('  Loaded free ports {0} based on existing clusters {1}'.format(ports, clusters))
        return ports

    @staticmethod
    def _destroy_node(cluster_name, node, delay_unregistration=False):
        """
        Cleans up a single node (remove services, directories and configuration files)
        """
        ArakoonInstaller._logger.debug('Destroy node {0} in cluster {1}'.format(node.ip, cluster_name))

        # Removes services for a cluster on a given node
        root_client = SSHClient(node.ip, username='root')
        ArakoonInstaller.stop(cluster_name=cluster_name, client=root_client)
        ArakoonInstaller.remove(cluster_name=cluster_name, client=root_client, delay_unregistration=delay_unregistration)

        # Cleans all directories on a given node
        abs_paths = {node.tlog_dir, node.home}  # That's a set
        if node.log_sinks.startswith('/'):
            abs_paths.add(os.path.dirname(os.path.abspath(node.log_sinks)))
        if node.crash_log_sinks.startswith('/'):
            abs_paths.add(os.path.dirname(os.path.abspath(node.crash_log_sinks)))
        root_client.dir_delete(list(abs_paths))
        ArakoonInstaller._logger.debug('Destroy node {0} in cluster {1} completed'.format(node.ip, cluster_name))

    @staticmethod
    def _deploy(config, offline_nodes=None, plugins=None, delay_service_registration=False):
        """
        Deploys a complete cluster: Distributing the configuration files, creating directories and services
        """
        ArakoonInstaller._logger.debug('Deploying cluster {0}'.format(config.cluster_id))
        if offline_nodes is None:
            offline_nodes = []

        service_metadata = {}
        for node in config.nodes:
            if node.ip in offline_nodes:
                continue
            ArakoonInstaller._logger.debug('  Deploying cluster {0} on {1}'.format(config.cluster_id, node.ip))
            root_client = SSHClient(node.ip, username='root')

            # Distributes a configuration file to all its nodes
            config.write_config(ip=node.ip)

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
            metadata = None
            if config.source_ip is None:
                configuration_key = ServiceManager.SERVICE_CONFIG_KEY.format(System.get_my_machine_id(root_client),
                                                                             ArakoonInstaller.get_service_name_for_cluster(cluster_name=config.cluster_id))
                # If the entry is stored in arakoon, it means the service file was previously made
                if Configuration.exists(configuration_key):
                    metadata = Configuration.get(configuration_key)
            if metadata is None:
                extra_version_cmd = ''
                if plugins is not None:
                    extra_version_cmd = ';'.join(plugins)
                    extra_version_cmd = extra_version_cmd.strip(';')
                metadata = ServiceManager.add_service(name='ovs-arakoon',
                                                      client=root_client,
                                                      params={'CLUSTER': config.cluster_id,
                                                              'NODE_ID': node.name,
                                                              'CONFIG_PATH': config.external_config_path,
                                                              'EXTRA_VERSION_CMD': extra_version_cmd},
                                                      target_name='ovs-arakoon-{0}'.format(config.cluster_id),
                                                      startup_dependency=('ovs-watcher-config' if config.source_ip is None else None),
                                                      delay_registration=delay_service_registration)
            service_metadata[node.ip] = metadata
            ArakoonInstaller._logger.debug('  Deploying cluster {0} on {1} completed'.format(config.cluster_id, node.ip))
        return service_metadata

    @staticmethod
    def start(cluster_name, client):
        """
        Starts a cluster service on the client provided
        :param cluster_name: The name of the cluster service to start
        :type cluster_name: str
        :param client: Client on which to start the service
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :return: None
        :rtype: NoneType
        """
        service_name = ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name)
        if ServiceManager.has_service(name=service_name, client=client) is True:
            ServiceManager.start_service(name=service_name, client=client)

    @staticmethod
    def stop(cluster_name, client):
        """
        Stops a cluster service on the client provided
        :param cluster_name: The name of the cluster service to stop
        :type cluster_name: str
        :param client: Client on which to stop the service
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :return: None
        :rtype: NoneType
        """
        service_name = ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name)
        if ServiceManager.has_service(name=service_name, client=client) is True:
            ServiceManager.stop_service(name=service_name, client=client)

    @staticmethod
    def is_running(cluster_name, client):
        """
        Checks if the cluster service is running on the client provided
        :param cluster_name: The name of the cluster service to check
        :type cluster_name: str
        :param client: Client on which to check the service
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :return: True if the cluster service is running, False otherwise
        :rtype: bool
        """
        service_name = ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name)
        if ServiceManager.has_service(name=service_name, client=client):
            return ServiceManager.get_service_status(name=service_name, client=client)[0]
        return False

    @staticmethod
    def remove(cluster_name, client, delay_unregistration=False):
        """
        Removes a cluster service from the client provided
        :param cluster_name: The name of the cluster service to remove
        :type cluster_name: str
        :param client: Client on which to remove the service
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :param delay_unregistration: Un-register the cluster service right away or not
        :type delay_unregistration: bool
        :return: None
        :rtype: NoneType
        """
        service_name = ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name)
        if ServiceManager.has_service(name=service_name, client=client) is True:
            ServiceManager.remove_service(name=service_name, client=client, delay_unregistration=delay_unregistration)

    @staticmethod
    def _wait_for_cluster(config):
        """
        Waits for an Arakoon cluster to be available (by sending a nop)
        """
        ArakoonInstaller._logger.debug('Waiting for cluster {0}'.format(config.cluster_id))
        arakoon_client = ArakoonInstaller.build_client(config)
        arakoon_client.nop()
        ArakoonInstaller._logger.debug('Waiting for cluster {0}: available'.format(config.cluster_id))
        return arakoon_client

    @staticmethod
    def start_cluster(metadata, ip=None):
        """
        Execute a start sequence (only makes sense for a fresh cluster)
        :param metadata: The metadata of the cluster
        :type metadata: dict
        :param ip: IP of one of the cluster nodes (Only required for filesystem Arakoons)
        :type ip: str
        :return: None
        :rtype: NoneType
        """
        cluster_name = metadata['cluster_name']
        cluster_type = metadata['cluster_type']
        _, ip = ArakoonInstaller._is_filesystem_cluster(cluster_type=cluster_type, ip=ip)
        config = ArakoonClusterConfig(cluster_id=cluster_name, source_ip=ip)
        root_clients = [SSHClient(endpoint=node.ip, username='root') for node in config.nodes]
        for client in root_clients:
            ArakoonInstaller.start(cluster_name=cluster_name, client=client)
        arakoon_client = ArakoonInstaller._wait_for_cluster(config=config)
        arakoon_client.set(ArakoonInstaller.INTERNAL_CONFIG_KEY, config.export_ini())

        metadata['in_use'] = True
        arakoon_client.set(ArakoonInstaller.METADATA_KEY, json.dumps(metadata, indent=4))

    @staticmethod
    def restart_node(metadata, client):
        """
        Execute a restart sequence for the cluster service running on the specified client
        This scenario is only supported when NO configuration changes have been applied
        and should have no impact on Arakoon performance if 1 node fails to restart due to backwards compatibility
        :param metadata: The metadata of the cluster
        :type metadata: dict
        :param client: Client on which to restart the cluster service
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :return: None
        :rtype: NoneType
        """
        cluster_name = metadata['cluster_name']
        cluster_type = metadata['cluster_type']
        ArakoonInstaller._logger.debug('Restarting node {0} for cluster {1}'.format(client.ip, cluster_name))
        config = ArakoonClusterConfig(cluster_id=cluster_name, source_ip=client.ip if cluster_type == ServiceType.ARAKOON_CLUSTER_TYPES.CFG else None)
        if len(config.nodes) > 0:
            ArakoonInstaller.stop(cluster_name=cluster_name, client=client)
            ArakoonInstaller.start(cluster_name=cluster_name, client=client)
            ArakoonInstaller._wait_for_cluster(config=config)
            ArakoonInstaller._logger.debug('Restarted node {0} on cluster {1}'.format(client.ip, cluster_name))

    @staticmethod
    def restart_cluster_add(cluster_name, current_ips, new_ip):
        """
        Execute a (re)start sequence after adding a new node to a cluster.
        :param cluster_name: Name of the cluster to restart
        :type cluster_name: str
        :param current_ips: IPs of the previous nodes
        :type current_ips: list
        :param new_ip: IP of the newly added node
        :type new_ip: str
        :return: None
        :rtype: NoneType
        """
        ArakoonInstaller._logger.debug('Restarting cluster {0} with current IPs: {1} and newly added IP {2}'.format(cluster_name, ', '.join(current_ips), new_ip))
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            filesystem = 'config' in cluster_name or 'cfg' in cluster_name
        else:
            filesystem = cluster_name == 'config'
        client = SSHClient(endpoint=new_ip, username=ArakoonInstaller.SSHCLIENT_USER)
        if ArakoonInstaller.is_running(cluster_name=cluster_name, client=client):
            ArakoonInstaller._logger.info('Arakoon service for {0} is already running'.format(cluster_name))
            return
        config = ArakoonClusterConfig(cluster_id=cluster_name, source_ip=new_ip if filesystem is True else None)
        arakoon_client = ArakoonInstaller.build_client(config)

        if len(config.nodes) > 1:
            ArakoonInstaller._logger.debug('Catching up new node {0} for cluster {1}'.format(new_ip, cluster_name))
            node_name = [node.name for node in config.nodes if node.ip == new_ip][0]
            client.run(['arakoon', '--node', node_name, '-config', config.external_config_path, '-catchup-only'])
            ArakoonInstaller._logger.debug('Catching up new node {0} for cluster {1} completed'.format(new_ip, cluster_name))

        # Restart current nodes in the cluster
        threshold = 2 if new_ip in current_ips else 1
        for ip in current_ips:
            if ip == new_ip:
                continue
            root_client = SSHClient(endpoint=ip, username='root')
            ArakoonInstaller.stop(cluster_name=cluster_name, client=root_client)
            ArakoonInstaller.start(cluster_name=cluster_name, client=root_client)
            ArakoonInstaller._logger.debug('  Restarted node {0} for cluster {1}'.format(root_client.ip, cluster_name))
            if len(current_ips) > threshold:  # A two node cluster needs all nodes running
                ArakoonInstaller._wait_for_cluster(config=config)

        # Start new node in the cluster
        client = SSHClient(endpoint=new_ip, username='root')
        ArakoonInstaller.start(cluster_name=cluster_name, client=client)
        ArakoonInstaller._wait_for_cluster(config=config)
        arakoon_client.set(ArakoonInstaller.INTERNAL_CONFIG_KEY, config.export_ini())
        ArakoonInstaller._logger.debug('Started node {0} for cluster {1}'.format(new_ip, cluster_name))

    @staticmethod
    def claim_cluster(cluster_name, ip=None):
        """
        Claims the cluster
        :param cluster_name: Name of the cluster to claim
        :type cluster_name: str
        :param ip: IP of one of the cluster nodes (Only required for filesystem Arakoons)
        :type ip: str|None
        :return: None
        :rtype: NoneType
        """
        config = ArakoonClusterConfig(cluster_id=cluster_name, source_ip=ip)
        arakoon_client = ArakoonInstaller.build_client(config)
        metadata = json.loads(arakoon_client.get(ArakoonInstaller.METADATA_KEY))
        metadata['in_use'] = True
        arakoon_client.set(ArakoonInstaller.METADATA_KEY, json.dumps(metadata, indent=4))

    @staticmethod
    def unclaim_cluster(cluster_name, ip=None):
        """
        Un-claims the cluster
        :param cluster_name: Name of the cluster to un-claim
        :type cluster_name: str
        :param ip: IP of one of the cluster nodes (Only required for filesystem Arakoons)
        :type ip: str|None
        :return: None
        :rtype: NoneType
        """
        config = ArakoonClusterConfig(cluster_id=cluster_name, source_ip=ip)
        arakoon_client = ArakoonInstaller.build_client(config=config)
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

    @staticmethod
    def get_service_name_for_cluster(cluster_name):
        """
        Retrieve the Arakoon service name for the cluster specified
        :param cluster_name: Name of the Arakoon cluster
        :type cluster_name: str
        :return: Name of the Arakoon service known on the system
        :rtype: str
        """
        return 'arakoon-{0}'.format(cluster_name)

    @staticmethod
    def _is_filesystem_cluster(cluster_type, ip):
        """
        Validate whether the cluster is a filesystem Arakoon cluster or not
        For filesystem Arakoon clusters an IP address must be provided
        """
        if cluster_type == ServiceType.ARAKOON_CLUSTER_TYPES.CFG:
            if ip is None:
                raise ValueError('IP is required for filesystem Arakoons')
            return True, ip
        return False, None  # Reset to None if passed, because only applicable for filesystem Arakoons
