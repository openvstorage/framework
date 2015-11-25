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
Module for SetupController
"""

import os
import re
import sys
import json
import time
import base64
import urllib2
import subprocess
from paramiko import AuthenticationException

from ConfigParser import RawConfigParser
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonInstaller, ArakoonClusterConfig
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.interactive import Interactive
from ovs.extensions.generic.remote import Remote
from ovs.extensions.generic.system import System
from ovs.log.logHandler import LogHandler
from ovs.lib.helpers.toolbox import Toolbox
from ovs.extensions.migration.migrator import Migrator
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagementEx
from ovs.extensions.packages.package import PackageManager
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.filemutex import FileMutex

logger = LogHandler.get('lib', name='setup')
logger.logger.propagate = False


class SetupController(object):
    """
    This class contains all logic for setting up an environment, installed with system-native packages
    """

    # Generic configuration files
    generic_configfiles = {'memcached': ('/opt/OpenvStorage/config/memcacheclient.cfg', 11211),
                           'rabbitmq': ('/opt/OpenvStorage/config/rabbitmqclient.cfg', 5672)}
    avahi_filename = '/etc/avahi/services/ovs_cluster.service'

    # Services
    model_services = ['memcached', 'arakoon-ovsdb']
    master_services = model_services + ['rabbitmq-server']
    extra_node_services = ['workers', 'volumerouter-consumer']
    master_node_services = master_services + ['scheduled-tasks', 'snmp', 'webapp-api', 'nginx',
                                              'volumerouter-consumer'] + extra_node_services

    discovered_nodes = {}
    host_ips = set()

    @staticmethod
    def setup_node(ip=None, force_type=None):
        """
        Sets up a node.
        1. Some magic figuring out here:
           - Which cluster (new, joining)
           - Cluster role (master, extra)
        2. Prepare cluster
        3. Depending on (2), setup first/extra node
        4. Depending on (2), promote new extra node
        """

        print Interactive.boxed_message(['Open vStorage Setup'])
        logger.info('Starting Open vStorage Setup')

        target_password = None
        cluster_name = None
        first_node = True
        nodes = []
        cluster_ip = None
        hypervisor_type = None
        hypervisor_name = None
        hypervisor_password = None
        hypervisor_ip = None
        hypervisor_username = 'root'
        known_passwords = {}
        master_ip = None
        join_cluster = False
        configure_memcached = True
        configure_rabbitmq = True
        enable_heartbeats = True
        ip_client_map = {}

        # Support non-interactive setup
        preconfig = '/tmp/openvstorage_preconfig.cfg'
        if os.path.exists(preconfig):
            config = RawConfigParser()
            config.read(preconfig)
            ip = config.get('setup', 'target_ip')
            target_password = config.get('setup', 'target_password')  # @TODO: Replace by using "known_passwords"
            cluster_ip = config.get('setup', 'cluster_ip')
            cluster_name = str(config.get('setup', 'cluster_name'))
            master_ip = config.get('setup', 'master_ip')
            hypervisor_type = config.get('setup', 'hypervisor_type')
            hypervisor_name = config.get('setup', 'hypervisor_name')
            hypervisor_ip = config.get('setup', 'hypervisor_ip')
            hypervisor_username = config.get('setup', 'hypervisor_username')
            hypervisor_password = config.get('setup', 'hypervisor_password')
            join_cluster = config.getboolean('setup', 'join_cluster')
            configure_memcached = config.getboolean('setup', 'configure_memcached')
            configure_rabbitmq = config.getboolean('setup', 'configure_rabbitmq')
            if config.has_option('setup', 'other_nodes'):
                SetupController.discovered_nodes = json.loads(config.get('setup', 'other_nodes'))
            if config.has_option('setup', 'passwords'):
                known_passwords = json.loads(config.get('setup', 'passwords'))
            enable_heartbeats = False

        try:
            if force_type is not None:
                force_type = force_type.lower()
                if force_type not in ['master', 'extra']:
                    raise ValueError("The force_type parameter should be 'master' or 'extra'.")

            # Create connection to target node
            print '\n+++ Setting up connections +++\n'
            logger.info('Setting up connections')

            if ip is None:
                ip = '127.0.0.1'
            if target_password is None:
                node_string = 'this node' if ip == '127.0.0.1' else ip
                target_node_password = SetupController._ask_validate_password(ip, username='root', node_string=node_string)
            else:
                target_node_password = target_password
            target_client = SSHClient(ip, username='root', password=target_node_password)
            ip_client_map[ip] = target_client

            logger.debug('Target client loaded')

            if target_client.config_read('ovs.core.setupcompleted') is True:
                raise RuntimeError('This node has already been configured for Open vStorage. Re-running the setup is not supported.')

            print '\n+++ Collecting cluster information +++\n'
            logger.info('Collecting cluster information')

            ipaddresses = target_client.run("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1").strip().splitlines()
            ipaddresses = [found_ip.strip() for found_ip in ipaddresses if found_ip.strip() != '127.0.0.1']
            SetupController.host_ips = set(ipaddresses)

            # Check whether running local or remote
            unique_id = System.get_my_machine_id(target_client)
            local_unique_id = System.get_my_machine_id()
            remote_install = unique_id != local_unique_id
            logger.debug('{0} installation'.format('Remote' if remote_install else 'Local'))
            try:
                _ = Configuration.get('ovs.grid.ip')
            except:
                raise RuntimeError("The 'openvstorage' package is not installed on {0}".format(ip))

            # Getting cluster information
            current_cluster_names = []
            clusters = []
            avahi_installed = SetupController._avahi_installed(target_client)
            discovery_result = {}
            if avahi_installed is True:
                discovery_result = SetupController._discover_nodes(target_client)
                if discovery_result:
                    clusters = discovery_result.keys()
                    clusters.sort()
                    current_cluster_names = clusters[:]
                    logger.debug('Cluster names: {0}'.format(current_cluster_names))
                else:
                    logger.debug('No clusters found')
            else:
                logger.debug('No avahi installed/detected')

            local_cluster_name = None
            if remote_install is True:
                if os.path.exists(SetupController.avahi_filename):
                    with open(SetupController.avahi_filename, 'r') as avahi_file:
                        avahi_contents = avahi_file.read()
                    match_groups = re.search('>ovs_cluster_(?P<cluster>[^_]+)_.+?<', avahi_contents).groupdict()
                    if 'cluster' in match_groups:
                        local_cluster_name = match_groups['cluster']

            node_name = target_client.run('hostname')
            logger.debug('Current host: {0}'.format(node_name))
            if cluster_name is None:
                while True:
                    logger.debug('Cluster selection')
                    new_cluster = 'Create a new cluster'
                    join_manually = 'Join {0} cluster'.format('a' if len(clusters) == 0 else 'a different')
                    cluster_options = [new_cluster] + clusters + [join_manually]
                    question = 'Select a cluster to join' if len(clusters) > 0 else 'No clusters found'
                    cluster_name = Interactive.ask_choice(cluster_options, question,
                                                          default_value=local_cluster_name,
                                                          sort_choices=False)
                    if cluster_name == new_cluster:
                        cluster_name = None
                        first_node = True
                    elif cluster_name == join_manually:
                        cluster_name = None
                        first_node = False
                        node_ip = Interactive.ask_string('Please enter the IP of one of the cluster\'s nodes')
                        if not re.match(SSHClient.IP_REGEX, node_ip):
                            print 'Incorrect IP provided'
                            continue
                        if node_ip in target_client.local_ips:
                            print "A local ip address was given, please select '{0}'".format(new_cluster)
                            continue
                        logger.debug('Trying to manually join cluster on {0}'.format(node_ip))

                        node_password = SetupController._ask_validate_password(node_ip, username='root')
                        storagerouters = {}
                        try:
                            from ovs.dal.lists.storagerouterlist import StorageRouterList
                            with Remote(node_ip, [StorageRouterList],
                                        username='root',
                                        password=node_password,
                                        strict_host_key_checking=False) as remote:
                                for sr in remote.StorageRouterList.get_storagerouters():
                                    storagerouters[sr.ip] = sr.name
                                    if sr.node_type == 'MASTER':
                                        if sr.ip == node_ip:
                                            master_ip = node_ip
                                            known_passwords[master_ip] = node_password
                                        elif master_ip is None:
                                            master_ip = sr.ip
                        except Exception, ex:
                            logger.error('Error loading storagerouters: {0}'.format(ex))
                        if len(storagerouters) == 0:
                            logger.debug('No StorageRouters could be loaded, cannot join the cluster')
                            print 'The cluster on the given master node cannot be joined as no StorageRouters could be loaded'
                            continue
                        correct = Interactive.ask_yesno(
                            message='Following StorageRouters were detected:\n    {0}\nAre they correct?'.format(
                                ', '.join(storagerouters.keys()))
                        )
                        if correct is False:
                            print 'The cluster on the given master node cannot be joined as not all StorageRouters could be loaded'
                            continue

                        known_passwords[node_ip] = node_password
                        if master_ip is not None and master_ip not in known_passwords:
                            master_password = SetupController._ask_validate_password(master_ip, username='root')
                            known_passwords[master_ip] = master_password
                        for sr_ip, sr_name in storagerouters.iteritems():
                            SetupController.discovered_nodes = {sr_name: {'ip': sr_ip,
                                                                          'type': 'unknown',
                                                                          'ip_list': [sr_ip]}}
                            nodes.append(sr_ip)
                        if node_ip not in ip_client_map:
                            ip_client_map[node_ip] = SSHClient(node_ip, username='root', password=node_password)
                    else:
                        logger.debug('Cluster {0} selected'.format(cluster_name))
                        SetupController.discovered_nodes = discovery_result[cluster_name]
                        nodes = [node_property['ip'] for node_property in discovery_result[cluster_name].values()]
                        if node_name in discovery_result[cluster_name].keys():
                            continue_install = Interactive.ask_yesno(
                                '{0} already exists in cluster {1}. Do you want to continue?'.format(
                                    node_name, cluster_name
                                ), default_value=True
                            )
                            if continue_install is False:
                                raise ValueError('Duplicate node name found.')
                        master_nodes = [this_node_name for this_node_name, node_properties in discovery_result[cluster_name].iteritems()
                                        if node_properties.get('type', None) == 'master']
                        if len(master_nodes) == 0:
                            raise RuntimeError('No master node could be found in cluster {0}'.format(cluster_name))
                        master_ip = discovery_result[cluster_name][master_nodes[0]]['ip']
                        master_password = SetupController._ask_validate_password(master_ip, username='root')
                        known_passwords[master_ip] = master_password
                        if master_ip not in ip_client_map:
                            ip_client_map[master_ip] = SSHClient(master_ip, username='root', password=master_password)
                        first_node = False
                    break

                if first_node is True and cluster_name is None:
                    while True:
                        cluster_name = Interactive.ask_string('Please enter the cluster name')
                        if cluster_name in current_cluster_names:
                            print 'The new cluster name should be unique.'
                        elif not re.match('^[0-9a-zA-Z]+(\-[0-9a-zA-Z]+)*$', cluster_name):
                            print "The new cluster name can only contain numbers, letters and dashes."
                        else:
                            break

            else:  # Automated install
                logger.debug('Automated installation')
                if SetupController._avahi_installed(target_client):
                    if cluster_name in discovery_result:
                        SetupController.discovered_nodes = discovery_result[cluster_name]
                nodes = [node_property['ip'] for node_property in SetupController.discovered_nodes.values()]
                first_node = not join_cluster
            if not cluster_name and first_node is False and avahi_installed is True:
                raise RuntimeError('The name of the cluster should be known by now.')

            # Get target cluster ip
            ipaddresses = target_client.run("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1").strip().splitlines()
            ipaddresses = [found_ip.strip() for found_ip in ipaddresses if found_ip.strip() != '127.0.0.1']
            if not cluster_ip:
                cluster_ip = Interactive.ask_choice(ipaddresses, 'Select the public ip address of {0}'.format(node_name))
                ip_client_map.pop(ip)
                ip_client_map[cluster_ip] = SSHClient(cluster_ip, username='root', password=target_node_password)
            known_passwords[cluster_ip] = target_node_password
            if cluster_ip not in nodes:
                nodes.append(cluster_ip)
            logger.debug('Cluster ip is selected as {0}'.format(cluster_ip))

            if target_password is not None:
                for node in nodes:
                    known_passwords[node] = target_password

            hypervisor_info, ip_client_map = SetupController._prepare_node(cluster_ip=cluster_ip,
                                                                           nodes=nodes,
                                                                           known_passwords=known_passwords,
                                                                           ip_client_map=ip_client_map,
                                                                           hypervisor_info={'type': hypervisor_type,
                                                                                            'name': hypervisor_name,
                                                                                            'username': hypervisor_username,
                                                                                            'ip': hypervisor_ip,
                                                                                            'password': hypervisor_password})
            if first_node is True:
                SetupController._setup_first_node(target_client=ip_client_map[cluster_ip],
                                                  unique_id=unique_id,
                                                  cluster_name=cluster_name,
                                                  node_name=node_name,
                                                  hypervisor_info=hypervisor_info,
                                                  enable_heartbeats=enable_heartbeats,
                                                  configure_memcached=configure_memcached,
                                                  configure_rabbitmq=configure_rabbitmq)
            else:
                # Deciding master/extra
                SetupController._setup_extra_node(cluster_ip=cluster_ip,
                                                  master_ip=master_ip,
                                                  cluster_name=cluster_name,
                                                  unique_id=unique_id,
                                                  ip_client_map=ip_client_map,
                                                  hypervisor_info=hypervisor_info,
                                                  configure_memcached=configure_memcached,
                                                  configure_rabbitmq=configure_rabbitmq)

                print 'Analyzing cluster layout'
                logger.info('Analyzing cluster layout')
                config = ArakoonClusterConfig('ovsdb')
                config.load_config(SSHClient(master_ip, username='root', password=known_passwords[master_ip]))
                logger.debug('{0} nodes for cluster {1} found'.format(len(config.nodes), 'ovsdb'))
                if (len(config.nodes) < 3 or force_type == 'master') and force_type != 'extra':
                    SetupController._promote_node(cluster_ip=cluster_ip,
                                                  master_ip=master_ip,
                                                  cluster_name=cluster_name,
                                                  ip_client_map=ip_client_map,
                                                  unique_id=unique_id,
                                                  configure_memcached=configure_memcached,
                                                  configure_rabbitmq=configure_rabbitmq)

            print ''
            print Interactive.boxed_message(['Setup complete.',
                                             'Point your browser to https://{0} to use Open vStorage'.format(cluster_ip)])
            logger.info('Setup complete')

        except Exception as exception:
            print ''  # Spacing
            print Interactive.boxed_message(['An unexpected error occurred:', str(exception)])
            logger.exception('Unexpected error')
            logger.error(str(exception))
            sys.exit(1)
        except KeyboardInterrupt:
            print ''
            print ''
            print Interactive.boxed_message(['This setup was aborted. Open vStorage may be in an inconsistent state, make sure to validate the installation.'])
            logger.error('Keyboard interrupt')
            sys.exit(1)

    @staticmethod
    def promote_or_demote_node(node_action):
        """
        Promotes or demotes the local node
        """

        if node_action not in ('promote', 'demote'):
            raise ValueError('Nodes can only be promoted or demoted')

        print Interactive.boxed_message(['Open vStorage Setup - {0}'.format(node_action.capitalize())])
        logger.info('Starting Open vStorage Setup - {0}'.format(node_action))

        try:
            print '\n+++ Collecting information +++\n'
            logger.info('Collecting information')

            if Configuration.get('ovs.core.setupcompleted') is False:
                raise RuntimeError('No local OVS setup found.')

            node_type = Configuration.get('ovs.core.nodetype')
            if node_action == 'promote' and node_type == 'MASTER':
                raise RuntimeError('This node is already master.')
            elif node_action == 'demote' and node_type == 'EXTRA':
                raise RuntimeError('This node should be a master.')
            elif node_type not in ['MASTER', 'EXTRA']:
                raise RuntimeError('This node is not correctly configured.')

            target_password = SetupController._ask_validate_password('127.0.0.1', username='root',
                                                                     node_string='this node')
            target_client = SSHClient('127.0.0.1', username='root', password=target_password)

            unique_id = System.get_my_machine_id(target_client)
            ip = target_client.config_read('ovs.grid.ip')

            cluster_name = None
            if SetupController._avahi_installed(target_client):
                with open(SetupController.avahi_filename, 'r') as avahi_file:
                    avahi_contents = avahi_file.read()
                match_groups = re.search('>ovs_cluster_(?P<cluster>[^_]+)_.+?<', avahi_contents).groupdict()
                if 'cluster' not in match_groups:
                    raise RuntimeError('No cluster information found.')
                cluster_name = match_groups['cluster']
                discovery_result = SetupController._discover_nodes(target_client)
                master_nodes = [this_node_name for this_node_name, node_properties in discovery_result[cluster_name].iteritems() if node_properties.get('type') == 'master']
                nodes = [node_property['ip'] for node_property in discovery_result[cluster_name].values()]
                if len(master_nodes) == 0:
                    if node_action == 'promote':
                        raise RuntimeError('No master node could be found in cluster {0}'.format(cluster_name))
                    else:
                        raise RuntimeError('It is not possible to remove the only master in cluster {0}'.format(cluster_name))
                master_ip = discovery_result[cluster_name][master_nodes[0]]['ip']
                nodes.append(ip)  # The client node is never included in the discovery results
            else:
                master_nodes = []
                nodes = []
                try:
                    from ovs.dal.lists.storagerouterlist import StorageRouterList
                    with Remote(target_client.ip, [StorageRouterList],
                                username='root',
                                password=target_password,
                                strict_host_key_checking=False) as remote:
                        for sr in remote.StorageRouterList.get_storagerouters():
                            nodes.append(sr.ip)
                            if sr.machine_id != unique_id and sr.node_type == 'MASTER':
                                master_nodes.append(ip)
                except Exception, ex:
                    logger.error('Error loading storagerouters: {0}'.format(ex))
                if len(master_nodes) == 0:
                    if node_action == 'promote':
                        raise RuntimeError('No master node could be found')
                    else:
                        raise RuntimeError('It is not possible to remove the only master')
                master_ip = master_nodes[0]

            ip_client_map = dict((node_ip, SSHClient(node_ip, username='root')) for node_ip in nodes if node_ip)
            configure_rabbitmq = True
            configure_memcached = True
            preconfig = '/tmp/openvstorage_preconfig.cfg'
            if os.path.exists(preconfig):
                config = RawConfigParser()
                config.read(preconfig)
                configure_memcached = config.getboolean('setup', 'configure_memcached')
                configure_rabbitmq = config.getboolean('setup', 'configure_rabbitmq')
            if node_action == 'promote':
                SetupController._promote_node(cluster_ip=ip,
                                              master_ip=master_ip,
                                              cluster_name=cluster_name,
                                              ip_client_map=ip_client_map,
                                              unique_id=unique_id,
                                              configure_memcached=configure_memcached,
                                              configure_rabbitmq=configure_rabbitmq)
            else:
                SetupController._demote_node(cluster_ip=ip,
                                             master_ip=master_ip,
                                             cluster_name=cluster_name,
                                             ip_client_map=ip_client_map,
                                             unique_id=unique_id,
                                             configure_memcached=configure_memcached,
                                             configure_rabbitmq=configure_rabbitmq)

            print ''
            print Interactive.boxed_message(['{0} complete.'.format(node_action.capitalize())])
            logger.info('Setup complete - {0}'.format(node_action))

        except Exception as exception:
            print ''  # Spacing
            print Interactive.boxed_message(['An unexpected error occurred:', str(exception)])
            logger.exception('Unexpected error')
            logger.error(str(exception))
            sys.exit(1)
        except KeyboardInterrupt:
            print ''
            print ''
            print Interactive.boxed_message(['This setup was aborted. Open vStorage may be in an inconsistent state, make sure to validate the installation.'])
            logger.error('Keyboard interrupt')
            sys.exit(1)

    @staticmethod
    def update_framework():
        file_mutex = FileMutex('system_update', wait=2)
        upgrade_file = '/etc/ready_for_upgrade'
        upgrade_ongoing_check_file = '/etc/upgrade_ongoing'
        ssh_clients = []
        try:
            file_mutex.acquire()
            SetupController._log_message('+++ Starting framework update +++')

            from ovs.dal.lists.storagerouterlist import StorageRouterList

            SetupController._log_message('Generating SSH client connections for each storage router')
            upgrade_file = '/etc/ready_for_upgrade'
            upgrade_ongoing_check_file = '/etc/upgrade_ongoing'
            storage_routers = StorageRouterList.get_storagerouters()
            ssh_clients = [SSHClient(storage_router.ip, 'root') for storage_router in storage_routers]
            this_client = [client for client in ssh_clients if client.is_local is True][0]

            # Commence update !!!!!!!
            # 0. Create locks
            SetupController._log_message('Creating lock files', client_ip=this_client.ip)
            for client in ssh_clients:
                client.run('touch {0}'.format(upgrade_file))  # Prevents manual install or upgrade individual packages
                client.run('touch {0}'.format(upgrade_ongoing_check_file))  # Prevents clicking x times on 'Update' btn

            # 1. Check requirements
            packages_to_update = set()
            all_services_to_restart = []
            for client in ssh_clients:
                for function in Toolbox.fetch_hooks('update', 'metadata'):
                    SetupController._log_message('Executing function {0}'.format(function.__name__),
                                                 client_ip=client.ip)
                    output = function(client)
                    for key, value in output.iteritems():
                        if key != 'framework':
                            continue
                        for package_info in value:
                            packages_to_update.update(package_info['packages'])
                            all_services_to_restart += package_info['services']

            services_to_restart = []
            for service in all_services_to_restart:
                if service not in services_to_restart:
                    services_to_restart.append(service)  # Filter out duplicates maintaining the order of services (eg: watcher-framework before memcached)

            SetupController._log_message('Services which will be restarted --> {0}'.format(', '.join(services_to_restart)))
            SetupController._log_message('Packages which will be installed --> {0}'.format(', '.join(packages_to_update)))

            # 2. Stop services
            if SetupController._change_services_state(services=services_to_restart,
                                                      ssh_clients=ssh_clients,
                                                      action='stop') is False:
                SetupController._log_message('Stopping all services on every node failed, cannot continue',
                                             client_ip=this_client.ip, severity='warning')
                SetupController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)

                # Start services again if a service could not be stopped
                SetupController._log_message('Attempting to start the services again', client_ip=this_client.ip)
                SetupController._change_services_state(services=services_to_restart,
                                                       ssh_clients=ssh_clients,
                                                       action='start')

                SetupController._log_message('Failed to stop all required services, aborting update',
                                             client_ip=this_client.ip, severity='error')
                return

            # 3. Update packages
            failed_clients = []
            for client in ssh_clients:
                PackageManager.update(client=client)
                try:
                    SetupController._log_message('Installing latest packages', client.ip)
                    for package in packages_to_update:
                        SetupController._log_message('Installing {0}'.format(package), client.ip)
                        PackageManager.install(package_name=package,
                                               client=client,
                                               force=True)
                        SetupController._log_message('Installed {0}'.format(package), client.ip)
                    client.file_delete(upgrade_file)
                except subprocess.CalledProcessError as cpe:
                    SetupController._log_message('Upgrade failed with error: {0}'.format(cpe.output), client.ip,
                                                 'error')
                    failed_clients.append(client)
                    break

            if failed_clients:
                SetupController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
                SetupController._log_message('Error occurred. Attempting to start all services again',
                                             client_ip=this_client.ip, severity='error')
                SetupController._change_services_state(services=services_to_restart,
                                                       ssh_clients=ssh_clients,
                                                       action='start')
                SetupController._log_message('Failed to upgrade following nodes:\n - {0}\nPlease check /var/log/ovs/lib.log on {1} for more information'.format('\n - '.join([client.ip for client in failed_clients])), this_client.ip, 'error')
                return

            # 4. Start services
            SetupController._log_message('Starting services', client_ip=this_client.ip)
            model_services = []
            if 'arakoon-ovsdb' in services_to_restart:
                model_services.append('arakoon-ovsdb')
                services_to_restart.remove('arakoon-ovsdb')
            if 'memcached' in services_to_restart:
                model_services.append('memcached')
                services_to_restart.remove('memcached')
            SetupController._change_services_state(services=model_services,
                                                   ssh_clients=ssh_clients,
                                                   action='start')

            # 5. Migrate
            SetupController._log_message('Started model migration', client_ip=this_client.ip)
            try:
                from ovs.dal.helpers import Migration
                Migration.migrate()
                SetupController._log_message('Finished model migration', client_ip=this_client.ip)
            except Exception as ex:
                SetupController._remove_lock_files([upgrade_ongoing_check_file], ssh_clients)
                SetupController._log_message('An unexpected error occurred: {0}'.format(ex), client_ip=this_client.ip,
                                             severity='error')
                return

            for client in ssh_clients:
                try:
                    SetupController._log_message('Started code migration', client.ip)
                    with Remote(client.ip, [Migrator]) as remote:
                        remote.Migrator.migrate()
                    SetupController._log_message('Finished code migration', client.ip)
                except Exception as ex:
                    SetupController._remove_lock_files([upgrade_ongoing_check_file], ssh_clients)
                    SetupController._log_message('Code migration failed with error: {0}'.format(ex), client.ip, 'error')
                    return

            # 6. Post upgrade actions
            SetupController._log_message('Executing post upgrade actions', client_ip=this_client.ip)
            for client in ssh_clients:
                with Remote(client.ip, [Toolbox, SSHClient]) as remote:
                    for function in remote.Toolbox.fetch_hooks('update', 'postupgrade'):
                        SetupController._log_message('Executing action {0}'.format(function.__name__),
                                                     client_ip=client.ip)
                        try:
                            function(remote.SSHClient(client.ip, username='root'))
                            SetupController._log_message('Executing action {0} completed'.format(function.__name__),
                                                         client_ip=client.ip)
                        except Exception as ex:
                            SetupController._log_message('Post upgrade action failed with error: {0}'.format(ex),
                                                         client.ip, 'error')

            # 7. Start watcher and restart support-agent
            SetupController._change_services_state(services=services_to_restart,
                                                   ssh_clients=ssh_clients,
                                                   action='start')
            SetupController._change_services_state(services=['support-agent'],
                                                   ssh_clients=ssh_clients,
                                                   action='restart')

            SetupController._remove_lock_files([upgrade_ongoing_check_file], ssh_clients)
            SetupController._log_message('+++ Finished updating +++')
        except RuntimeError as rte:
            if 'Could not acquire lock' in rte.message:
                SetupController._log_message('Another framework update is currently in progress!')
            else:
                SetupController._log_message('Error during framework update: {0}'.format(rte), severity='error')
                SetupController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
        except Exception as ex:
            SetupController._log_message('Error during framework update: {0}'.format(ex), severity='error')
            SetupController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
        finally:
            file_mutex.release()

    @staticmethod
    def update_volumedriver():
        file_mutex = FileMutex('system_update', wait=2)
        upgrade_file = '/etc/ready_for_upgrade'
        upgrade_ongoing_check_file = '/etc/upgrade_ongoing'
        ssh_clients = []
        try:
            file_mutex.acquire()
            SetupController._log_message('+++ Starting volumedriver update +++')

            from ovs.dal.lists.storagerouterlist import StorageRouterList

            SetupController._log_message('Generating SSH client connections for each storage router')
            storage_routers = StorageRouterList.get_storagerouters()
            ssh_clients = [SSHClient(storage_router.ip, 'root') for storage_router in storage_routers]
            this_client = [client for client in ssh_clients if client.is_local is True][0]

            # Commence update !!!!!!!
            # 0. Create locks
            SetupController._log_message('Creating lock files', client_ip=this_client.ip)
            for client in ssh_clients:
                client.run('touch {0}'.format(upgrade_file))  # Prevents manual install or upgrade individual packages
                client.run('touch {0}'.format(upgrade_ongoing_check_file))  # Prevents clicking x times on 'Update' btn

            # 1. Check requirements
            packages_to_update = set()
            all_services_to_restart = []
            for client in ssh_clients:
                for function in Toolbox.fetch_hooks('update', 'metadata'):
                    SetupController._log_message('Executing function {0}'.format(function.__name__),
                                                 client_ip=client.ip)
                    output = function(client)
                    for key, value in output.iteritems():
                        if key != 'volumedriver':
                            continue
                        for package_info in value:
                            packages_to_update.update(package_info['packages'])
                            all_services_to_restart += package_info['services']

            services_to_restart = []
            for service in all_services_to_restart:
                if service not in services_to_restart:
                    services_to_restart.append(service)  # Filter out duplicates keeping the order of services (eg: watcher-framework before memcached)

            SetupController._log_message('Services which will be restarted --> {0}'.format(', '.join(services_to_restart)))
            SetupController._log_message('Packages which will be installed --> {0}'.format(', '.join(packages_to_update)))

            # 1. Stop services
            if SetupController._change_services_state(services=services_to_restart,
                                                      ssh_clients=ssh_clients,
                                                      action='stop') is False:
                SetupController._log_message('Stopping all services on every node failed, cannot continue',
                                             client_ip=this_client.ip, severity='warning')
                SetupController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)

                SetupController._log_message('Attempting to start the services again', client_ip=this_client.ip)
                SetupController._change_services_state(services=services_to_restart,
                                                       ssh_clients=ssh_clients,
                                                       action='start')
                SetupController._log_message('Failed to stop all required services, update aborted',
                                             client_ip=this_client.ip, severity='error')
                return

            # 2. Update packages
            failed_clients = []
            for client in ssh_clients:
                PackageManager.update(client=client)
                try:
                    for package_name in packages_to_update:
                        SetupController._log_message('Installing {0}'.format(package_name), client.ip)
                        PackageManager.install(package_name=package_name,
                                               client=client,
                                               force=True)
                        SetupController._log_message('Installed {0}'.format(package_name), client.ip)
                    client.file_delete(upgrade_file)
                except subprocess.CalledProcessError as cpe:
                    SetupController._log_message('Upgrade failed with error: {0}'.format(cpe.output), client.ip,
                                                 'error')
                    failed_clients.append(client)
                    break

            if failed_clients:
                SetupController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
                SetupController._log_message('Error occurred. Attempting to start all services again',
                                             client_ip=this_client.ip, severity='error')
                SetupController._change_services_state(services=services_to_restart,
                                                       ssh_clients=ssh_clients,
                                                       action='start')
                SetupController._log_message('Failed to upgrade following nodes:\n - {0}\nPlease check /var/log/ovs/lib.log on {1} for more information'.format('\n - '.join([client.ip for client in failed_clients])), this_client.ip, 'error')
                return

            # 3. Post upgrade actions
            SetupController._log_message('Executing post upgrade actions', client_ip=this_client.ip)
            for client in ssh_clients:
                for function in Toolbox.fetch_hooks('update', 'postupgrade'):
                    SetupController._log_message('Executing action: {0}'.format(function.__name__), client_ip=client.ip)
                    try:
                        function(client)
                    except Exception as ex:
                        SetupController._log_message('Post upgrade action failed with error: {0}'.format(ex),
                                                     client.ip, 'error')

            # 4. Start services
            SetupController._log_message('Starting services', client_ip=this_client.ip)
            SetupController._change_services_state(services=services_to_restart,
                                                   ssh_clients=ssh_clients,
                                                   action='start')

            SetupController._remove_lock_files([upgrade_ongoing_check_file], ssh_clients)
            SetupController._log_message('+++ Finished updating +++')
        except RuntimeError as rte:
            if 'Could not acquire lock' in rte.message:
                SetupController._log_message('Another volumedriver update is currently in progress!')
            else:
                SetupController._log_message('Error during volumedriver update: {0}'.format(rte), severity='error')
                SetupController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
        except Exception as ex:
            SetupController._log_message('Error during volumedriver update: {0}'.format(ex), severity='error')
            SetupController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
        finally:
            file_mutex.release()

    @staticmethod
    def _prepare_node(cluster_ip, nodes, known_passwords, ip_client_map, hypervisor_info):
        """
        Prepares a node:
        - Exchange SSH keys
        - Update hosts files
        - Request hypervisor information
        """

        print '\n+++ Preparing node +++\n'
        logger.info('Preparing node')

        # Exchange ssh keys
        print 'Exchanging SSH keys and updating hosts files'
        logger.info('Exchanging SSH keys and updating hosts files')
        passwords = {}
        first_request = True
        prev_node_password = ''
        for node in nodes:
            if node in known_passwords:
                passwords[node] = known_passwords[node]
                if node not in ip_client_map:
                    ip_client_map[node] = SSHClient(node, username='root', password=known_passwords[node])
                continue
            if first_request is True:
                prev_node_password = SetupController._ask_validate_password(node, username='root')
                logger.debug('Custom password for {0}'.format(node))
                passwords[node] = prev_node_password
                first_request = False
                if node not in ip_client_map:
                    ip_client_map[node] = SSHClient(node, username='root', password=prev_node_password)
            else:
                this_node_password = SetupController._ask_validate_password(node, username='root',
                                                                            previous=prev_node_password)
                passwords[node] = this_node_password
                prev_node_password = this_node_password
                if node not in ip_client_map:
                    ip_client_map[node] = SSHClient(node, username='root', password=this_node_password)

        logger.debug('Nodes: {0}'.format(nodes))
        logger.debug('Discovered nodes: \n{0}'.format(SetupController.discovered_nodes))
        all_ips = set()
        all_hostnames = set()
        for hostname, node_details in SetupController.discovered_nodes.iteritems():
            for ip in node_details['ip_list']:
                all_ips.add(ip)
            all_hostnames.add(hostname)
        all_ips.update(SetupController.host_ips)

        root_ssh_folder = '/root/.ssh'
        ovs_ssh_folder = '/opt/OpenvStorage/.ssh'
        public_key_filename = '{0}/id_rsa.pub'
        authorized_keys_filename = '{0}/authorized_keys'
        known_hosts_filename = '{0}/known_hosts'
        authorized_keys = ''
        mapping = {}
        for node, node_client in ip_client_map.iteritems():
            if node_client.file_exists(authorized_keys_filename.format(root_ssh_folder)):
                existing_keys = node_client.file_read(authorized_keys_filename.format(root_ssh_folder)).split('\n')
                for existing_key in existing_keys:
                    if existing_key not in authorized_keys:
                        authorized_keys += "{0}\n".format(existing_key)
            if node_client.file_exists(authorized_keys_filename.format(ovs_ssh_folder)):
                existing_keys = node_client.file_read(authorized_keys_filename.format(ovs_ssh_folder))
                for existing_key in existing_keys:
                    if existing_key not in authorized_keys:
                        authorized_keys += "{0}\n".format(existing_key)
            root_pub_key = node_client.file_read(public_key_filename.format(root_ssh_folder))
            ovs_pub_key = node_client.file_read(public_key_filename.format(ovs_ssh_folder))
            if root_pub_key not in authorized_keys:
                authorized_keys += '{0}\n'.format(root_pub_key)
            if ovs_pub_key not in authorized_keys:
                authorized_keys += '{0}\n'.format(ovs_pub_key)
            node_hostname = node_client.run('hostname')
            all_hostnames.add(node_hostname)
            mapping[node] = node_hostname

        for node, node_client in ip_client_map.iteritems():
            for hostname_node, hostname in mapping.iteritems():
                System.update_hosts_file(hostname, hostname_node, node_client)
            node_client.file_write(authorized_keys_filename.format(root_ssh_folder), authorized_keys)
            node_client.file_write(authorized_keys_filename.format(ovs_ssh_folder), authorized_keys)
            cmd = 'cp {1} {1}.tmp; ssh-keyscan -t rsa {0} {2} 2> /dev/null >> {1}.tmp; cat {1}.tmp | sort -u - > {1}'
            node_client.run(cmd.format(' '.join(all_ips), known_hosts_filename.format(root_ssh_folder),
                                       ' '.join(all_hostnames)))
            cmd = 'su - ovs -c "cp {1} {1}.tmp; ssh-keyscan -t rsa {0} {2} 2> /dev/null  >> {1}.tmp; cat {1}.tmp | sort -u - > {1}"'
            node_client.run(cmd.format(' '.join(all_ips), known_hosts_filename.format(ovs_ssh_folder),
                                       ' '.join(all_hostnames)))

        target_client = ip_client_map[cluster_ip]

        print 'Collecting hypervisor information'
        logger.info('Collecting hypervisor information')

        # Collecting hypervisor data
        possible_hypervisor = None
        module = target_client.run('lsmod | grep kvm || true').strip()
        if module != '':
            possible_hypervisor = 'KVM'
        else:
            disktypes = target_client.run('dmesg | grep VMware || true').strip()
            if disktypes != '':
                possible_hypervisor = 'VMWARE'

        if not hypervisor_info.get('type'):
            hypervisor_info['type'] = Interactive.ask_choice(['VMWARE', 'KVM'],
                                                             question='Which type of hypervisor is this Storage Router backing?',
                                                             default_value=possible_hypervisor)
            logger.debug('Selected hypervisor type {0}'.format(hypervisor_info['type']))
        default_name = ('esxi{0}' if hypervisor_info['type'] == 'VMWARE' else 'kvm{0}').format(cluster_ip.split('.')[-1])
        if not hypervisor_info.get('name'):
            hypervisor_info['name'] = Interactive.ask_string('Enter hypervisor hostname', default_value=default_name)
        if hypervisor_info['type'] == 'VMWARE':
            first_request = True  # If parameters are wrong, we need to re-ask it
            while True:
                if not hypervisor_info.get('ip') or not first_request:
                    hypervisor_info['ip'] = Interactive.ask_string('Enter hypervisor ip address',
                                                                   default_value=hypervisor_info.get('ip'))
                if not hypervisor_info.get('username') or not first_request:
                    hypervisor_info['username'] = Interactive.ask_string('Enter hypervisor username',
                                                                         default_value=hypervisor_info['username'])
                if not hypervisor_info.get('password') or not first_request:
                    hypervisor_info['password'] = Interactive.ask_password('Enter hypervisor {0} password'.format(hypervisor_info.get('username')))
                try:
                    request = urllib2.Request('https://{0}/mob'.format(hypervisor_info['ip']))
                    auth = base64.encodestring('{0}:{1}'.format(hypervisor_info['username'], hypervisor_info['password'])).replace('\n', '')
                    request.add_header("Authorization", "Basic %s" % auth)
                    urllib2.urlopen(request).read()
                    break
                except Exception as ex:
                    first_request = False
                    print 'Could not connect to {0}: {1}'.format(hypervisor_info['ip'], ex)
        elif hypervisor_info['type'] == 'KVM':
            hypervisor_info['ip'] = cluster_ip
            hypervisor_info['password'] = None
            hypervisor_info['username'] = 'root'
        logger.debug('Hypervisor at {0} with username {1}'.format(hypervisor_info['ip'], hypervisor_info['username']))

        return hypervisor_info, ip_client_map

    @staticmethod
    def _log_message(message, client_ip=None, severity='info'):
        if client_ip is not None:
            message = '{0:<15}: {1}'.format(client_ip, message)
        if severity == 'info':
            logger.info(message, print_msg=True)
        elif severity == 'warning':
            logger.warning(message, print_msg=True)
        elif severity == 'error':
            logger.error(message, print_msg=True)

    @staticmethod
    def _remove_lock_files(files, ssh_clients):
        for ssh_client in ssh_clients:
            for file_name in files:
                if ssh_client.file_exists(file_name):
                    ssh_client.file_delete(file_name)

    @staticmethod
    def _change_services_state(services, ssh_clients, action):
        """
        Stop/start services on SSH clients
        If action is start, we ignore errors and try to start other services on other nodes
        """
        if action == 'start':
            services.reverse()  # Start services again in reverse order of stopping
        for service_name in services:
            for ssh_client in ssh_clients:
                description = 'stopping' if action == 'stop' else 'starting' if action == 'start' else 'restarting'
                try:
                    if ServiceManager.has_service(service_name, client=ssh_client):
                        SetupController._log_message('{0} service {1}'.format(description.capitalize(), service_name),
                                                     ssh_client.ip)
                        SetupController.change_service_state(client=ssh_client,
                                                             name=service_name,
                                                             state=action)
                        SetupController._log_message('{0} service {1}'.format('Stopped' if action == 'stop' else 'Started' if action == 'start' else 'Restarted', service_name), ssh_client.ip)
                except Exception as exc:
                    SetupController._log_message('Something went wrong {0} service {1}: {2}'.format(description, service_name, exc), ssh_client.ip, severity='warning')
                    if action == 'stop':
                        return False
        return True

    @staticmethod
    def _setup_first_node(target_client, unique_id, cluster_name, node_name, hypervisor_info, enable_heartbeats,
                          configure_memcached, configure_rabbitmq):
        """
        Sets up the first node services. This node is always a master
        """

        print '\n+++ Setting up first node +++\n'
        logger.info('Setting up first node')

        print 'Setting up Arakoon'
        logger.info('Setting up Arakoon')
        cluster_ip = target_client.ip
        result = ArakoonInstaller.create_cluster('ovsdb', cluster_ip, [], target_client.config_read('ovs.core.ovsdb'))
        arakoon_ports = [result['client_port'], result['messaging_port']]

        SetupController._configure_logstash(target_client)
        SetupController._add_services(target_client, unique_id, 'master')
        config_types = []
        if configure_rabbitmq:
            config_types.append('rabbitmq')
            SetupController._configure_rabbitmq(target_client)
        if configure_memcached:
            config_types.append('memcached')
            SetupController._configure_memcached(target_client)

        print 'Build configuration files'
        logger.info('Build configuration files')
        for config_type in config_types:
            config_file, port = SetupController.generic_configfiles[config_type]
            config = RawConfigParser()
            config.add_section('main')
            config.set('main', 'nodes', unique_id)
            config.add_section(unique_id)
            config.set(unique_id, 'location', '{0}:{1}'.format(cluster_ip, port))
            target_client.rawconfig_write(config_file, config)

        print 'Starting model services'
        logger.debug('Starting model services')
        for service in SetupController.model_services:
            if ServiceManager.has_service(service, client=target_client):
                ServiceManager.enable_service(service, client=target_client)
                SetupController.change_service_state(target_client, service, 'restart')

        print 'Start model migration'
        logger.debug('Start model migration')
        from ovs.dal.helpers import Migration
        Migration.migrate()

        print '\n+++ Finalizing setup +++\n'
        logger.info('Finalizing setup')
        storagerouter = SetupController._finalize_setup(target_client, node_name, 'MASTER', hypervisor_info, unique_id)

        from ovs.dal.lists.servicetypelist import ServiceTypeList
        from ovs.dal.hybrids.service import Service
        service = Service()
        service.name = 'arakoon-ovsdb'
        service.type = ServiceTypeList.get_by_name('Arakoon')
        service.ports = arakoon_ports
        service.storagerouter = storagerouter
        service.save()

        print 'Updating configuration files'
        logger.info('Updating configuration files')
        target_client.config_set('ovs.grid.ip', cluster_ip)
        target_client.config_set('ovs.support.cid', Toolbox.get_hash())
        target_client.config_set('ovs.support.nid', Toolbox.get_hash())

        print 'Starting services'
        logger.info('Starting services for join master')
        for service in SetupController.master_services:
            if ServiceManager.has_service(service, client=target_client):
                ServiceManager.enable_service(service, client=target_client)
                SetupController.change_service_state(target_client, service, 'start')
        # Enable HA for the rabbitMQ queues
        SetupController._check_rabbitmq_and_enable_ha_mode(target_client)

        ServiceManager.enable_service('watcher-framework', client=target_client)
        SetupController.change_service_state(target_client, 'watcher-framework', 'start')

        logger.debug('Restarting workers')
        ServiceManager.enable_service('workers', client=target_client)
        SetupController.change_service_state(target_client, 'workers', 'restart')

        SetupController._run_hooks('firstnode', cluster_ip)

        target_client.config_set('ovs.support.cid', Toolbox.get_hash())
        target_client.config_set('ovs.support.nid', Toolbox.get_hash())
        if enable_heartbeats is None:
            print '\n+++ Heartbeat +++\n'
            logger.info('Heartbeat')
            print Interactive.boxed_message(['Open vStorage has the option to send regular heartbeats with metadata to a centralized server.' +
                                             'The metadata contains anonymous data like Open vStorage\'s version and status of the Open vStorage services. These heartbeats are optional and can be turned on/off at any time via the GUI.'],
                                            character=None)
            enable_heartbeats = Interactive.ask_yesno('Do you want to enable Heartbeats?', default_value=True)
        if enable_heartbeats is True:
            target_client.config_set('ovs.support.enabled', True)
            service = 'support-agent'
            ServiceManager.add_service(service, client=target_client)
            ServiceManager.enable_service(service, client=target_client)
            SetupController.change_service_state(target_client, service, 'start')

        if SetupController._avahi_installed(target_client) is True:
            SetupController._configure_avahi(target_client, cluster_name, node_name, 'master')
        target_client.config_set('ovs.core.setupcompleted', True)
        target_client.config_set('ovs.core.nodetype', 'MASTER')
        target_client.config_set('ovs.core.install_time', time.time())
        target_client.run('chown -R ovs:ovs /opt/OpenvStorage/config')

        logger.info('First node complete')

    @staticmethod
    def _setup_extra_node(cluster_ip, master_ip, cluster_name, unique_id, ip_client_map, hypervisor_info,
                          configure_memcached, configure_rabbitmq):
        """
        Sets up an additional node
        """

        print '\n+++ Adding extra node +++\n'
        logger.info('Adding extra node')

        target_client = ip_client_map[cluster_ip]
        SetupController._configure_logstash(target_client)
        SetupController._add_services(target_client, unique_id, 'extra')

        print 'Configuring services'
        logger.info('Copying client configurations')
        ArakoonInstaller.deploy_to_slave(master_ip, cluster_ip, 'ovsdb')
        config_types = []
        if configure_rabbitmq:
            config_types.append('rabbitmq')
        if configure_memcached:
            config_types.append('memcached')
        master_client = ip_client_map[master_ip]
        for config_type in config_types:
            config = SetupController.generic_configfiles[config_type][0]
            client_config = master_client.rawconfig_read(config)
            target_client.rawconfig_write(config, client_config)

        cid = master_client.config_read('ovs.support.cid')
        enabled = master_client.config_read('ovs.support.enabled')
        enablesupport = master_client.config_read('ovs.support.enablesupport')
        registered = master_client.config_read('ovs.core.registered')
        target_client.config_set('ovs.support.nid', Toolbox.get_hash())
        target_client.config_set('ovs.support.cid', cid)
        target_client.config_set('ovs.support.enabled', enabled)
        target_client.config_set('ovs.support.enablesupport', enablesupport)
        target_client.config_set('ovs.core.registered', registered)
        if enabled is True:
            service = 'support-agent'
            ServiceManager.add_service(service, client=target_client)
            ServiceManager.enable_service(service, client=target_client)
            SetupController.change_service_state(target_client, service, 'start')

        node_name = target_client.run('hostname')
        SetupController._finalize_setup(target_client, node_name, 'EXTRA', hypervisor_info, unique_id)

        print 'Updating configuration files'
        logger.info('Updating configuration files')
        target_client.config_set('ovs.grid.ip', cluster_ip)

        print 'Starting services'
        ServiceManager.enable_service('watcher-framework', client=target_client)
        SetupController.change_service_state(target_client, 'watcher-framework', 'start')

        logger.debug('Restarting workers')
        for node_client in ip_client_map.itervalues():
            ServiceManager.enable_service('workers', client=node_client)
            SetupController.change_service_state(node_client, 'workers', 'restart')

        SetupController._run_hooks('extranode', cluster_ip, master_ip)

        if SetupController._avahi_installed(target_client) is True:
            SetupController._configure_avahi(target_client, cluster_name, node_name, 'extra')
        target_client.config_set('ovs.core.setupcompleted', True)
        target_client.config_set('ovs.core.nodetype', 'EXTRA')
        target_client.config_set('ovs.core.install_time', time.time())
        target_client.run('chown -R ovs:ovs /opt/OpenvStorage/config')
        logger.info('Extra node complete')

    @staticmethod
    def _promote_node(cluster_ip, master_ip, cluster_name, ip_client_map, unique_id, configure_memcached,
                      configure_rabbitmq):
        """
        Promotes a given node
        """
        from ovs.dal.lists.storagerouterlist import StorageRouterList
        from ovs.dal.lists.servicetypelist import ServiceTypeList
        from ovs.dal.lists.servicelist import ServiceList
        from ovs.dal.hybrids.service import Service

        print '\n+++ Promoting node +++\n'
        logger.info('Promoting node')

        if configure_memcached:
            if SetupController._validate_local_memcache_servers(ip_client_map) is False:
                raise RuntimeError('Not all memcache nodes can be reached which is required for promoting a node.')

        target_client = ip_client_map[cluster_ip]
        node_name = target_client.run('hostname')

        storagerouter = StorageRouterList.get_by_machine_id(unique_id)
        storagerouter.node_type = 'MASTER'
        storagerouter.save()

        # Find other (arakoon) master nodes
        config = ArakoonClusterConfig('ovsdb')
        config.load_config(SSHClient(master_ip, username='root'))
        master_nodes = [node.ip for node in config.nodes]
        if cluster_ip in master_nodes:
            master_nodes.remove(cluster_ip)
        if len(master_nodes) == 0:
            raise RuntimeError('There should be at least one other master node')

        SetupController._configure_logstash(target_client)
        if configure_memcached:
            SetupController._configure_memcached(target_client)
        SetupController._add_services(target_client, unique_id, 'master')

        print 'Joining arakoon cluster'
        logger.info('Joining arakoon cluster')
        exclude_ports = ServiceList.get_ports_for_ip(cluster_ip)
        result = ArakoonInstaller.extend_cluster(master_ip, cluster_ip, 'ovsdb', exclude_ports, target_client.config_read('ovs.core.ovsdb'))
        arakoon_ports = [result['client_port'], result['messaging_port']]

        print 'Distribute configuration files'
        logger.info('Distribute configuration files')
        config_types = []
        if configure_rabbitmq:
            config_types.append('rabbitmq')
        if configure_memcached:
            config_types.append('memcached')
        master_client = ip_client_map[master_ip]
        for config_type in config_types:
            config_file, port = SetupController.generic_configfiles[config_type]
            config = master_client.rawconfig_read(config_file)
            config_nodes = [n.strip() for n in config.get('main', 'nodes').split(',')]
            if unique_id not in config_nodes:
                config.set('main', 'nodes', ', '.join(config_nodes + [unique_id]))
                config.add_section(unique_id)
                config.set(unique_id, 'location', '{0}:{1}'.format(cluster_ip, port))
            for node_client in ip_client_map.itervalues():
                node_client.rawconfig_write(config_file, config)

        print 'Restarting master node services'
        logger.info('Restarting master node services')
        ArakoonInstaller.restart_cluster_add('ovsdb', master_nodes, cluster_ip)
        PersistentFactory.store = None
        VolatileFactory.store = None

        service = Service()
        service.name = 'arakoon-ovsdb'
        service.type = ServiceTypeList.get_by_name('Arakoon')
        service.ports = arakoon_ports
        service.storagerouter = storagerouter
        service.save()

        if configure_rabbitmq:
            SetupController._configure_rabbitmq(target_client)

            # Copy rabbitmq cookie
            logger.debug('Copying Rabbit MQ cookie')
            rabbitmq_cookie_file = '/var/lib/rabbitmq/.erlang.cookie'
            contents = master_client.file_read(rabbitmq_cookie_file)
            master_hostname = master_client.run('hostname')
            target_client.dir_create(os.path.dirname(rabbitmq_cookie_file))
            target_client.file_write(rabbitmq_cookie_file, contents)
            target_client.file_attribs(rabbitmq_cookie_file, mode=400)
            target_client.run('rabbitmq-server -detached 2> /dev/null; sleep 5; rabbitmqctl stop_app; sleep 5;')
            target_client.run('rabbitmqctl join_cluster rabbit@{0}; sleep 5;'.format(master_hostname))
            target_client.run('rabbitmqctl stop; sleep 5;')

            # Enable HA for the rabbitMQ queues
            SetupController.change_service_state(target_client, 'rabbitmq-server', 'start')
            SetupController._check_rabbitmq_and_enable_ha_mode(target_client)

        SetupController._configure_amqp_to_volumedriver(ip_client_map)

        print 'Starting services'
        logger.info('Starting services')
        for service in SetupController.master_services:
            if ServiceManager.has_service(service, client=target_client):
                ServiceManager.enable_service(service, client=target_client)
                SetupController.change_service_state(target_client, service, 'start')

        print 'Restarting services'
        SetupController._restart_framework_and_memcache_services(ip_client_map)

        if SetupController._run_hooks('promote', cluster_ip, master_ip):
            print 'Restarting services'
            SetupController._restart_framework_and_memcache_services(ip_client_map)

        if SetupController._avahi_installed(target_client) is True:
            SetupController._configure_avahi(target_client, cluster_name, node_name, 'master')
        target_client.config_set('ovs.core.nodetype', 'MASTER')
        target_client.run('chown -R ovs:ovs /opt/OpenvStorage/config')

        logger.info('Promote complete')

    @staticmethod
    def _demote_node(cluster_ip, master_ip, cluster_name, ip_client_map, unique_id, configure_memcached,
                     configure_rabbitmq):
        """
        Demotes a given node
        """
        from ovs.dal.lists.storagerouterlist import StorageRouterList

        print '\n+++ Demoting node +++\n'
        logger.info('Demoting node')

        if configure_memcached:
            if SetupController._validate_local_memcache_servers(ip_client_map) is False:
                raise RuntimeError('Not all memcache nodes can be reached which is required for demoting a node.')

        target_client = ip_client_map[cluster_ip]
        node_name = target_client.run('hostname')

        storagerouter = StorageRouterList.get_by_machine_id(unique_id)
        storagerouter.node_type = 'EXTRA'
        storagerouter.save()

        # Find other (arakoon) master nodes
        config = ArakoonClusterConfig('ovsdb')
        config.load_config(SSHClient(master_ip, username='root'))
        master_nodes = [node.ip for node in config.nodes]
        if cluster_ip in master_nodes:
            master_nodes.remove(cluster_ip)
        if len(master_nodes) == 0:
            raise RuntimeError('There should be at least one other master node')

        print 'Leaving arakoon cluster'
        logger.info('Leaving arakoon cluster')
        ArakoonInstaller.shrink_cluster(master_ip, cluster_ip, 'ovsdb')

        print 'Distribute configuration files'
        logger.info('Distribute configuration files')
        master_client = ip_client_map[master_ip]
        config_types = []
        if configure_memcached:
            config_types.append('memcached')
        if configure_rabbitmq:
            config_types.append('rabbitmq')
        for config_type in config_types:
            config_file, port = SetupController.generic_configfiles[config_type]
            config = master_client.rawconfig_read(config_file)
            config_nodes = [n.strip() for n in config.get('main', 'nodes').split(',')]
            if unique_id in config_nodes:
                config_nodes.remove(unique_id)
                config.set('main', 'nodes', ', '.join(config_nodes))
                config.remove_section(unique_id)
            for node_client in ip_client_map.itervalues():
                node_client.rawconfig_write(config_file, config)

        print 'Restarting master node services'
        logger.info('Restarting master node services')
        remaining_nodes = ip_client_map.keys()[:]
        if cluster_ip in remaining_nodes:
            remaining_nodes.remove(cluster_ip)
        ArakoonInstaller.restart_cluster_remove('ovsdb', remaining_nodes)
        PersistentFactory.store = None
        VolatileFactory.store = None

        for service in storagerouter.services:
            if service.name == 'arakoon-ovsdb':
                service.delete()

        if configure_rabbitmq:
            print 'Removing/unconfiguring RabbitMQ'
            logger.debug('Removing/unconfiguring RabbitMQ')
            if ServiceManager.has_service('rabbitmq-server', client=target_client):
                target_client.run('rabbitmq-server -detached 2> /dev/null; sleep 5; rabbitmqctl stop_app; sleep 5;')
                target_client.run('rabbitmqctl reset; sleep 5;')
                target_client.run('rabbitmqctl stop; sleep 5;')
                SetupController.change_service_state(target_client, 'rabbitmq-server', 'stop')
                target_client.file_unlink("/var/lib/rabbitmq/.erlang.cookie")

        print 'Removing services'
        logger.info('Removing services')
        services = [s for s in SetupController.master_node_services if s not in (SetupController.extra_node_services + ['arakoon-ovsdb'])]
        if not configure_rabbitmq:
            services.remove('rabbitmq-server')
        if not configure_memcached:
            services.remove('memcached')
        for service in services:
            if ServiceManager.has_service(service, client=target_client):
                logger.debug('Removing service {0}'.format(service))
                SetupController.change_service_state(target_client, service, 'stop')
                ServiceManager.remove_service(service, client=target_client)

        if ServiceManager.has_service('workers', client=target_client):
            ServiceManager.add_service(name='workers',
                                       client=target_client,
                                       params={'MEMCACHE_NODE_IP': cluster_ip,
                                               'WORKER_QUEUE': '{0}'.format(unique_id)})

        SetupController._configure_amqp_to_volumedriver(ip_client_map)

        print 'Restarting services'
        logger.debug('Restarting services')
        SetupController._restart_framework_and_memcache_services(ip_client_map, target_client)

        if SetupController._run_hooks('demote', cluster_ip, master_ip):
            print 'Restarting services'
            SetupController._restart_framework_and_memcache_services(ip_client_map, target_client)

        if SetupController._avahi_installed(target_client) is True:
            SetupController._configure_avahi(target_client, cluster_name, node_name, 'extra')
        target_client.config_set('ovs.core.nodetype', 'EXTRA')

        logger.info('Demote complete')

    @staticmethod
    def _restart_framework_and_memcache_services(ip_client_map, memcached_exclude_client=None):
        for service_info in [('watcher-framework', 'stop'),
                             ('memcached', 'restart'),
                             ('watcher-framework', 'start')]:
            for node_client in ip_client_map.itervalues():
                if memcached_exclude_client is not None and memcached_exclude_client.ip == node_client.ip and service_info[0] == 'memcached':
                    continue  # Skip memcached for demoted nodes, because they don't run that service
                SetupController.change_service_state(node_client, service_info[0], service_info[1])
        VolatileFactory.store = None

    @staticmethod
    def _configure_memcached(client):
        print "Setting up Memcached"
        client.run("""sed -i 's/^-l.*/-l 0.0.0.0/g' /etc/memcached.conf""")
        client.run("""sed -i 's/^-m.*/-m 1024/g' /etc/memcached.conf""")
        client.run("""sed -i -E 's/^-v(.*)/# -v\1/g' /etc/memcached.conf""")  # Put all -v, -vv, ... back in comment
        client.run("""sed -i 's/^# -v[^v]*$/-v/g' /etc/memcached.conf""")     # Uncomment only -v

    @staticmethod
    def _configure_rabbitmq(client):
        print 'Setting up RabbitMQ'
        logger.debug('Setting up RabbitMQ')
        rabbitmq_port = client.config_read('ovs.core.broker.port')
        rabbitmq_login = client.config_read('ovs.core.broker.login')
        rabbitmq_password = client.config_read('ovs.core.broker.password')
        client.run("""cat > /etc/rabbitmq/rabbitmq.config << EOF
[
   {{rabbit, [{{tcp_listeners, [{0}]}},
              {{default_user, <<"{1}">>}},
              {{default_pass, <<"{2}">>}},
              {{vm_memory_high_watermark, 0.2}}]}}
].
EOF
""".format(rabbitmq_port, rabbitmq_login, rabbitmq_password))
        rabbitmq_running, same_process = SetupController._is_rabbitmq_running(client)
        if rabbitmq_running is True:
            SetupController.change_service_state(client, 'rabbitmq-server', 'stop')

        client.run('rabbitmq-server -detached 2> /dev/null; sleep 5;')

        # Sometimes/At random the rabbitmq server takes longer than 5 seconds to start,
        #  and the next command fails so the best solution is to retry several times
        # Also retry the add_user/set_permissions, and validate the result
        retry = 0
        while retry < 10:
            users = Toolbox.retry_client_run(client,
                                             'rabbitmqctl list_users',
                                             logger=logger).splitlines()[1:-1]
            users = [usr.split('\t')[0] for usr in users]
            logger.debug('Rabbitmq users {0}'.format(users))
            if 'ovs' in users:
                logger.debug('User ovs configured in rabbitmq')
                break
            else:
                logger.debug(Toolbox.retry_client_run(client,
                                                      'rabbitmqctl add_user {0} {1}'.format(rabbitmq_login, rabbitmq_password),
                                                      logger=logger))
                logger.debug(Toolbox.retry_client_run(client,
                                                      'rabbitmqctl set_permissions {0} ".*" ".*" ".*"'.format(rabbitmq_login),
                                                      logger=logger))
                retry += 1
                time.sleep(1)
        users = Toolbox.retry_client_run(client,
                                         'rabbitmqctl list_users',
                                         logger=logger).splitlines()[1:-1]
        users = [usr.split('\t')[0] for usr in users]
        logger.debug('Rabbitmq users {0}'.format(users))
        client.run('rabbitmqctl stop; sleep 5;')

    @staticmethod
    def _check_rabbitmq_and_enable_ha_mode(client):
        if not ServiceManager.has_service('rabbitmq-server', client):
            raise RuntimeError('Service rabbitmq-server has not been added on node {0}'.format(client.ip))
        rabbitmq_running, same_process = SetupController._is_rabbitmq_running(client)
        if rabbitmq_running is False or same_process is False:
            SetupController.change_service_state(client, 'rabbitmq-server', 'restart')

        client.run('sleep 5;rabbitmqctl set_policy ha-all "^(volumerouter|ovs_.*)$" \'{"ha-mode":"all"}\'')

    @staticmethod
    def _configure_amqp_to_volumedriver(node_ips):
        print 'Update existing vPools'
        logger.info('Update existing vPools')
        for node_ip in node_ips:
            with Remote(node_ip, [os, RawConfigParser, Configuration, StorageDriverConfiguration, ArakoonManagementEx],
                        'ovs') as remote:
                login = remote.Configuration.get('ovs.core.broker.login')
                password = remote.Configuration.get('ovs.core.broker.password')
                protocol = remote.Configuration.get('ovs.core.broker.protocol')

                cfg = remote.RawConfigParser()
                cfg.read('/opt/OpenvStorage/config/rabbitmqclient.cfg')

                uris = []
                for node in [n.strip() for n in cfg.get('main', 'nodes').split(',')]:
                    uris.append({'amqp_uri': '{0}://{1}:{2}@{3}'.format(protocol, login, password, cfg.get(node,
                                                                                                           'location'))})

                configuration_dir = '{0}/storagedriver/storagedriver'.format(remote.Configuration.get('ovs.core.cfgdir'))
                if not remote.os.path.exists(configuration_dir):
                    remote.os.makedirs(configuration_dir)
                for json_file in remote.os.listdir(configuration_dir):
                    vpool_name = json_file.replace('.json', '')
                    if json_file.endswith('.json'):
                        if remote.os.path.exists('{0}/{1}.cfg'.format(configuration_dir, vpool_name)):
                            continue  # There's also a .cfg file, so this is an alba_proxy configuration file
                        storagedriver_config = remote.StorageDriverConfiguration('storagedriver', vpool_name)
                        storagedriver_config.load()
                        storagedriver_config.configure_event_publisher(events_amqp_routing_key=remote.Configuration.get('ovs.core.broker.queues.storagedriver'),
                                                                       events_amqp_uris=uris)
                        storagedriver_config.save()

    @staticmethod
    def _avahi_installed(client):
        installed = client.run('which avahi-daemon')
        if installed == '':
            logger.debug('Avahi not installed')
            return False
        else:
            logger.debug('Avahi installed')
            return True

    @staticmethod
    def _logstash_installed(client):
        if client.file_exists('/usr/sbin/logstash') \
           or client.file_exists('/usr/bin/logstash') \
           or client.file_exists('/opt/logstash/bin/logstash'):
            return True
        return False

    @staticmethod
    def _configure_logstash(client):
        if not SetupController._logstash_installed(client):
            logger.debug("Logstash is not installed, skipping it's configuration")
            return False

        print 'Configuring logstash'
        logger.info('Configuring logstash')
        if ServiceManager.has_service('logstash', client) is False:
            ServiceManager.add_service('logstash', client)
        SetupController.change_service_state(client, 'logstash', 'restart')

    @staticmethod
    def _configure_avahi(client, cluster_name, node_name, node_type):
        print '\n+++ Announcing service +++\n'
        logger.info('Announcing service')

        client.run("""cat > {3} <<EOF
<?xml version="1.0" standalone='no'?>
<!--*-nxml-*-->
<!DOCTYPE service-group SYSTEM "avahi-service.dtd">
<!-- $Id$ -->
<service-group>
    <name replace-wildcards="yes">ovs_cluster_{0}_{1}_{4}</name>
    <service>
        <type>_ovs_{2}_node._tcp</type>
        <port>443</port>
    </service>
</service-group>
EOF
""".format(cluster_name, node_name, node_type, SetupController.avahi_filename, client.ip.replace('.', '_')))
        client.run('avahi-daemon --reload')
        SetupController.change_service_state(client, 'avahi-daemon', 'restart')

    @staticmethod
    def _add_services(client, unique_id, node_type):
        if node_type == 'master':
            services = SetupController.master_node_services
            if 'arakoon-ovsdb' in services:
                services.remove('arakoon-ovsdb')
            worker_queue = '{0},ovs_masters'.format(unique_id)
        else:
            services = SetupController.extra_node_services
            worker_queue = unique_id

        print 'Adding services'
        logger.info('Adding services')
        params = {'MEMCACHE_NODE_IP': client.ip,
                  'WORKER_QUEUE': worker_queue}
        for service in services + ['watcher-framework']:
            logger.debug('Adding service {0}'.format(service))
            ServiceManager.add_service(service, params=params, client=client)

    @staticmethod
    def _finalize_setup(client, node_name, node_type, hypervisor_info, unique_id):
        cluster_ip = client.ip
        client.dir_create('/opt/OpenvStorage/webapps/frontend/logging')
        if SetupController._logstash_installed(client):
            SetupController.change_service_state(client, 'logstash', 'restart')
        SetupController._replace_param_in_config(client=client,
                                                 config_file='/opt/OpenvStorage/webapps/frontend/logging/config.js',
                                                 old_value='http://"+window.location.hostname+":9200',
                                                 new_value='http://' + cluster_ip + ':9200')

        # Imports, not earlier than here, as all required config files should be in place.
        from ovs.lib.disk import DiskController
        from ovs.lib.storagerouter import StorageRouterController
        from ovs.dal.hybrids.pmachine import PMachine
        from ovs.dal.hybrids.storagerouter import StorageRouter
        from ovs.dal.lists.failuredomainlist import FailureDomainList
        from ovs.dal.lists.pmachinelist import PMachineList
        from ovs.dal.lists.storagerouterlist import StorageRouterList

        print 'Configuring/updating model'
        logger.info('Configuring/updating model')
        pmachine = None
        for current_pmachine in PMachineList.get_pmachines():
            if current_pmachine.ip == hypervisor_info['ip'] and current_pmachine.hvtype == hypervisor_info['type']:
                pmachine = current_pmachine
                break
        if pmachine is None:
            pmachine = PMachine()
            pmachine.ip = hypervisor_info['ip']
            pmachine.username = hypervisor_info['username']
            pmachine.password = hypervisor_info['password']
            pmachine.hvtype = hypervisor_info['type']
            pmachine.name = hypervisor_info['name']
            pmachine.save()
        storagerouter = None
        for current_storagerouter in StorageRouterList.get_storagerouters():
            if current_storagerouter.ip == cluster_ip and current_storagerouter.machine_id == unique_id:
                storagerouter = current_storagerouter
                break

        if storagerouter is None:
            failure_domains = FailureDomainList.get_failure_domains()
            failure_domain_usages = sys.maxint
            failure_domain = None
            for current_failure_domain in failure_domains:
                current_failure_domain_usages = len(current_failure_domain.primary_storagerouters)
                if current_failure_domain_usages < failure_domain_usages:
                    failure_domain = current_failure_domain
                    failure_domain_usages = current_failure_domain_usages
            if failure_domain is None:
                failure_domain = failure_domains[0]

            storagerouter = StorageRouter()
            storagerouter.name = node_name
            storagerouter.machine_id = unique_id
            storagerouter.ip = cluster_ip
            storagerouter.primary_failure_domain = failure_domain
            storagerouter.rdma_capable = False
        storagerouter.node_type = node_type
        storagerouter.pmachine = pmachine
        storagerouter.save()

        StorageRouterController.set_rdma_capability(storagerouter.guid)
        DiskController.sync_with_reality(storagerouter.guid)

        return storagerouter

    @staticmethod
    def _discover_nodes(client):
        nodes = {}
        SetupController.change_service_state(client, 'dbus', 'start')
        SetupController.change_service_state(client, 'avahi-daemon', 'start')
        discover_result = client.run('timeout -k 60 45 avahi-browse -artp 2> /dev/null | grep ovs_cluster || true')
        for entry in discover_result.splitlines():
            entry_parts = entry.split(';')
            if entry_parts[0] == '=' and entry_parts[2] == 'IPv4' and entry_parts[7] not in SetupController.host_ips:
                # =;eth0;IPv4;ovs_cluster_kenneth_ovs100;_ovs_master_node._tcp;local;ovs100.local;172.22.1.10;443;
                # split(';') -> [3]  = ovs_cluster_kenneth_ovs100
                #               [4]  = _ovs_master_node._tcp -> contains _ovs_<type>_node
                #               [7]  = 172.22.1.10 (ip)
                # split('_') -> [-1] = ovs100 (node name)
                #               [-2] = kenneth (cluster name)
                cluster_info = entry_parts[3].split('_')
                cluster_name = cluster_info[2]
                node_name = cluster_info[3]
                if cluster_name not in nodes:
                    nodes[cluster_name] = {}
                if node_name not in nodes[cluster_name]:
                    nodes[cluster_name][node_name] = {'ip': '', 'type': '', 'ip_list': []}
                try:
                    ip = '{0}.{1}.{2}.{3}'.format(cluster_info[4], cluster_info[5], cluster_info[6], cluster_info[7])
                except IndexError:
                    ip = entry_parts[7]
                nodes[cluster_name][node_name]['ip'] = ip
                nodes[cluster_name][node_name]['type'] = entry_parts[4].split('_')[2]
                nodes[cluster_name][node_name]['ip_list'].append(ip)
        return nodes

    @staticmethod
    def _replace_param_in_config(client, config_file, old_value, new_value):
        if client.file_exists(config_file):
            contents = client.file_read(config_file)
            if new_value in contents and new_value.find(old_value) > 0:
                pass
            elif old_value in contents:
                contents = contents.replace(old_value, new_value)
            client.file_write(config_file, contents)

    @staticmethod
    def change_service_state(client, name, state):
        """
        Starts/stops/restarts a service
        """
        action = None
        # Enable service before changing the state
        status = ServiceManager.is_enabled(name, client=client)
        if status is False:
            logger.debug('  Enabling service {0}'.format(name))
            ServiceManager.enable_service(name, client=client)

        status = ServiceManager.get_service_status(name, client=client)
        if status is False and state in ['start', 'restart']:
            logger.debug('  Starting service {0}'.format(name))
            ServiceManager.start_service(name, client=client)
            action = 'started'
        elif status is True and state == 'stop':
            logger.debug('  Stopping service {0}'.format(name))
            ServiceManager.stop_service(name, client=client)
            action = 'stopped'
        elif status is True and state == 'restart':
            logger.debug('  Restarting service {0}'.format(name))
            ServiceManager.restart_service(name, client=client)
            action = 'restarted'

        if action is None:
            print '  [{0}] {1} already {2}'.format(client.ip, name, 'running' if status is True else 'halted')
        else:
            logger.debug('  Service {0} {1}'.format(name, action))
            print '  [{0}] {1} {2}'.format(client.ip, name, action)

    @staticmethod
    def _is_rabbitmq_running(client):
        rabbitmq_running = False
        rabbitmq_pid_ctl = -1
        rabbitmq_pid_sm = -1
        output = client.run('rabbitmqctl status || true')
        if output:
            match = re.search('\{pid,(?P<pid>\d+?)\}', output)
            if match is not None:
                match_groups = match.groupdict()
                if 'pid' in match_groups:
                    rabbitmq_running = True
                    rabbitmq_pid_ctl = match_groups['pid']

        if ServiceManager.has_service('rabbitmq-server', client) \
                and ServiceManager.get_service_status('rabbitmq-server', client):
            rabbitmq_running = True
            rabbitmq_pid_sm = ServiceManager.get_service_pid('rabbitmq-server', client)

        same_process = rabbitmq_pid_ctl == rabbitmq_pid_sm
        logger.debug('Rabbitmq is reported {0}running, pids: {1} and {2}'.format('' if rabbitmq_running else 'not ',
                                                                                 rabbitmq_pid_ctl,
                                                                                 rabbitmq_pid_sm))
        return rabbitmq_running, same_process

    @staticmethod
    def _run_hooks(hook_type, cluster_ip, master_ip=None):
        """
        Execute hooks
        """
        if hook_type != 'firstnode' and master_ip is None:
            raise ValueError('Master IP needs to be specified')

        functions = Toolbox.fetch_hooks('setup', hook_type)
        functions_found = len(functions) > 0
        if functions_found is True:
            print '\n+++ Running hooks +++\n'
        for function in functions:
            if master_ip is None:
                function(cluster_ip=cluster_ip)
            else:
                function(cluster_ip=cluster_ip, master_ip=master_ip)
        return functions_found

    @staticmethod
    def _ask_validate_password(ip, username='root', previous=None, node_string=None):
        """
        Asks a user to enter the password for a given user on a given ip and validates it
        """
        while True:
            try:
                try:
                    client = SSHClient(ip, username)
                    client.run('ls /')
                    return None
                except AuthenticationException:
                    pass
                extra = ''
                if previous is not None:
                    extra = ', just press enter if identical as above'
                password = Interactive.ask_password('Enter the {0} password for {1}{2}'.format(
                    username,
                    ip if node_string is None else node_string,
                    extra
                ))
                if password == '':
                    password = previous
                if password is None:
                    continue
                client = SSHClient(ip, username=username, password=password)
                client.run('ls /')
                return password
            except KeyboardInterrupt:
                raise
            except:
                previous = None
                print 'Password invalid or could not connect to this node'

    @staticmethod
    def _validate_local_memcache_servers(ip_client_map):
        """
        Reads the memcache client configuration file from one of the given nodes, and validates whether it can reach all
        nodes to handle a possible future memcache restart
        """
        if len(ip_client_map) <= 1:
            return True
        client = ip_client_map.values()[0]
        config = client.rawconfig_read('{0}/{1}'.format(client.config_read('ovs.core.cfgdir'), 'memcacheclient.cfg'))
        nodes = [node.strip() for node in config.get('main', 'nodes').split(',')]
        ips = map(lambda n: config.get(n, 'location').split(':')[0], nodes)
        for ip in ips:
            if ip not in ip_client_map:
                return False
        return True
