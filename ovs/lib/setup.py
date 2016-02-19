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
from paramiko import AuthenticationException
from etcd import EtcdConnectionFailed, EtcdKeyNotFound
from ConfigParser import RawConfigParser
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonClusterConfig
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonInstaller
from ovs.extensions.db.etcd.installer import EtcdInstaller
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.generic.interactive import Interactive
from ovs.extensions.generic.remote import Remote
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.sshclient import UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
from ovs.lib.helpers.toolbox import Toolbox
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('lib', name='setup')
logger.logger.propagate = False


class SetupController(object):
    """
    This class contains all logic for setting up an environment, installed with system-native packages
    """

    # Generic configuration files
    avahi_filename = '/etc/avahi/services/ovs_cluster.service'

    # Services
    model_services = ['memcached', 'arakoon-ovsdb']
    master_services = model_services + ['rabbitmq-server', 'etcd-config']
    extra_node_services = ['workers', 'volumerouter-consumer']
    master_node_services = master_services + extra_node_services + ['scheduled-tasks', 'snmp', 'webapp-api', 'volumerouter-consumer']

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
        :param ip: IP of the node to set up
        :param force_type: Force master or extra node
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
        external_etcd = None  # Example: 'abcdef0123456789=http://1.2.3.4:2380'
        enable_heartbeats = True
        ip_client_map = {}
        setupcompleted, promotecompleted = None, None

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
            if config.has_option('setup', 'external_etcd'):
                external_etcd = json.loads(config.get('setup', 'external_etcd'))
            if config.has_option('setup', 'enable_heartbeats'):
                enable_heartbeats = config.getboolean('setup', 'enable_heartbeats')
            else:
                enable_heartbeats = True

        # Support resume setup - store entered parameters so when retrying, we have the values
        resumeconfig = '/tmp/openvstorage_resumeconfig.cfg'
        if not os.path.exists(resumeconfig):
            with open(resumeconfig, 'w') as f:
                f.write('')
        r_config = RawConfigParser()
        r_config.read(resumeconfig)
        if not r_config.has_section('setup'):
            r_config.add_section('setup')
        if not r_config.has_section('ip_client_map'):
            r_config.add_section('ip_client_map')
        with open(resumeconfig, 'w') as fp:
            r_config.write(fp)

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
            unique_id = System.get_my_machine_id(target_client)
            ip_client_map[ip] = target_client

            logger.debug('Target client loaded')

            try:
                setupcompleted = EtcdConfiguration.get('/ovs/framework/hosts/{0}/setupcompleted'.format(unique_id)) is True
                promotecompleted = EtcdConfiguration.get('/ovs/framework/hosts/{0}/promotecompleted'.format(unique_id)) is True
                if setupcompleted and promotecompleted:
                    raise RuntimeError('This node has already been configured for Open vStorage. Re-running the setup is not supported.')
            except (EtcdConnectionFailed, EtcdKeyNotFound):
                pass

            if not setupcompleted:
                print '\n+++ Collecting cluster information +++\n'
                logger.info('Collecting cluster information')

                ipaddresses = target_client.run("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1").strip().splitlines()
                ipaddresses = [found_ip.strip() for found_ip in ipaddresses if found_ip.strip() != '127.0.0.1']
                SetupController.host_ips = set(ipaddresses)

                # Check whether running local or remote
                local_unique_id = System.get_my_machine_id()
                remote_install = unique_id != local_unique_id
                logger.debug('{0} installation'.format('Remote' if remote_install else 'Local'))
                try:
                    target_client.file_exists('/etc/openvstorage_id')
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
                if r_config.has_option('setup', 'cluster_name'):
                    cluster_name = r_config.get('setup', 'cluster_name')
                if r_config.has_option('setup', 'master_ip'):
                    master_ip = r_config.get('setup', 'master_ip')
                if r_config.has_option('setup', 'cluster_ip'):
                    cluster_ip = r_config.get('setup', 'cluster_ip')

                if r_config.has_option('setup', 'hypervisor_type'):
                    hypervisor_type = r_config.get('setup', 'hypervisor_type')
                if r_config.has_option('setup', 'hypervisor_name'):
                    hypervisor_name = r_config.get('setup', 'hypervisor_name')
                if r_config.has_option('setup', 'hypervisor_ip'):
                    hypervisor_ip = r_config.get('setup', 'hypervisor_ip')
                if r_config.has_option('setup', 'hypervisor_username'):
                    hypervisor_username = r_config.get('setup', 'hypervisor_username')
                if r_config.has_option('setup', 'hypervisor_password'):
                    hypervisor_password = r_config.get('setup', 'hypervisor_password')

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
                            except Exception as ex:
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
                                SetupController.discovered_nodes[sr_name] = {'ip': sr_ip,
                                                                             'type': 'unknown',
                                                                             'ip_list': [sr_ip]}
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
                            for _, node_info in discovery_result[cluster_name].iteritems():
                                if node_info['ip'] != ip:
                                    master_ip = node_info['ip']
                                    break
                            if master_ip is None:
                                raise RuntimeError('Could not find appropriate master')
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
                r_config.read(resumeconfig)
                r_config.set('setup', 'cluster_ip', cluster_ip)
                r_config.set('setup', 'master_ip', master_ip)
                r_config.set('setup', 'cluster_name', cluster_name)
                r_config.set('setup', 'unique_id', unique_id)
                r_config.set('setup', 'configure_memcached', configure_memcached)
                r_config.set('setup', 'configure_rabbitmq', configure_rabbitmq)
                r_config.set('setup', 'hypervisor_type', hypervisor_info.get('type', None))
                r_config.set('setup', 'hypervisor_name', hypervisor_info.get('name', None))
                r_config.set('setup', 'hypervisor_ip', hypervisor_info.get('ip', None))
                r_config.set('setup', 'hypervisor_username', hypervisor_info.get('username', None))
                r_config.set('setup', 'hypervisor_password', hypervisor_info.get('password', None))
                for ip, client in ip_client_map.items():
                    r_config.set('ip_client_map', ip, client.password)
                with open(resumeconfig, 'w') as fp:
                    r_config.write(fp)

                if first_node is True:
                    try:
                        SetupController._setup_first_node(target_client=ip_client_map[cluster_ip],
                                                          unique_id=unique_id,
                                                          cluster_name=cluster_name,
                                                          node_name=node_name,
                                                          hypervisor_info=hypervisor_info,
                                                          enable_heartbeats=enable_heartbeats,
                                                          configure_memcached=configure_memcached,
                                                          configure_rabbitmq=configure_rabbitmq,
                                                          external_etcd=external_etcd)
                    except Exception as ex:
                        SetupController._print_log_error('setup first node, rolling back', ex)
                        SetupController._rollback_setup_first_node(target_client=ip_client_map[cluster_ip])
                        raise
                else:
                    # Deciding master/extra
                    try:
                        SetupController._setup_extra_node(cluster_ip=cluster_ip,
                                                          master_ip=master_ip,
                                                          cluster_name=cluster_name,
                                                          unique_id=unique_id,
                                                          ip_client_map=ip_client_map,
                                                          hypervisor_info=hypervisor_info,
                                                          external_etcd=external_etcd)
                    except Exception as ex:
                        SetupController._print_log_error('setup extra node, rolling back', ex)
                        SetupController._rollback_setup_extra_node(target_client=ip_client_map[cluster_ip])
                        raise

                    if not promotecompleted:
                        r_config = RawConfigParser()
                        r_config.read(resumeconfig)
                        if cluster_ip is None:
                            cluster_ip = r_config.get('setup', 'cluster_ip')
                        if master_ip is None:
                            master_ip = r_config.get('setup', 'master_ip')
                        if cluster_name is None:
                            cluster_name = r_config.get('setup', 'cluster_name')
                        for item in r_config.items('ip_client_map'):
                            ip, password = item
                            if ip not in ip_client_map:
                                ip_client_map[ip] = SSHClient(ip, username='root', password=password)
                        if unique_id is None:
                            unique_id = r_config.get('setup', 'unique_id')
                        if configure_memcached is None:
                            configure_memcached = r_config.get('setup', 'configure_memcached')
                        if configure_rabbitmq is None:
                            configure_rabbitmq = r_config.get('setup', 'configure_rabbitmq')


                        print 'Analyzing cluster layout'
                        logger.info('Analyzing cluster layout')
                        config = ArakoonClusterConfig('ovsdb')
                        config.load_config()
                        logger.debug('{0} nodes for cluster {1} found'.format(len(config.nodes), 'ovsdb'))
                        if (len(config.nodes) < 3 or force_type == 'master') and force_type != 'extra':
                            try:
                                SetupController._promote_node(cluster_ip=cluster_ip,
                                                              master_ip=master_ip,
                                                              cluster_name=cluster_name,
                                                              ip_client_map=ip_client_map,
                                                              unique_id=unique_id,
                                                              configure_memcached=configure_memcached,
                                                              configure_rabbitmq=configure_rabbitmq)
                            except Exception as ex:
                                SetupController._print_log_error('promote node, rolling back', ex)
                                SetupController._demote_node(cluster_ip=cluster_ip,
                                                             master_ip=master_ip,
                                                             cluster_name=cluster_name,
                                                             ip_client_map=ip_client_map,
                                                             unique_id=unique_id,
                                                             unconfigure_memcached=configure_memcached,
                                                             unconfigure_rabbitmq=configure_rabbitmq)
                                raise

            print ''
            print Interactive.boxed_message(['Setup complete.',
                                             'Point your browser to https://{0} to use Open vStorage'.format(cluster_ip)])
            logger.info('Setup complete')

            try:
                # Try to trigger setups from possibly installed other packages
                sys.path.append('/opt/asd-manager/')
                from source.asdmanager import setup
                print ''
                print 'A local ASD Manager was detected for which the setup will now be launched.'
                print ''
                setup()
            except:
                pass

        except Exception as exception:
            print ''  # Spacing
            logger.exception('Unexpected error')
            logger.error(str(exception))
            print Interactive.boxed_message(['An unexpected error occurred:', str(exception)])
            sys.exit(1)
        except KeyboardInterrupt:
            print ''
            print ''
            print Interactive.boxed_message(['This setup was aborted. Open vStorage may be in an inconsistent state, make sure to validate the installation.'])
            logger.error('Keyboard interrupt')
            sys.exit(1)

    @staticmethod
    def promote_or_demote_node(node_action, cluster_ip=None):
        """
        Promotes or demotes the local node
        :param node_action: Demote or promote
        :param cluster_ip: IP of node to promote or demote
        """

        if node_action not in ('promote', 'demote'):
            raise ValueError('Nodes can only be promoted or demoted')

        print Interactive.boxed_message(['Open vStorage Setup - {0}'.format(node_action.capitalize())])
        logger.info('Starting Open vStorage Setup - {0}'.format(node_action))

        try:
            print '\n+++ Collecting information +++\n'
            logger.info('Collecting information')

            machine_id = System.get_my_machine_id()
            if EtcdConfiguration.get('/ovs/framework/hosts/{0}/setupcompleted'.format(machine_id)) is False:
                raise RuntimeError('No local OVS setup found.')

            node_type = EtcdConfiguration.get('/ovs/framework/hosts/{0}/type'.format(machine_id))
            if node_action == 'promote' and node_type == 'MASTER':
                raise RuntimeError('This node is already master.')
            elif node_action == 'demote' and node_type == 'EXTRA':
                raise RuntimeError('This node should be a master.')
            elif node_type not in ['MASTER', 'EXTRA']:
                raise RuntimeError('This node is not correctly configured.')
            elif cluster_ip and not re.match(Toolbox.regex_ip, cluster_ip):
                raise RuntimeError('Incorrect IP provided ({0})'.format(cluster_ip))

            master_ip = None
            offline_nodes = []
            configure_rabbitmq = True
            configure_memcached = True
            preconfig = '/tmp/openvstorage_preconfig.cfg'
            if os.path.exists(preconfig):
                config = RawConfigParser()
                config.read(preconfig)
                configure_memcached = config.getboolean('setup', 'configure_memcached')
                configure_rabbitmq = config.getboolean('setup', 'configure_rabbitmq')
            if node_action == 'demote' and cluster_ip:
                from ovs.dal.lists.storagerouterlist import StorageRouterList
                from ovs.lib.storagedriver import StorageDriverController

                ip = cluster_ip
                online = True
                unique_id = None
                cluster_name = None
                ip_client_map = {}
                for storage_router in StorageRouterList.get_storagerouters():
                    try:
                        client = SSHClient(storage_router.ip, username='root')
                        client.run('pwd')
                        if storage_router.node_type == 'MASTER':
                            master_ip = storage_router.ip
                        ip_client_map[storage_router.ip] = client
                    except UnableToConnectException:
                        if storage_router.ip == cluster_ip:
                            online = False
                            unique_id = storage_router.machine_id
                            StorageDriverController.move_away(storagerouter_guid=storage_router.guid)
                        offline_nodes.append(storage_router)
                if online is True:
                    raise RuntimeError("If the node is online, please use 'ovs setup demote' executed on the node you wish to demote")

            else:
                target_password = SetupController._ask_validate_password('127.0.0.1', username='root',
                                                                         node_string='this node')
                target_client = SSHClient('127.0.0.1', username='root', password=target_password)

                unique_id = System.get_my_machine_id(target_client)
                ip = EtcdConfiguration.get('/ovs/framework/hosts/{0}/ip'.format(unique_id))

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
                    for node_name, node_info in discovery_result[cluster_name].iteritems():
                        if node_info['ip'] != ip:
                            master_ip = node_info['ip']
                            break
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
                    except Exception as ex:
                        logger.error('Error loading storagerouters: {0}'.format(ex))
                    if len(master_nodes) == 0:
                        if node_action == 'promote':
                            raise RuntimeError('No master node could be found')
                        else:
                            raise RuntimeError('It is not possible to remove the only master')
                    for possible_ip in master_nodes:
                        if ip != possible_ip:
                            master_ip = possible_ip
                            break

                ip_client_map = dict((node_ip, SSHClient(node_ip, username='root')) for node_ip in nodes if node_ip)

            if master_ip is None:
                raise RuntimeError('Failed to retrieve another responsive MASTER node')
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
                                             unconfigure_memcached=configure_memcached,
                                             unconfigure_rabbitmq=configure_rabbitmq,
                                             offline_nodes=offline_nodes)

            print ''
            print Interactive.boxed_message(['{0} complete.'.format(node_action.capitalize())])
            logger.info('Setup complete - {0}'.format(node_action))

        except Exception as exception:
            print ''  # Spacing
            logger.exception('Unexpected error')
            logger.error(str(exception))
            print Interactive.boxed_message(['An unexpected error occurred:', str(exception)])
            sys.exit(1)
        except KeyboardInterrupt:
            print ''
            print ''
            print Interactive.boxed_message(['This setup was aborted. Open vStorage may be in an inconsistent state, make sure to validate the installation.'])
            logger.error('Keyboard interrupt')
            sys.exit(1)

    @staticmethod
    def remove_nodes(node_ips):
        """
        Remove the nodes with specified IPs from the cluster
        :param node_ips: IPs of nodes to remove
        :type node_ips: str
        :return: None
        """
        from ovs.lib.storagedriver import StorageDriverController
        from ovs.lib.storagerouter import StorageRouterController
        from ovs.dal.lists.storagerouterlist import StorageRouterList
        from ovs.dal.lists.vdisklist import VDiskList

        SetupController._log_message('+++ Remove nodes started +++')
        SetupController._log_message('\nWARNING: Some of these steps may take a very long time, please check /var/log/ovs/lib.log on this node for more logging information\n\n')

        ###############
        # VALIDATIONS #
        ###############
        if not isinstance(node_ips, str):
            raise ValueError('Node IPs must be a comma separated string of IPs or a single IP')

        storage_router_ips_to_remove = set()
        node_ips = node_ips.rstrip(',')
        for storage_router_ip in node_ips.split(','):
            storage_router_ip = storage_router_ip.strip()
            if not storage_router_ip:
                raise ValueError("An IP or multiple IPs of the Storage Routers to remove must be provided")
            if not re.match(SSHClient.IP_REGEX, storage_router_ip.strip()):
                raise ValueError('Invalid IP {0} specified'.format(storage_router_ip))
            storage_router_ips_to_remove.add(storage_router_ip)

        SetupController._log_message('Following nodes with IPs will be removed from the cluster: {0}'.format(list(storage_router_ips_to_remove)))
        storage_router_all = StorageRouterList.get_storagerouters()
        storage_router_masters = StorageRouterList.get_masters()
        storage_router_all_ips = set([storage_router.ip for storage_router in storage_router_all])
        storage_router_master_ips = set([storage_router.ip for storage_router in storage_router_masters])
        storage_routers_to_remove = [StorageRouterList.get_by_ip(storage_router_ip) for storage_router_ip in storage_router_ips_to_remove]
        unknown_ips = storage_router_ips_to_remove.difference(storage_router_all_ips)
        if unknown_ips:
            raise ValueError('Unknown IPs specified\nKnown in model:\n - {0}\nSpecified for removal:\n - {1}'.format('\n - '.join(storage_router_all_ips),
                                                                                                                     '\n - '.join(unknown_ips)))

        if len(storage_router_ips_to_remove) == len(storage_router_all_ips):
            raise RuntimeError("Removing all nodes wouldn't be very smart now, would it?")

        if not storage_router_master_ips.difference(storage_router_ips_to_remove):
            raise RuntimeError("Removing all master nodes wouldn't be very smart now, would it?")

        if System.get_my_storagerouter() in storage_routers_to_remove:
            raise RuntimeError('The node to be removed cannot be identical to the node on which the removal is initiated')

        SetupController._log_message('Creating SSH connections to remaining master nodes')
        master_ip = None
        ip_client_map = {}
        storage_routers_offline = []
        storage_routers_to_remove_online = []
        storage_routers_to_remove_offline = []
        for storage_router in storage_router_all:
            try:
                client = SSHClient(storage_router, username='root')
                if client.run('pwd'):
                    SetupController._log_message('  Node with IP {0:<15} successfully connected to'.format(storage_router.ip))
                    ip_client_map[storage_router.ip] = SSHClient(storage_router.ip, username='root')
                    if storage_router not in storage_routers_to_remove and storage_router.node_type == 'MASTER':
                        master_ip = storage_router.ip
                if storage_router in storage_routers_to_remove:
                    storage_routers_to_remove_online.append(storage_router)
            except UnableToConnectException:
                SetupController._log_message('  Node with IP {0:<15} is unreachable'.format(storage_router.ip))
                storage_routers_offline.append(storage_router)
                if storage_router in storage_routers_to_remove:
                    storage_routers_to_remove_offline.append(storage_router)

        if len(ip_client_map) == 0 or master_ip is None:
            raise RuntimeError('Could not connect to any master node in the cluster')

        if len(storage_routers_to_remove_online) > 0 and len(storage_routers_to_remove_offline) > 0:
            raise RuntimeError('Both on- and offline nodes have been specified for removal')  # Technically (might be) possible, but to prevent screw-ups

        online_storage_router_guids = [sr.guid for sr in storage_routers_to_remove_online]
        for vd in VDiskList.get_vdisks():
            if vd.storagerouter_guid and vd.storagerouter_guid in online_storage_router_guids:
                raise RuntimeError("Still vDisks attached to Storage Router with guid {0}".format(vd.storagerouter_guid))

        ###########
        # REMOVAL #
        ###########
        try:
            SetupController._log_message('Starting removal of nodes')
            for storage_router in storage_routers_to_remove:
                if storage_router in storage_routers_to_remove_offline:
                    SetupController._log_message('  Marking all Storage Drivers served by Storage Router {0} as offline'.format(storage_router.ip))
                    StorageDriverController.move_away(storagerouter_guid=storage_router.guid)
                    for storagedriver in storage_router.storagedrivers:
                        target_sr = None
                        for sd in storagedriver.vpool.storagedrivers:
                            sr = sd.storagerouter
                            if sr.guid != storage_router and sr not in storage_routers_to_remove and sr not in storage_routers_offline:
                                target_sr = sr
                                break
                        if target_sr is not None:
                            client = SSHClient(target_sr)
                            old_storage_router_path = '{0}/{1}'.format(storagedriver.mountpoint, storage_router.machine_id)
                            if client.dir_exists(old_storage_router_path):
                                # make sure files are "stolen" from the watcher
                                client.run('ls -Ral {0}'.format(SSHClient.shell_safe(old_storage_router_path)))

            for storage_router in storage_routers_to_remove:
                # 2. Remove vPools
                SetupController._log_message('  Cleaning up node with IP {0}'.format(storage_router.ip))
                storage_routers_offline_guids = [sr.guid for sr in storage_routers_offline if sr.guid != storage_router.guid]
                for storage_driver in storage_router.storagedrivers:
                    SetupController._log_message('    Removing vPool {0} from node'.format(storage_driver.vpool.name))
                    StorageRouterController.remove_storagedriver(storagedriver_guid=storage_driver.guid,
                                                                 offline_storage_router_guids=storage_routers_offline_guids)

                # 3. Demote if MASTER
                if storage_router.node_type == 'MASTER':
                    unconfigure_rabbitmq = True
                    unconfigure_memcached = True
                    preconfig = '/tmp/openvstorage_preconfig.cfg'
                    if os.path.exists(preconfig):
                        config = RawConfigParser()
                        config.read(preconfig)
                        unconfigure_memcached = config.getboolean('setup', 'configure_memcached')
                        unconfigure_rabbitmq = config.getboolean('setup', 'configure_rabbitmq')
                    SetupController._demote_node(cluster_ip=storage_router.ip,
                                                 master_ip=master_ip,
                                                 cluster_name=None,
                                                 ip_client_map=ip_client_map,
                                                 unique_id=storage_router.machine_id,
                                                 unconfigure_memcached=unconfigure_memcached,
                                                 unconfigure_rabbitmq=unconfigure_rabbitmq,
                                                 offline_nodes=storage_routers_offline)

                # 4. Clean up model
                SetupController._log_message('    Removing node from model')
                SetupController._run_hooks('remove', storage_router.ip)

                for disk in storage_router.disks:
                    for partition in disk.partitions:
                        partition.delete()
                    disk.delete()

                pmachine = storage_router.pmachine
                for vmachine in pmachine.vmachines:
                    vmachine.delete(abandon=['vdisks'])
                storage_router.delete()
                if len(pmachine.storagerouters) == 0:
                    pmachine.delete()
                EtcdConfiguration.delete('/ovs/framework/hosts/{0}'.format(storage_router.machine_id))

                master_ips = [sr.ip for sr in storage_router_masters]
                slave_ips = [sr.ip for sr in StorageRouterList.get_slaves()]
                offline_node_ips = [node.ip for node in storage_routers_offline]
                SetupController._restart_framework_and_memcache_services(master_ips, slave_ips, ip_client_map, offline_node_ips)

                SetupController._log_message('    Successfully removed node\n')
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
        SetupController._log_message('+++ Remove nodes finished +++')

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
        password = None
        if known_passwords:
            password = known_passwords.values()[0]
        for node in nodes:
            if node in known_passwords:
                passwords[node] = known_passwords[node]
                if node not in ip_client_map:
                    ip_client_map[node] = SSHClient(node, username='root', password=known_passwords[node])
                continue
            password = SetupController._ask_validate_password(node, username='root', previous=password)
            passwords[node] = password
            if node not in ip_client_map:
                ip_client_map[node] = SSHClient(node, username='root', password=password)

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
    def _setup_first_node(target_client, unique_id, cluster_name, node_name, hypervisor_info, enable_heartbeats,
                          configure_memcached, configure_rabbitmq, external_etcd):
        """
        Sets up the first node services. This node is always a master
        """
        print '\n+++ Setting up first node +++\n'
        logger.info('Setting up first node')
        cluster_ip = target_client.ip
        machine_id = System.get_my_machine_id(target_client)

        print 'Setting up Etcd'
        logger.info('Setting up Etcd')
        if external_etcd is None:
            EtcdInstaller.create_cluster('config', cluster_ip)
        else:
            EtcdInstaller.use_external(external_etcd, cluster_ip, 'config')
        EtcdConfiguration.initialize(external_etcd=external_etcd)
        EtcdConfiguration.initialize_host(machine_id)

        if ServiceManager.has_fleet():
            print('Setting up fleet ')
            logger.info('Setting up fleet')
            ServiceManager.setup_fleet()

        print 'Setting up Arakoon'
        logger.info('Setting up Arakoon')
        result = ArakoonInstaller.create_cluster('ovsdb', cluster_ip, EtcdConfiguration.get('/ovs/framework/paths|ovsdb'), locked=False)
        arakoon_ports = [result['client_port'], result['messaging_port']]

        SetupController._configure_logstash(target_client)
        SetupController._add_services(target_client, unique_id, 'master')

        print 'Build configuration files'
        logger.info('Build configuration files')

        if configure_rabbitmq:
            SetupController._configure_rabbitmq(target_client)
            EtcdConfiguration.set('/ovs/framework/messagequeue|endpoints', ['{0}:{1}'.format(cluster_ip, 5672)])
        if configure_memcached:
            SetupController._configure_memcached(target_client)
            EtcdConfiguration.set('/ovs/framework/memcache|endpoints', ['{0}:{1}'.format(cluster_ip, 11211)])

        print 'Starting model services'
        logger.debug('Starting model services')
        for service in SetupController.model_services:
            if ServiceManager.has_service(service, client=target_client):
                ServiceManager.enable_service(service, client=target_client)
                Toolbox.change_service_state(target_client, service, 'restart', logger)

        print 'Start model migration'
        logger.debug('Start model migration')
        from ovs.dal.helpers import Migration
        Migration.migrate()

        print '\n+++ Finalizing setup +++\n'
        logger.info('Finalizing setup')
        storagerouter = SetupController._finalize_setup(target_client, node_name, 'MASTER', hypervisor_info, unique_id)

        from ovs.dal.lists.servicelist import ServiceList
        if 'arakoon-ovsdb' not in [s.name for s in ServiceList.get_services()]:
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
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/ip'.format(machine_id), cluster_ip)

        print 'Starting services'
        logger.info('Starting services for join master')
        for service in [s for s in SetupController.master_services if s not in ['etcd-config']]:
            if ServiceManager.has_service(service, client=target_client):
                ServiceManager.enable_service(service, client=target_client)
                Toolbox.change_service_state(target_client, service, 'start', logger)
        # Enable HA for the rabbitMQ queues
        SetupController._check_rabbitmq_and_enable_ha_mode(target_client)

        ServiceManager.enable_service('watcher-framework', client=target_client)
        Toolbox.change_service_state(target_client, 'watcher-framework', 'start', logger)

        logger.debug('Restarting workers')
        ServiceManager.enable_service('workers', client=target_client)
        Toolbox.change_service_state(target_client, 'workers', 'restart', logger)

        SetupController._run_hooks('firstnode', cluster_ip)

        if enable_heartbeats is None:
            print '\n+++ Heartbeat +++\n'
            logger.info('Heartbeat')
            print Interactive.boxed_message(['Open vStorage has the option to send regular heartbeats with metadata to a centralized server.' +
                                             'The metadata contains anonymous data like Open vStorage\'s version and status of the Open vStorage services. These heartbeats are optional and can be turned on/off at any time via the GUI.'],
                                            character=None)
            enable_heartbeats = Interactive.ask_yesno('Do you want to enable Heartbeats?', default_value=True)
        if enable_heartbeats is False:
            EtcdConfiguration.set('/ovs/framework/support|enabled', False)
        else:
            service = 'support-agent'
            if not ServiceManager.has_service(service, target_client):
                ServiceManager.add_service(service, client=target_client)
                ServiceManager.enable_service(service, client=target_client)
                Toolbox.change_service_state(target_client, service, 'start', logger)

        if SetupController._avahi_installed(target_client) is True:
            SetupController._configure_avahi(target_client, cluster_name, node_name, 'master')
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/setupcompleted'.format(machine_id), True)
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/promotecompleted'.format(machine_id), True)
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/type'.format(machine_id), 'MASTER')
        EtcdConfiguration.set('/ovs/framework/install_time', time.time())
        target_client.run('chown -R ovs:ovs /opt/OpenvStorage/config')

        logger.info('First node complete')

    @staticmethod
    def _rollback_setup_first_node(target_client):
        """
        Rollback a failed setup - first node
        """
        print '\n+++ Rolling back setup of first node +++\n'
        logger.info('Rolling back setup of first node')

        cluster_ip = target_client.ip
        machine_id = System.get_my_machine_id(target_client)
        try:
            EtcdConfiguration.delete('/ovs/framework/hosts/{0}/ip'.format(machine_id))
            EtcdConfiguration.delete('/ovs/framework/hosts/{0}/setupcompleted'.format(machine_id))
            EtcdConfiguration.delete('/ovs/framework/hosts/{0}/type'.format(machine_id))
            EtcdConfiguration.delete('/ovs/framework/install_time')
        except Exception as ex:
            SetupController._print_log_error('clean config keys', ex)
        try:
            target_client.dir_delete('/opt/OpenvStorage/webapps/frontend/logging')
        except Exception as ex:
            SetupController._print_log_error('clean config files', ex)

        print 'Stopping services'
        logger.debug('Stopping services')
        for service in SetupController.model_services + ['watcher-framework', 'workers', 'support-agent']:
            if ServiceManager.has_service(service, client=target_client):
                ServiceManager.disable_service(service, client=target_client)
                Toolbox.change_service_state(target_client, service, 'stop', logger)

        print 'Remove configuration files'
        logger.info('Remove configuration files')
        try:
            if EtcdConfiguration.get('/ovs/framework/messagequeue|endpoints') is not None:
                try:
                    SetupController._unconfigure_rabbitmq(target_client)
                except Exception as ex:
                    SetupController._print_log_error('unconfigure rabbitmq', ex)
                EtcdConfiguration.delete('/ovs/framework/messagequeue|endpoints')
            if EtcdConfiguration.get('/ovs/framework/memcache|endpoints') is not None:
                ServiceManager.stop_service('memcached', target_client)
                EtcdConfiguration.delete('/ovs/framework/memcache|endpoints')
        except Exception as ex:
            SetupController._print_log_error('remove configuration files', ex)

        SetupController._remove_services(target_client, machine_id, 'master')
        SetupController._unconfigure_logstash(target_client)

        print 'Unconfigure Arakoon'
        logger.info('Unconfigure Arakoon')
        try:
            ArakoonInstaller.delete_cluster('ovsdb', cluster_ip)
        except Exception as ex:
            SetupController._print_log_error('delete cluster', ex)

        try:
            base_dir = EtcdConfiguration.get('/ovs/framework/paths|ovsdb')
            cluster_name = 'ovsdb'
            home_dir = ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, cluster_name)
            log_dir = ArakoonInstaller.ARAKOON_LOG_DIR.format(cluster_name)
            tlog_dir = ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, cluster_name)
            ArakoonInstaller.clean_leftover_arakoon_data(cluster_ip, {log_dir: True,
                                                                      home_dir: False,
                                                                      tlog_dir: False})
        except Exception as ex:
            SetupController._print_log_error('clean arakoon data', ex)

        print 'Unconfigure Etcd'
        logger.info('Unconfigure Etcd')
        try:
            external_etcd = EtcdConfiguration.get('/ovs/framework/external_etcd')
            if external_etcd is None:
                print 'Removing Etcd cluster'
                logger.info('Removing Etcd cluster')
                try:
                    EtcdInstaller.stop('config', target_client)
                    EtcdInstaller.remove('config', target_client)
                except Exception as ex:
                    SetupController._print_log_error('unconfigure etcd', ex)
        except Exception as ex:
            SetupController._print_log_error('unconfigure etcd', ex)

    @staticmethod
    def _setup_extra_node(cluster_ip, master_ip, cluster_name, unique_id, ip_client_map, hypervisor_info, external_etcd):
        """
        Sets up an additional node
        """

        print '\n+++ Adding extra node +++\n'
        logger.info('Adding extra node')
        target_client = ip_client_map[cluster_ip]
        machine_id = System.get_my_machine_id(target_client)

        print 'Configuring services'
        logger.info('Copying client configurations')
        if external_etcd is None:
            EtcdInstaller.deploy_to_slave(master_ip, cluster_ip, 'config')
        else:
            EtcdInstaller.use_external(external_etcd, cluster_ip, 'config')
        EtcdConfiguration.initialize_host(machine_id)

        if ServiceManager.has_fleet():
            print('Setting up fleet ')
            logger.info('Setting up fleet')
            ServiceManager.setup_fleet()

        SetupController._configure_logstash(target_client)
        SetupController._add_services(target_client, unique_id, 'extra')

        enabled = EtcdConfiguration.get('/ovs/framework/support|enabled')
        if enabled is True:
            service = 'support-agent'
            if not ServiceManager.has_service(service, target_client):
                ServiceManager.add_service(service, client=target_client)
                ServiceManager.enable_service(service, client=target_client)
                Toolbox.change_service_state(target_client, service, 'start', logger)

        node_name = target_client.run('hostname')
        SetupController._finalize_setup(target_client, node_name, 'EXTRA', hypervisor_info, unique_id)

        EtcdConfiguration.set('/ovs/framework/hosts/{0}/ip'.format(machine_id), cluster_ip)

        print 'Starting services'
        ServiceManager.enable_service('watcher-framework', client=target_client)
        Toolbox.change_service_state(target_client, 'watcher-framework', 'start', logger)

        logger.debug('Restarting workers')
        for node_client in ip_client_map.itervalues():
            ServiceManager.enable_service('workers', client=node_client)
            Toolbox.change_service_state(node_client, 'workers', 'restart', logger)

        SetupController._run_hooks('extranode', cluster_ip, master_ip)

        if SetupController._avahi_installed(target_client) is True:
            SetupController._configure_avahi(target_client, cluster_name, node_name, 'extra')
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/setupcompleted'.format(machine_id), True)
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/type'.format(machine_id), 'EXTRA')
        target_client.run('chown -R ovs:ovs /opt/OpenvStorage/config')
        logger.info('Extra node complete')

    @staticmethod
    def _rollback_setup_extra_node(target_client):
        print '\n+++ Rolling back add extra node +++\n'
        logger.info('Rolling back ad extra node')

        machine_id = System.get_my_machine_id(target_client)
        try:
            EtcdConfiguration.delete('/ovs/framework/hosts/{0}/ip'.format(machine_id))
            EtcdConfiguration.delete('/ovs/framework/hosts/{0}/setupcompleted'.format(machine_id))
            EtcdConfiguration.delete('/ovs/framework/hosts/{0}/type'.format(machine_id))
        except Exception as ex:
            SetupController._print_log_error('clean config keys', ex)
        try:
            target_client.dir_delete('/opt/OpenvStorage/webapps/frontend/logging')
        except Exception as ex:
            SetupController._print_log_error('clean config files', ex)

        print 'Stopping services'
        logger.debug('Stopping services')
        for service in SetupController.model_services + ['watcher-framework', 'workers', 'support-agent']:
            if ServiceManager.has_service(service, client=target_client):
                ServiceManager.disable_service(service, client=target_client)
                Toolbox.change_service_state(target_client, service, 'stop', logger)

        SetupController._remove_services(target_client, machine_id, 'extra')
        SetupController._unconfigure_logstash(target_client)

        print 'Unconfigure Etcd'
        logger.info('Unconfigure Etcd')
        try:
            external_etcd = EtcdConfiguration.get('/ovs/framework/external_etcd')
            if external_etcd is None:
                print 'Removing Etcd cluster'
                logger.info('Removing Etcd cluster')
                try:
                    EtcdInstaller.stop('config', target_client)
                    EtcdInstaller.remove('config', target_client)
                except Exception as ex:
                    SetupController._print_log_error('unconfigure etcd', ex)
        except Exception as ex:
            SetupController._print_log_error('unconfigure etcd', ex)

    @staticmethod
    def _promote_node(cluster_ip, master_ip, cluster_name, ip_client_map, unique_id, configure_memcached, configure_rabbitmq):
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
        machine_id = System.get_my_machine_id(target_client)
        node_name = target_client.run('hostname')
        master_client = ip_client_map[master_ip]

        storagerouter = StorageRouterList.get_by_machine_id(unique_id)
        storagerouter.node_type = 'MASTER'
        storagerouter.save()

        # Find other (arakoon) master nodes
        config = ArakoonClusterConfig('ovsdb')
        config.load_config()
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
        result = ArakoonInstaller.extend_cluster(master_ip, cluster_ip, 'ovsdb', EtcdConfiguration.get('/ovs/framework/paths|ovsdb'))
        arakoon_ports = [result['client_port'], result['messaging_port']]

        external_etcd = EtcdConfiguration.get('/ovs/framework/external_etcd')
        if external_etcd is None:
            print 'Joining etcd cluster'
            logger.info('Joining etcd cluster')
            EtcdInstaller.extend_cluster(master_ip, cluster_ip, 'config')

        print 'Update configurations'
        logger.info('Update configurations')
        if configure_memcached is True:
            endpoints = EtcdConfiguration.get('/ovs/framework/memcache|endpoints')
            endpoint = '{0}:{1}'.format(cluster_ip, 11211)
            if endpoint not in endpoints:
                endpoints.append(endpoint)
                EtcdConfiguration.set('/ovs/framework/memcache|endpoints', endpoints)
        if configure_rabbitmq is True:
            endpoints = EtcdConfiguration.get('/ovs/framework/messagequeue|endpoints')
            endpoint = '{0}:{1}'.format(cluster_ip, 5672)
            if endpoint not in endpoint:
                endpoints.append(endpoint)
                EtcdConfiguration.set('/ovs/framework/messagequeue|endpoints', endpoints)

        print 'Restarting master node services'
        logger.info('Restarting master node services')
        ArakoonInstaller.restart_cluster_add('ovsdb', master_nodes, cluster_ip)
        PersistentFactory.store = None
        VolatileFactory.store = None

        if 'arakoon-ovsdb' not in [s.name for s in ServiceList.get_services()]:
            service = Service()
            service.name = 'arakoon-ovsdb'
            service.type = ServiceTypeList.get_by_name('Arakoon')
            service.ports = arakoon_ports
            service.storagerouter = storagerouter
            service.save()

        if configure_rabbitmq:
            SetupController._configure_rabbitmq(target_client)
            # Copy rabbitmq cookie
            rabbitmq_cookie_file = '/var/lib/rabbitmq/.erlang.cookie'
            if not target_client.file_exists(rabbitmq_cookie_file):
                logger.debug('Copying Rabbit MQ cookie')

                contents = master_client.file_read(rabbitmq_cookie_file)
                master_hostname = master_client.run('hostname')
                target_client.dir_create(os.path.dirname(rabbitmq_cookie_file))
                target_client.file_write(rabbitmq_cookie_file, contents)
                target_client.file_attribs(rabbitmq_cookie_file, mode=400)
                target_client.run('rabbitmq-server -detached 2> /dev/null; sleep 5; rabbitmqctl stop_app; sleep 5;')
                target_client.run('rabbitmqctl join_cluster rabbit@{0}; sleep 5;'.format(master_hostname))
                target_client.run('rabbitmqctl stop; sleep 5;')

            # Enable HA for the rabbitMQ queues
            Toolbox.change_service_state(target_client, 'rabbitmq-server', 'start', logger)
            SetupController._check_rabbitmq_and_enable_ha_mode(target_client)

        SetupController._configure_amqp_to_volumedriver()

        print 'Starting services'
        logger.info('Starting services')
        for service in SetupController.master_services:
            if ServiceManager.has_service(service, client=target_client):
                ServiceManager.enable_service(service, client=target_client)
                Toolbox.change_service_state(target_client, service, 'start', logger)

        print 'Restarting services'
        master_ips = [sr.ip for sr in StorageRouterList.get_masters()]
        slave_ips = [sr.ip for sr in StorageRouterList.get_slaves()]
        SetupController._restart_framework_and_memcache_services(master_ips, slave_ips, ip_client_map)

        if SetupController._run_hooks('promote', cluster_ip, master_ip):
            print 'Restarting services'
            SetupController._restart_framework_and_memcache_services(master_ips, slave_ips, ip_client_map)

        if SetupController._avahi_installed(target_client) is True:
            SetupController._configure_avahi(target_client, cluster_name, node_name, 'master')
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/type'.format(machine_id), 'MASTER')
        target_client.run('chown -R ovs:ovs /opt/OpenvStorage/config')
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/promotecompleted'.format(machine_id), True)

        logger.info('Promote complete')

    @staticmethod
    def _demote_node(cluster_ip, master_ip, cluster_name, ip_client_map, unique_id, unconfigure_memcached, unconfigure_rabbitmq, offline_nodes=None):
        """
        Demotes a given node
        """
        from ovs.dal.lists.storagerouterlist import StorageRouterList

        print '\n+++ Demoting node +++\n'
        logger.info('Demoting node')

        if offline_nodes is None:
            offline_nodes = []

        if unconfigure_memcached is True and len(offline_nodes) == 0:
            if SetupController._validate_local_memcache_servers(ip_client_map) is False:
                raise RuntimeError('Not all memcache nodes can be reached which is required for demoting a node.')

        # Find other (arakoon) master nodes
        config = ArakoonClusterConfig('ovsdb')
        config.load_config()
        master_nodes = [node.ip for node in config.nodes]
        if cluster_ip in master_nodes:
            master_nodes.remove(cluster_ip)
        if len(master_nodes) == 0:
            raise RuntimeError('There should be at least one other master node')

        storagerouter = StorageRouterList.get_by_machine_id(unique_id)
        storagerouter.node_type = 'EXTRA'
        storagerouter.save()

        print 'Leaving arakoon ovsdb cluster'
        logger.info('Leaving arakoon ovsdb cluster')
        offline_node_ips = [node.ip for node in offline_nodes]
        ArakoonInstaller.shrink_cluster(cluster_ip, 'ovsdb', offline_node_ips)

        try:
            external_etcd = EtcdConfiguration.get('/ovs/framework/external_etcd')
            if external_etcd is None:
                print 'Leaving Etcd cluster'
                logger.info('Leaving Etcd cluster')
                EtcdInstaller.shrink_cluster(master_ip, cluster_ip, 'config', offline_node_ips)
        except Exception as ex:
            SetupController._print_log_error('leave etcd cluster', ex)

        print 'Update configurations'
        logger.info('Update configurations')
        try:
            if unconfigure_memcached is True:
                endpoints = EtcdConfiguration.get('/ovs/framework/memcache|endpoints')
                endpoint = '{0}:{1}'.format(cluster_ip, 11211)
                if endpoint in endpoints:
                    endpoints.remove(endpoint)
                EtcdConfiguration.set('/ovs/framework/memcache|endpoints', endpoints)
            if unconfigure_rabbitmq is True:
                endpoints = EtcdConfiguration.get('/ovs/framework/messagequeue|endpoints')
                endpoint = '{0}:{1}'.format(cluster_ip, 5672)
                if endpoint in endpoints:
                    endpoints.remove(endpoint)
                EtcdConfiguration.set('/ovs/framework/messagequeue|endpoints', endpoints)
        except Exception as ex:
            SetupController._print_log_error('update configurations', ex)

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

        if storagerouter in offline_nodes:
            if unconfigure_rabbitmq is True:
                print 'Removing/unconfiguring offline RabbitMQ node'
                logger.debug('Removing/unconfiguring offline RabbitMQ node')
                client = ip_client_map[master_ip]
                try:
                    client.run('rabbitmqctl forget_cluster_node rabbit@{0}'.format(storagerouter.name))
                except Exception as ex:
                    SetupController._print_log_error('forget RabbitMQ cluster node', ex)
        else:
            target_client = ip_client_map[cluster_ip]
            if unconfigure_rabbitmq is True:
                print 'Removing/unconfiguring RabbitMQ'
                logger.debug('Removing/unconfiguring RabbitMQ')
                try:
                    if ServiceManager.has_service('rabbitmq-server', client=target_client):
                        target_client.run('rabbitmq-server -detached 2> /dev/null; sleep 5; rabbitmqctl stop_app; sleep 5;')
                        target_client.run('rabbitmqctl reset; sleep 5;')
                        target_client.run('rabbitmqctl stop; sleep 5;')
                        Toolbox.change_service_state(target_client, 'rabbitmq-server', 'stop', logger)
                        target_client.file_unlink("/var/lib/rabbitmq/.erlang.cookie")
                except Exception as ex:
                    SetupController._print_log_error('remove/unconfigure RabbitMQ', ex)

            print 'Removing services'
            logger.info('Removing services')
            services = [s for s in SetupController.master_node_services if s not in (SetupController.extra_node_services + ['arakoon-ovsdb', 'etcd-config'])]
            if unconfigure_rabbitmq is False:
                services.remove('rabbitmq-server')
            if unconfigure_memcached is False:
                services.remove('memcached')
            for service in services:
                if ServiceManager.has_service(service, client=target_client):
                    logger.debug('Removing service {0}'.format(service))
                    try:
                        Toolbox.change_service_state(target_client, service, 'stop', logger)
                        ServiceManager.remove_service(service, client=target_client)
                    except Exception as ex:
                        SetupController._print_log_error('remove service {0}'.format(service), ex)

            if ServiceManager.has_service('workers', client=target_client):
                ServiceManager.add_service(name='workers',
                                           client=target_client,
                                           params={'MEMCACHE_NODE_IP': cluster_ip,
                                                   'WORKER_QUEUE': '{0}'.format(unique_id)})
        try:
            SetupController._configure_amqp_to_volumedriver()
        except Exception as ex:
            SetupController._print_log_error('configure amqp to volumedriver', ex)

        print 'Restarting services'
        logger.debug('Restarting services')
        master_ips = [sr.ip for sr in StorageRouterList.get_masters()]
        slave_ips = [sr.ip for sr in StorageRouterList.get_slaves()]
        SetupController._restart_framework_and_memcache_services(master_ips, slave_ips, ip_client_map, offline_node_ips)

        if SetupController._run_hooks('demote', cluster_ip, master_ip, offline_node_ips=offline_node_ips):
            print 'Restarting services'
            SetupController._restart_framework_and_memcache_services(master_ips, slave_ips, ip_client_map, offline_node_ips)

        if storagerouter not in offline_nodes:
            target_client = ip_client_map[cluster_ip]
            node_name = target_client.run('hostname')
            if SetupController._avahi_installed(target_client) is True:
                SetupController._configure_avahi(target_client, cluster_name, node_name, 'extra')
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/type'.format(storagerouter.machine_id), 'EXTRA')

        logger.info('Demote complete')

    @staticmethod
    def _restart_framework_and_memcache_services(masters, slaves, clients, offline_node_ips=None):
        if offline_node_ips is None:
            offline_node_ips = []
        memcached = 'memcached'
        watcher = 'watcher-framework'
        for ip in masters + slaves:
            if ip not in offline_node_ips:
                if ServiceManager.has_service(watcher, clients[ip]):
                    Toolbox.change_service_state(clients[ip], watcher, 'stop', logger)
        for ip in masters:
            if ip not in offline_node_ips:
                Toolbox.change_service_state(clients[ip], memcached, 'restart', logger)
        for ip in masters + slaves:
            if ip not in offline_node_ips:
                if ServiceManager.has_service(watcher, clients[ip]):
                    Toolbox.change_service_state(clients[ip], watcher, 'start', logger)
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
        rabbitmq_port = EtcdConfiguration.get('/ovs/framework/messagequeue|port')
        rabbitmq_login = EtcdConfiguration.get('/ovs/framework/messagequeue|user')
        rabbitmq_password = EtcdConfiguration.get('/ovs/framework/messagequeue|password')
        if not client.file_exists('/etc/rabbitmq/rabbitmq.config'):
            client.run("""cat > /etc/rabbitmq/rabbitmq.config << EOF
[
   {{rabbit, [{{tcp_listeners, [{0}]}},
              {{default_user, <<"{1}">>}},
              {{default_pass, <<"{2}">>}},
              {{log_levels, [{{connection, warning}}]}},
              {{vm_memory_high_watermark, 0.2}}]}}
].
EOF
""".format(rabbitmq_port, rabbitmq_login, rabbitmq_password))
        rabbitmq_running, same_process = SetupController._is_rabbitmq_running(client)
        if rabbitmq_running is True:
            Toolbox.change_service_state(client, 'rabbitmq-server', 'stop', logger)

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
    def _unconfigure_rabbitmq(client):
        rabbitmq_running, same_process = SetupController._is_rabbitmq_running(client)
        if rabbitmq_running is True:
            Toolbox.change_service_state(client, 'rabbitmq-server', 'stop', logger)
        client.file_delete('/etc/rabbitmq/rabbitmq.config')


    @staticmethod
    def _check_rabbitmq_and_enable_ha_mode(client):
        if not ServiceManager.has_service('rabbitmq-server', client):
            raise RuntimeError('Service rabbitmq-server has not been added on node {0}'.format(client.ip))
        rabbitmq_running, same_process = SetupController._is_rabbitmq_running(client)
        if rabbitmq_running is False or same_process is False:
            Toolbox.change_service_state(client, 'rabbitmq-server', 'restart', logger)

        client.run('sleep 5;rabbitmqctl set_policy ha-all "^(volumerouter|ovs_.*)$" \'{"ha-mode":"all"}\'')

    @staticmethod
    def _configure_amqp_to_volumedriver():
        print 'Update existing vPools'
        logger.info('Update existing vPools')
        login = EtcdConfiguration.get('/ovs/framework/messagequeue|user')
        password = EtcdConfiguration.get('/ovs/framework/messagequeue|password')
        protocol = EtcdConfiguration.get('/ovs/framework/messagequeue|protocol')

        uris = []
        for endpoint in EtcdConfiguration.get('/ovs/framework/messagequeue|endpoints'):
            uris.append({'amqp_uri': '{0}://{1}:{2}@{3}'.format(protocol, login, password, endpoint)})

        if EtcdConfiguration.dir_exists('/ovs/vpools'):
            for vpool_guid in EtcdConfiguration.list('/ovs/vpools'):
                for storagedriver_id in EtcdConfiguration.list('/ovs/vpools/{0}/hosts'.format(vpool_guid)):
                    storagedriver_config = StorageDriverConfiguration('storagedriver', vpool_guid, storagedriver_id)
                    storagedriver_config.load()
                    storagedriver_config.configure_event_publisher(events_amqp_routing_key=EtcdConfiguration.get('/ovs/framework/messagequeue|queues.storagedriver'),
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
            logger.debug("Logstash is not installed, skipping its configuration")
            return False

        print 'Configuring logstash'
        logger.info('Configuring logstash')
        if ServiceManager.has_service('logstash', client) is False:
            ServiceManager.add_service('logstash', client)
        Toolbox.change_service_state(client, 'logstash', 'restart', logger)

    @staticmethod
    def _unconfigure_logstash(client):
        if not SetupController._logstash_installed(client):
            logger.debug("Logstash is not installed, skipping its configuration")
            return False

        print 'Unconfiguring logstash'
        logger.info('Unconfiguring logstash')
        Toolbox.change_service_state(client, 'logstash', 'stop', logger)
        if ServiceManager.has_service('logstash', client) is True:
            ServiceManager.remove_service('logstash', client)


    @staticmethod
    def _configure_avahi(client, cluster_name, node_name, node_type):
        print '\n+++ Announcing service +++\n'
        logger.info('Announcing service')

        if client.file_exists(SetupController.avahi_filename):
            logger.info('Avahi already configured')
            return

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
        Toolbox.change_service_state(client, 'avahi-daemon', 'restart', logger)

    @staticmethod
    def _unconfigure_avahi(client):
        filename = SetupController.avahi_filename
        if client.file_exists(filename):
            client.file_delete(filename)

    @staticmethod
    def _add_services(client, unique_id, node_type):
        if node_type == 'master':
            services = SetupController.master_node_services
            if 'arakoon-ovsdb' in services:
                services.remove('arakoon-ovsdb')
            if 'etcd-config' in services:
                services.remove('etcd-config')
            worker_queue = '{0},ovs_masters'.format(unique_id)
        else:
            services = SetupController.extra_node_services
            worker_queue = unique_id

        print 'Adding services'
        logger.info('Adding services')
        params = {'MEMCACHE_NODE_IP': client.ip,
                  'WORKER_QUEUE': worker_queue}
        for service in services + ['watcher-framework']:
            if not ServiceManager.has_service(service, client):
                logger.debug('Adding service {0}'.format(service))
                ServiceManager.add_service(service, params=params, client=client)

    @staticmethod
    def _remove_services(client, unique_id, node_type):
        if node_type == 'master':
            services = SetupController.master_node_services
            if 'arakoon-ovsdb' in services:
                services.remove('arakoon-ovsdb')
            if 'etcd-config' in services:
                services.remove('etcd-config')
        else:
            services = SetupController.extra_node_services

        print 'Removing services'
        logger.info('Removing services')

        for service in services + ['support-agent', 'watcher-framework']:
            if ServiceManager.has_service(service, client=client):
                logger.debug('Removing service {0}'.format(service))
                ServiceManager.stop_service(service, client=client)
                ServiceManager.remove_service(service, client=client)

    @staticmethod
    def _finalize_setup(client, node_name, node_type, hypervisor_info, unique_id):
        cluster_ip = client.ip
        client.dir_create('/opt/OpenvStorage/webapps/frontend/logging')
        if SetupController._logstash_installed(client):
            Toolbox.change_service_state(client, 'logstash', 'restart', logger)
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
        Toolbox.change_service_state(client, 'dbus', 'start', logger)
        Toolbox.change_service_state(client, 'avahi-daemon', 'start', logger)
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
    def _run_hooks(hook_type, cluster_ip, master_ip=None, **kwargs):
        """
        Execute hooks
        """
        if hook_type not in ('firstnode', 'remove') and master_ip is None:
            raise ValueError('Master IP needs to be specified')

        functions = Toolbox.fetch_hooks('setup', hook_type)
        functions_found = len(functions) > 0
        if functions_found is True:
            print '\n+++ Running "{0}" hooks +++\n'.format(hook_type)
        for function in functions:
            if master_ip is None:
                function(cluster_ip=cluster_ip, **kwargs)
            else:
                function(cluster_ip=cluster_ip, master_ip=master_ip, **kwargs)
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
                if previous is not None:
                    try:
                        client = SSHClient(ip, username=username, password=previous)
                        client.run('ls /')
                        return previous
                    except:
                        pass
                password = Interactive.ask_password('Enter the {0} password for {1}'.format(
                    username,
                    ip if node_string is None else node_string
                ))
                if password in ['', None]:
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
        ips = [endpoint.split(':')[0] for endpoint in EtcdConfiguration.get('/ovs/framework/memcache|endpoints')]
        for ip in ips:
            if ip not in ip_client_map:
                return False
        return True

    @staticmethod
    def _print_log_error(action, exception):
        print '\n Failed to {0}. \n Error: {1} {2}'.format(action, type(exception), exception)
        logger.warning('Failed to {0}. Error: {1}'.format(action, exception))
