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

"""
Module for SetupController
"""

import os
import re
import sys
import copy
import time
import uuid
import glob
import base64
import urllib2
import subprocess
from string import digits
from pyudev import Context

from ConfigParser import RawConfigParser
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonInstaller, ArakoonClusterConfig
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.interactive import Interactive
from ovs.extensions.generic.remote import Remote
from ovs.extensions.generic.system import System
from ovs.log.logHandler import LogHandler
from ovs.lib.helpers.toolbox import Toolbox
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagementEx
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.generic.configuration import Configuration

logger = LogHandler('lib', name='setup')
logger.logger.propagate = False

# @TODO: Make the setup_node re-entrant
# @TODO: Make it possible to run as a non-privileged user
# @TODO: Node password identical for all nodes


class SetupController(object):
    """
    This class contains all logic for setting up an environment, installed with system-native packages
    """

    ARAKOON_OVSDB = 'arakoon-ovsdb'
    ARAKOON_VOLDRV = 'arakoon-voldrv'
    PARTITION_DEFAULTS = {'device': 'DIR_ONLY', 'percentage': 'NA', 'label': 'cache1', 'type': 'storage', 'ssd': False}

    # Arakoon
    arakoon_clusters = {'ovsdb': ARAKOON_OVSDB,
                        'voldrv': ARAKOON_VOLDRV}

    # Generic configfiles
    generic_configfiles = {'/opt/OpenvStorage/config/memcacheclient.cfg': 11211,
                           '/opt/OpenvStorage/config/rabbitmqclient.cfg': 5672}
    ovs_config_filename = '/opt/OpenvStorage/config/ovs.cfg'
    avahi_filename = '/etc/avahi/services/ovs_cluster.service'

    # Services
    model_services = ['memcached', ARAKOON_OVSDB]
    master_services = model_services + ['rabbitmq', ARAKOON_VOLDRV]
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
        auto_config = None
        disk_layout = None
        arakoon_mountpoint = None
        join_cluster = False
        enable_heartbeats = None
        ip_client_map = {}

        # Support non-interactive setup
        preconfig = '/tmp/openvstorage_preconfig.cfg'
        if os.path.exists(preconfig):
            config = RawConfigParser()
            config.read(preconfig)
            ip = config.get('setup', 'target_ip')
            target_password = config.get('setup', 'target_password')
            cluster_ip = config.get('setup', 'cluster_ip')
            cluster_name = str(config.get('setup', 'cluster_name'))
            master_ip = config.get('setup', 'master_ip')
            hypervisor_type = config.get('setup', 'hypervisor_type')
            hypervisor_name = config.get('setup', 'hypervisor_name')
            hypervisor_ip = config.get('setup', 'hypervisor_ip')
            hypervisor_username = config.get('setup', 'hypervisor_username')
            hypervisor_password = config.get('setup', 'hypervisor_password')
            arakoon_mountpoint = config.get('setup', 'arakoon_mountpoint')
            verbose = config.getboolean('setup', 'verbose')
            auto_config = config.getboolean('setup', 'auto_config')
            disk_layout = eval(config.get('setup', 'disk_layout'))
            join_cluster = config.getboolean('setup', 'join_cluster')
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
                target_node_password = Interactive.ask_password('Enter the root password for {0}'.format(node_string))
            else:
                target_node_password = target_password
            target_client = SSHClient(ip, username='root', password=target_node_password)
            ip_client_map[ip] = target_client

            logger.debug('Target client loaded')

            print '\n+++ Collecting cluster information +++\n'
            logger.info('Collecting cluster information')

            # Check whether running local or remote
            unique_id = System.get_my_machine_id(target_client)
            local_unique_id = System.get_my_machine_id()
            remote_install = unique_id != local_unique_id
            logger.debug('{0} installation'.format('Remote' if remote_install else 'Local'))
            if not target_client.file_exists(SetupController.ovs_config_filename):
                raise RuntimeError("The 'openvstorage' package is not installed on {0}".format(ip))
            target_client.config_set('ovs.core.uniqueid', unique_id)

            # Getting cluster information
            current_cluster_names = []
            clusters = []
            discovery_result = SetupController._discover_nodes(target_client)
            if discovery_result:
                clusters = discovery_result.keys()
                current_cluster_names = clusters[:]
                logger.debug('Cluster names: {0}'.format(current_cluster_names))
            else:
                print 'No existing Open vStorage clusters are found.'
                logger.debug('No clusters found')

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
                if len(clusters) > 0:
                    clusters.sort()
                    dont_join = "Don't join any of these clusters."
                    logger.debug('Manual cluster selection')
                    if force_type in [None, 'master']:
                        clusters = [dont_join] + clusters
                    print 'Following Open vStorage clusters are found.'
                    cluster_name = Interactive.ask_choice(clusters, 'Select a cluster to join', default_value=local_cluster_name, sort_choices=False)
                    if cluster_name != dont_join:
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
                        # @TODO: we should be able to choose the ip here too in a multiple nic setup?
                        master_ip = discovery_result[cluster_name][master_nodes[0]]['ip']
                        master_password = Interactive.ask_password('Enter the root password for {0}'.format(master_ip))
                        known_passwords[master_ip] = master_password
                        if master_ip not in ip_client_map:
                            ip_client_map[master_ip] = SSHClient(master_ip, username='root', password=master_password)
                        first_node = False
                    else:
                        cluster_name = None
                        logger.debug('No cluster will be joined')
                elif force_type is not None and force_type != 'master':
                    raise RuntimeError('No clusters were found. Only a Master node can be set up.')

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
                if cluster_name in discovery_result:
                    SetupController.discovered_nodes = discovery_result[cluster_name]
                    # @TODO: update the ip to the chosen one in autoconfig file?
                    nodes = [node_property['ip'] for node_property in discovery_result[cluster_name].values()]
                first_node = not join_cluster
            if not cluster_name:
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

            mountpoints, hypervisor_info, writecaches, ip_client_map = SetupController._prepare_node(cluster_ip=cluster_ip,
                                                                                                     nodes=nodes,
                                                                                                     known_passwords=known_passwords,
                                                                                                     ip_client_map=ip_client_map,
                                                                                                     hypervisor_info={'type': hypervisor_type,
                                                                                                                      'name': hypervisor_name,
                                                                                                                      'username': hypervisor_username,
                                                                                                                      'ip': hypervisor_ip,
                                                                                                                      'password': hypervisor_password},
                                                                                                     auto_config=auto_config,
                                                                                                     disk_layout=disk_layout)
            if first_node is True:
                SetupController._setup_first_node(target_client=ip_client_map[cluster_ip],
                                                  unique_id=unique_id,
                                                  mountpoints=mountpoints,
                                                  cluster_name=cluster_name,
                                                  node_name=node_name,
                                                  hypervisor_info=hypervisor_info,
                                                  arakoon_mountpoint=arakoon_mountpoint,
                                                  enable_heartbeats=enable_heartbeats,
                                                  writecaches=writecaches)
            else:
                # Deciding master/extra
                print 'Analyzing cluster layout'
                logger.info('Analyzing cluster layout')
                promote = False
                for cluster in SetupController.arakoon_clusters:
                    config = ArakoonClusterConfig(cluster)
                    config.load_config(SSHClient(master_ip, username='root', password=known_passwords[master_ip]))
                    logger.debug('{0} nodes for cluster {1} found'.format(len(config.nodes), cluster))
                    if (len(config.nodes) < 3 or force_type == 'master') and force_type != 'extra':
                        promote = True
                        break

                SetupController._setup_extra_node(cluster_ip=cluster_ip,
                                                  master_ip=master_ip,
                                                  cluster_name=cluster_name,
                                                  unique_id=unique_id,
                                                  ip_client_map=ip_client_map,
                                                  hypervisor_info=hypervisor_info)
                if promote:
                    SetupController._promote_node(cluster_ip=cluster_ip,
                                                  master_ip=master_ip,
                                                  cluster_name=cluster_name,
                                                  ip_client_map=ip_client_map,
                                                  unique_id=unique_id,
                                                  mountpoints=mountpoints,
                                                  arakoon_mountpoint=arakoon_mountpoint,
                                                  writecaches=writecaches)

            print ''
            print Interactive.boxed_message(['Setup complete.',
                                             'Point your browser to http://{0} to use Open vStorage'.format(cluster_ip)])
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

            if not os.path.exists(SetupController.avahi_filename):
                raise RuntimeError('No local OVS setup found.')
            with open(SetupController.avahi_filename, 'r') as avahi_file:
                avahi_contents = avahi_file.read()

            if node_action == 'promote' and '_ovs_master_node._tcp' in avahi_contents:
                raise RuntimeError('This node is already master.')
            elif node_action == 'demote' and '_ovs_master_node._tcp' not in avahi_contents:
                raise RuntimeError('This node should be a master.')

            match_groups = re.search('>ovs_cluster_(?P<cluster>[^_]+)_.+?<', avahi_contents).groupdict()
            if 'cluster' not in match_groups:
                raise RuntimeError('No cluster information found.')
            cluster_name = match_groups['cluster']

            target_password = Interactive.ask_password('Enter the root password for this node')
            target_client = SSHClient('127.0.0.1', username='root', password=target_password)
            discovery_result = SetupController._discover_nodes(target_client)
            master_nodes = [this_node_name for this_node_name, node_properties in discovery_result[cluster_name].iteritems() if node_properties.get('type') == 'master']
            nodes = [node_property['ip'] for node_property in discovery_result[cluster_name].values()]
            if len(master_nodes) == 0:
                if node_action == 'promote':
                    raise RuntimeError('No master node could be found in cluster {0}'.format(cluster_name))
                else:
                    raise RuntimeError('It is not possible to remove the only master in cluster {0}'.format(cluster_name))
            master_ip = discovery_result[cluster_name][master_nodes[0]]['ip']

            ovs_config = target_client.rawconfig_read(SetupController.ovs_config_filename)
            unique_id = ovs_config.get('core', 'uniqueid')
            ip = ovs_config.get('grid', 'ip')
            nodes.append(ip)  # The client node is never included in the discovery results

            ip_client_map = dict((node_ip, SSHClient(node_ip, username='root', password=target_password)) for node_ip in nodes if node_ip)
            if node_action == 'promote':
                SetupController._promote_node(cluster_ip=ip,
                                              master_ip=master_ip,
                                              cluster_name=cluster_name,
                                              ip_client_map=ip_client_map,
                                              unique_id=unique_id,
                                              mountpoints=None,
                                              arakoon_mountpoint=None,
                                              writecaches=None)
            else:
                SetupController._demote_node(cluster_ip=ip,
                                             master_ip=master_ip,
                                             cluster_name=cluster_name,
                                             ip_client_map=ip_client_map,
                                             unique_id=unique_id)

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
    def _prepare_node(cluster_ip, nodes, known_passwords, ip_client_map, hypervisor_info, auto_config, disk_layout):
        """
        Prepares a node:
        - Exchange SSH keys
        - Update hosts files
        - Partitioning
        - Request hypervisor information
        """

        print '\n+++ Preparing node +++\n'
        logger.info('Preparing node')

        # Exchange ssh keys
        print 'Exchanging SSH keys'
        logger.info('Exchanging SSH keys')
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
                prev_node_password = Interactive.ask_password('Enter root password for {0}'.format(node))
                logger.debug('Custom password for {0}'.format(node))
                passwords[node] = prev_node_password
                first_request = False
                if node not in ip_client_map:
                    ip_client_map[node] = SSHClient(node, username='root', password=prev_node_password)
            else:
                this_node_password = Interactive.ask_password('Enter root password for {0}, just press enter if identical as above'.format(node))
                if this_node_password == '':
                    logger.debug('Identical password for {0}'.format(node))
                    this_node_password = prev_node_password
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
                    if not existing_key in authorized_keys:
                        authorized_keys += "{0}\n".format(existing_key)
            if node_client.file_exists(authorized_keys_filename.format(ovs_ssh_folder)):
                existing_keys = node_client.file_read(authorized_keys_filename.format(ovs_ssh_folder))
                for existing_key in existing_keys:
                    if not existing_key in authorized_keys:
                        authorized_keys += "{0}\n".format(existing_key)
            root_pub_key = node_client.file_read(public_key_filename.format(root_ssh_folder))
            ovs_pub_key = node_client.file_read(public_key_filename.format(ovs_ssh_folder))
            if not root_pub_key in authorized_keys:
                authorized_keys += '{0}\n'.format(root_pub_key)
            if not ovs_pub_key in authorized_keys:
                authorized_keys += '{0}\n'.format(ovs_pub_key)
            node_hostname = node_client.run('hostname')
            all_hostnames.add(node_hostname)
            mapping[node] = node_hostname

        print 'Updating hosts files'
        logger.debug('Updating hosts files')
        for node, node_client in ip_client_map.iteritems():
            for hostname_node, hostname in mapping.iteritems():
                System.update_hosts_file(hostname, hostname_node, node_client)
            node_client.file_write(authorized_keys_filename.format(root_ssh_folder), authorized_keys)
            node_client.file_write(authorized_keys_filename.format(ovs_ssh_folder), authorized_keys)
            cmd = 'cp {1} {1}.tmp; ssh-keyscan -t rsa {0} {2} 2> /dev/null >> {1}.tmp; cat {1}.tmp | sort -u - > {1}'
            node_client.run(cmd.format(' '.join(all_ips), known_hosts_filename.format(root_ssh_folder), ' '.join(all_hostnames)))
            cmd = 'su - ovs -c "cp {1} {1}.tmp; ssh-keyscan -t rsa {0} {2} 2> /dev/null  >> {1}.tmp; cat {1}.tmp | sort -u - > {1}"'
            node_client.run(cmd.format(' '.join(all_ips), known_hosts_filename.format(ovs_ssh_folder), ' '.join(all_hostnames)))

        # Creating filesystems
        print 'Creating filesystems'
        logger.info('Creating filesystems')

        target_client = ip_client_map[cluster_ip]
        disk_layout = SetupController.apply_flexible_disk_layout(target_client, auto_config, disk_layout)

        # add directory mountpoints to ovs.cfg
        config = target_client.rawconfig_read(SetupController.ovs_config_filename)
        partition_key = 'vpool_partitions'
        if config.has_section(partition_key):
            config.remove_section(partition_key)
        config.add_section(partition_key)

        readcaches = list()
        writecaches = list()
        storage = list()
        for mountpoint, details in disk_layout.iteritems():
            if 'readcache' in details['type']:
                readcaches.append(mountpoint)
                continue
            elif 'writecache' in details['type']:
                writecaches.append(mountpoint)
                continue
            else:
                storage.append(mountpoint)

        config.set(partition_key, 'readcaches', ','.join(map(str, readcaches)))
        config.set(partition_key, 'writecaches', ','.join(map(str, writecaches)))
        config.set(partition_key, 'storage', ','.join(map(str, storage)))
        target_client.rawconfig_write(SetupController.ovs_config_filename, config)

        mountpoints = disk_layout.keys()
        mountpoints.sort()

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
            hypervisor_info['password'] = passwords[cluster_ip]
            hypervisor_info['username'] = 'root'
        logger.debug('Hypervisor at {0} with username {1}'.format(hypervisor_info['ip'], hypervisor_info['username']))

        return mountpoints, hypervisor_info, writecaches, ip_client_map

    @staticmethod
    def _setup_first_node(target_client, unique_id, mountpoints, cluster_name, node_name, hypervisor_info, arakoon_mountpoint, enable_heartbeats, writecaches):
        """
        Sets up the first node services. This node is always a master
        """

        print '\n+++ Setting up first node +++\n'
        logger.info('Setting up first node')

        print 'Setting up Arakoon'
        logger.info('Setting up Arakoon')
        # Loading arakoon mountpoint
        cluster_ip = target_client.ip
        if arakoon_mountpoint is None:
            arakoon_mountpoint = Interactive.ask_choice(mountpoints, question='Select arakoon database mountpoint',
                                                        default_value=writecaches[0] if writecaches else '')
        target_client.config_set('ovs.core.db.arakoon.location', arakoon_mountpoint)
        target_client.config_set('ovs.arakoon.base.dir', arakoon_mountpoint)
        arakoon_ports = {}
        exclude_ports = []
        for cluster in SetupController.arakoon_clusters:
            result = ArakoonInstaller.create_cluster(cluster, cluster_ip, exclude_ports)
            arakoon_ports[cluster] = [result['client_port'], result['messaging_port']]
            exclude_ports += arakoon_ports[cluster]

        SetupController._configure_logstash(target_client, cluster_name)
        SetupController._add_services(target_client, unique_id, 'master')
        SetupController._configure_rabbitmq(target_client)

        print 'Build configuration files'
        logger.info('Build configuration files')
        for config_file, port in SetupController.generic_configfiles.iteritems():
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
                SetupController._change_service_state(target_client, service, 'start')

        print 'Start model migration'
        logger.debug('Start model migration')
        from ovs.dal.helpers import Migration
        Migration.migrate()

        print '\n+++ Finalizing setup +++\n'
        logger.info('Finalizing setup')
        storagerouter = SetupController._finalize_setup(target_client, node_name, 'MASTER', hypervisor_info, unique_id)

        from ovs.dal.lists.servicetypelist import ServiceTypeList
        from ovs.dal.hybrids.service import Service
        arakoonservice_type = ServiceTypeList.get_by_name('Arakoon')
        for cluster, ports in arakoon_ports.iteritems():
            service = Service()
            service.name = SetupController.arakoon_clusters[cluster]
            service.type = arakoonservice_type
            service.ports = ports
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
                SetupController._change_service_state(target_client, service, 'start')
        # Enable HA for the rabbitMQ queues
        SetupController._start_rabbitmq_and_check_process(target_client)

        for service in ['watcher-framework', 'watcher-volumedriver']:
            ServiceManager.enable_service(service, client=target_client)
            SetupController._change_service_state(target_client, service, 'start')

        logger.debug('Restarting workers')
        ServiceManager.enable_service('workers', client=target_client)
        SetupController._change_service_state(target_client, 'workers', 'restart')

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
            target_client.config_set('ovs.support.enabled', 1)
            service = 'support-agent'
            ServiceManager.add_service(service, client=target_client)
            ServiceManager.enable_service(service, client=target_client)
            SetupController._change_service_state(target_client, service, 'start')

        SetupController._configure_avahi(target_client, cluster_name, node_name, 'master')
        target_client.run('chown -R ovs:ovs /opt/OpenvStorage/config')

        logger.info('First node complete')

    @staticmethod
    def _setup_extra_node(cluster_ip, master_ip, cluster_name, unique_id, ip_client_map, hypervisor_info):
        """
        Sets up an additional node
        """

        print '\n+++ Adding extra node +++\n'
        logger.info('Adding extra node')

        target_client = ip_client_map[cluster_ip]
        SetupController._configure_logstash(target_client, cluster_name)
        SetupController._add_services(target_client, unique_id, 'extra')

        print 'Configuring services'
        logger.info('Copying client configurations')
        for cluster in SetupController.arakoon_clusters:
            ArakoonInstaller.deploy_to_slave(master_ip, cluster_ip, cluster)
        master_client = ip_client_map[master_ip]
        for config in SetupController.generic_configfiles.keys():
            client_config = master_client.rawconfig_read(config)
            target_client.rawconfig_write(config, client_config)

        cid = master_client.config_read('ovs.support.cid')
        enabled = master_client.config_read('ovs.support.enabled')
        enablesupport = master_client.config_read('ovs.support.enablesupport')
        target_client.config_set('ovs.support.nid', str(uuid.uuid4()))
        target_client.config_set('ovs.support.cid', cid)
        target_client.config_set('ovs.support.enabled', enabled)
        target_client.config_set('ovs.support.enablesupport', enablesupport)
        if int(enabled) > 0:
            service = 'support-agent'
            ServiceManager.add_service(service, client=target_client)
            ServiceManager.enable_service(service, client=target_client)
            SetupController._change_service_state(target_client, service, 'start')

        node_name = target_client.run('hostname')
        SetupController._finalize_setup(target_client, node_name, 'EXTRA', hypervisor_info, unique_id)

        print 'Updating configuration files'
        logger.info('Updating configuration files')
        target_client.config_set('ovs.grid.ip', cluster_ip)

        print 'Starting services'
        for service in ['watcher-framework', 'watcher-volumedriver']:
            ServiceManager.enable_service(service, client=target_client)
            SetupController._change_service_state(target_client, service, 'start')

        logger.debug('Restarting workers')
        for node_client in ip_client_map.itervalues():
            ServiceManager.enable_service('workers', client=node_client)
            SetupController._change_service_state(node_client, 'workers', 'restart')

        SetupController._run_hooks('extranode', cluster_ip, master_ip)

        SetupController._configure_avahi(target_client, cluster_name, node_name, 'extra')
        target_client.run('chown -R ovs:ovs /opt/OpenvStorage/config')
        logger.info('Extra node complete')

    @staticmethod
    def _promote_node(cluster_ip, master_ip, cluster_name, ip_client_map, unique_id, mountpoints, arakoon_mountpoint, writecaches):
        """
        Promotes a given node
        """
        from ovs.dal.lists.storagerouterlist import StorageRouterList
        from ovs.dal.lists.servicetypelist import ServiceTypeList
        from ovs.dal.lists.servicelist import ServiceList
        from ovs.dal.hybrids.service import Service

        print '\n+++ Promoting node +++\n'
        logger.info('Promoting node')

        target_client = ip_client_map[cluster_ip]
        node_name = target_client.run('hostname')

        storagerouter = StorageRouterList.get_by_machine_id(unique_id)
        storagerouter.node_type = 'MASTER'
        storagerouter.save()

        # Find other (arakoon) master nodes
        master_nodes = []
        for cluster in SetupController.arakoon_clusters:
            config = ArakoonClusterConfig(cluster)
            config.load_config(SSHClient(master_ip, username='root'))
            master_nodes = [node.ip for node in config.nodes]
            if cluster_ip in master_nodes:
                master_nodes.remove(cluster_ip)
        if len(master_nodes) == 0:
            raise RuntimeError('There should be at least one other master node')

        SetupController._configure_logstash(target_client, cluster_name)
        SetupController._add_services(target_client, unique_id, 'master')

        print 'Joining arakoon cluster'
        logger.info('Joining arakoon cluster')
        # Loading arakoon mountpoint
        if arakoon_mountpoint is None:
            if mountpoints:
                manual = 'Enter custom path'
                mountpoints.sort()
                mountpoints = [manual] + mountpoints
                arakoon_mountpoint = Interactive.ask_choice(mountpoints, question='Select arakoon database mountpoint',
                                                            default_value=writecaches[0] if len(writecaches) > 0 else None,
                                                            sort_choices=False)
                if arakoon_mountpoint == manual:
                    arakoon_mountpoint = None
            if arakoon_mountpoint is None:
                while True:
                    arakoon_mountpoint = Interactive.ask_string('Enter arakoon database path').strip().rstrip('/')
                    if target_client.dir_exists(arakoon_mountpoint):
                        break
                    else:
                        print '  Invalid path, please retry'
        target_client.config_set('ovs.core.db.arakoon.location', arakoon_mountpoint)
        target_client.config_set('ovs.arakoon.base.dir', arakoon_mountpoint)
        arakoon_ports = {}
        exclude_ports = ServiceList.get_ports_for_ip(cluster_ip)
        for cluster in SetupController.arakoon_clusters:
            result = ArakoonInstaller.extend_cluster(master_ip, cluster_ip, cluster, exclude_ports)
            arakoon_ports[cluster] = [result['client_port'], result['messaging_port']]
            exclude_ports += arakoon_ports[cluster]

        print 'Distribute configuration files'
        logger.info('Distribute configuration files')
        master_client = ip_client_map[master_ip]
        for config_file, port in SetupController.generic_configfiles.iteritems():
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
        for cluster in SetupController.arakoon_clusters:
            ArakoonInstaller.restart_cluster_add(cluster, master_nodes, cluster_ip)
        PersistentFactory.store = None
        VolatileFactory.store = None

        arakoonservice_type = ServiceTypeList.get_by_name('Arakoon')
        for cluster, ports in arakoon_ports.iteritems():
            service = Service()
            service.name = SetupController.arakoon_clusters[cluster]
            service.type = arakoonservice_type
            service.ports = ports
            service.storagerouter = storagerouter
            service.save()

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
        SetupController._change_service_state(target_client, 'rabbitmq', 'start')
        SetupController._start_rabbitmq_and_check_process(target_client)
        SetupController._configure_amqp_to_volumedriver(ip_client_map)

        print 'Starting services'
        logger.info('Starting services')
        for service in SetupController.master_services:
            if ServiceManager.has_service(service, client=target_client):
                ServiceManager.enable_service(service, client=target_client)
                SetupController._change_service_state(target_client, service, 'start')

        print 'Restarting services'
        SetupController._change_service_state(target_client, 'watcher-volumedriver', 'restart')
        SetupController._restart_framework_and_memcache_services(ip_client_map)

        if SetupController._run_hooks('promote', cluster_ip, master_ip):
            print 'Restarting services'
            SetupController._restart_framework_and_memcache_services(ip_client_map)

        SetupController._configure_avahi(target_client, cluster_name, node_name, 'master')
        target_client.run('chown -R ovs:ovs /opt/OpenvStorage/config')

        logger.info('Promote complete')

    @staticmethod
    def _demote_node(cluster_ip, master_ip, cluster_name, ip_client_map, unique_id):
        """
        Demotes a given node
        """
        from ovs.dal.lists.storagerouterlist import StorageRouterList

        print '\n+++ Demoting node +++\n'
        logger.info('Demoting node')

        target_client = ip_client_map[cluster_ip]
        node_name = target_client.run('hostname')

        storagerouter = StorageRouterList.get_by_machine_id(unique_id)
        storagerouter.node_type = 'EXTRA'
        storagerouter.save()

        # Find other (arakoon) master nodes
        master_nodes = []
        for cluster in SetupController.arakoon_clusters:
            config = ArakoonClusterConfig(cluster)
            config.load_config(SSHClient(master_ip, username='root'))
            master_nodes = [node.ip for node in config.nodes]
            if cluster_ip in master_nodes:
                master_nodes.remove(cluster_ip)
        if len(master_nodes) == 0:
            raise RuntimeError('There should be at least one other master node')

        print 'Leaving arakoon cluster'
        logger.info('Leaving arakoon cluster')
        for cluster in SetupController.arakoon_clusters:
            ArakoonInstaller.shrink_cluster(master_ip, cluster_ip, cluster)

        SetupController._configure_amqp_to_volumedriver(ip_client_map)

        print 'Distribute configuration files'
        logger.info('Distribute configuration files')
        master_client = ip_client_map[master_ip]
        for config_file, port in SetupController.generic_configfiles.iteritems():
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
        for cluster in SetupController.arakoon_clusters:
            ArakoonInstaller.restart_cluster_remove(cluster, remaining_nodes)
        PersistentFactory.store = None
        VolatileFactory.store = None

        service_names = []
        for cluster in SetupController.arakoon_clusters:
            service_names.append(SetupController.arakoon_clusters[cluster])
        for service in storagerouter.services:
            if service.name in service_names:
                service.delete()

        print 'Removing/unconfiguring RabbitMQ'
        logger.debug('Removing/unconfiguring RabbitMQ')
        if ServiceManager.has_service('rabbitmq', client=target_client):
            target_client.run('rabbitmq-server -detached 2> /dev/null; sleep 5; rabbitmqctl stop_app; sleep 5;')
            target_client.run('rabbitmqctl reset; sleep 5;')
            target_client.run('rabbitmqctl stop; sleep 5;')
            SetupController._change_service_state(target_client, 'rabbitmq', 'stop')
            ServiceManager.remove_service('rabbitmq', client=target_client)
            target_client.file_unlink("/var/lib/rabbitmq/.erlang.cookie")

        print 'Removing services'
        logger.info('Removing services')
        for service in [s for s in SetupController.master_node_services if s not in (SetupController.extra_node_services + [SetupController.ARAKOON_OVSDB, SetupController.ARAKOON_VOLDRV])]:
            if ServiceManager.has_service(service, client=target_client):
                logger.debug('Removing service {0}'.format(service))
                SetupController._change_service_state(target_client, service, 'stop')
                ServiceManager.remove_service(service, client=target_client)

        if ServiceManager.has_service('workers', client=target_client):
            ServiceManager.add_service(name='workers',
                                       client=target_client,
                                       params={'MEMCACHE_NODE_IP': cluster_ip,
                                               'WORKER_QUEUE': '{0}'.format(unique_id)})

        print 'Restarting services'
        logger.debug('Restarting services')
        SetupController._change_service_state(target_client, 'watcher-volumedriver', 'restart')
        SetupController._restart_framework_and_memcache_services(ip_client_map, target_client)

        if SetupController._run_hooks('demote', cluster_ip, master_ip):
            print 'Restarting services'
            SetupController._restart_framework_and_memcache_services(ip_client_map, target_client)

        SetupController._configure_avahi(target_client, cluster_name, node_name, 'extra')

        logger.info('Demote complete')

    @staticmethod
    def _restart_framework_and_memcache_services(ip_client_map, memcached_exclude_client=None):
        for service_info in [('watcher-framework', 'stop'),
                             ('memcached', 'restart'),
                             ('watcher-framework', 'start')]:
            for node_client in ip_client_map.itervalues():
                if memcached_exclude_client is not None and memcached_exclude_client.ip == node_client.ip and service_info[0] == 'memcached':
                    continue  # Skip memcached for demoted nodes, because they don't run that service
                SetupController._change_service_state(node_client, service_info[0], service_info[1])

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
              {{default_pass, <<"{2}">>}}]}}
].
EOF
""".format(rabbitmq_port, rabbitmq_login, rabbitmq_password))
        rabbitmq_running, rabbitmq_pid, _, _ = SetupController._is_rabbitmq_running(client)
        if rabbitmq_running is True and rabbitmq_pid:
            print('  WARNING: an instance of rabbitmq-server is running, this needs to be stopped')
            try:
                client.run('service rabbitmq-server stop')
            except subprocess.CalledProcessError:
                print('  Failure stopping the rabbitmq-server process')
            time.sleep(5)
            try:
                client.run('kill {0}'.format(rabbitmq_pid))
                print('  Process killed')
            except subprocess.CalledProcessError:
                print('  Process already stopped')
        client.run('rabbitmq-server -detached 2> /dev/null; sleep 5;')

        retry = 0
        while retry < 5:
            try:
                users = client.run('rabbitmqctl list_users').splitlines()[1:-1]
                users = [usr.split('\t')[0] for usr in users]

                if 'ovs' not in users:
                    client.run('rabbitmqctl add_user {0} {1}'.format(rabbitmq_login, rabbitmq_password))
                    client.run('rabbitmqctl set_permissions {0} ".*" ".*" ".*"'.format(rabbitmq_login))
                break
            except subprocess.CalledProcessError as cpe:
                logger.error(cpe)
                time.sleep(5)
                retry += 1
        client.run('rabbitmqctl stop; sleep 5;')

    @staticmethod
    def _start_rabbitmq_and_check_process(client):
        output = client.run('sleep 5;rabbitmqctl set_policy ha-all "^(volumerouter|ovs_.*)$" \'{"ha-mode":"all"}\'').splitlines()
        for line in output:
            if 'Error: unable to connect to node ' in line:
                rabbitmq_running, rabbitmq_pid, _, _ = SetupController._is_rabbitmq_running(client)
                if rabbitmq_running is True and rabbitmq_pid:
                    client.run('kill {0}'.format(rabbitmq_pid))
                    print('  Process killed, restarting')
                    client.run('service ovs-rabbitmq start')
                    client.run('sleep 5;rabbitmqctl set_policy ha-all "^(volumerouter|ovs_.*)$" \'{"ha-mode":"all"}\'')
                    break

        rabbitmq_running, rabbitmq_pid, ovs_rabbitmq_running, same_process = SetupController._is_rabbitmq_running(client)
        if ovs_rabbitmq_running is True and same_process is True:  # Correct process is running
            pass
        elif rabbitmq_running is True and ovs_rabbitmq_running is False:  # Wrong process is running, must be stopped and correct one started
            print('  WARNING: an instance of rabbitmq-server is running, this needs to be stopped, ovs-rabbitmq will be started instead')
            client.run('service rabbitmq-server stop')
            time.sleep(5)
            try:
                client.run('kill {0}'.format(rabbitmq_pid))
                print('  Process killed')
            except SystemExit:
                print('  Process already stopped')
            client.run('service ovs-rabbitmq start')
        elif rabbitmq_running is False and ovs_rabbitmq_running is False:  # Neither running
            client.run('service ovs-rabbitmq start')

    @staticmethod
    def _configure_amqp_to_volumedriver(node_ips):
        print 'Update existing vPools'
        logger.info('Update existing vPools')
        for node_ip in node_ips:
            with Remote(node_ip, [os, RawConfigParser, Configuration, StorageDriverConfiguration, ArakoonManagementEx], 'ovs') as remote:
                login = remote.Configuration.get('ovs.core.broker.login')
                password = remote.Configuration.get('ovs.core.broker.password')
                protocol = remote.Configuration.get('ovs.core.broker.protocol')

                cfg = remote.RawConfigParser()
                cfg.read('/opt/OpenvStorage/config/rabbitmqclient.cfg')

                uris = []
                for node in [n.strip() for n in cfg.get('main', 'nodes').split(',')]:
                    uris.append({'amqp_uri': '{0}://{1}:{2}@{3}'.format(protocol, login, password, cfg.get(node, 'location'))})

                arakoon_cluster_config = remote.ArakoonManagementEx().getCluster('voldrv').getClientConfig()
                arakoon_nodes = []
                for node_id, node_config in arakoon_cluster_config.iteritems():
                    arakoon_nodes.append({'host': node_config[0][0],
                                          'port': node_config[1],
                                          'node_id': node_id})
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
                        storagedriver_config.configure_volume_registry(vregistry_arakoon_cluster_id='voldrv',
                                                                       vregistry_arakoon_cluster_nodes=arakoon_nodes)
                        storagedriver_config.configure_event_publisher(events_amqp_routing_key=remote.Configuration.get('ovs.core.broker.volumerouter.queue'),
                                                                       events_amqp_uris=uris)
                        storagedriver_config.save()

    @staticmethod
    def _configure_logstash(client, cluster_name):
        print 'Configuring logstash'
        logger.info('Configuring logstash')
        SetupController._replace_param_in_config(client=client,
                                                 config_file='/etc/logstash/conf.d/indexer.conf',
                                                 old_value='<CLUSTER_NAME>',
                                                 new_value='ovses_{0}'.format(cluster_name))
        SetupController._change_service_state(client, 'logstash', 'restart')

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
        SetupController._change_service_state(client, 'avahi-daemon', 'restart')

    @staticmethod
    def _add_services(client, unique_id, node_type):
        if node_type == 'master':
            services = SetupController.master_node_services
            if SetupController.ARAKOON_VOLDRV in services:
                services.remove(SetupController.ARAKOON_VOLDRV)
            if SetupController.ARAKOON_OVSDB in services:
                services.remove(SetupController.ARAKOON_OVSDB)
            worker_queue = '{0},ovs_masters'.format(unique_id)
        else:
            services = SetupController.extra_node_services
            worker_queue = unique_id

        print 'Adding services'
        logger.info('Adding services')
        params = {'MEMCACHE_NODE_IP': client.ip,
                  'WORKER_QUEUE': worker_queue}
        for service in services + ['watcher-framework', 'watcher-volumedriver']:
            logger.debug('Adding service {0}'.format(service))
            ServiceManager.add_service(service, params=params, client=client)

    @staticmethod
    def _finalize_setup(client, node_name, node_type, hypervisor_info, unique_id):
        cluster_ip = client.ip
        client.dir_create('/opt/OpenvStorage/webapps/frontend/logging')
        SetupController._change_service_state(client, 'logstash', 'restart')
        SetupController._replace_param_in_config(client=client,
                                                 config_file='/opt/OpenvStorage/webapps/frontend/logging/config.js',
                                                 old_value='http://"+window.location.hostname+":9200',
                                                 new_value='http://' + cluster_ip + ':9200')

        # Imports, not earlier than here, as all required config files should be in place.
        from ovs.dal.hybrids.pmachine import PMachine
        from ovs.dal.lists.pmachinelist import PMachineList
        from ovs.dal.hybrids.storagerouter import StorageRouter
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
            storagerouter = StorageRouter()
            storagerouter.name = node_name
            storagerouter.machine_id = unique_id
            storagerouter.ip = cluster_ip
        storagerouter.node_type = node_type
        storagerouter.pmachine = pmachine
        storagerouter.save()
        return storagerouter

    @staticmethod
    def _get_disk_configuration(client):
        """
        Connect to target host and retrieve sata/ssd/raid configuration
        """
        with Remote(client.ip, [glob, os, Context]) as remote:
            def get_value(location):
                file_open = remote.os.open(location, remote.os.O_RDONLY)
                file_content = remote.os.read(file_open, 10240)
                remote.os.close(file_open)
                return str(file_content)

            content = get_value('/etc/mtab')
            boot_device = None
            for line in content.splitlines():
                if ' / ' in line:
                    boot_partition = line.split()[0]
                    boot_device = boot_partition.lstrip('/dev/').translate(None, digits)

            blk_devices = dict()
            devices = [remote.os.path.basename(device_path) for device_path in remote.glob.glob('/sys/block/*')]
            matching_devices = [device for device in devices if re.match('^(?:sd|fio|vd|xvd).*', device)]

            for matching_device in matching_devices:
                model = ''
                sectors = get_value('/sys/block/{0}/size'.format(matching_device))
                sector_size = get_value('/sys/block/{0}/queue/hw_sector_size'.format(matching_device))
                rotational = get_value('/sys/block/{0}/queue/rotational'.format(matching_device))
                context = remote.Context()
                devices = context.list_devices(subsystem='block')
                is_raid_member = False

                for entry in devices:
                    if matching_device not in entry['DEVNAME']:
                        continue

                    if entry['DEVTYPE'] == 'partition' and 'ID_FS_USAGE' in entry:
                        if 'raid' in entry['ID_FS_USAGE'].lower():
                            is_raid_member = True

                    if entry['DEVTYPE'] == 'disk' and 'ID_MODEL' in entry:
                        model = str(entry['ID_MODEL'])

                if 'fio' in matching_device:
                    model = 'FUSIONIO'

                device_details = {'size': float(sectors) * float(sector_size),
                                  'type': 'disk' if '1' in rotational else 'ssd',
                                  'software_raid': is_raid_member,
                                  'model': model,
                                  'boot_device': matching_device == boot_device}

                blk_devices[matching_device] = device_details

        # cross-check ssd devices - flawed detection on vmware
        for disk in blk_devices.keys():
            output = str(client.run("hdparm -I {0} 2> /dev/null | grep 'Solid State' || true".format('/dev/' + disk)).strip())
            if 'Solid State' in output and blk_devices[disk]['type'] == 'disk':
                print 'Updating device type for /dev/{0} to ssd'.format(disk)
                blk_devices[disk]['type'] = 'ssd'

        return blk_devices

    @staticmethod
    def _generate_default_partition_layout(blk_devices):
        """
        Process detected block devices while
        - ignoring bootdevice unless it's the only one
        - ignoring devices part of a software raid

        """

        mps_to_allocate = {'/mnt/cache1': {'device': 'DIR_ONLY', 'ssd': False, 'percentage': 100, 'label': 'cache1', 'type': 'writecache'},
                           '/mnt/cache2': {'device': 'DIR_ONLY', 'ssd': False, 'percentage': 100, 'label': 'cache2', 'type': 'readcache'},
                           '/mnt/bfs': {'device': 'DIR_ONLY', 'ssd': False, 'percentage': 100, 'label': 'backendfs', 'type': 'storage'},
                           '/var/tmp': {'device': 'DIR_ONLY', 'ssd': False, 'percentage': 100, 'label': 'tempfs', 'type': 'storage'}}

        selected_devices = dict(blk_devices)
        skipped_devices = set()
        for device, values in blk_devices.iteritems():
            if values['boot_device']:
                skipped_devices.add(device)
            if values['software_raid']:
                skipped_devices.add(device)

        for device in skipped_devices:
            selected_devices.pop(device)

        ssd_devices = list()
        disk_devices = list()

        for device, values in selected_devices.iteritems():
            if values['type'] == 'ssd':
                ssd_devices.append('/dev/' + device)
            if values['type'] == 'disk':
                disk_devices.append('/dev/' + device)

        nr_of_ssds = len(ssd_devices)
        nr_of_disks = len(disk_devices)

        print '{0} ssd devices: {1}'.format(nr_of_ssds, str(ssd_devices))
        print '{0} sata drives: {1}'.format(nr_of_disks, str(disk_devices))
        print

        if nr_of_ssds == 1:
            mps_to_allocate['/mnt/cache1']['device'] = ssd_devices[0]
            mps_to_allocate['/mnt/cache1']['percentage'] = 50
            mps_to_allocate['/mnt/cache1']['label'] = 'cache1'
            mps_to_allocate['/mnt/cache1']['type'] = 'writecache'
            mps_to_allocate['/mnt/cache2']['device'] = ssd_devices[0]
            mps_to_allocate['/mnt/cache2']['percentage'] = 50
            mps_to_allocate['/mnt/cache2']['label'] = 'cache2'
            mps_to_allocate['/mnt/cache2']['type'] = 'readcache'

        elif nr_of_ssds > 1:
            for count in xrange(nr_of_ssds):
                marker = '/mnt/cache' + str(count + 1)
                mps_to_allocate[marker] = dict()
                mps_to_allocate[marker]['device'] = ssd_devices[count]
                mps_to_allocate[marker]['type'] = 'readcache' if count > 0 else 'writecache'
                mps_to_allocate[marker]['percentage'] = 100
                mps_to_allocate[marker]['label'] = 'cache' + str(count + 1)

        for mp, values in mps_to_allocate.iteritems():
            if values['device'] in ssd_devices:
                mps_to_allocate[mp]['ssd'] = True
            else:
                mps_to_allocate[mp]['ssd'] = False

        return mps_to_allocate, skipped_devices

    @staticmethod
    def _partition_disks(client, partition_layout):
        fstab_entry = 'LABEL={0}    {1}         ext4    defaults,nobootwait,noatime,discard    0    2'
        fstab_separator = ('# BEGIN Open vStorage', '# END Open vStorage')  # Don't change, for backwards compatibility
        mounted = [device.strip() for device in client.run("cat /etc/mtab | cut -d ' ' -f 2").strip().splitlines()]

        boot_disk = ''
        unique_disks = set()
        for mp, values in partition_layout.iteritems():
            unique_disks.add(values['device'])
            if 'boot_device' in values and values['boot_device']:
                boot_disk = values['device']
                print 'Boot disk {0} will not be cleared'.format(boot_disk)
            # Umount partitions
            if mp in mounted:
                print 'Unmounting {0}'.format(mp)
                client.run('umount {0}'.format(mp))

        mounted_devices = [device.strip() for device in client.run("cat /etc/mtab | cut -d ' ' -f 1").strip().splitlines()]

        for mounted_device in mounted_devices:
            for chosen_device in unique_disks:
                if boot_disk in mounted_device:
                    continue
                if chosen_device in mounted_device:
                    print 'Unmounting {0}'.format(mounted_device)
                    client.run('umount {0}'.format(mounted_device))

        # Wipe disks
        for disk in unique_disks:
            if disk == 'DIR_ONLY':
                continue
            if disk == boot_disk:
                continue

            print "Partitioning disk {0}".format(disk)
            client.run('parted {0} -s mklabel gpt'.format(disk))

        # Pre process partition info (disk as key)
        mountpoints = partition_layout.keys()
        mountpoints.sort()
        partitions_by_disk = dict()
        for mp in mountpoints:
            partition = partition_layout[mp]
            disk = partition['device']
            percentage = partition['percentage']
            label = partition['label']
            if disk in partitions_by_disk:
                partitions_by_disk[disk].append((mp, percentage, label))
            else:
                partitions_by_disk[disk] = [(mp, percentage, label)]

        # Partition and format disks
        mpts_to_mount = []
        fstab_entries = ['{0} - Do not edit anything in this block'.format(fstab_separator[0])]
        for disk, partitions in partitions_by_disk.iteritems():
            if disk == 'DIR_ONLY':
                for directory, _, _ in partitions:
                    client.dir_create(directory)
                continue

            if disk == boot_disk:
                for mp, percentage, label in partitions:
                    print 'Partitioning free space on bootdisk: {0}'.format(disk)
                    command = """parted {0} """.format(boot_disk)
                    command += """unit % print free | grep 'Free Space' | tail -n1 | awk '{print $1}'"""
                    start = int(float(client.run(command).split('%')[0])) + 1
                    nr_of_partitions = int(client.run('lsblk {0} | grep part | wc -l'.format(boot_disk))) + 1
                    client.run('parted -s -a optimal {0} unit % mkpart primary ext4 {1}% 100%'.format(disk, start))
                    fstab_entries.append(fstab_entry.format(label, mp))
                    client.run('mkfs.ext4 -q {0} -L {1}'.format(boot_disk + str(nr_of_partitions), label))
                    mpts_to_mount.append(mp)
                continue

            start = '2MB'
            count = 1
            for mp, percentage, label in partitions:
                if start == '2MB':
                    size_in_percentage = int(percentage)
                    client.run('parted {0} -s mkpart {1} {2} {3}%'.format(disk, label, start, size_in_percentage))
                else:
                    size_in_percentage = int(start) + int(percentage)
                    client.run('parted {0} -s mkpart {1} {2}% {3}%'.format(disk, label, start, size_in_percentage))
                client.run('mkfs.ext4 -q {0} -L {1}'.format(disk + str(count), label))
                fstab_entries.append(fstab_entry.format(label, mp))
                mpts_to_mount.append(mp)
                count += 1
                start = size_in_percentage

        fstab_entries.append(fstab_separator[1])

        # Update fstab
        original_content = [line.strip() for line in client.file_read('/etc/fstab').strip().splitlines()]
        new_content = []
        skip = False
        for line in original_content:
            if skip is False:
                if line.startswith(fstab_separator[0]):
                    skip = True
                else:
                    new_content.append(line)
            elif line.startswith(fstab_separator[1]):
                skip = False
        new_content += fstab_entries
        client.file_write('/etc/fstab', '{0}\n'.format('\n'.join(new_content)))

        print 'Mounting all partitions ...'
        for mp in mpts_to_mount:
            client.dir_create(mp)
            client.run('mount {0}'.format(mp))
        client.run('chmod 1777 /var/tmp')

    @staticmethod
    def apply_flexible_disk_layout(client, auto_config=False, default=None):
        def print_and_sleep(message, sleep_count=1):
            if not message.startswith('\n>>>'):
                message = '\n>>> {0}'.format(message)
            if not message.endswith('\n'):
                message = '{0}\n'.format(message)
            print message
            time.sleep(sleep_count)

        def show_layout(proposed):
            print
            print 'Proposed partition layout:'
            print
            print '! Mark fastest ssd device as writecache'
            print '! Leave /mnt/bfs as DIR_ONLY when not using a local vpool'
            print
            key_map = list()
            longest_mp = max([len(mp) for mp in proposed])
            for mp in sorted(proposed):
                key_map.append(mp)
                if not proposed[mp]['device'] or proposed[mp]['device'] == 'DIR_ONLY':
                    print "{0:{1}} :  device : DIR_ONLY".format(mp, longest_mp)
                    continue

                mp_values = ''
                for dict_key in sorted(proposed[mp]):
                    value = str(proposed[mp][dict_key])
                    if dict_key == 'device' and value and value != 'DIR_ONLY':
                        size = device_size_map[value]
                        size_in_gb = int(size / 1000.0 / 1000.0 / 1000.0)
                        value = value + ' ({0} GB)'.format(size_in_gb)
                    if dict_key == 'device':
                        mp_values += ' {0} : {1:20}'.format(dict_key, value)
                    elif dict_key == 'label':
                        mp_values += ' {0} : {1:10}'.format(dict_key, value)
                    else:
                        mp_values += ' {0} : {1:5}'.format(dict_key, value)

                print "{0:{1}} : {2}".format(mp, longest_mp, mp_values)

            return key_map

        def check_percentages(percentage_mapping, device_to_check):
            total_percentage_assigned = sum([perc for perc in percentage_mapping[device_to_check].itervalues()])
            if total_percentage_assigned > 100:
                print_and_sleep('More than 100% specified for device {0}, please update manually'.format(device_to_check))
            elif total_percentage_assigned < 100:
                print_and_sleep('Less than 100% specified for device {0}, please update manually if required'.format(device_to_check))

        blk_devices = SetupController._get_disk_configuration(client)
        boot_disk = ''
        # check for free space on bootdevice
        if auto_config is False:
            for disk, values in blk_devices.iteritems():
                if values['boot_device'] and values['type'] in ['ssd']:
                    boot_disk += disk
                    break

            if not boot_disk:
                print 'No SSD boot disk detected ...'
                print 'Skipping partitioning of free space on boot disk ...'
            else:
                command = """parted /dev/{0} """.format(boot_disk)
                command += """unit GB print free | grep 'Free Space' | tail -n1 | awk '{print $3}'"""
                free_space = float(client.run(command).split('GB')[0])

                if free_space < 1.0:
                    print 'Skipping auto partitioning of free space on bootdisk as it is < 1 GiB'
                    print 'If required partition and mount manually for use in add vpool'
                    boot_disk = ''

        skipped = set()
        if default is None:
            default, skipped = SetupController._generate_default_partition_layout(blk_devices)

        ssd_devices = set([mountpoint_info['device'] for mountpoint_info in default.itervalues() if mountpoint_info.get('ssd', False) is True])

        print 'Excluded: {0}'.format(skipped)
        print '-> bootdisk or part of software RAID configuration'
        print

        if boot_disk:
            print 'Adding free space on boot disk - only mountpoint, label and type can be changed!'
            default['/mnt/os_cache'] = {'device': "/dev/{0}".format(boot_disk), 'ssd': True, 'boot_device': True, 'percentage': 100, 'label': 'os_cache', 'type': 'readcache'}

        device_size_map = dict()
        for key, values in blk_devices.iteritems():
            device_size_map['/dev/' + key] = values['size']

        if auto_config is True:
            SetupController._partition_disks(client, default)
            return default

        choices = show_layout(default)
        percentage_map = {}
        for mountpoint, info in default.iteritems():
            device = info.get('device')
            if device == 'DIR_ONLY':
                continue

            if device not in percentage_map:
                percentage_map[device] = {mountpoint: 0}
            elif mountpoint not in percentage_map[device]:
                percentage_map[device][mountpoint] = 0
            percentage_map[device][mountpoint] += info['percentage']

        while True:
            menu_actions = ['Add', 'Remove', 'Update', 'Print', 'Apply', 'Quit']
            chosen = Interactive.ask_choice(menu_actions, 'Make a choice', sort_choices=False)

            if chosen == 'Add':
                to_add = Interactive.ask_string('Enter mountpoint to add')
                if to_add in default:
                    print_and_sleep('Mountpoint {0} already exists'.format(to_add))
                else:
                    # Calculcate new default labelname
                    label_counters = []
                    for mountpoint_info in default.itervalues():
                        if re.match('^cache[0-9]{1,2}$', mountpoint_info['label'].strip()):
                            label_counters.append(int(mountpoint_info['label'].strip().split('cache')[1]))

                    new_label = None
                    for new_counter in xrange(1, 1000):
                        if new_counter in label_counters:
                            continue
                        new_label = 'cache{0}'.format(new_counter)
                        break
                    default[to_add] = dict(SetupController.PARTITION_DEFAULTS)
                    default[to_add]['label'] = new_label

                choices = show_layout(default)

            elif chosen == 'Remove':
                to_remove = Interactive.ask_string('Enter mountpoint to remove')
                if to_remove in default:
                    copied_map = copy.deepcopy(percentage_map)
                    for device, mp_info in copied_map.iteritems():
                        for mountp in copied_map[device]:
                            if mountp == to_remove:
                                percentage_map[device].pop(to_remove)
                    default.pop(to_remove)
                else:
                    print_and_sleep('Mountpoint {0} not found, no action taken'.format(to_remove))
                choices = show_layout(default)

            elif chosen == 'Quit':
                return 'QUIT'

            elif chosen == 'Print':
                show_layout(default)

            elif chosen == 'Update':
                to_update = Interactive.ask_choice(choices)
                subitem = default[to_update]
                is_boot_device = 'boot_device' in subitem and subitem['boot_device'] is True
                submenu_items = subitem.keys()
                if 'boot_device' in submenu_items:
                    submenu_items.remove('boot_device')
                submenu_items.remove('ssd')
                submenu_items.append('mountpoint')
                submenu_items.append('finish')

                while True:
                    for sub_key in sorted(subitem):
                        print "{0:15} : {1}".format(sub_key, subitem[sub_key])
                    print "{0:15} : {1}".format('mountpoint', to_update)
                    print

                    subitem_chosen = Interactive.ask_choice(submenu_items, sort_choices=False)
                    if subitem_chosen == 'finish':
                        break
                    elif is_boot_device and subitem_chosen not in ['type', 'label']:
                        print '\nOnly mountpoint, label and type can be changed for a boot device'
                        print 'All free space will be allocated to the new partition'
                        time.sleep(1)
                    elif subitem_chosen == 'percentage':
                        answer = Interactive.ask_integer('Please specify the percentage: ', 1, 100)
                        subitem['percentage'] = answer
                        device = subitem['device']
                        # Recalculate percentages
                        if device in percentage_map:
                            percentage_map[device][to_update] = answer
                            if len(percentage_map[device]) != 2:
                                check_percentages(percentage_map, device)
                            else:
                                total_percentage = sum([percent for percent in percentage_map[device].itervalues()])
                                if total_percentage > 100:
                                    for mountp in percentage_map[device].iterkeys():
                                        if mountp != to_update:
                                            percentage_map[device][mountp] = 100 - answer
                                            default[mountp]['percentage'] = 100 - answer
                                            print_and_sleep('Overallocation detected, updated {0} on device {1} to {2}%'.format(mountp, device, 100 - answer))
                    elif subitem_chosen == 'type':
                        answer = Interactive.ask_choice(['readcache', 'writecache', 'storage'], 'Please set the type', 'storage', False)
                        subitem['type'] = answer
                    elif subitem_chosen == 'label':
                        answer = Interactive.ask_string('Please set the label')
                        if not re.match('^[a-z]+[0-9]*', answer):
                            print_and_sleep('Invalid entry {0} for label'.format(answer))
                        else:
                            subitem['label'] = answer
                    elif subitem_chosen == 'mountpoint':
                        answer = Interactive.ask_string('Please set the mountpoint')
                        if not re.match('^/(?:[a-zA-Z0-9_-]+/)*[a-zA-Z0-9_-]+$', answer):
                            print_and_sleep('Invalid entry {0} for mountpoint'.format(answer))
                        elif answer in default:
                            print_and_sleep('New mountpoint already exists!')
                        else:
                            default.pop(to_update)
                            default[answer] = subitem
                            for dev, mp_info in percentage_map.iteritems():
                                for mountp in mp_info.iterkeys():
                                    if mountp == to_update:
                                        percentage = percentage_map[dev].pop(mountp)
                                        percentage_map[dev][answer] = percentage
                            to_update = answer
                    elif subitem_chosen == 'device':
                        answer = Interactive.ask_string('Please set the device')
                        if not re.match('^/dev/[a-z]{3}$', answer):
                            print_and_sleep('Invalid entry {0} for device'.format(answer))
                        elif answer not in device_size_map:
                            print_and_sleep('Device {0} does not exist'.format(answer))
                        elif answer == subitem['device']:
                            print_and_sleep('Same device specified, nothing will be updated')
                        else:
                            mountpoint = to_update
                            orig_device = subitem['device']
                            percentage = subitem['percentage']
                            subitem['ssd'] = answer in ssd_devices
                            subitem['device'] = answer

                            if orig_device in percentage_map:
                                # Update original device
                                percentage_map[orig_device].pop(mountpoint)
                                if len(percentage_map[orig_device]) > 1:
                                    check_percentages(percentage_map, orig_device)

                                # Update new device
                                if answer in percentage_map:
                                    percentage_map[answer][mountpoint] = percentage
                                    if len(percentage_map[answer]) == 2:
                                        default[mountpoint]['percentage'] = percentage
                                        total_percentage = sum([percent for percent in percentage_map[answer].itervalues()])
                                        if total_percentage > 100:
                                            for mountp in percentage_map[answer].iterkeys():
                                                if mountp != mountpoint:
                                                    percentage_map[answer][mountp] = 100 - percentage
                                                    default[mountp]['percentage'] = 100 - percentage
                                                    print_and_sleep('Overallocation detected, updated {0} on device {1} to {2}%'.format(mountp, answer, 100 - percentage))
                                    elif len(percentage_map[answer]) > 2:
                                        check_percentages(percentage_map, answer)

                            if answer not in percentage_map:
                                percentage_map[answer] = {mountpoint: percentage if isinstance(percentage, int) else 0}
                            elif mountpoint not in percentage_map[answer]:
                                percentage_map[answer][mountpoint] = percentage if isinstance(percentage, int) else 0
                                check_percentages(percentage_map, answer)

                choices = show_layout(default)

            elif chosen == 'Apply':
                total = dict()
                valid_percentages = True
                for details in default.itervalues():
                    device = details['device']
                    if device == 'DIR_ONLY':
                        continue
                    if details['percentage'] == 'NA' or details['percentage'] == 0:
                        print '>>> Invalid percentage value for device: {0}'.format(device)
                        print
                        time.sleep(1)
                        valid_percentages = False
                        break
                    percentage = int(details['percentage'])
                    if device in total:
                        total[device] += percentage
                    else:
                        total[device] = percentage

                if valid_percentages is True:
                    for device, percentage in total.iteritems():
                        if 0 < percentage <= 100:
                            continue
                        else:
                            print '>>> Invalid total {0}% for device: {1}'.format(percentage, device)
                            print
                            time.sleep(1)
                            valid_percentages = False
                            break

                if valid_percentages is False:
                    choices = show_layout(default)
                    continue

                valid_labels = True
                partitions = set()
                nr_of_labels = 0
                for details in default.itervalues():
                    if 'DIR_ONLY' not in details['device']:
                        partitions.add(details['label'])
                        nr_of_labels += 1
                if len(partitions) < nr_of_labels:
                    print '! Partition labels should be unique across partitions'
                    print
                    time.sleep(1)
                    valid_labels = False

                if valid_labels is False:
                    choices = show_layout(default)
                    continue

                show_layout(default)
                confirmation = Interactive.ask_yesno('Please confirm the partition layout, ALL DATA WILL BE ERASED ON THE DISKS ABOVE!', False)
                if confirmation is True:
                    print 'Applying partition layout ...'
                    SetupController._partition_disks(client, default)
                    return default
                else:
                    print 'Please confirm by typing "y"'

    @staticmethod
    def _discover_nodes(client):
        nodes = {}
        ipaddresses = client.run("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1").strip().splitlines()
        ipaddresses = [found_ip.strip() for found_ip in ipaddresses if found_ip.strip() != '127.0.0.1']
        SetupController.host_ips = set(ipaddresses)
        SetupController._change_service_state(client, 'dbus', 'start')
        SetupController._change_service_state(client, 'avahi-daemon', 'start')
        discover_result = client.run('timeout -k 60 45 avahi-browse -artp 2> /dev/null | grep ovs_cluster || true')
        logger.debug('Avahi discovery result:\n{0}'.format(discover_result))
        for entry in discover_result.splitlines():
            entry_parts = entry.split(';')
            if entry_parts[0] == '=' and entry_parts[2] == 'IPv4' and entry_parts[7] not in ipaddresses:
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
    def _change_service_state(client, name, state):
        """
        Starts/stops/restarts a service
        """
        action = None
        status = ServiceManager.get_service_status(name, client=client)
        if status is False and state in ['start', 'restart']:
            ServiceManager.start_service(name, client=client)
            action = 'started'
        elif status is True and state == 'stop':
            ServiceManager.stop_service(name, client=client)
            action = 'stopped'
        elif status is True and state == 'restart':
            ServiceManager.restart_service(name, client=client)
            action = 'restarted'

        if action is None:
            print '  [{0}] {1} already {2}'.format(client.ip, name, 'running' if status is True else 'halted')
        else:
            timeout = 300
            safetycounter = 0
            while safetycounter < timeout:
                status = ServiceManager.get_service_status(name, client=client)
                if (status is False and state == 'stop') or (status is True and state in ['start', 'restart']):
                    break
                safetycounter += 1
                time.sleep(1)
            if safetycounter == timeout:
                raise RuntimeError('Service {0} could not be {1} on node {2}'.format(name, action, client.ip))
            print '  [{0}] {1} {2}'.format(client.ip, name, action)

    @staticmethod
    def _is_rabbitmq_running(client):
        def check_rabbitmq_status(service):
            try:
                out = client.run('service {0} status'.format(service))
            except subprocess.CalledProcessError:
                out = client.run('service {0} status | true'.format(service))
            return out

        rabbitmq_running, rabbitmq_pid = False, 0
        ovs_rabbitmq_running, pid = False, -1
        output = check_rabbitmq_status('rabbitmq-server')
        if 'unrecognized service' in output:
            output = None
        if output:
            output = output.splitlines()
            for line in output:
                if 'pid' in line:
                    rabbitmq_running = True
                    rabbitmq_pid = line.split(',')[1].replace('}', '')
                    break
        else:
            try:
                output = client.run('ps aux | grep rabbit@ | grep -v grep')
            except subprocess.CalledProcessError:
                output = client.run('ps aux | grep rabbit@ | grep -v grep | true')

            output = output.split(' ')
            if output[0] == 'rabbitmq':
                rabbitmq_pid = output[1]
                for item in output[2:]:
                    if 'erlang' in item or 'rabbitmq' in item or 'beam' in item:
                        rabbitmq_running = True
        output = check_rabbitmq_status('ovs-rabbitmq')
        if 'stop/waiting' in output:
            pass
        if 'start/running' in output:
            pid = output.split('process ')[1].strip()
            ovs_rabbitmq_running = True
        same_process = rabbitmq_pid == pid
        return rabbitmq_running, rabbitmq_pid, ovs_rabbitmq_running, same_process

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
            print '\n+++ Running plugin hooks +++\n'
        for function in functions:
            if master_ip is None:
                function(cluster_ip=cluster_ip)
            else:
                function(cluster_ip=cluster_ip, master_ip=master_ip)
        return functions_found
