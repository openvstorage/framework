# Copyright 2016 iNuron NV
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
import json
import time
import base64
import urllib2
from etcd import EtcdConnectionFailed, EtcdException, EtcdKeyError, EtcdKeyNotFound
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonClusterConfig, ArakoonInstaller
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.db.etcd.installer import EtcdInstaller
from ovs.extensions.generic.interactive import Interactive
from ovs.extensions.generic.remote import Remote
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
from ovs.lib.helpers.toolbox import Toolbox
from ovs.log.logHandler import LogHandler
from paramiko import AuthenticationException

logger = LogHandler.get('lib', name='setup')
logger.logger.propagate = False


class SetupController(object):
    """
    This class contains all logic for setting up an environment, installed with system-native packages
    """

    # Generic configuration files
    avahi_filename = '/etc/avahi/services/ovs_cluster.service'
    nodes = {}
    host_ips = set()

    @staticmethod
    def setup_node(node_type=None):
        """
        Sets up a node.
        1. Some magic figuring out here:
           - Which cluster (new, joining)
           - Cluster role (master, extra)
        2. Prepare cluster
        3. Depending on (2), setup first/extra node
        4. Depending on (2), promote new extra node

        :param node_type: Type of node to install (master or extra node)
        :type node_type: str

        :return: None
        """
        SetupController._log(messages='Open vStorage Setup', boxed=True)
        Toolbox.verify_required_params(actual_params={'node_type': node_type},
                                       required_params={'node_type': (str, ['master', 'extra'], False)})

        master_ip = None
        cluster_ip = None
        external_etcd = None  # Example: 'etcd0123456789=http://1.2.3.4:2380'
        hypervisor_ip = None
        hypervisor_name = None
        hypervisor_type = None
        master_password = None
        enable_heartbeats = True
        hypervisor_password = None
        hypervisor_username = 'root'

        try:
            # Support non-interactive setup
            config = SetupController._validate_and_retrieve_pre_config()
            if config is not None:
                # Required fields
                master_ip = config['master_ip']
                hypervisor_name = config['hypervisor_name']
                hypervisor_type = config['hypervisor_type']
                master_password = config['master_password']

                # Optional fields
                node_type = config.get('node_type', node_type)
                cluster_ip = config.get('cluster_ip', master_ip)  # If cluster_ip not provided, we assume 1st node installation
                external_etcd = config.get('external_etcd')
                hypervisor_ip = config.get('hypervisor_ip')
                enable_heartbeats = config.get('enable_heartbeats', enable_heartbeats)
                hypervisor_password = config.get('hypervisor_password')
                hypervisor_username = config.get('hypervisor_username', hypervisor_username)

            # Support resume setup - store entered parameters so when retrying, we have the values
            resume_config = {}
            resume_config_file = '/opt/OpenvStorage/config/openvstorage_resumeconfig.json'
            if os.path.exists(resume_config_file):
                with open(resume_config_file, 'r') as resume_cfg:
                    resume_config = json.loads(resume_cfg.read())

            # Create connection to target node
            SetupController._log(messages='Setting up connections', title=True)

            root_client = SSHClient(endpoint='127.0.0.1', username='root')
            unique_id = System.get_my_machine_id(root_client)

            ipaddresses = root_client.run("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1").strip().splitlines()
            SetupController.host_ips = set([found_ip.strip() for found_ip in ipaddresses if found_ip.strip() != '127.0.0.1'])

            setup_completed = False
            promote_completed = False
            try:
                type_node = EtcdConfiguration.get('/ovs/framework/hosts/{0}/type'.format(unique_id))
                setup_completed = EtcdConfiguration.get('/ovs/framework/hosts/{0}/setupcompleted'.format(unique_id))
                if type_node == 'MASTER':
                    promote_completed = EtcdConfiguration.get('/ovs/framework/hosts/{0}/promotecompleted'.format(unique_id))
                if setup_completed is True and (promote_completed is True or type_node == 'EXTRA'):
                    raise RuntimeError('This node has already been configured for Open vStorage. Re-running the setup is not supported.')
            except (EtcdConnectionFailed, EtcdKeyNotFound, EtcdException):
                pass

            if setup_completed is False:
                SetupController._log(messages='Collecting cluster information', title=True)

                if root_client.file_exists('/etc/openvstorage_id') is False:
                    raise RuntimeError("The 'openvstorage' package is not installed on this node")

                node_name = root_client.run('hostname -s')
                fqdn_name = root_client.run('hostname -f || hostname -s')
                avahi_installed = SetupController._avahi_installed(root_client)

                logger.debug('Current host: {0}'.format(node_name))
                node_type = resume_config.get('node_type', node_type)
                master_ip = resume_config.get('master_ip', master_ip)
                cluster_ip = resume_config.get('cluster_ip', cluster_ip)
                cluster_name = resume_config.get('cluster_name')
                external_etcd = resume_config.get('external_etcd', external_etcd)
                hypervisor_ip = resume_config.get('hypervisor_ip', hypervisor_ip)
                hypervisor_name = resume_config.get('hypervisor_name', hypervisor_name)
                hypervisor_type = resume_config.get('hypervisor_type', hypervisor_type)
                enable_heartbeats = resume_config.get('enable_heartbeats', enable_heartbeats)
                hypervisor_username = resume_config.get('hypervisor_username', hypervisor_username)

                if cluster_name is not None and master_ip == cluster_ip and external_etcd is None and config is None:  # Failed setup with connectivity issues to the external Etcd
                    external_etcd = SetupController._retrieve_external_etcd_info()

                if config is None:  # Non-automated install
                    logger.debug('Cluster selection')
                    new_cluster = 'Create a new cluster'
                    discovery_result = SetupController._discover_nodes(root_client) if avahi_installed is True else {}
                    join_manually = 'Join {0} cluster'.format('a' if len(discovery_result) == 0 else 'a different')
                    cluster_options = [new_cluster] + sorted(discovery_result.keys()) + [join_manually]
                    cluster_name = Interactive.ask_choice(choice_options=cluster_options,
                                                          question='Select a cluster to join' if len(discovery_result) > 0 else 'No clusters found',
                                                          sort_choices=False)
                    if cluster_name == new_cluster:  # Create a new OVS cluster
                        first_node = True
                        while True:
                            cluster_name = Interactive.ask_string(message='Please enter the cluster name',
                                                                  regex_info={'regex': '^[0-9a-zA-Z]+(\-[0-9a-zA-Z]+)*$',
                                                                              'message': 'The new cluster name can only contain numbers, letters and dashes.'})
                            if cluster_name in discovery_result:
                                SetupController._log(messages='The new cluster name should be unique.')
                                continue
                            break
                        master_ip = Interactive.ask_choice(SetupController.host_ips, 'Select the public IP address of {0}'.format(node_name))
                        cluster_ip = master_ip
                        SetupController.nodes = {node_name: {'ip': master_ip,
                                                             'type': 'master'}}
                        if external_etcd is None:
                            external_etcd = SetupController._retrieve_external_etcd_info()

                    elif cluster_name == join_manually:  # Join an existing cluster manually
                        first_node = False
                        cluster_name = None
                        cluster_ip = Interactive.ask_choice(SetupController.host_ips, 'Select the public IP address of {0}'.format(node_name))
                        master_ip = Interactive.ask_string(message='Please enter the IP of one of the cluster\'s master nodes',
                                                           regex_info={'regex': SSHClient.IP_REGEX,
                                                                       'message': 'Incorrect IP provided'})
                        if master_ip in root_client.local_ips:
                            raise ValueError("A local IP address was given, please select '{0}' or provide another IP address".format(new_cluster))

                        logger.debug('Trying to manually join cluster on {0}'.format(master_ip))

                        master_password = SetupController._ask_validate_password(master_ip, username='root')
                        SetupController.nodes = SetupController._retrieve_storagerouters(ip=master_ip, password=master_password)
                        master_ips = [sr_info['ip'] for sr_info in SetupController.nodes.itervalues() if sr_info['type'] == 'master']
                        if master_ip not in master_ips:
                            raise ValueError('Incorrect master IP provided, please choose from: {0}'.format(', '.join(master_ips)))

                        current_sr_message = []
                        for sr_name in sorted(SetupController.nodes):
                            current_sr_message.append('{0:<15} - {1}'.format(SetupController.nodes[sr_name]['ip'], sr_name))
                        if Interactive.ask_yesno(message='Following StorageRouters were detected:\n  -  {0}\nIs this correct?'.format('\n  -  '.join(current_sr_message)),
                                                 default_value=True) is False:
                            raise Exception('The cluster on the given master node cannot be joined as not all StorageRouters could be loaded')

                    else:  # Join an existing cluster automatically
                        logger.debug('Cluster {0} selected'.format(cluster_name))
                        first_node = False
                        for host_name, node_info in discovery_result.get(cluster_name, {}).iteritems():
                            if host_name != node_name and node_info.get('type') == 'master':
                                master_ip = node_info['ip']
                                break
                        if master_ip is None:
                            raise RuntimeError('Could not find appropriate master')

                        master_password = SetupController._ask_validate_password(master_ip, username='root')
                        cluster_ip = Interactive.ask_choice(SetupController.host_ips, 'Select the public IP address of {0}'.format(node_name))
                        SetupController.nodes = SetupController._retrieve_storagerouters(ip=master_ip, password=master_password)

                else:  # Automated install
                    logger.debug('Automated installation')
                    cluster_ip = master_ip if cluster_ip is None else cluster_ip
                    first_node = master_ip == cluster_ip
                    cluster_name = 'preconfig-{0}'.format(master_ip.replace('.', '-'))
                    logger.info('Detected{0} a 1st node installation'.format('' if first_node is True else ' not'))

                    if first_node is False:
                        SetupController.nodes = SetupController._retrieve_storagerouters(ip=master_ip, password=master_password)
                    else:
                        SetupController.nodes[node_name] = {'ip': master_ip,
                                                            'type': 'master'}

                    # Validation of parameters
                    if master_ip != cluster_ip:
                        master_ips = [sr_info['ip'] for sr_info in SetupController.nodes.itervalues() if sr_info['type'] == 'master']
                        if master_ip not in master_ips:
                            raise ValueError('Incorrect master IP provided, please choose from: {0}'.format(', '.join(master_ips)))
                    else:
                        if node_type == 'extra':
                            raise ValueError('A 1st node can never be installed as an "extra" node')
                    if cluster_ip not in SetupController.host_ips:
                        raise ValueError('{0} IP provided {1} is not in the list of local IPs: {2}'.format('Master' if master_ip == cluster_ip else 'Cluster',
                                                                                                           cluster_ip,
                                                                                                           ', '.join(SetupController.host_ips)))

                if len(SetupController.nodes) == 0:
                    logger.debug('No StorageRouters could be loaded, cannot join the cluster')
                    raise RuntimeError('The cluster on the given master node cannot be joined as no StorageRouters could be loaded')

                if cluster_ip is None or master_ip is None:  # Master IP and cluster IP must be known by now, cluster_ip == master_ip for 1st node
                    raise ValueError('Something must have gone wrong retrieving IP information')

                if avahi_installed is True and cluster_name is None:
                    raise RuntimeError('The name of the cluster should be known by now.')

                ip_hostname_map = {cluster_ip: list({node_name, fqdn_name})}
                for node_host_name, node_info in SetupController.nodes.iteritems():
                    ip = node_info['ip']
                    if ip == master_ip:
                        node_client = node_info.get('client', SSHClient(endpoint=ip, username='root', password=master_password))
                        node_info['client'] = node_client
                        master_fqdn_name = node_client.run('hostname -f || hostname -s')
                        ip_hostname_map[ip] = list({node_host_name, master_fqdn_name})
                        break

                if node_name not in SetupController.nodes:
                    SetupController.nodes[node_name] = {'ip': cluster_ip,
                                                        'type': 'unknown',
                                                        'client': SSHClient(endpoint=cluster_ip, username='root')}

                SetupController._log(messages='Preparing node', title=True)
                SetupController._log(messages='Exchanging SSH keys and updating hosts files')

                # Exchange SSH keys
                all_ips = SetupController.host_ips
                local_client = None
                master_client = None
                for node_info in SetupController.nodes.itervalues():
                    node_ip = node_info['ip']
                    all_ips.add(node_ip)
                    if node_ip == cluster_ip:
                        local_client = node_info['client']
                    if node_ip == master_ip:
                        master_client = node_info['client']

                if local_client is None or master_client is None:
                    raise ValueError('Retrieving client information failed')

                known_hosts_ovs = '/opt/OpenvStorage/.ssh/known_hosts'
                known_hosts_root = '/root/.ssh/known_hosts'
                ssh_public_key_ovs = '/opt/OpenvStorage/.ssh/id_rsa.pub'
                ssh_public_key_root = '/root/.ssh/id_rsa.pub'
                authorized_keys_ovs = '/opt/OpenvStorage/.ssh/authorized_keys'
                authorized_keys_root = '/root/.ssh/authorized_keys'

                missing_files = set()
                for required_file in [known_hosts_ovs, known_hosts_root, ssh_public_key_ovs, ssh_public_key_root]:
                    if not local_client.file_exists(ssh_public_key_ovs):
                        missing_files.add('Could not find file {0} on node with IP {1}'.format(required_file, local_client.ip))
                if missing_files:
                    raise ValueError('Missing files:\n - {0}'.format('\n - '.join(sorted(list(missing_files)))))

                # Retrieve local public SSH keys
                local_pub_key_ovs = local_client.file_read(ssh_public_key_ovs)
                local_pub_key_root = local_client.file_read(ssh_public_key_root)
                if not local_pub_key_ovs or not local_pub_key_root:
                    raise ValueError('Missing contents in the public SSH keys on node {0}'.format(local_client.ip))

                # Connect to master and add the ovs and root public SSH key to all other nodes in the cluster
                all_pub_keys = [local_pub_key_ovs, local_pub_key_root]
                if first_node is False:
                    with Remote(master_client.ip, [SSHClient], 'root', master_password) as remote:
                        for node_host_name, node in SetupController.nodes.iteritems():
                            node_ip = node['ip']
                            if node_ip == cluster_ip:
                                continue
                            client = remote.SSHClient(node_ip, 'root')
                            if client.ip not in ip_hostname_map:
                                node_fqdn_name = client.run('hostname -f || hostname -s')
                                ip_hostname_map[client.ip] = list({node_host_name, node_fqdn_name})
                            for authorized_key in [authorized_keys_ovs, authorized_keys_root]:
                                if client.file_exists(authorized_key):
                                    master_authorized_keys = client.file_read(authorized_key)
                                    for local_pub_key in [local_pub_key_ovs, local_pub_key_root]:
                                        if local_pub_key not in master_authorized_keys:
                                            master_authorized_keys += '\n{0}'.format(local_pub_key)
                                            client.file_write(authorized_key, master_authorized_keys)
                            all_pub_keys.append(client.file_read(ssh_public_key_ovs))
                            all_pub_keys.append(client.file_read(ssh_public_key_root))

                # Now add all public keys of all nodes in the cluster to the local node
                for authorized_keys in [authorized_keys_ovs, authorized_keys_root]:
                    if local_client.file_exists(authorized_keys):
                        keys = local_client.file_read(authorized_keys)
                        for public_key in all_pub_keys:
                            if public_key not in keys:
                                keys += '\n{0}'.format(public_key)
                        local_client.file_write(authorized_keys, keys)

                # Configure /etc/hosts and execute ssh-keyscan
                for node_details in SetupController.nodes.itervalues():
                    node_client = node_details.get('client', SSHClient(endpoint=node_details['ip'], username='root'))
                    System.update_hosts_file(ip_hostname_map, node_client)
                    cmd = 'cp {{0}} {{0}}.tmp; ssh-keyscan -t rsa {0} {1} 2> /dev/null >> {{0}}.tmp; cat {{0}}.tmp | sort -u - > {{0}}'.format(' '.join(all_ips), ' '.join(SetupController.nodes.keys()))
                    root_command = cmd.format(known_hosts_root)
                    ovs_command = cmd.format(known_hosts_ovs)
                    ovs_command = 'su - ovs -c "{0}"'.format(ovs_command)
                    node_client.run(root_command)
                    node_client.run(ovs_command)

                # Collecting hypervisor data
                SetupController._log(messages='Collecting hypervisor information')
                possible_hypervisor = None
                module = local_client.run('lsmod | grep kvm || true').strip()
                if module != '':
                    possible_hypervisor = 'KVM'
                else:
                    disktypes = local_client.run('dmesg | grep VMware || true').strip()
                    if disktypes != '':
                        possible_hypervisor = 'VMWARE'

                hypervisor_info = {'ip': hypervisor_ip,
                                   'name': hypervisor_name,
                                   'type': hypervisor_type,
                                   'username': hypervisor_username,
                                   'password': hypervisor_password}
                if hypervisor_type is None:
                    hypervisor_type = Interactive.ask_choice(choice_options=['VMWARE', 'KVM'],
                                                             question='Which type of hypervisor is this Storage Router backing?',
                                                             default_value=possible_hypervisor)
                    logger.debug('Selected hypervisor type {0}'.format(hypervisor_type))
                if hypervisor_name is None:
                    default_name = ('esxi{0}' if hypervisor_type == 'VMWARE' else 'kvm{0}').format(cluster_ip.split('.')[-1])
                    hypervisor_name = Interactive.ask_string('Enter hypervisor hostname', default_value=default_name)
                if hypervisor_type == 'VMWARE':
                    first_request = True  # If parameters are wrong, we need to re-ask it
                    while True:
                        if hypervisor_ip is None or first_request is False:
                            hypervisor_ip = Interactive.ask_string(message='Enter hypervisor IP address',
                                                                   default_value=hypervisor_ip,
                                                                   regex_info={'regex': SSHClient.IP_REGEX,
                                                                               'message': 'Invalid hypervisor IP specified'})
                        if hypervisor_username is None or first_request is False:
                            hypervisor_username = Interactive.ask_string(message='Enter hypervisor username',
                                                                         default_value=hypervisor_username)
                        if hypervisor_password is None or first_request is False:
                            hypervisor_password = Interactive.ask_password(message='Enter hypervisor {0} password'.format(hypervisor_username))
                        try:
                            SetupController._validate_hypervisor_information(ip=hypervisor_ip,
                                                                             username=hypervisor_username,
                                                                             password=hypervisor_password)
                            break
                        except Exception as ex:
                            first_request = False
                            SetupController._log(messages='Could not connect to {0}: {1}'.format(hypervisor_ip, ex))
                    hypervisor_info['ip'] = hypervisor_ip
                    hypervisor_info['username'] = hypervisor_username
                    hypervisor_info['password'] = hypervisor_password
                elif hypervisor_type == 'KVM':
                    hypervisor_info['ip'] = cluster_ip
                    hypervisor_info['password'] = None
                    hypervisor_info['username'] = 'root'

                hypervisor_info['name'] = hypervisor_name
                hypervisor_info['type'] = hypervisor_type

                logger.debug('Hypervisor at {0} with username {1}'.format(hypervisor_info['ip'], hypervisor_info['username']))

                # Write resume config
                resume_config['node_type'] = node_type
                resume_config['master_ip'] = master_ip
                resume_config['unique_id'] = unique_id
                resume_config['cluster_ip'] = cluster_ip
                resume_config['cluster_name'] = cluster_name
                resume_config['external_etcd'] = external_etcd
                resume_config['hypervisor_ip'] = hypervisor_info['ip']
                resume_config['hypervisor_type'] = hypervisor_info['type']
                resume_config['hypervisor_name'] = hypervisor_info['name']
                resume_config['enable_heartbeats'] = enable_heartbeats
                resume_config['hypervisor_username'] = hypervisor_info['username']
                with open(resume_config_file, 'w') as resume_cfg:
                    resume_cfg.write(json.dumps(resume_config))

                ip_client_map = dict((info['ip'], SSHClient(info['ip'], username='root')) for info in SetupController.nodes.itervalues())
                if first_node is True:
                    try:
                        SetupController._setup_first_node(target_client=ip_client_map[cluster_ip],
                                                          unique_id=unique_id,
                                                          cluster_name=cluster_name,
                                                          node_name=node_name,
                                                          hypervisor_info=hypervisor_info,
                                                          enable_heartbeats=enable_heartbeats,
                                                          external_etcd=external_etcd)
                    except Exception as ex:
                        SetupController._log(messages=['Failed to setup first node', ex], loglevel='error')
                        SetupController._rollback_setup(target_client=ip_client_map[cluster_ip],
                                                        first_node=True)
                        raise
                else:
                    # Deciding master/extra
                    try:
                        SetupController._setup_extra_node(cluster_ip=cluster_ip,
                                                          master_ip=master_ip,
                                                          cluster_name=cluster_name,
                                                          unique_id=unique_id,
                                                          ip_client_map=ip_client_map,
                                                          hypervisor_info=hypervisor_info)
                    except Exception as ex:
                        SetupController._log(messages=['Failed to setup extra node', ex], loglevel='error')
                        SetupController._rollback_setup(target_client=ip_client_map[cluster_ip],
                                                        first_node=False)
                        raise

                    if promote_completed is False:
                        SetupController._log(messages='Analyzing cluster layout')
                        framework_cluster_name = str(EtcdConfiguration.get('/ovs/framework/arakoon_clusters|ovsdb'))
                        config = ArakoonClusterConfig(framework_cluster_name)
                        config.load_config()
                        logger.debug('{0} nodes for cluster {1} found'.format(len(config.nodes), framework_cluster_name))
                        if (len(config.nodes) < 3 or node_type == 'master') and node_type != 'extra':
                            configure_rabbitmq = SetupController._is_internally_managed(service='rabbitmq')
                            configure_memcached = SetupController._is_internally_managed(service='memcached')
                            try:
                                SetupController._promote_node(cluster_ip=cluster_ip,
                                                              master_ip=master_ip,
                                                              cluster_name=cluster_name,
                                                              ip_client_map=ip_client_map,
                                                              unique_id=unique_id,
                                                              configure_memcached=configure_memcached,
                                                              configure_rabbitmq=configure_rabbitmq)
                            except Exception as ex:
                                SetupController._log(messages=['\nFailed to promote node, rolling back', ex], loglevel='error')
                                SetupController._demote_node(cluster_ip=cluster_ip,
                                                             master_ip=master_ip,
                                                             cluster_name=cluster_name,
                                                             ip_client_map=ip_client_map,
                                                             unique_id=unique_id,
                                                             unconfigure_memcached=configure_memcached,
                                                             unconfigure_rabbitmq=configure_rabbitmq)
                                raise

            root_client.file_delete(resume_config_file)
            if enable_heartbeats is True:
                SetupController._log(messages='')
                SetupController._log(messages=['Open vStorage securely sends a minimal set of error, usage and health',
                                               'information. This information is used to keep the quality and performance',
                                               'of the code at the highest possible levels.',
                                               'Please refer to the documentation for more information.'],
                                     boxed=True)

            is_master = [node for node in SetupController.nodes.itervalues() if node['type'] == 'master' and node['ip'] == cluster_ip]
            SetupController._log(messages='')
            SetupController._log(messages=['Setup complete.',
                                           'Point your browser to https://{0} to use Open vStorage'.format(cluster_ip if len(is_master) > 0 else master_ip)],
                                 boxed=True)
            logger.info('Setup complete')

            try:
                # Try to trigger setups from possibly installed other packages
                sys.path.append('/opt/asd-manager/')
                from source.asdmanager import setup
                SetupController._log(messages='\nA local ASD Manager was detected for which the setup will now be launched.\n')
                setup()
            except:
                pass

        except Exception as exception:
            SetupController._log(messages='\n')
            SetupController._log(messages=['An unexpected error occurred:', str(exception).lstrip('\n')], boxed=True, loglevel='error')
            sys.exit(1)
        except KeyboardInterrupt:
            SetupController._log(messages='\n')
            SetupController._log(messages='This setup was aborted. Open vStorage may be in an inconsistent state, make sure to validate the installation.', boxed=True, loglevel='error')
            sys.exit(1)

    @staticmethod
    def promote_or_demote_node(node_action, cluster_ip=None):
        """
        Promotes or demotes the local node
        :param node_action: Demote or promote
        :type node_action: str

        :param cluster_ip: IP of node to promote or demote
        :type cluster_ip: str

        :return: None
        """

        if node_action not in ('promote', 'demote'):
            raise ValueError('Nodes can only be promoted or demoted')

        SetupController._log(messages='Open vStorage Setup - {0}'.format(node_action.capitalize()), boxed=True)
        try:
            SetupController._log(messages='Collecting information', title=True)

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
            cluster_name = None
            offline_nodes = []

            if node_action == 'demote' and cluster_ip:  # Demote an offline node
                from ovs.dal.lists.storagerouterlist import StorageRouterList
                from ovs.lib.storagedriver import StorageDriverController

                ip = cluster_ip
                online = True
                unique_id = None
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
                if master_ip is None:
                    raise RuntimeError('Failed to retrieve another responsive MASTER node')

            else:
                target_password = SetupController._ask_validate_password('127.0.0.1', username='root')
                target_client = SSHClient('127.0.0.1', username='root', password=target_password)

                unique_id = System.get_my_machine_id(target_client)
                ip = EtcdConfiguration.get('/ovs/framework/hosts/{0}/ip'.format(unique_id))

                if SetupController._avahi_installed(target_client):
                    with open(SetupController.avahi_filename, 'r') as avahi_file:
                        avahi_contents = avahi_file.read()
                    match_groups = re.search('>ovs_cluster_(?P<cluster>[^_]+)_.+?<', avahi_contents).groupdict()
                    if 'cluster' not in match_groups:
                        raise RuntimeError('No cluster information found.')
                    cluster_name = match_groups['cluster']

                storagerouter_info = SetupController._retrieve_storagerouters(ip=target_client.ip, password=target_password)
                node_ips = [sr_info['ip'] for sr_info in storagerouter_info.itervalues()]
                master_node_ips = [sr_info['ip'] for sr_info in storagerouter_info.itervalues() if sr_info['type'] == 'master' and sr_info['ip'] != ip]
                if len(master_node_ips) == 0:
                    if node_action == 'promote':
                        raise RuntimeError('No master node could be found')
                    else:
                        raise RuntimeError('It is not possible to remove the only master')

                master_ip = master_node_ips[0]
                ip_client_map = dict((node_ip, SSHClient(node_ip, username='root')) for node_ip in node_ips)

            configure_rabbitmq = SetupController._is_internally_managed(service='rabbitmq')
            configure_memcached = SetupController._is_internally_managed(service='memcached')
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

            SetupController._log(messages='\n')
            SetupController._log(messages='{0} complete.'.format(node_action.capitalize()), boxed=True)
        except Exception as exception:
            SetupController._log(messages='\n')
            SetupController._log(messages=['An unexpected error occurred:', str(exception)], boxed=True, loglevel='error')
            sys.exit(1)
        except KeyboardInterrupt:
            SetupController._log(messages='\n')
            SetupController._log(messages='This setup was aborted. Open vStorage may be in an inconsistent state, make sure to validate the installation.', boxed=True, loglevel='error')
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

        SetupController._log(messages='Remove nodes started', title=True)
        SetupController._log(messages='\nWARNING: Some of these steps may take a very long time, please check /var/log/ovs/lib.log on this node for more logging information\n\n')

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

        SetupController._log(messages='Following nodes with IPs will be removed from the cluster: {0}'.format(list(storage_router_ips_to_remove)))
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

        SetupController._log(messages='Creating SSH connections to remaining master nodes')
        master_ip = None
        ip_client_map = {}
        storage_routers_offline = []
        storage_routers_to_remove_online = []
        storage_routers_to_remove_offline = []
        for storage_router in storage_router_all:
            try:
                client = SSHClient(storage_router, username='root')
                if client.run('pwd'):
                    SetupController._log(messages='  Node with IP {0:<15} successfully connected to'.format(storage_router.ip))
                    ip_client_map[storage_router.ip] = SSHClient(storage_router.ip, username='root')
                    if storage_router not in storage_routers_to_remove and storage_router.node_type == 'MASTER':
                        master_ip = storage_router.ip
                if storage_router in storage_routers_to_remove:
                    storage_routers_to_remove_online.append(storage_router)
            except UnableToConnectException:
                SetupController._log(messages='  Node with IP {0:<15} is unreachable'.format(storage_router.ip))
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
            SetupController._log(messages='Starting removal of nodes')
            for storage_router in storage_routers_to_remove:
                if storage_router in storage_routers_to_remove_offline:
                    SetupController._log(messages='  Marking all Storage Drivers served by Storage Router {0} as offline'.format(storage_router.ip))
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
                SetupController._log(messages='  Cleaning up node with IP {0}'.format(storage_router.ip))
                storage_routers_offline_guids = [sr.guid for sr in storage_routers_offline if sr.guid != storage_router.guid]
                for storage_driver in storage_router.storagedrivers:
                    SetupController._log(messages='    Removing vPool {0} from node'.format(storage_driver.vpool.name))
                    StorageRouterController.remove_storagedriver(storagedriver_guid=storage_driver.guid,
                                                                 offline_storage_router_guids=storage_routers_offline_guids)

                # 3. Demote if MASTER
                if storage_router.node_type == 'MASTER':
                    unconfigure_rabbitmq = SetupController._is_internally_managed(service='rabbitmq')
                    unconfigure_memcached = SetupController._is_internally_managed(service='memcached')
                    SetupController._demote_node(cluster_ip=storage_router.ip,
                                                 master_ip=master_ip,
                                                 cluster_name=None,
                                                 ip_client_map=ip_client_map,
                                                 unique_id=storage_router.machine_id,
                                                 unconfigure_memcached=unconfigure_memcached,
                                                 unconfigure_rabbitmq=unconfigure_rabbitmq,
                                                 offline_nodes=storage_routers_offline)

                # 4. Clean up model
                SetupController._log(messages='    Removing node from model')
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

                SetupController._log(messages='    Successfully removed node\n')
        except Exception as exception:
            SetupController._log(messages='\n')
            SetupController._log(messages=['An unexpected error occurred:', str(exception)], boxed=True, loglevel='error')
            sys.exit(1)
        except KeyboardInterrupt:
            SetupController._log(messages='\n')
            SetupController._log(messages='This setup was aborted. Open vStorage may be in an inconsistent state, make sure to validate the installation.', boxed=True, loglevel='error')
            sys.exit(1)
        SetupController._log(messages='Remove nodes finished', title=True)

    @staticmethod
    def _setup_first_node(target_client, unique_id, cluster_name, node_name, hypervisor_info, enable_heartbeats, external_etcd):
        """
        Sets up the first node services. This node is always a master
        """
        SetupController._log(messages='Setting up first node', title=True)
        cluster_ip = target_client.ip
        machine_id = System.get_my_machine_id(target_client)

        SetupController._log(messages='Setting up Etcd')
        if external_etcd is None:
            EtcdInstaller.create_cluster('config', cluster_ip)
        else:
            try:
                EtcdInstaller.use_external(external_etcd, cluster_ip, 'config')
            except (EtcdConnectionFailed, EtcdException, EtcdKeyError):
                SetupController._log(messages='Failed to set up Etcd proxy')
                resume_config_file = '/opt/OpenvStorage/config/openvstorage_resumeconfig.json'
                if target_client.file_exists(resume_config_file):
                    with open(resume_config_file, 'r') as resume_cfg:
                        resume_config = json.loads(resume_cfg.read())
                        if 'external_etcd' in resume_config:
                            resume_config.pop('external_etcd')
                    with open(resume_config_file, 'w') as resume_cfg:
                        resume_cfg.write(json.dumps(resume_config))
                raise

        EtcdConfiguration.initialize(external_etcd=external_etcd)
        EtcdConfiguration.initialize_host(machine_id)

        if ServiceManager.has_fleet():
            SetupController._log(messages='Setting up Fleet')
            ServiceManager.setup_fleet()

        metadata = ArakoonInstaller.get_unused_arakoon_metadata_and_claim(cluster_type=ServiceType.ARAKOON_CLUSTER_TYPES.FWK, locked=False)
        arakoon_ports = []
        if metadata is None:  # No externally managed cluster found, we create 1 ourselves
            SetupController._log(messages='Setting up Arakoon cluster ovsdb')
            internal = True
            result = ArakoonInstaller.create_cluster(cluster_name='ovsdb',
                                                     cluster_type=ServiceType.ARAKOON_CLUSTER_TYPES.FWK,
                                                     ip=cluster_ip,
                                                     base_dir=EtcdConfiguration.get('/ovs/framework/paths|ovsdb'),
                                                     locked=False,
                                                     claim=True)
            arakoon_ports = [result['client_port'], result['messaging_port']]
            metadata = result['metadata']
        else:
            SetupController._log(messages='Externally managed Arakoon cluster of type {0} found with name {1}'.format(ServiceType.ARAKOON_CLUSTER_TYPES.FWK, metadata.cluster_id))
            internal = False

        EtcdConfiguration.set('/ovs/framework/arakoon_clusters|ovsdb', metadata.cluster_id)
        SetupController._add_services(target_client, unique_id, 'master')
        SetupController._log(messages='Build configuration files')

        configure_rabbitmq = SetupController._is_internally_managed(service='rabbitmq')
        configure_memcached = SetupController._is_internally_managed(service='memcached')
        if configure_rabbitmq is True:
            EtcdConfiguration.set('/ovs/framework/messagequeue|endpoints', ['{0}:5672'.format(cluster_ip)])
            SetupController._configure_rabbitmq(target_client)
        if configure_memcached is True:
            EtcdConfiguration.set('/ovs/framework/memcache|endpoints', ['{0}:11211'.format(cluster_ip)])
            SetupController._configure_memcached(target_client)
        VolatileFactory.store = None

        SetupController._log(messages='Starting model services', loglevel='debug')
        model_services = ['memcached', 'arakoon-ovsdb'] if internal is True else ['memcached']
        for service in model_services:
            if ServiceManager.has_service(service, client=target_client):
                ServiceManager.enable_service(service, client=target_client)
                Toolbox.change_service_state(target_client, service, 'restart', logger)

        SetupController._log(messages='Start model migration', loglevel='debug')
        from ovs.dal.helpers import Migration
        Migration.migrate()

        SetupController._log(messages='Finalizing setup', title=True)
        storagerouter = SetupController._finalize_setup(target_client, node_name, 'MASTER', hypervisor_info, unique_id)

        from ovs.dal.lists.servicelist import ServiceList
        if 'arakoon-ovsdb' not in [s.name for s in ServiceList.get_services()]:
            from ovs.dal.lists.servicetypelist import ServiceTypeList
            from ovs.dal.hybrids.service import Service
            service = Service()
            service.name = 'arakoon-ovsdb'
            service.type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON)
            service.ports = arakoon_ports
            service.storagerouter = storagerouter if internal is True else None
            service.save()

        SetupController._log(messages='Updating configuration files')
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/ip'.format(machine_id), cluster_ip)

        SetupController._log(messages='Starting services on 1st node')
        for service in model_services + ['rabbitmq-server']:
            if ServiceManager.has_service(service, client=target_client):
                ServiceManager.enable_service(service, client=target_client)
                Toolbox.change_service_state(target_client, service, 'start', logger)
        # Enable HA for the rabbitMQ queues
        SetupController._check_rabbitmq_and_enable_ha_mode(target_client)

        ServiceManager.enable_service('watcher-framework', client=target_client)
        Toolbox.change_service_state(target_client, 'watcher-framework', 'start', logger)

        SetupController._log(messages='Check ovs-workers', loglevel='debug')
        # Workers are started by ovs-watcher-framework, but for a short time they are in pre-start
        ServiceManager.enable_service('workers', client=target_client)
        Toolbox.wait_for_service(target_client, 'workers', True, logger)

        SetupController._run_hooks('firstnode', cluster_ip)

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
        SetupController._log(messages='First node complete')

    @staticmethod
    def _rollback_setup(target_client, first_node):
        """
        Rollback a failed setup
        """
        from ovs.dal.lists.servicetypelist import ServiceTypeList
        SetupController._log(messages='Rolling back setup of current node', title=True)

        cluster_ip = target_client.ip
        machine_id = System.get_my_machine_id(target_client)
        try:
            EtcdInstaller.wait_for_cluster(cluster_name='a_name_that_does_not_matter_at_all', client=target_client)
            etcd_running = True
        except EtcdConnectionFailed:
            etcd_running = False
        unconfigure_rabbitmq = False
        unconfigure_memcached = False

        etcd_required_info = {'/ovs/framework/memcache': None,
                              '/ovs/framework/paths|ovsdb': '',
                              '/ovs/framework/external_etcd': None,
                              '/ovs/framework/memcache|endpoints': [],
                              '/ovs/framework/arakoon_clusters|ovsdb': None,
                              '/ovs/framework/messagequeue|endpoints': []}

        SetupController._log(messages='Etcd is{0} running'.format('' if etcd_running is True else ' NOT'))
        if etcd_running is True:
            for key in etcd_required_info:
                try:
                    etcd_required_info[key] = EtcdConfiguration.get(key=key)
                except (EtcdKeyNotFound, KeyError):
                    pass
            unconfigure_rabbitmq = SetupController._is_internally_managed(service='rabbitmq')
            unconfigure_memcached = SetupController._is_internally_managed(service='memcached')

        target_client.dir_delete('/opt/OpenvStorage/webapps/frontend/logging')

        SetupController._log(messages='Stopping services', loglevel='debug')
        for service in ['memcached', 'arakoon-ovsdb', 'watcher-framework', 'workers', 'support-agent']:
            if ServiceManager.has_service(service, client=target_client):
                ServiceManager.disable_service(service, client=target_client)
                Toolbox.change_service_state(target_client, service, 'stop', logger)

        if etcd_running is True:
            endpoints = etcd_required_info['/ovs/framework/messagequeue|endpoints']
            if len(endpoints) > 0 and unconfigure_rabbitmq is True:
                SetupController._log(messages='Unconfiguring RabbitMQ')
                try:
                    SetupController._unconfigure_rabbitmq(target_client)
                except Exception as ex:
                    SetupController._log(messages=['Failed to unconfigure RabbitMQ', ex], loglevel='error')

                for endpoint in endpoints:
                    if endpoint.startswith(target_client.ip):
                        endpoints.remove(endpoint)
                        break
                if len(endpoints) == 0:
                    EtcdConfiguration.delete('/ovs/framework/messagequeue')
                else:
                    EtcdConfiguration.set('/ovs/framework/messagequeue|endpoints', endpoints)

            SetupController._log(messages='Unconfiguring Memcached')
            endpoints = etcd_required_info['/ovs/framework/memcache|endpoints']
            if len(endpoints) > 0 and unconfigure_memcached is True:
                ServiceManager.stop_service('memcached', target_client)
                for endpoint in endpoints:
                    if endpoint.startswith(target_client.ip):
                        endpoints.remove(endpoint)
                        break
                if len(endpoints) == 0:
                    EtcdConfiguration.delete('/ovs/framework/memcache')
                else:
                    EtcdConfiguration.set('/ovs/framework/memcache|endpoints', endpoints)

        SetupController._remove_services(target_client, 'master')

        if first_node is True and etcd_running is True:
            SetupController._log(messages='Unconfigure Arakoon')
            cluster_name = etcd_required_info['/ovs/framework/arakoon_clusters|ovsdb']
            try:
                metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)
            except ValueError:
                metadata = None
            if metadata is not None and metadata.internal is True:
                try:
                    ArakoonInstaller.delete_cluster(cluster_name, cluster_ip)
                except Exception as ex:
                    SetupController._log(messages=['\nFailed to delete cluster', ex], loglevel='error')
                base_dir = etcd_required_info['/ovs/framework/paths|ovsdb']
                directory_info = {ArakoonInstaller.ARAKOON_LOG_DIR.format(cluster_name): True,
                                  ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, cluster_name): False,
                                  ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, cluster_name): False}

                try:
                    ArakoonInstaller.clean_leftover_arakoon_data(ip=cluster_ip,
                                                                 directories=directory_info)
                except Exception as ex:
                    SetupController._log(messages=['Failed to clean Arakoon data', ex])

        SetupController._log(messages='Unconfigure Etcd')
        if etcd_running is True:
            external_etcd = etcd_required_info['/ovs/framework/external_etcd']
            if external_etcd is None:
                SetupController._log(messages='Removing Etcd cluster')
                try:
                    EtcdInstaller.stop('config', target_client)
                    EtcdInstaller.remove('config', target_client)
                except Exception as ex:
                    SetupController._log(messages=['\nFailed to unconfigure Etcd', ex], loglevel='error')

            SetupController._log(messages='Cleaning up model')
            memcache_configured = etcd_required_info['/ovs/framework/memcache']
            pmachine = None
            storagerouter = None
            if memcache_configured is not None:
                try:
                    storagerouter = System.get_my_storagerouter()
                    pmachine = storagerouter.pmachine
                except Exception as ex:
                    SetupController._log(messages='Retrieving storagerouter and pmachine information failed with error: {0}'.format(ex), loglevel='error')

                if pmachine is not None:  # Pmachine will be None if storagerouter not yet modeled
                    try:
                        for service in storagerouter.services:
                            service.delete()
                        storagerouter.delete()
                        if len(pmachine.storagerouters) == 0:
                            pmachine.delete()
                    except Exception as ex:
                        SetupController._log(messages='Cleaning up model failed with error: {0}'.format(ex), loglevel='error')

                if first_node is True:
                    try:
                        for service in ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON).services:  # Externally managed Arakoon services not linked to the storagerouter
                            service.delete()
                    except Exception as ex:
                        SetupController._log(messages='Cleaning up services failed with error: {0}'.format(ex), loglevel='error')

            for key in EtcdConfiguration.base_config.keys() + ['install_time', 'plugins', 'hosts/{0}'.format(machine_id)]:
                try:
                    EtcdConfiguration.delete(key='/ovs/framework/{0}'.format(key))
                except (EtcdKeyNotFound, KeyError):
                    pass
        SetupController._log(messages='Removing Etcd proxy')
        EtcdInstaller.remove_proxy('config', cluster_ip)

    @staticmethod
    def _setup_extra_node(cluster_ip, master_ip, cluster_name, unique_id, ip_client_map, hypervisor_info):
        """
        Sets up an additional node
        """
        SetupController._log(messages='Adding extra node', title=True)
        target_client = ip_client_map[cluster_ip]
        machine_id = System.get_my_machine_id(target_client)

        SetupController._log(messages='Extending Etcd cluster to this node')
        EtcdInstaller.deploy_to_slave(master_ip, cluster_ip, 'config')
        EtcdConfiguration.initialize_host(machine_id)

        if ServiceManager.has_fleet():
            SetupController._log(messages='Setting up fleet')
            ServiceManager.setup_fleet()

        SetupController._add_services(target_client, unique_id, 'extra')

        enabled = EtcdConfiguration.get('/ovs/framework/support|enabled')
        if enabled is True:
            service = 'support-agent'
            if not ServiceManager.has_service(service, target_client):
                ServiceManager.add_service(service, client=target_client)
                ServiceManager.enable_service(service, client=target_client)
                Toolbox.change_service_state(target_client, service, 'start', logger)

        node_name = target_client.run('hostname -s')
        SetupController._finalize_setup(target_client, node_name, 'EXTRA', hypervisor_info, unique_id)

        EtcdConfiguration.set('/ovs/framework/hosts/{0}/ip'.format(machine_id), cluster_ip)

        SetupController._log(messages='Starting services')
        ServiceManager.enable_service('watcher-framework', client=target_client)
        Toolbox.change_service_state(target_client, 'watcher-framework', 'start', logger)

        SetupController._log(messages='Check ovs-workers')
        # Workers are started by ovs-watcher-framework, but for a short time they are in pre-start
        ServiceManager.enable_service('workers', client=target_client)
        Toolbox.wait_for_service(target_client, 'workers', True, logger)

        SetupController._log(messages='Restarting workers', loglevel='debug')
        for node_client in ip_client_map.itervalues():
            ServiceManager.enable_service('workers', client=node_client)
            Toolbox.change_service_state(node_client, 'workers', 'restart', logger)

        SetupController._run_hooks('extranode', cluster_ip, master_ip)

        if SetupController._avahi_installed(target_client) is True:
            SetupController._configure_avahi(target_client, cluster_name, node_name, 'extra')
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/setupcompleted'.format(machine_id), True)
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/type'.format(machine_id), 'EXTRA')
        target_client.run('chown -R ovs:ovs /opt/OpenvStorage/config')
        SetupController._log(messages='Extra node complete')

    @staticmethod
    def _promote_node(cluster_ip, master_ip, cluster_name, ip_client_map, unique_id, configure_memcached, configure_rabbitmq):
        """
        Promotes a given node
        """
        from ovs.dal.lists.storagerouterlist import StorageRouterList
        from ovs.dal.lists.servicetypelist import ServiceTypeList
        from ovs.dal.lists.servicelist import ServiceList
        from ovs.dal.hybrids.service import Service

        SetupController._log(messages='Promoting node', title=True)
        if configure_memcached:
            if SetupController._validate_local_memcache_servers(ip_client_map) is False:
                raise RuntimeError('Not all memcache nodes can be reached which is required for promoting a node.')

        target_client = ip_client_map[cluster_ip]
        machine_id = System.get_my_machine_id(target_client)
        node_name = target_client.run('hostname -s')
        master_client = ip_client_map[master_ip]

        storagerouter = StorageRouterList.get_by_machine_id(unique_id)
        storagerouter.node_type = 'MASTER'
        storagerouter.save()

        # Find other (arakoon) master nodes
        arakoon_cluster_name = str(EtcdConfiguration.get('/ovs/framework/arakoon_clusters|ovsdb'))
        arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=arakoon_cluster_name)
        config = ArakoonClusterConfig(arakoon_cluster_name)
        config.load_config()
        master_nodes = [node.ip for node in config.nodes]
        if cluster_ip in master_nodes:
            master_nodes.remove(cluster_ip)
        if len(master_nodes) == 0:
            raise RuntimeError('There should be at least one other master node')

        if configure_memcached:
            SetupController._configure_memcached(target_client)
        SetupController._add_services(target_client, unique_id, 'master')

        arakoon_ports = []
        if arakoon_metadata.internal is True:
            SetupController._log(messages='Joining Arakoon cluster')
            result = ArakoonInstaller.extend_cluster(master_ip=master_ip,
                                                     new_ip=cluster_ip,
                                                     cluster_name=arakoon_cluster_name,
                                                     base_dir=EtcdConfiguration.get('/ovs/framework/paths|ovsdb'))
            arakoon_ports = [result['client_port'], result['messaging_port']]

        external_etcd = EtcdConfiguration.get('/ovs/framework/external_etcd')
        if external_etcd is None:
            SetupController._log(messages='Joining Etcd cluster')
            EtcdInstaller.extend_cluster(master_ip, cluster_ip, 'config')

        SetupController._log(messages='Update configurations')
        if configure_memcached is True:
            endpoints = EtcdConfiguration.get('/ovs/framework/memcache|endpoints')
            endpoint = '{0}:11211'.format(cluster_ip)
            if endpoint not in endpoints:
                endpoints.append(endpoint)
                EtcdConfiguration.set('/ovs/framework/memcache|endpoints', endpoints)
        if configure_rabbitmq is True:
            endpoints = EtcdConfiguration.get('/ovs/framework/messagequeue|endpoints')
            endpoint = '{0}:5672'.format(cluster_ip)
            if endpoint not in endpoint:
                endpoints.append(endpoint)
                EtcdConfiguration.set('/ovs/framework/messagequeue|endpoints', endpoints)

        if arakoon_metadata.internal is True:
            SetupController._log(messages='Restarting master node services')
            ArakoonInstaller.restart_cluster_add(arakoon_cluster_name, master_nodes, cluster_ip)
            PersistentFactory.store = None
            VolatileFactory.store = None

            if 'arakoon-ovsdb' not in [s.name for s in ServiceList.get_services() if s.is_internal is False or s.storagerouter.ip == cluster_ip]:
                service = Service()
                service.name = 'arakoon-ovsdb'
                service.type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON)
                service.ports = arakoon_ports
                service.storagerouter = storagerouter
                service.save()

        if configure_rabbitmq:
            SetupController._configure_rabbitmq(target_client)
            # Copy rabbitmq cookie
            rabbitmq_cookie_file = '/var/lib/rabbitmq/.erlang.cookie'

            SetupController._log(messages='Copying Rabbit MQ cookie', loglevel='debug')
            contents = master_client.file_read(rabbitmq_cookie_file)
            master_hostname = master_client.run('hostname -s')
            target_client.dir_create(os.path.dirname(rabbitmq_cookie_file))
            target_client.file_write(rabbitmq_cookie_file, contents)
            target_client.file_chmod(rabbitmq_cookie_file, mode=400)
            target_client.run('rabbitmq-server -detached 2> /dev/null; sleep 5; rabbitmqctl stop_app; sleep 5;')
            target_client.run('rabbitmqctl join_cluster rabbit@{0}; sleep 5;'.format(master_hostname))
            target_client.run('rabbitmqctl stop; sleep 5;')

            # Enable HA for the rabbitMQ queues
            Toolbox.change_service_state(target_client, 'rabbitmq-server', 'start', logger)
            SetupController._check_rabbitmq_and_enable_ha_mode(target_client)

        SetupController._configure_amqp_to_volumedriver()

        SetupController._log(messages='Starting services')
        services = ['memcached', 'arakoon-ovsdb', 'rabbitmq-server', 'etcd-config']
        if arakoon_metadata.internal is True:
            services.remove('arakoon-ovsdb')
        for service in services:
            if ServiceManager.has_service(service, client=target_client):
                ServiceManager.enable_service(service, client=target_client)
                Toolbox.change_service_state(target_client, service, 'start', logger)

        SetupController._log(messages='Restarting services')
        master_ips = [sr.ip for sr in StorageRouterList.get_masters()]
        slave_ips = [sr.ip for sr in StorageRouterList.get_slaves()]
        SetupController._restart_framework_and_memcache_services(master_ips, slave_ips, ip_client_map)

        if SetupController._run_hooks('promote', cluster_ip, master_ip):
            SetupController._log(messages='Restarting services')
            SetupController._restart_framework_and_memcache_services(master_ips, slave_ips, ip_client_map)

        if SetupController._avahi_installed(target_client) is True:
            SetupController._configure_avahi(target_client, cluster_name, node_name, 'master')
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/type'.format(machine_id), 'MASTER')
        target_client.run('chown -R ovs:ovs /opt/OpenvStorage/config')
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/promotecompleted'.format(machine_id), True)
        SetupController._log(messages='Promote complete')

    @staticmethod
    def _demote_node(cluster_ip, master_ip, cluster_name, ip_client_map, unique_id, unconfigure_memcached, unconfigure_rabbitmq, offline_nodes=None):
        """
        Demotes a given node
        """
        from ovs.dal.lists.storagerouterlist import StorageRouterList

        SetupController._log(messages='Demoting node', title=True)
        if offline_nodes is None:
            offline_nodes = []

        if unconfigure_memcached is True and len(offline_nodes) == 0:
            if SetupController._validate_local_memcache_servers(ip_client_map) is False:
                raise RuntimeError('Not all memcache nodes can be reached which is required for demoting a node.')

        # Find other (arakoon) master nodes
        arakoon_cluster_name = str(EtcdConfiguration.get('/ovs/framework/arakoon_clusters|ovsdb'))
        arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=arakoon_cluster_name)
        config = ArakoonClusterConfig(arakoon_cluster_name)
        config.load_config()
        master_nodes = [node.ip for node in config.nodes]
        if cluster_ip in master_nodes:
            master_nodes.remove(cluster_ip)
        if len(master_nodes) == 0:
            raise RuntimeError('There should be at least one other master node')

        storagerouter = StorageRouterList.get_by_machine_id(unique_id)
        storagerouter.node_type = 'EXTRA'
        storagerouter.save()

        offline_node_ips = [node.ip for node in offline_nodes]
        if arakoon_metadata.internal is True:
            SetupController._log(messages='Leaving Arakoon {0} cluster'.format(arakoon_cluster_name))
            ArakoonInstaller.shrink_cluster(deleted_node_ip=cluster_ip, cluster_name=arakoon_cluster_name, offline_nodes=offline_node_ips)

        try:
            external_etcd = EtcdConfiguration.get('/ovs/framework/external_etcd')
            if external_etcd is None:
                SetupController._log(messages='Leaving Etcd cluster')
                EtcdInstaller.shrink_cluster(master_ip, cluster_ip, 'config', offline_node_ips)
        except Exception as ex:
            SetupController._log(messages=['\nFailed to leave Etcd cluster', ex], loglevel='error')

        SetupController._log(messages='Update configurations')
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
            SetupController._log(messages=['\nFailed to update configurations', ex], loglevel='error')

        if arakoon_metadata.internal is True:
            SetupController._log(messages='Restarting master node services')
            remaining_nodes = ip_client_map.keys()[:]
            if cluster_ip in remaining_nodes:
                remaining_nodes.remove(cluster_ip)

            ArakoonInstaller.restart_cluster_remove(arakoon_cluster_name, remaining_nodes)
            PersistentFactory.store = None
            VolatileFactory.store = None

            for service in storagerouter.services:
                if service.name == 'arakoon-ovsdb':
                    service.delete()

        if storagerouter in offline_nodes:
            if unconfigure_rabbitmq is True:
                SetupController._log(messages='Removing/unconfiguring offline RabbitMQ node', loglevel='debug')
                client = ip_client_map[master_ip]
                try:
                    client.run('rabbitmqctl forget_cluster_node rabbit@{0}'.format(storagerouter.name))
                except Exception as ex:
                    SetupController._log(messages=['\nFailed to forget RabbitMQ cluster node', ex], loglevel='error')
        else:
            target_client = ip_client_map[cluster_ip]
            if unconfigure_rabbitmq is True:
                SetupController._log(messages='Removing/unconfiguring RabbitMQ', loglevel='debug')
                try:
                    if ServiceManager.has_service('rabbitmq-server', client=target_client):
                        target_client.run('rabbitmq-server -detached 2> /dev/null; sleep 5; rabbitmqctl stop_app; sleep 5;')
                        target_client.run('rabbitmqctl reset; sleep 5;')
                        target_client.run('rabbitmqctl stop; sleep 5;')
                        Toolbox.change_service_state(target_client, 'rabbitmq-server', 'stop', logger)
                        target_client.file_unlink("/var/lib/rabbitmq/.erlang.cookie")
                except Exception as ex:
                    SetupController._log(messages=['\nFailed to remove/unconfigure RabbitMQ', ex], loglevel='error')

            SetupController._log(messages='Removing services')
            services = ['memcached', 'rabbitmq-server', 'scheduled-tasks', 'snmp', 'webapp-api']
            if unconfigure_rabbitmq is False:
                services.remove('rabbitmq-server')
            if unconfigure_memcached is False:
                services.remove('memcached')
            for service in services:
                if ServiceManager.has_service(service, client=target_client):
                    SetupController._log(messages='Removing service {0}'.format(service), loglevel='debug')
                    try:
                        Toolbox.change_service_state(target_client, service, 'stop', logger)
                        ServiceManager.remove_service(service, client=target_client)
                    except Exception as ex:
                        SetupController._log(messages=['\nFailed to remove service'.format(service), ex], loglevel='error')

            if ServiceManager.has_service('workers', client=target_client):
                ServiceManager.add_service(name='workers',
                                           client=target_client,
                                           params={'MEMCACHE_NODE_IP': cluster_ip,
                                                   'WORKER_QUEUE': '{0}'.format(unique_id)})
        try:
            SetupController._configure_amqp_to_volumedriver()
        except Exception as ex:
            SetupController._log(messages=['\nFailed to configure AMQP to Storage Driver', ex], loglevel='error')

        SetupController._log(messages='Restarting services', loglevel='debug')
        master_ips = [sr.ip for sr in StorageRouterList.get_masters()]
        slave_ips = [sr.ip for sr in StorageRouterList.get_slaves()]
        SetupController._restart_framework_and_memcache_services(master_ips, slave_ips, ip_client_map, offline_node_ips)

        if SetupController._run_hooks('demote', cluster_ip, master_ip, offline_node_ips=offline_node_ips):
            SetupController._log(messages='Restarting services', loglevel='debug')
            SetupController._restart_framework_and_memcache_services(master_ips, slave_ips, ip_client_map, offline_node_ips)

        if storagerouter not in offline_nodes:
            target_client = ip_client_map[cluster_ip]
            node_name = target_client.run('hostname -s')
            if SetupController._avahi_installed(target_client) is True:
                SetupController._configure_avahi(target_client, cluster_name, node_name, 'extra')
        EtcdConfiguration.set('/ovs/framework/hosts/{0}/type'.format(storagerouter.machine_id), 'EXTRA')
        SetupController._log(messages='Demote complete')

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
        SetupController._log(messages='Setting up Memcached')
        client.run("""sed -i 's/^-l.*/-l 0.0.0.0/g' /etc/memcached.conf""")
        client.run("""sed -i 's/^-m.*/-m 1024/g' /etc/memcached.conf""")
        client.run("""sed -i -E 's/^-v(.*)/# -v\1/g' /etc/memcached.conf""")  # Put all -v, -vv, ... back in comment
        client.run("""sed -i 's/^# -v[^v]*$/-v/g' /etc/memcached.conf""")     # Uncomment only -v

    @staticmethod
    def _configure_rabbitmq(client):
        SetupController._log(messages='Setting up RabbitMQ', loglevel='debug')
        rabbitmq_port = EtcdConfiguration.get('/ovs/framework/messagequeue|endpoints')[0].split(':')[1]
        rabbitmq_login = EtcdConfiguration.get('/ovs/framework/messagequeue|user')
        rabbitmq_password = EtcdConfiguration.get('/ovs/framework/messagequeue|password')
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
            users = [user.split('\t')[0] for user in client.run('rabbitmqctl list_users').splitlines()[1:-1]]
            if 'ovs' in users:
                SetupController._log(messages='Already configured RabbitMQ')
                return
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
        SetupController._log(messages='Update existing vPools')
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
        installed = client.run('which avahi-daemon || :')
        if installed == '':
            SetupController._log(messages='Avahi not installed', loglevel='debug')
            return False
        else:
            SetupController._log(messages='Avahi installed', loglevel='debug')
            return True

    @staticmethod
    def _configure_avahi(client, cluster_name, node_name, node_type):
        SetupController._log(messages='Announcing service', title=True)
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
    def _add_services(client, unique_id, node_type):
        SetupController._log(messages='Adding services')
        services = ['workers', 'volumerouter-consumer', 'watcher-framework']
        worker_queue = unique_id
        if node_type == 'master':
            services += ['memcached', 'rabbitmq-server', 'scheduled-tasks', 'snmp', 'webapp-api']
            worker_queue += ',ovs_masters'

        params = {'MEMCACHE_NODE_IP': client.ip,
                  'WORKER_QUEUE': worker_queue}
        for service in services:
            if not ServiceManager.has_service(service, client):
                SetupController._log(messages='Adding service {0}'.format(service), loglevel='debug')
                ServiceManager.add_service(service, params=params, client=client)

    @staticmethod
    def _remove_services(client, node_type):
        SetupController._log(messages='Removing services')
        services = ['workers', 'volumerouter-consumer', 'support-agent', 'watcher-framework']
        if node_type == 'master':
            services += ['memcached', 'rabbitmq-server', 'scheduled-tasks', 'snmp', 'webapp-api']

        for service in services:
            if ServiceManager.has_service(service, client=client):
                SetupController._log(messages='Removing service {0}'.format(service), loglevel='debug')
                ServiceManager.stop_service(service, client=client)
                ServiceManager.remove_service(service, client=client)

    @staticmethod
    def _finalize_setup(client, node_name, node_type, hypervisor_info, unique_id):
        cluster_ip = client.ip
        client.dir_create('/opt/OpenvStorage/webapps/frontend/logging')
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

        SetupController._log(messages='Configuring/updating model')
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
                    nodes[cluster_name][node_name] = {'ip': '', 'type': ''}
                try:
                    ip = '{0}.{1}.{2}.{3}'.format(cluster_info[4], cluster_info[5], cluster_info[6], cluster_info[7])
                except IndexError:
                    ip = entry_parts[7]
                nodes[cluster_name][node_name]['ip'] = ip
                nodes[cluster_name][node_name]['type'] = entry_parts[4].split('_')[2]
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
            SetupController._log(messages='Running "{0}" hooks'.format(hook_type), title=True)
        for function in functions:
            if master_ip is None:
                function(cluster_ip=cluster_ip, **kwargs)
            else:
                function(cluster_ip=cluster_ip, master_ip=master_ip, **kwargs)
        return functions_found

    @staticmethod
    def _ask_validate_password(ip, username='root', previous=None):
        """
        Asks a user to enter the password for a given user on a given ip and validates it
        If previous is provided, we first attempt to login using the previous password, if successful, we don't ask for a password
        """
        while True:
            try:
                try:
                    SSHClient(ip, username)
                    return None
                except AuthenticationException:
                    pass
                if previous is not None:
                    try:
                        SSHClient(ip, username=username, password=previous)
                        return previous
                    except:
                        pass
                node_string = 'this node' if ip == '127.0.0.1' else ip
                password = Interactive.ask_password('Enter the {0} password for {1}'.format(username, node_string))
                if password in ['', None]:
                    continue
                SSHClient(ip, username=username, password=password)
                return password
            except KeyboardInterrupt:
                raise
            except:
                previous = None
                SetupController._log(messages='Password invalid or could not connect to this node')

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
    def _validate_and_retrieve_pre_config():
        """
        Validate whether the values in the pre-configuration file are valid
        :return: JSON contents
        """
        preconfig = '/opt/OpenvStorage/config/openvstorage_preconfig.json'
        if not os.path.exists(preconfig):
            return

        config = {}
        with open(preconfig, 'r') as pre_config:
            try:
                config = json.loads(pre_config.read())
            except Exception as ex:
                raise ValueError('JSON contents could not be retrieved from file {0}.\nError message: {1}'.format(preconfig, ex))

        if 'setup' not in config or not isinstance(config['setup'], dict):
            raise ValueError('The OpenvStorage pre-configuration file must contain a "setup" key with a dictionary as value')

        errors = []
        config = config['setup']
        actual_keys = config.keys()
        expected_keys = ['cluster_ip', 'enable_heartbeats', 'external_etcd', 'hypervisor_ip', 'hypervisor_name', 'hypervisor_password',
                         'hypervisor_type', 'hypervisor_username', 'master_ip', 'master_password', 'node_type']
        for key in actual_keys:
            if key not in expected_keys:
                errors.append('Key {0} is not supported by OpenvStorage to be used in the pre-configuration JSON'.format(key))
        if len(errors) > 0:
            raise ValueError('\nErrors found while verifying pre-configuration:\n - {0}\n\nAllowed keys:\n - {1}'.format('\n - '.join(errors), '\n - '.join(expected_keys)))

        Toolbox.verify_required_params(actual_params=config,
                                       required_params={'cluster_ip': (str, Toolbox.regex_ip, False),
                                                        'enable_heartbeats': (bool, None, False),
                                                        'external_etcd': (str, None, False),
                                                        'hypervisor_ip': (str, Toolbox.regex_ip, False),
                                                        'hypervisor_name': (str, None),
                                                        'hypervisor_password': (str, None, False),
                                                        'hypervisor_type': (str, ['VMWARE', 'KVM']),
                                                        'hypervisor_username': (str, None, False),
                                                        'master_ip': (str, Toolbox.regex_ip),
                                                        'master_password': (str, None),
                                                        'node_type': (str, ['master', 'extra'], False)})
        if config['hypervisor_type'] == 'VMWARE':
            ip = config.get('hypervisor_ip')
            username = config.get('hypervisor_username')
            password = config.get('hypervisor_password')
            if ip is None or username is None or password is None:
                raise ValueError('Hypervisor credentials and IP are required for VMWARE unattended installation')
            try:
                SetupController._validate_hypervisor_information(ip=ip,
                                                                 username=username,
                                                                 password=password)
            except Exception as ex:
                raise RuntimeError('Could not connect to {0}: {1}'.format(ip, ex))
        return config

    @staticmethod
    def _retrieve_storagerouters(ip, password):
        """
        Retrieve the storagerouters from model
        """
        storagerouters = {}
        try:
            from ovs.dal.lists.storagerouterlist import StorageRouterList
            with Remote(ip_info=ip, modules=[StorageRouterList], username='root', password=password, strict_host_key_checking=False) as remote:
                for sr in remote.StorageRouterList.get_storagerouters():
                    storagerouters[sr.name] = {'ip': sr.ip,
                                               'type': sr.node_type.lower()}
        except Exception as ex:
            logger.error('Error loading storagerouters: {0}'.format(ex))
        return storagerouters

    @staticmethod
    def _is_internally_managed(service):
        """
        Validate whether the service is internally or externally managed
        Etcd has been verified at this point and should be reachable
        :param service: Service to verify (either memcached or rabbitmq)
        :type service: str

        :return: True or False
        """
        if service not in ['memcached', 'rabbitmq']:
            raise ValueError('Can only check memcached or rabbitmq')

        etcd_key = {'memcached': 'memcache',
                    'rabbitmq': 'messagequeue'}[service]
        etcd_key = '/ovs/framework/{0}'.format(etcd_key)
        if not EtcdConfiguration.exists(key=etcd_key):
            return True

        if not EtcdConfiguration.exists(key='{0}|metadata'.format(etcd_key)):
            raise ValueError('Not all required keys ({0}) for {1} are present in the Etcd cluster'.format(etcd_key, service))
        metadata = EtcdConfiguration.get('{0}|metadata'.format(etcd_key))
        if 'internal' not in metadata:
            raise ValueError('Internal flag not present in metadata for {0}.\nPlease provide a key: {1} and value "metadata": {{"internal": True/False}}'.format(service, etcd_key))

        internal = metadata['internal']
        if internal is False:
            if not EtcdConfiguration.exists(key='{0}|endpoints'.format(etcd_key)):
                raise ValueError('Externally managed {0} cluster must have "endpoints" information\nPlease provide a key: {1} and value "endpoints": [<ip:port>]'.format(service, etcd_key))
            endpoints = EtcdConfiguration.get(key='{0}|endpoints'.format(etcd_key))
            if not isinstance(endpoints, list) or len(endpoints) == 0:
                raise ValueError('The endpoints for {0} cannot be empty and must be a list'.format(service))
        return internal

    @staticmethod
    def _validate_hypervisor_information(ip, username, password):
        """
        Validate the hypervisor information provided either by preconfig or by manually entering it
        :param ip: IP of the hypervisor
        :type ip: str

        :param username: Username used to login on hypervisor
        :type username: str

        :param password: Password used to login on hypervisor
        :type password: str

        :return: None
        """
        request = urllib2.Request('https://{0}/mob'.format(ip))
        auth = base64.encodestring('{0}:{1}'.format(username, password)).replace('\n', '')
        request.add_header("Authorization", "Basic %s" % auth)
        urllib2.urlopen(request).read()

    @staticmethod
    def _retrieve_external_etcd_info():
        """
        Retrieve external Etcd information interactively
        :return: External Etcd or None
        """
        external_etcd = None
        if Interactive.ask_yesno(message='Use an external Etcd cluster?', default_value=False) is True:
            SetupController._log(messages='Provide the connection information to 1 of the external Etcd servers (Can be requested by executing "etcdctl member list")')
            etcd_ip = Interactive.ask_string(message='Provide the peer IP address of that member',
                                             regex_info={'regex': SSHClient.IP_REGEX,
                                                         'message': 'Incorrect Etcd IP provided'})
            etcd_port = Interactive.ask_integer(question='Provide the port for the given IP address of that member',
                                                min_value=1025, max_value=65535, default_value=2380)
            external_etcd = 'config=http://{0}:{1}'.format(etcd_ip, etcd_port)
        return external_etcd

    @staticmethod
    def _log(messages, title=False, boxed=False, loglevel='info'):
        """
        Print a message on stdout and log to file
        :param messages: Messages to print and log
        :type messages: str or list

        :param title: If True some extra chars will be pre- and appended
        :type title: bool

        :param boxed: Use the Interactive boxed message print option
        :type boxed: bool

        :param loglevel: level to log on
        :type loglevel: str

        :return: None
        """
        if type(messages) in (str, basestring, unicode):
            messages = [messages]
        if boxed is True:
            print Interactive.boxed_message(lines=messages)
        else:
            for message in messages:
                if title is True:
                    message = '\n+++ {0} +++\n'.format(message)
                if loglevel == 'error':
                    message = 'ERROR: {0}'.format(message)
                print message

        for message in messages:
            getattr(logger, loglevel)(message)
