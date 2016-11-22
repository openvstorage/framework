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
Module for SetupController
"""

import os
import re
import sys
import json
import time
import signal
from paramiko import AuthenticationException
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonClusterConfig, ArakoonInstaller
from ovs.extensions.db.arakoon.configuration import ArakoonConfiguration
from ovs.extensions.generic.configuration import Configuration, NotFoundException, ConnectionException
from ovs.extensions.generic.interactive import Interactive
from ovs.extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
from ovs.lib.helpers.toolbox import Toolbox
from ovs.log.log_handler import LogHandler


class SetupController(object):
    """
    This class contains all logic for setting up an environment, installed with system-native packages
    """
    _logger = LogHandler.get('lib', name='setup')
    _logger.logger.propagate = False

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

        rdma = None
        master_ip = None
        cluster_ip = None
        logging_target = None
        external_config = None
        master_password = None
        config_mgmt_type = None
        enable_heartbeats = True

        try:
            # Support non-interactive setup
            config = SetupController._validate_and_retrieve_pre_config()
            if config is not None:
                # Required fields
                master_ip = config['master_ip']
                master_password = config['master_password']

                # Optional fields
                rdma = config.get('rdma', False)
                node_type = config.get('node_type', node_type)
                cluster_ip = config.get('cluster_ip', master_ip)  # If cluster_ip not provided, we assume 1st node installation
                config_mgmt_type = config.get('config_mgmt_type')
                logging_target = config.get('logging_target', logging_target)
                external_config = config.get('external_config')
                enable_heartbeats = config.get('enable_heartbeats', enable_heartbeats)

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

            ipaddresses = root_client.run("ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1", allow_insecure=True).strip().splitlines()
            SetupController.host_ips = set([found_ip.strip() for found_ip in ipaddresses if found_ip.strip() != '127.0.0.1'])

            setup_completed = False
            promote_completed = False
            try:
                type_node = Configuration.get('/ovs/framework/hosts/{0}/type'.format(unique_id))
                setup_completed = Configuration.get('/ovs/framework/hosts/{0}/setupcompleted'.format(unique_id))
                if type_node == 'MASTER':
                    promote_completed = Configuration.get('/ovs/framework/hosts/{0}/promotecompleted'.format(unique_id))
                if setup_completed is True and (promote_completed is True or type_node == 'EXTRA'):
                    raise RuntimeError('This node has already been configured for Open vStorage. Re-running the setup is not supported.')
            except (IOError, NotFoundException, ConnectionException):
                pass

            if setup_completed is False:
                SetupController._log(messages='Collecting cluster information', title=True)

                if root_client.file_exists('/etc/openvstorage_id') is False:
                    raise RuntimeError("The 'openvstorage' package is not installed on this node")

                node_name, fqdn_name = root_client.get_hostname()
                avahi_installed = SetupController._avahi_installed(root_client)

                SetupController._logger.debug('Current host: {0}'.format(node_name))
                node_type = resume_config.get('node_type', node_type)
                master_ip = resume_config.get('master_ip', master_ip)
                cluster_ip = resume_config.get('cluster_ip', cluster_ip)
                external_config = resume_config.get('external_config', external_config)
                config_mgmt_type = resume_config.get('config_mgmt_type', config_mgmt_type)
                enable_heartbeats = resume_config.get('enable_heartbeats', enable_heartbeats)

                if config is None:  # Non-automated install
                    SetupController._logger.debug('Cluster selection')
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

                    elif cluster_name == join_manually:  # Join an existing cluster manually
                        first_node = False
                        cluster_name = None
                        cluster_ip = Interactive.ask_choice(SetupController.host_ips, 'Select the public IP address of {0}'.format(node_name))
                        master_ip = Interactive.ask_string(message='Please enter the IP of one of the cluster\'s master nodes',
                                                           regex_info={'regex': SSHClient.IP_REGEX,
                                                                       'message': 'Incorrect IP provided'})
                        if master_ip in root_client.local_ips:
                            raise ValueError("A local IP address was given, please select '{0}' or provide another IP address".format(new_cluster))

                        SetupController._logger.debug('Trying to manually join cluster on {0}'.format(master_ip))

                        master_password = SetupController._ask_validate_password(master_ip, username='root')
                        SetupController.nodes = SetupController._retrieve_storagerouters(ip=master_ip, password=master_password)
                        master_ips = [sr_info['ip'] for sr_info in SetupController.nodes.itervalues() if sr_info['type'] == 'master']
                        if master_ip not in master_ips:
                            if master_ips:
                                raise ValueError('Incorrect master IP provided, please choose from: {0}'.format(', '.join(master_ips)))
                            else:
                                raise ValueError('Could not load master information at {0}. Is that node running correctly?'.format(master_ip))

                        current_sr_message = []
                        for sr_name in sorted(SetupController.nodes):
                            current_sr_message.append('{0:<15} - {1}'.format(SetupController.nodes[sr_name]['ip'], sr_name))
                        if Interactive.ask_yesno(message='Following StorageRouters were detected:\n  -  {0}\nIs this correct?'.format('\n  -  '.join(current_sr_message)),
                                                 default_value=True) is False:
                            raise Exception('The cluster on the given master node cannot be joined as not all StorageRouters could be loaded')

                    else:  # Join an existing cluster automatically
                        SetupController._logger.debug('Cluster {0} selected'.format(cluster_name))
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
                    SetupController._logger.debug('Automated installation')
                    cluster_ip = master_ip if cluster_ip is None else cluster_ip
                    first_node = master_ip == cluster_ip
                    cluster_name = 'preconfig-{0}'.format(master_ip.replace('.', '-'))
                    SetupController._logger.info('Detected{0} a 1st node installation'.format('' if first_node is True else ' not'))

                    if first_node is False:
                        SetupController.nodes = SetupController._retrieve_storagerouters(ip=master_ip, password=master_password)
                    else:
                        SetupController.nodes[node_name] = {'ip': master_ip,
                                                            'type': 'master'}

                    # Validation of parameters
                    if master_ip != cluster_ip:
                        master_ips = [sr_info['ip'] for sr_info in SetupController.nodes.itervalues() if sr_info['type'] == 'master']
                        if master_ip not in master_ips:
                            if master_ips:
                                raise ValueError('Incorrect master IP provided, please choose from: {0}'.format(', '.join(master_ips)))
                            else:
                                raise ValueError('Could not load master information at {0}. Is that node running correctly?'.format(master_ip))
                    else:
                        if node_type == 'extra':
                            raise ValueError('A 1st node can never be installed as an "extra" node')
                    if cluster_ip not in SetupController.host_ips:
                        raise ValueError('{0} IP provided {1} is not in the list of local IPs: {2}'.format('Master' if master_ip == cluster_ip else 'Cluster',
                                                                                                           cluster_ip,
                                                                                                           ', '.join(SetupController.host_ips)))

                if len(SetupController.nodes) == 0:
                    SetupController._logger.debug('No StorageRouters could be loaded, cannot join the cluster')
                    raise RuntimeError('The cluster on the given master node cannot be joined as no StorageRouters could be loaded')

                if cluster_ip is None or master_ip is None:  # Master IP and cluster IP must be known by now, cluster_ip == master_ip for 1st node
                    raise ValueError('Something must have gone wrong retrieving IP information')

                if node_name != fqdn_name:
                    ip_hostname_map = {cluster_ip: [fqdn_name, node_name]}
                else:
                    ip_hostname_map = {cluster_ip: [fqdn_name]}

                for node_host_name, node_info in SetupController.nodes.iteritems():
                    ip = node_info['ip']
                    if ip == master_ip:
                        node_client = node_info.get('client', SSHClient(endpoint=ip, username='root', password=master_password))
                        node_info['client'] = node_client

                        _, master_fqdn_name = node_client.get_hostname()
                        if node_host_name != master_fqdn_name:
                            ip_hostname_map[ip] = [master_fqdn_name, node_host_name]
                        else:
                            ip_hostname_map[ip] = [master_fqdn_name]
                        break

                if node_name in SetupController.nodes:
                    SetupController.nodes[node_name]['client'] = SSHClient(endpoint=cluster_ip, username='root')
                else:
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
                    if not local_client.file_exists(required_file):
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
                    with remote(master_client.ip, [SSHClient], 'root', master_password) as rem:
                        for node_host_name, node in SetupController.nodes.iteritems():
                            node_ip = node['ip']
                            if node_ip == cluster_ip:
                                continue
                            client = rem.SSHClient(node_ip, 'root')
                            if client.ip not in ip_hostname_map:
                                _, node_fqdn_name = client.get_hostname()
                                if node_host_name != node_fqdn_name:
                                    ip_hostname_map[client.ip] = [node_fqdn_name, node_host_name]
                                else:
                                    ip_hostname_map[client.ip] = [node_fqdn_name]
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
                def _raise_timeout(*args, **kwargs):
                    _ = args, kwargs
                    raise RuntimeError('Timeout during ssh keyscan, please check node inter-connectivity')
                signal.signal(signal.SIGALRM, _raise_timeout)
                for node_details in SetupController.nodes.itervalues():
                    signal.alarm(30)
                    node_client = node_details.get('client', SSHClient(endpoint=node_details['ip'], username='root'))
                    System.update_hosts_file(ip_hostname_map, node_client)
                    cmd = 'cp {{0}} {{0}}.tmp; ssh-keyscan -t rsa {0} {1} 2> /dev/null >> {{0}}.tmp; cat {{0}}.tmp | sort -u - > {{0}}'.format(
                        ' '.join([node_client.shell_safe(_ip) for _ip in all_ips]),
                        ' '.join([node_client.shell_safe(_key) for _key in SetupController.nodes.keys()])
                    )
                    root_command = cmd.format(known_hosts_root)
                    ovs_command = cmd.format(known_hosts_ovs)
                    ovs_command = 'su - ovs -c "{0}"'.format(ovs_command)
                    node_client.run(root_command, allow_insecure=True)
                    node_client.run(ovs_command, allow_insecure=True)
                    signal.alarm(0)

                # Write resume config
                resume_config['node_type'] = node_type
                resume_config['master_ip'] = master_ip
                resume_config['unique_id'] = unique_id
                resume_config['cluster_ip'] = cluster_ip
                resume_config['cluster_name'] = cluster_name
                resume_config['external_config'] = external_config
                resume_config['config_mgmt_type'] = config_mgmt_type
                resume_config['enable_heartbeats'] = enable_heartbeats
                with open(resume_config_file, 'w') as resume_cfg:
                    resume_cfg.write(json.dumps(resume_config))

                ip_client_map = dict((info['ip'], SSHClient(info['ip'], username='root')) for info in SetupController.nodes.itervalues())
                if first_node is True:
                    try:
                        SetupController._setup_first_node(target_client=ip_client_map[cluster_ip],
                                                          unique_id=unique_id,
                                                          cluster_name=cluster_name,
                                                          node_name=node_name,
                                                          enable_heartbeats=enable_heartbeats,
                                                          external_config={'type': config_mgmt_type,
                                                                           'external': external_config},
                                                          logging_target=logging_target,
                                                          rdma=rdma)
                    except Exception as ex:
                        SetupController._log(messages=['Failed to setup first node', ex], loglevel='exception')
                        SetupController._rollback_setup(target_client=ip_client_map[cluster_ip],
                                                        first_node=True)
                        raise
                else:
                    # Deciding master/extra
                    try:
                        SetupController._setup_extra_node(cluster_ip=cluster_ip,
                                                          master_ip=master_ip,
                                                          unique_id=unique_id,
                                                          ip_client_map=ip_client_map)
                    except Exception as ex:
                        SetupController._log(messages=['Failed to setup extra node', ex], loglevel='exception')
                        SetupController._rollback_setup(target_client=ip_client_map[cluster_ip],
                                                        first_node=False)
                        raise

                    if promote_completed is False:
                        SetupController._log(messages='Analyzing cluster layout')
                        framework_cluster_name = str(Configuration.get('/ovs/framework/arakoon_clusters|ovsdb'))
                        config = ArakoonClusterConfig(cluster_id=framework_cluster_name, filesystem=False)
                        config.load_config()
                        SetupController._logger.debug('{0} nodes for cluster {1} found'.format(len(config.nodes), framework_cluster_name))
                        if (len(config.nodes) < 3 or node_type == 'master') and node_type != 'extra':
                            configure_rabbitmq = SetupController._is_internally_managed(service='rabbitmq')
                            configure_memcached = SetupController._is_internally_managed(service='memcached')
                            try:
                                SetupController._promote_node(cluster_ip=cluster_ip,
                                                              master_ip=master_ip,
                                                              ip_client_map=ip_client_map,
                                                              unique_id=unique_id,
                                                              configure_memcached=configure_memcached,
                                                              configure_rabbitmq=configure_rabbitmq)
                            except Exception as ex:
                                SetupController._log(messages=['\nFailed to promote node, rolling back', ex], loglevel='exception')
                                SetupController._demote_node(cluster_ip=cluster_ip,
                                                             master_ip=master_ip,
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
            SetupController._logger.info('Setup complete')

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
            SetupController._log(messages=['An unexpected error occurred:', str(exception).lstrip('\n')], boxed=True, loglevel='exception')
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
            if Configuration.get('/ovs/framework/hosts/{0}/setupcompleted'.format(machine_id)) is False:
                raise RuntimeError('No local OVS setup found.')

            node_type = Configuration.get('/ovs/framework/hosts/{0}/type'.format(machine_id))
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
                        client.run(['pwd'])
                        if storage_router.node_type == 'MASTER':
                            master_ip = storage_router.ip
                        ip_client_map[storage_router.ip] = client
                    except UnableToConnectException:
                        if storage_router.ip == cluster_ip:
                            online = False
                            unique_id = storage_router.machine_id
                            StorageDriverController.mark_offline(storagerouter_guid=storage_router.guid)
                        offline_nodes.append(storage_router)
                if online is True:
                    raise RuntimeError("If the node is online, please use 'ovs setup demote' executed on the node you wish to demote")
                if master_ip is None:
                    raise RuntimeError('Failed to retrieve another responsive MASTER node')

            else:
                target_password = SetupController._ask_validate_password('127.0.0.1', username='root')
                target_client = SSHClient('127.0.0.1', username='root', password=target_password)

                unique_id = System.get_my_machine_id(target_client)
                ip = Configuration.get('/ovs/framework/hosts/{0}/ip'.format(unique_id))

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

            if node_action == 'demote':
                for cluster_name in Configuration.list('/ovs/arakoon'):
                    config = ArakoonClusterConfig(cluster_name, False)
                    config.load_config()
                    arakoon_client = ArakoonInstaller.build_client(config)
                    metadata = json.loads(arakoon_client.get(ArakoonInstaller.METADATA_KEY))
                    if len(config.nodes) == 1 and config.nodes[0].ip == master_ip and metadata.get('internal') is True:
                        raise RuntimeError('Demote is not supported when single node Arakoon cluster(s) are present, please promote another node first')

            configure_rabbitmq = SetupController._is_internally_managed(service='rabbitmq')
            configure_memcached = SetupController._is_internally_managed(service='memcached')
            if node_action == 'promote':
                SetupController._promote_node(cluster_ip=ip,
                                              master_ip=master_ip,
                                              ip_client_map=ip_client_map,
                                              unique_id=unique_id,
                                              configure_memcached=configure_memcached,
                                              configure_rabbitmq=configure_rabbitmq)
            else:
                SetupController._demote_node(cluster_ip=ip,
                                             master_ip=master_ip,
                                             ip_client_map=ip_client_map,
                                             unique_id=unique_id,
                                             unconfigure_memcached=configure_memcached,
                                             unconfigure_rabbitmq=configure_rabbitmq,
                                             offline_nodes=offline_nodes)

            SetupController._log(messages='\n')
            SetupController._log(messages='{0} complete.'.format(node_action.capitalize()), boxed=True)
        except Exception as exception:
            SetupController._log(messages='\n')
            SetupController._log(messages=['An unexpected error occurred:', str(exception)], boxed=True, loglevel='exception')
            sys.exit(1)
        except KeyboardInterrupt:
            SetupController._log(messages='\n')
            SetupController._log(messages='This setup was aborted. Open vStorage may be in an inconsistent state, make sure to validate the installation.', boxed=True, loglevel='error')
            sys.exit(1)

    @staticmethod
    def remove_node(node_ip, silent=None):
        """
        Remove the node with specified IP from the cluster
        :param node_ip: IP of the node to remove
        :type node_ip: str
        :param silent: If silent == '--force-yes' no question will be asked to confirm the removal
        :type silent: str
        :return: None
        """
        from ovs.lib.storagedriver import StorageDriverController
        from ovs.lib.storagerouter import StorageRouterController
        from ovs.dal.lists.storagerouterlist import StorageRouterList

        SetupController._log(messages='Remove node', boxed=True)
        SetupController._log(messages='WARNING: Some of these steps may take a very long time, please check the logs for more information\n\n')

        ###############
        # VALIDATIONS #
        ###############
        node_ip = node_ip.strip()
        if not isinstance(node_ip, str):
            raise ValueError('Node IP must be a string')
        if not re.match(SSHClient.IP_REGEX, node_ip):
            raise ValueError('Invalid IP {0} specified'.format(node_ip))

        storage_router_all = StorageRouterList.get_storagerouters()
        storage_router_masters = StorageRouterList.get_masters()
        storage_router_all_ips = set([storage_router.ip for storage_router in storage_router_all])
        storage_router_master_ips = set([storage_router.ip for storage_router in storage_router_masters])
        storage_router_to_remove = StorageRouterList.get_by_ip(node_ip)

        if node_ip not in storage_router_all_ips:
            raise ValueError('Unknown IP specified\nKnown in model:\n - {0}\nSpecified for removal:\n - {1}'.format('\n - '.join(storage_router_all_ips), node_ip))

        if len(storage_router_all_ips) == 1:
            raise RuntimeError("Removing the only node wouldn't be very smart now, would it?")

        if node_ip in storage_router_master_ips and len(storage_router_master_ips) == 1:
            raise RuntimeError("Removing the only master node wouldn't be very smart now, would it?")

        if System.get_my_storagerouter() == storage_router_to_remove:
            raise RuntimeError('The node to be removed cannot be identical to the node on which the removal is initiated')

        SetupController._log(messages='Creating SSH connections to remaining master nodes')
        master_ip = None
        ip_client_map = {}
        storage_routers_offline = []
        storage_router_to_remove_online = True
        for storage_router in storage_router_all:
            try:
                client = SSHClient(storage_router, username='root')
                if client.run(['pwd']):
                    SetupController._log(messages='  Node with IP {0:<15} successfully connected to'.format(storage_router.ip))
                    ip_client_map[storage_router.ip] = client
                    if storage_router != storage_router_to_remove and storage_router.node_type == 'MASTER':
                        master_ip = storage_router.ip
            except UnableToConnectException:
                SetupController._log(messages='  Node with IP {0:<15} is unreachable'.format(storage_router.ip))
                storage_routers_offline.append(storage_router)
                if storage_router == storage_router_to_remove:
                    storage_router_to_remove_online = False

        if len(ip_client_map) == 0 or master_ip is None:
            raise RuntimeError('Could not connect to any master node in the cluster')

        storage_router_to_remove.invalidate_dynamics('vdisks_guids')
        if len(storage_router_to_remove.vdisks_guids) > 0:  # vDisks are supposed to be moved away manually before removing a node
            raise RuntimeError("Still vDisks attached to Storage Router {0}".format(storage_router_to_remove.name))

        internal_memcached = SetupController._is_internally_managed(service='memcached')
        internal_rabbit_mq = SetupController._is_internally_managed(service='rabbitmq')
        memcached_endpoints = Configuration.get(key='/ovs/framework/memcache|endpoints')
        rabbit_mq_endpoints = Configuration.get(key='/ovs/framework/messagequeue|endpoints')
        copy_memcached_endpoints = list(memcached_endpoints)
        copy_rabbit_mq_endpoints = list(rabbit_mq_endpoints)
        for endpoint in memcached_endpoints:
            if endpoint.startswith(storage_router_to_remove.ip):
                copy_memcached_endpoints.remove(endpoint)
        for endpoint in rabbit_mq_endpoints:
            if endpoint.startswith(storage_router_to_remove.ip):
                copy_rabbit_mq_endpoints.remove(endpoint)
        if len(copy_memcached_endpoints) == 0 and internal_memcached is True:
            raise RuntimeError('Removal of provided nodes will result in a complete removal of the memcached service')
        if len(copy_rabbit_mq_endpoints) == 0 and internal_rabbit_mq is True:
            raise RuntimeError('Removal of provided nodes will result in a complete removal of the messagequeue service')

        #################
        # CONFIRMATIONS #
        #################
        interactive = silent != '--force-yes'
        remove_asd_manager = not interactive  # Remove ASD manager if non-interactive else ask
        if interactive is True:
            proceed = Interactive.ask_yesno(message='Are you sure you want to remove node {0}?'.format(storage_router_to_remove.name), default_value=False)
            if proceed is False:
                SetupController._log(messages='Abort removal', title=True)
                sys.exit(1)

            if storage_router_to_remove_online is True:
                client = SSHClient(endpoint=storage_router_to_remove, username='root')
                if ServiceManager.has_service(name='asd-manager', client=client):
                    remove_asd_manager = Interactive.ask_yesno(message='Do you also want to remove the ASD manager and related ASDs?', default_value=False)

            if remove_asd_manager is True or storage_router_to_remove_online is False:
                for function in Toolbox.fetch_hooks('setup', 'validate_asd_removal'):
                    validation_output = function(storage_router_to_remove.ip)
                    if validation_output['confirm'] is True:
                        if Interactive.ask_yesno(message=validation_output['question'], default_value=False) is False:
                            remove_asd_manager = False
                            break

        ###########
        # REMOVAL #
        ###########
        try:
            SetupController._log(messages='Starting removal of node {0} - {1}'.format(storage_router_to_remove.name, storage_router_to_remove.ip))
            if storage_router_to_remove_online is False:
                SetupController._log(messages='  Marking all Storage Drivers served by Storage Router {0} as offline'.format(storage_router_to_remove.ip))
                StorageDriverController.mark_offline(storagerouter_guid=storage_router_to_remove.guid)

            # Remove vPools
            SetupController._log(messages='  Removing vPools from node'.format(storage_router_to_remove.ip))
            storage_routers_offline_guids = [sr.guid for sr in storage_routers_offline if sr.guid != storage_router_to_remove.guid]
            for storage_driver in storage_router_to_remove.storagedrivers:
                SetupController._log(messages='    Removing vPool {0} from node'.format(storage_driver.vpool.name))
                StorageRouterController.remove_storagedriver(storagedriver_guid=storage_driver.guid,
                                                             offline_storage_router_guids=storage_routers_offline_guids)

            # Demote if MASTER
            if storage_router_to_remove.node_type == 'MASTER':
                SetupController._demote_node(cluster_ip=storage_router_to_remove.ip,
                                             master_ip=master_ip,
                                             ip_client_map=ip_client_map,
                                             unique_id=storage_router_to_remove.machine_id,
                                             unconfigure_memcached=internal_memcached,
                                             unconfigure_rabbitmq=internal_rabbit_mq,
                                             offline_nodes=storage_routers_offline)

            # Stop / remove services
            SetupController._log(messages='    Stopping and removing services')
            config_store = Configuration.get_store()
            if storage_router_to_remove_online is True:
                client = SSHClient(endpoint=storage_router_to_remove, username='root')
                SetupController._remove_services(client=client, node_type=storage_router_to_remove.node_type.lower())
                service = 'watcher-config'
                if ServiceManager.has_service(service, client=target_client):
                    SetupController._log(messages='Removing service {0}'.format(service), loglevel='debug')
                    ServiceManager.stop_service(service, client=target_client)
                    ServiceManager.remove_service(service, client=target_client)

                if config_store == 'etcd':
                    from ovs.extensions.db.etcd.installer import EtcdInstaller

                    if Configuration.get(key='/ovs/framework/external_config') is None:
                        SetupController._log(messages='      Removing Etcd cluster')
                        try:
                            EtcdInstaller.stop('config', client)
                            EtcdInstaller.remove('config', client)
                        except Exception as ex:
                            SetupController._log(messages=['\nFailed to unconfigure Etcd', ex], loglevel='exception')

                    SetupController._log(messages='      Removing Etcd proxy')
                    EtcdInstaller.remove_proxy('config', client.ip)

            # Clean up model
            SetupController._log(messages='    Removing node from model')
            SetupController._run_hooks('remove', storage_router_to_remove.ip, complete_removal=remove_asd_manager)
            for service in storage_router_to_remove.services:
                service.delete()
            for disk in storage_router_to_remove.disks:
                for partition in disk.partitions:
                    partition.delete()
                disk.delete()
            for j_domain in storage_router_to_remove.domains:
                j_domain.delete()
            Configuration.delete('/ovs/framework/hosts/{0}'.format(storage_router_to_remove.machine_id))

            master_ips = [sr.ip for sr in storage_router_masters]
            slave_ips = [sr.ip for sr in StorageRouterList.get_slaves()]
            offline_node_ips = [node.ip for node in storage_routers_offline]
            SetupController._restart_framework_and_memcache_services(master_ips, slave_ips, ip_client_map, offline_node_ips)

            if storage_router_to_remove_online is True:
                client = SSHClient(endpoint=storage_router_to_remove, username='root')
                if config_store == 'arakoon':
                    client.file_delete(filenames=[ArakoonConfiguration.CACC_LOCATION])
                client.file_delete(filenames=[Configuration.BOOTSTRAP_CONFIG_LOCATION])
            storage_router_to_remove.delete()
            SetupController._log(messages='    Successfully removed node\n')
        except Exception as exception:
            SetupController._log(messages='\n')
            SetupController._log(messages=['An unexpected error occurred:', str(exception)], boxed=True, loglevel='exception')
            sys.exit(1)
        except KeyboardInterrupt:
            SetupController._log(messages='\n')
            SetupController._log(messages='This setup was aborted. Open vStorage may be in an inconsistent state, make sure to validate the installation.', boxed=True, loglevel='error')
            sys.exit(1)

        if remove_asd_manager is True:
            SetupController._log(messages='\nRemoving ASD Manager')
            with remote(storage_router_to_remove.ip, [os]) as rem:
                rem.os.system('asd-manager remove --force-yes')
        SetupController._log(messages='Remove nodes finished', title=True)

    @staticmethod
    def _setup_first_node(target_client, unique_id, cluster_name, node_name, enable_heartbeats, external_config, logging_target, rdma):
        """
        Sets up the first node services. This node is always a master
        """
        SetupController._log(messages='Setting up first node', title=True)
        cluster_ip = target_client.ip
        machine_id = System.get_my_machine_id(target_client)

        SetupController._log(messages='Setting up configuration management')
        if external_config['type'] is None:
            store = Interactive.ask_choice(['Arakoon', 'Etcd'],
                                           question='Select the configuration management system',
                                           default_value='Arakoon').lower()
            external = None
            if Interactive.ask_yesno(message='Use an external cluster?', default_value=False) is True:
                if store == 'arakoon':
                    from ovs.extensions.db.arakoon.configuration import ArakoonConfiguration
                    file_location = ArakoonConfiguration.CACC_LOCATION
                    while not target_client.file_exists(file_location):
                        SetupController._log(messages='Please place a copy of the Arakoon\'s client configuration file at: {0}'.format(file_location))
                        Interactive.ask_continue()
                    external = True
                else:
                    SetupController._log(messages='Provide the connection information to 1 of the Etcd servers (Can be requested by executing "etcdctl member list")')
                    etcd_ip = Interactive.ask_string(message='Provide the peer IP address of that member',
                                                     regex_info={'regex': SSHClient.IP_REGEX,
                                                                 'message': 'Incorrect Etcd IP provided'})
                    etcd_port = Interactive.ask_integer(question='Provide the port for the given IP address of that member',
                                                        min_value=1025, max_value=65535, default_value=2380)
                    external = 'config=http://{0}:{1}'.format(etcd_ip, etcd_port)
            config = {'type': store,
                      'external': external}
        else:
            config = external_config
        if config['type'] == 'arakoon':
            SetupController._log(messages='Setting up configuration Arakoon')
            from ovs.extensions.db.arakoon.configuration import ArakoonConfiguration
            if config['external'] is None:
                info = ArakoonInstaller.create_cluster(cluster_name='config',
                                                       cluster_type=ServiceType.ARAKOON_CLUSTER_TYPES.CFG,
                                                       ip=cluster_ip,
                                                       base_dir='/opt/OpenvStorage/db',
                                                       locked=False,
                                                       filesystem=True,
                                                       ports=[26400, 26401])
                ArakoonInstaller.start_cluster(cluster_name='config',
                                               master_ip=cluster_ip,
                                               filesystem=True)
                ArakoonInstaller.claim_cluster(cluster_name='config',
                                               master_ip=cluster_ip,
                                               filesystem=True,
                                               metadata=info['metadata'])
                contents = target_client.file_read(ArakoonClusterConfig.CONFIG_FILE.format('config'))
                target_client.file_write(ArakoonConfiguration.CACC_LOCATION, contents)
            else:
                ArakoonInstaller.claim_cluster(cluster_name='cacc',
                                               master_ip=cluster_ip,
                                               filesystem=True,
                                               metadata={'internal': False,
                                                         'cluster_name': 'cacc',
                                                         'cluster_type': ServiceType.ARAKOON_CLUSTER_TYPES.CFG,
                                                         'in_use': True})
                arakoon_config = ArakoonClusterConfig('cacc', True)
                arakoon_config.load_config(cluster_ip)
                arakoon_client = ArakoonInstaller.build_client(arakoon_config)
                arakoon_client.set(ArakoonInstaller.INTERNAL_CONFIG_KEY, arakoon_config.export_ini())

        else:
            SetupController._log(messages='Setting up Etcd')
            from etcd import EtcdConnectionFailed, EtcdException, EtcdKeyError
            from ovs.extensions.db.etcd.installer import EtcdInstaller
            if config['external'] is None:
                EtcdInstaller.create_cluster('config', cluster_ip)
            else:
                try:
                    EtcdInstaller.use_external(config['external'], cluster_ip, 'config')
                except (EtcdConnectionFailed, EtcdException, EtcdKeyError):
                    SetupController._log(messages='Failed to set up Etcd proxy')
                    resume_config_file = '/opt/OpenvStorage/config/openvstorage_resumeconfig.json'
                    if target_client.file_exists(resume_config_file):
                        with open(resume_config_file, 'r') as resume_cfg:
                            resume_config = json.loads(resume_cfg.read())
                            if 'external_config' in resume_config:
                                resume_config.pop('external_config')
                        with open(resume_config_file, 'w') as resume_cfg:
                            resume_cfg.write(json.dumps(resume_config))
                    raise
        bootstrap_location = Configuration.BOOTSTRAP_CONFIG_LOCATION
        if not target_client.file_exists(bootstrap_location):
            target_client.file_create(bootstrap_location)
        target_client.file_write(bootstrap_location, json.dumps({'configuration_store': config['type']}, indent=4))

        Configuration.initialize(external_config=external_config['external'], logging_target=logging_target)
        Configuration.initialize_host(machine_id)

        if rdma is None:
            rdma = Interactive.ask_yesno(message='Enable RDMA?', default_value=False)
        Configuration.set('/ovs/framework/rdma', rdma)
        Configuration.set('/ovs/framework/cluster_name', cluster_name)

        service = 'watcher-config'
        if not ServiceManager.has_service(service, target_client):
            SetupController._log(messages='Adding service {0}'.format(service), loglevel='debug')
            ServiceManager.add_service(service, params={}, client=target_client)
            Toolbox.change_service_state(target_client, service, 'start', SetupController._logger)

        metadata = ArakoonInstaller.get_unused_arakoon_metadata_and_claim(cluster_type=ServiceType.ARAKOON_CLUSTER_TYPES.FWK, locked=False)
        arakoon_ports = []
        if metadata is None:  # No externally managed cluster found, we create 1 ourselves
            SetupController._log(messages='Setting up Arakoon cluster ovsdb')
            internal = True
            result = ArakoonInstaller.create_cluster(cluster_name='ovsdb',
                                                     cluster_type=ServiceType.ARAKOON_CLUSTER_TYPES.FWK,
                                                     ip=cluster_ip,
                                                     base_dir=Configuration.get('/ovs/framework/paths|ovsdb'),
                                                     locked=False)
            ArakoonInstaller.start_cluster(cluster_name='ovsdb',
                                           master_ip=cluster_ip,
                                           filesystem=False)
            ArakoonInstaller.claim_cluster(cluster_name='ovsdb',
                                           master_ip=cluster_ip,
                                           filesystem=False,
                                           metadata=result['metadata'])
            arakoon_ports = [result['client_port'], result['messaging_port']]
            metadata = result['metadata']
        else:
            SetupController._log(messages='Externally managed Arakoon cluster of type {0} found with name {1}'.format(ServiceType.ARAKOON_CLUSTER_TYPES.FWK, metadata['cluster_name']))
            internal = False

        Configuration.set('/ovs/framework/arakoon_clusters|ovsdb', metadata['cluster_name'])
        SetupController._add_services(target_client, unique_id, 'master')
        SetupController._log(messages='Build configuration files')

        configure_rabbitmq = SetupController._is_internally_managed(service='rabbitmq')
        configure_memcached = SetupController._is_internally_managed(service='memcached')
        if configure_rabbitmq is True:
            Configuration.set('/ovs/framework/messagequeue|endpoints', ['{0}:5672'.format(cluster_ip)])
            SetupController._configure_rabbitmq(target_client)
        if configure_memcached is True:
            Configuration.set('/ovs/framework/memcache|endpoints', ['{0}:11211'.format(cluster_ip)])
            SetupController._configure_memcached(target_client)
        SetupController._configure_redis(target_client)
        Toolbox.change_service_state(target_client, 'redis-server', 'restart', SetupController._logger)
        VolatileFactory.store = None

        SetupController._log(messages='Starting model services', loglevel='debug')
        model_services = ['memcached', 'arakoon-ovsdb'] if internal is True else ['memcached']
        for service in model_services:
            if ServiceManager.has_service(service, client=target_client):
                Toolbox.change_service_state(target_client, service, 'restart', SetupController._logger)

        SetupController._log(messages='Start model migration', loglevel='debug')
        from ovs.dal.helpers import Migration
        Migration.migrate()

        SetupController._log(messages='Finalizing setup', title=True)
        storagerouter = SetupController._finalize_setup(target_client, node_name, 'MASTER', unique_id)

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
        Configuration.set('/ovs/framework/hosts/{0}/ip'.format(machine_id), cluster_ip)

        SetupController._log(messages='Starting services on 1st node')
        for service in model_services + ['rabbitmq-server']:
            if ServiceManager.has_service(service, client=target_client):
                Toolbox.change_service_state(target_client, service, 'start', SetupController._logger)
        # Enable HA for the rabbitMQ queues
        SetupController._check_rabbitmq_and_enable_ha_mode(target_client)

        for service in ['watcher-framework', 'watcher-config']:
            Toolbox.change_service_state(target_client, service, 'start', SetupController._logger)

        SetupController._log(messages='Check ovs-workers', loglevel='debug')
        # Workers are started by ovs-watcher-framework, but for a short time they are in pre-start
        Toolbox.wait_for_service(target_client, 'workers', True, SetupController._logger)

        SetupController._run_hooks('firstnode', cluster_ip)

        if enable_heartbeats is False:
            Configuration.set('/ovs/framework/support|enabled', False)
        else:
            service = 'support-agent'
            if not ServiceManager.has_service(service, target_client):
                ServiceManager.add_service(service, client=target_client)
                Toolbox.change_service_state(target_client, service, 'start', SetupController._logger)

        if SetupController._avahi_installed(target_client) is True:
            SetupController._configure_avahi(target_client, node_name, 'master')
        Configuration.set('/ovs/framework/hosts/{0}/setupcompleted'.format(machine_id), True)
        Configuration.set('/ovs/framework/hosts/{0}/promotecompleted'.format(machine_id), True)
        Configuration.set('/ovs/framework/hosts/{0}/type'.format(machine_id), 'MASTER')
        Configuration.set('/ovs/framework/install_time', time.time())
        target_client.run(['chown', '-R', 'ovs:ovs', '/opt/OpenvStorage/config'])
        SetupController._log(messages='First node complete')

    @staticmethod
    def _rollback_setup(target_client, first_node):
        """
        Rollback a failed setup
        """
        import etcd
        from ovs.dal.lists.servicetypelist import ServiceTypeList
        SetupController._log(messages='Rolling back setup of current node', title=True)

        cluster_ip = target_client.ip
        machine_id = System.get_my_machine_id(target_client)
        config_store = Configuration.get_store()
        cfg_mgmt_running = True
        if config_store == 'etcd':
            from ovs.extensions.db.etcd.installer import EtcdInstaller
            try:
                EtcdInstaller.wait_for_cluster(cluster_name='a_name_that_does_not_matter_at_all', client=target_client)
            except etcd.EtcdConnectionFailed:
                cfg_mgmt_running = False
        unconfigure_rabbitmq = False
        unconfigure_memcached = False

        required_info = {'/ovs/framework/memcache': None,
                         '/ovs/framework/paths|ovsdb': '',
                         '/ovs/framework/external_config': None,
                         '/ovs/framework/memcache|endpoints': [],
                         '/ovs/framework/arakoon_clusters|ovsdb': None,
                         '/ovs/framework/messagequeue|endpoints': []}

        SetupController._log(messages='Config management is{0} running'.format('' if cfg_mgmt_running is True else ' NOT'))
        if cfg_mgmt_running is True:
            for key in required_info:
                try:
                    required_info[key] = Configuration.get(key=key)
                except (etcd.EtcdKeyNotFound, KeyError):
                    pass
            unconfigure_rabbitmq = SetupController._is_internally_managed(service='rabbitmq')
            unconfigure_memcached = SetupController._is_internally_managed(service='memcached')

        target_client.dir_delete('/opt/OpenvStorage/webapps/frontend/logging')

        SetupController._log(messages='Stopping services', loglevel='debug')
        for service in ['watcher-framework', 'watcher-config', 'workers', 'support-agent']:
            if ServiceManager.has_service(service, client=target_client):
                Toolbox.change_service_state(target_client, service, 'stop', SetupController._logger)

        if cfg_mgmt_running is True:
            endpoints = required_info['/ovs/framework/messagequeue|endpoints']
            if len(endpoints) > 0 and unconfigure_rabbitmq is True:
                SetupController._log(messages='Unconfiguring RabbitMQ')
                try:
                    SetupController._unconfigure_rabbitmq(target_client)
                except Exception as ex:
                    SetupController._log(messages=['Failed to unconfigure RabbitMQ', ex], loglevel='exception')

                for endpoint in endpoints:
                    if endpoint.startswith(target_client.ip):
                        endpoints.remove(endpoint)
                        break
                if len(endpoints) == 0:
                    Configuration.delete('/ovs/framework/messagequeue')
                else:
                    Configuration.set('/ovs/framework/messagequeue|endpoints', endpoints)

            SetupController._log(messages='Unconfiguring Memcached')
            endpoints = required_info['/ovs/framework/memcache|endpoints']
            if len(endpoints) > 0 and unconfigure_memcached is True:
                ServiceManager.stop_service('memcached', target_client)
                for endpoint in endpoints:
                    if endpoint.startswith(target_client.ip):
                        endpoints.remove(endpoint)
                        break
                if len(endpoints) == 0:
                    Configuration.delete('/ovs/framework/memcache')
                else:
                    Configuration.set('/ovs/framework/memcache|endpoints', endpoints)

        SetupController._remove_services(target_client, 'master')
        service = 'watcher-config'
        if ServiceManager.has_service(service, client=target_client):
            SetupController._log(messages='Removing service {0}'.format(service), loglevel='debug')
            ServiceManager.stop_service(service, client=target_client)
            ServiceManager.remove_service(service, client=target_client)

        if cfg_mgmt_running is True:
            external_config = required_info['/ovs/framework/external_config']
            SetupController._log(messages='Cleaning up model')
            #  Model is completely cleaned up when the arakoon cluster is destroyed
            memcache_configured = required_info['/ovs/framework/memcache']
            storagerouter = None
            if memcache_configured is not None:
                try:
                    storagerouter = System.get_my_storagerouter()
                except Exception as ex:
                    SetupController._log(messages='Retrieving storagerouter information failed with error: {0}'.format(ex), loglevel='error')

                if storagerouter is not None:  # StorageRouter will be None if storagerouter not yet modeled
                    try:
                        for service in storagerouter.services:
                            service.delete()
                        for disk in storagerouter.disks:
                            for partition in disk.partitions:
                                partition.delete()
                            disk.delete()
                        if storagerouter.alba_node is not None:
                            storagerouter.alba_node.delete()
                        storagerouter.delete()
                    except Exception as ex:
                        SetupController._log(messages='Cleaning up model failed with error: {0}'.format(ex), loglevel='error')
                if first_node is True:
                    try:
                        for service in ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON).services:  # Externally managed Arakoon services not linked to the storagerouter
                            service.delete()
                    except Exception as ex:
                        SetupController._log(messages='Cleaning up services failed with error: {0}'.format(ex), loglevel='error')
            if first_node is True:
                for key in Configuration.base_config.keys() + ['install_time', 'plugins']:
                    try:
                        Configuration.delete(key='/ovs/framework/{0}'.format(key))
                    except (etcd.EtcdKeyNotFound, etcd.EtcdConnectionFailed, KeyError):
                        pass

            try:
                Configuration.delete(key='/ovs/framework/hosts/{0}'.format(machine_id))
            except (etcd.EtcdKeyNotFound, etcd.EtcdConnectionFailed, KeyError):
                pass

            #  Memcached, Arakoon and ETCD must be the last services to be removed
            services = ['memcached']
            cluster_name = required_info['/ovs/framework/arakoon_clusters|ovsdb']
            try:
                metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)
            except ValueError:
                metadata = None
            if metadata is not None and metadata['internal'] is True:
                services.append('arakoon-{0}'.format(cluster_name))
            for service in services:
                if ServiceManager.has_service(service, client=target_client):
                    Toolbox.change_service_state(target_client, service, 'stop', SetupController._logger)

            if first_node is True:
                SetupController._log(messages='Unconfigure Arakoon')
                if metadata is not None and metadata['internal'] is True:
                    try:
                        ArakoonInstaller.delete_cluster(cluster_name, cluster_ip)
                    except Exception as ex:
                        SetupController._log(messages=['\nFailed to delete cluster', ex], loglevel='exception')
                    base_dir = required_info['/ovs/framework/paths|ovsdb']
                    #  ArakoonInstall.delete_cluster calls destroy_node which removes these directories already
                    directory_info = [ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, cluster_name),
                                      ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, cluster_name)]
                    try:
                        ArakoonInstaller.clean_leftover_arakoon_data(ip=cluster_ip,
                                                                     directories=directory_info)
                    except Exception as ex:
                        SetupController._log(messages=['Failed to clean Arakoon data', ex])

            SetupController._log(messages='Unconfigure Etcd')
            if external_config is None:
                if config_store == 'etcd':
                    from ovs.extensions.db.etcd.installer import EtcdInstaller
                    SetupController._log(messages='Removing Etcd cluster')
                    try:
                        EtcdInstaller.stop('config', target_client)
                        EtcdInstaller.remove('config', target_client)
                    except Exception as ex:
                        SetupController._log(messages=['\nFailed to unconfigure Etcd', ex], loglevel='exception')
        if config_store == 'etcd':
            from ovs.extensions.db.etcd.installer import EtcdInstaller
            SetupController._log(messages='Removing Etcd proxy')
            EtcdInstaller.remove_proxy('config', cluster_ip)

    @staticmethod
    def _setup_extra_node(cluster_ip, master_ip, unique_id, ip_client_map):
        """
        Sets up an additional node
        """
        SetupController._log(messages='Adding extra node', title=True)
        target_client = ip_client_map[cluster_ip]
        master_client = ip_client_map[master_ip]
        machine_id = System.get_my_machine_id(target_client)

        target_client.file_write(Configuration.BOOTSTRAP_CONFIG_LOCATION,
                                 master_client.file_read(Configuration.BOOTSTRAP_CONFIG_LOCATION))
        config_store = Configuration.get_store()
        if config_store == 'etcd':
            from ovs.extensions.db.etcd.installer import EtcdInstaller
            SetupController._log(messages='Extending Etcd cluster')
            EtcdInstaller.deploy_to_slave(master_ip, cluster_ip, 'config')
        else:
            from ovs.extensions.db.arakoon.configuration import ArakoonConfiguration
            target_client.file_write(ArakoonConfiguration.CACC_LOCATION,
                                     master_client.file_read(ArakoonConfiguration.CACC_LOCATION))

        Configuration.initialize_host(machine_id)

        service = 'watcher-config'
        if not ServiceManager.has_service(service, target_client):
            SetupController._log(messages='Adding service {0}'.format(service), loglevel='debug')
            ServiceManager.add_service(service, params={}, client=target_client)
        SetupController._add_services(target_client, unique_id, 'extra')

        SetupController._configure_redis(target_client)
        Toolbox.change_service_state(target_client, 'redis-server', 'restart', SetupController._logger)

        enabled = Configuration.get('/ovs/framework/support|enabled')
        if enabled is True:
            service = 'support-agent'
            if not ServiceManager.has_service(service, target_client):
                ServiceManager.add_service(service, client=target_client)
                Toolbox.change_service_state(target_client, service, 'start', SetupController._logger)

        node_name, _ = target_client.get_hostname()
        SetupController._finalize_setup(target_client, node_name, 'EXTRA', unique_id)

        Configuration.set('/ovs/framework/hosts/{0}/ip'.format(machine_id), cluster_ip)

        SetupController._log(messages='Starting services')
        for service in ['watcher-framework', 'watcher-config']:
            if ServiceManager.get_service_status(service, target_client)[0] is False:
                Toolbox.change_service_state(target_client, service, 'start', SetupController._logger)

        SetupController._log(messages='Check ovs-workers', loglevel='debug')
        # Workers are started by ovs-watcher-framework, but for a short time they are in pre-start
        Toolbox.wait_for_service(target_client, 'workers', True, SetupController._logger)

        SetupController._log(messages='Restarting workers', loglevel='debug')
        for node_client in ip_client_map.itervalues():
            Toolbox.change_service_state(node_client, 'workers', 'restart', SetupController._logger)

        SetupController._run_hooks('extranode', cluster_ip, master_ip)

        if SetupController._avahi_installed(target_client) is True:
            SetupController._configure_avahi(target_client, node_name, 'extra')
        Configuration.set('/ovs/framework/hosts/{0}/setupcompleted'.format(machine_id), True)
        Configuration.set('/ovs/framework/hosts/{0}/type'.format(machine_id), 'EXTRA')
        target_client.run(['chown', '-R', 'ovs:ovs', '/opt/OpenvStorage/config'])
        SetupController._log(messages='Extra node complete')

    @staticmethod
    def _promote_node(cluster_ip, master_ip, ip_client_map, unique_id, configure_memcached, configure_rabbitmq):
        """
        Promotes a given node
        """
        from ovs.dal.lists.storagerouterlist import StorageRouterList
        from ovs.dal.lists.servicetypelist import ServiceTypeList
        from ovs.dal.lists.servicelist import ServiceList
        from ovs.dal.hybrids.service import Service

        SetupController._log(messages='Promoting node', title=True)
        if configure_memcached is True:
            if SetupController._validate_local_memcache_servers(ip_client_map) is False:
                raise RuntimeError('Not all memcache nodes can be reached which is required for promoting a node.')

        target_client = ip_client_map[cluster_ip]
        machine_id = System.get_my_machine_id(target_client)
        node_name, _ = target_client.get_hostname()
        master_client = ip_client_map[master_ip]

        storagerouter = StorageRouterList.get_by_machine_id(unique_id)
        storagerouter.node_type = 'MASTER'
        storagerouter.save()

        external_config = Configuration.get('/ovs/framework/external_config')
        if external_config is None:
            config_store = Configuration.get_store()
            if config_store == 'arakoon':
                SetupController._log(messages='Joining Arakoon configuration cluster')
                metadata = ArakoonInstaller.extend_cluster(master_ip=master_ip,
                                                           new_ip=cluster_ip,
                                                           cluster_name='config',
                                                           base_dir=Configuration.get('/ovs/framework/paths|ovsdb'),
                                                           ports=[26400, 26401],
                                                           filesystem=True)
                ArakoonInstaller.restart_cluster_add('config', metadata['ips'], cluster_ip, filesystem=True)
            else:
                from ovs.extensions.db.etcd.installer import EtcdInstaller
                SetupController._log(messages='Joining Etcd cluster')
                EtcdInstaller.extend_cluster(master_ip, cluster_ip, 'config')

        # Find other (arakoon) master nodes
        arakoon_cluster_name = str(Configuration.get('/ovs/framework/arakoon_clusters|ovsdb'))
        arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=arakoon_cluster_name)
        config = ArakoonClusterConfig(cluster_id=arakoon_cluster_name, filesystem=False)
        config.load_config()
        master_node_ips = [node.ip for node in config.nodes]
        if cluster_ip in master_node_ips:
            master_node_ips.remove(cluster_ip)
        if len(master_node_ips) == 0:
            raise RuntimeError('There should be at least one other master node')

        arakoon_ports = []
        if arakoon_metadata['internal'] is True:
            SetupController._log(messages='Joining Arakoon OVS DB cluster')
            result = ArakoonInstaller.extend_cluster(master_ip=master_ip,
                                                     new_ip=cluster_ip,
                                                     cluster_name=arakoon_cluster_name,
                                                     base_dir=Configuration.get('/ovs/framework/paths|ovsdb'))
            ArakoonInstaller.restart_cluster_add(arakoon_cluster_name, result['ips'], cluster_ip, filesystem=False)
            arakoon_ports = [result['client_port'], result['messaging_port']]

        if configure_memcached is True:
            SetupController._configure_memcached(target_client)
        SetupController._add_services(target_client, unique_id, 'master')

        SetupController._log(messages='Update configurations')
        if configure_memcached is True:
            endpoints = Configuration.get('/ovs/framework/memcache|endpoints')
            endpoint = '{0}:11211'.format(cluster_ip)
            if endpoint not in endpoints:
                endpoints.append(endpoint)
                Configuration.set('/ovs/framework/memcache|endpoints', endpoints)
        if configure_rabbitmq is True:
            endpoints = Configuration.get('/ovs/framework/messagequeue|endpoints')
            endpoint = '{0}:5672'.format(cluster_ip)
            if endpoint not in endpoints:
                endpoints.append(endpoint)
                Configuration.set('/ovs/framework/messagequeue|endpoints', endpoints)

        if arakoon_metadata['internal'] is True:
            SetupController._log(messages='Restarting master node services')
            ArakoonInstaller.restart_cluster_add(cluster_name=arakoon_cluster_name,
                                                 current_ips=master_node_ips,
                                                 new_ip=cluster_ip,
                                                 filesystem=False)
            PersistentFactory.store = None
            VolatileFactory.store = None

            if 'arakoon-ovsdb' not in [s.name for s in ServiceList.get_services() if s.is_internal is False or s.storagerouter.ip == cluster_ip]:
                service = Service()
                service.name = 'arakoon-ovsdb'
                service.type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON)
                service.ports = arakoon_ports
                service.storagerouter = storagerouter
                service.save()

        if configure_rabbitmq is True:
            SetupController._configure_rabbitmq(target_client)
            # Copy rabbitmq cookie
            rabbitmq_cookie_file = '/var/lib/rabbitmq/.erlang.cookie'

            SetupController._log(messages='Copying Rabbit MQ cookie', loglevel='debug')
            contents = master_client.file_read(rabbitmq_cookie_file)
            master_hostname, _ = master_client.get_hostname()
            target_client.dir_create(os.path.dirname(rabbitmq_cookie_file))
            target_client.file_write(rabbitmq_cookie_file, contents)
            target_client.file_chmod(rabbitmq_cookie_file, mode=400)
            target_client.run(['rabbitmq-server', '-detached'])
            time.sleep(5)
            target_client.run(['rabbitmqctl', 'stop_app'])
            time.sleep(5)
            target_client.run(['rabbitmqctl', 'join_cluster', 'rabbit@{0}'.format(master_hostname)])
            time.sleep(5)
            target_client.run(['rabbitmqctl', 'stop'])
            time.sleep(5)

            # Enable HA for the rabbitMQ queues
            Toolbox.change_service_state(target_client, 'rabbitmq-server', 'start', SetupController._logger)
            SetupController._check_rabbitmq_and_enable_ha_mode(target_client)

        SetupController._configure_amqp_to_volumedriver()

        SetupController._log(messages='Starting services')
        services = ['memcached', 'arakoon-ovsdb', 'rabbitmq-server', 'etcd-config']
        if arakoon_metadata['internal'] is True:
            services.remove('arakoon-ovsdb')
        for service in services:
            if ServiceManager.has_service(service, client=target_client):
                Toolbox.change_service_state(target_client, service, 'start', SetupController._logger)

        SetupController._log(messages='Restarting services')
        master_ips = [sr.ip for sr in StorageRouterList.get_masters()]
        slave_ips = [sr.ip for sr in StorageRouterList.get_slaves()]
        SetupController._restart_framework_and_memcache_services(master_ips, slave_ips, ip_client_map)

        if SetupController._run_hooks('promote', cluster_ip, master_ip):
            SetupController._log(messages='Restarting services')
            SetupController._restart_framework_and_memcache_services(master_ips, slave_ips, ip_client_map)

        if SetupController._avahi_installed(target_client) is True:
            SetupController._configure_avahi(target_client, node_name, 'master')
        Configuration.set('/ovs/framework/hosts/{0}/type'.format(machine_id), 'MASTER')
        target_client.run(['chown', '-R', 'ovs:ovs', '/opt/OpenvStorage/config'])
        Configuration.set('/ovs/framework/hosts/{0}/promotecompleted'.format(machine_id), True)
        SetupController._log(messages='Promote complete')

    @staticmethod
    def _demote_node(cluster_ip, master_ip, ip_client_map, unique_id, unconfigure_memcached, unconfigure_rabbitmq, offline_nodes=None):
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
        arakoon_cluster_name = str(Configuration.get('/ovs/framework/arakoon_clusters|ovsdb'))
        arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=arakoon_cluster_name)
        config = ArakoonClusterConfig(cluster_id=arakoon_cluster_name, filesystem=False)
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
        if arakoon_metadata['internal'] is True:
            SetupController._log(messages='Leaving Arakoon {0} cluster'.format(arakoon_cluster_name))
            remaining_ips = ArakoonInstaller.shrink_cluster(deleted_node_ip=cluster_ip,
                                                            remaining_node_ip=master_nodes[0],
                                                            cluster_name=arakoon_cluster_name,
                                                            offline_nodes=offline_node_ips)
            ArakoonInstaller.restart_cluster_remove(arakoon_cluster_name, remaining_ips, filesystem=False)

        try:
            external_config = Configuration.get('/ovs/framework/external_config')
            if external_config is None:
                config_store = Configuration.get_store()
                if config_store == 'arakoon':
                    SetupController._log(messages='Leaving Arakoon config cluster')
                    remaining_ips = ArakoonInstaller.shrink_cluster(deleted_node_ip=cluster_ip,
                                                                    remaining_node_ip=master_nodes[0],
                                                                    cluster_name='config',
                                                                    offline_nodes=offline_node_ips,
                                                                    filesystem=True)
                    ArakoonInstaller.restart_cluster_remove('config', remaining_ips, filesystem=True)

                else:
                    from ovs.extensions.db.etcd.installer import EtcdInstaller
                    SetupController._log(messages='Leaving Etcd cluster')
                    EtcdInstaller.shrink_cluster(master_ip, cluster_ip, 'config', offline_node_ips)
        except Exception as ex:
            SetupController._log(messages=['\nFailed to leave configuration cluster', ex], loglevel='exception')

        SetupController._log(messages='Update configurations')
        try:
            if unconfigure_memcached is True:
                endpoints = Configuration.get('/ovs/framework/memcache|endpoints')
                endpoint = '{0}:{1}'.format(cluster_ip, 11211)
                if endpoint in endpoints:
                    endpoints.remove(endpoint)
                Configuration.set('/ovs/framework/memcache|endpoints', endpoints)
            if unconfigure_rabbitmq is True:
                endpoints = Configuration.get('/ovs/framework/messagequeue|endpoints')
                endpoint = '{0}:{1}'.format(cluster_ip, 5672)
                if endpoint in endpoints:
                    endpoints.remove(endpoint)
                Configuration.set('/ovs/framework/messagequeue|endpoints', endpoints)
        except Exception as ex:
            SetupController._log(messages=['\nFailed to update configurations', ex], loglevel='exception')

        if arakoon_metadata['internal'] is True:
            SetupController._log(messages='Restarting master node services')
            remaining_nodes = ip_client_map.keys()[:]
            if cluster_ip in remaining_nodes:
                remaining_nodes.remove(cluster_ip)

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
                    client.run(['rabbitmqctl', 'forget_cluster_node', 'rabbit@{0}'.format(storagerouter.name)])
                except Exception as ex:
                    SetupController._log(messages=['\nFailed to forget RabbitMQ cluster node', ex], loglevel='exception')
        else:
            target_client = ip_client_map[cluster_ip]
            if unconfigure_rabbitmq is True:
                SetupController._log(messages='Removing/unconfiguring RabbitMQ', loglevel='debug')
                try:
                    if ServiceManager.has_service('rabbitmq-server', client=target_client):
                        target_client.run(['rabbitmq-server', '-detached'])
                        time.sleep(5)
                        target_client.run(['rabbitmqctl', 'stop_app'])
                        time.sleep(5)
                        target_client.run(['rabbitmqctl', 'reset'])
                        time.sleep(5)
                        target_client.run(['rabbitmqctl', 'stop'])
                        time.sleep(5)
                        Toolbox.change_service_state(target_client, 'rabbitmq-server', 'stop', SetupController._logger)
                        target_client.file_unlink("/var/lib/rabbitmq/.erlang.cookie")
                except Exception as ex:
                    SetupController._log(messages=['\nFailed to remove/unconfigure RabbitMQ', ex], loglevel='exception')

            SetupController._log(messages='Removing services')
            services = ['memcached', 'rabbitmq-server', 'scheduled-tasks', 'webapp-api', 'volumerouter-consumer']
            if unconfigure_rabbitmq is False:
                services.remove('rabbitmq-server')
            if unconfigure_memcached is False:
                services.remove('memcached')
            for service in services:
                if ServiceManager.has_service(service, client=target_client):
                    SetupController._log(messages='Removing service {0}'.format(service), loglevel='debug')
                    try:
                        Toolbox.change_service_state(target_client, service, 'stop', SetupController._logger)
                        ServiceManager.remove_service(service, client=target_client)
                    except Exception as ex:
                        SetupController._log(messages=['\nFailed to remove service'.format(service), ex], loglevel='exception')

            if ServiceManager.has_service('workers', client=target_client):
                ServiceManager.add_service(name='workers',
                                           client=target_client,
                                           params={'MEMCACHE_NODE_IP': cluster_ip,
                                                   'WORKER_QUEUE': '{0}'.format(unique_id)})
        try:
            SetupController._configure_amqp_to_volumedriver()
        except Exception as ex:
            SetupController._log(messages=['\nFailed to configure AMQP to Storage Driver', ex], loglevel='exception')

        SetupController._log(messages='Restarting services', loglevel='debug')
        master_ips = [sr.ip for sr in StorageRouterList.get_masters()]
        slave_ips = [sr.ip for sr in StorageRouterList.get_slaves()]
        SetupController._restart_framework_and_memcache_services(master_ips, slave_ips, ip_client_map, offline_node_ips)

        if SetupController._run_hooks('demote', cluster_ip, master_ip, offline_node_ips=offline_node_ips):
            SetupController._log(messages='Restarting services', loglevel='debug')
            SetupController._restart_framework_and_memcache_services(master_ips, slave_ips, ip_client_map, offline_node_ips)

        if storagerouter not in offline_nodes:
            target_client = ip_client_map[cluster_ip]
            node_name, _ = target_client.get_hostname()
            if SetupController._avahi_installed(target_client) is True:
                SetupController._configure_avahi(target_client, node_name, 'extra')
        Configuration.set('/ovs/framework/hosts/{0}/type'.format(storagerouter.machine_id), 'EXTRA')
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
                    Toolbox.change_service_state(clients[ip], watcher, 'stop', SetupController._logger)
        for ip in masters:
            if ip not in offline_node_ips:
                Toolbox.change_service_state(clients[ip], memcached, 'restart', SetupController._logger)
        for ip in masters + slaves:
            if ip not in offline_node_ips:
                if ServiceManager.has_service(watcher, clients[ip]):
                    Toolbox.change_service_state(clients[ip], watcher, 'start', SetupController._logger)
        VolatileFactory.store = None

    @staticmethod
    def _configure_memcached(client):
        SetupController._log(messages='Setting up Memcached')
        client.run(['sed', '-i', 's/^-l.*/-l 0.0.0.0/g', '/etc/memcached.conf'])
        client.run(['sed', '-i', 's/^-m.*/-m 1024/g', '/etc/memcached.conf'])
        client.run(['sed', '-i', '-E', 's/^-v(.*)/# -v\1/g', '/etc/memcached.conf'])  # Put all -v, -vv, ... back in comment
        client.run(['sed', '-i', 's/^# -v[^v]*$/-v/g', '/etc/memcached.conf'])     # Uncomment only -v

    @staticmethod
    def _configure_redis(client):
        SetupController._log(messages='Setting up Redis')
        client.run(['sed', '-i', 's/^# maxmemory <bytes>.*/maxmemory 128mb/g', '/etc/redis/redis.conf'])
        client.run(['sed', '-i', 's/^# maxmemory-policy .*/maxmemory-policy allkeys-lru/g', '/etc/redis/redis.conf'])

    @staticmethod
    def _configure_rabbitmq(client):
        SetupController._log(messages='Setting up RabbitMQ', loglevel='debug')
        rabbitmq_port = Configuration.get('/ovs/framework/messagequeue|endpoints')[0].split(':')[1]
        rabbitmq_login = Configuration.get('/ovs/framework/messagequeue|user')
        rabbitmq_password = Configuration.get('/ovs/framework/messagequeue|password')
        client.file_write('/etc/rabbitmq/rabbitmq.config', """[
   {{rabbit, [{{tcp_listeners, [{0}]}},
              {{default_user, <<"{1}">>}},
              {{default_pass, <<"{2}">>}},
              {{log_levels, [{{connection, warning}}]}},
              {{vm_memory_high_watermark, 0.2}}]}}
].""".format(rabbitmq_port, rabbitmq_login, rabbitmq_password))

        rabbitmq_running, same_process = SetupController._is_rabbitmq_running(client)
        if rabbitmq_running is True:
            # Example output of 'list_users' command
            # Listing users ...
            # guest   [administrator]
            # ovs     []
            # ... done.
            users = [user.split('\t')[0] for user in client.run(['rabbitmqctl', 'list_users']).splitlines() if '\t' in user and '[' in user and ']' in user]
            if 'ovs' in users:
                SetupController._log(messages='Already configured RabbitMQ')
                return
            Toolbox.change_service_state(client, 'rabbitmq-server', 'stop', SetupController._logger)

        client.run(['rabbitmq-server', '-detached'])
        time.sleep(5)

        # Sometimes/At random the rabbitmq server takes longer than 5 seconds to start,
        #  and the next command fails so the best solution is to retry several times
        # Also retry the add_user/set_permissions, and validate the result
        retry = 0
        while retry < 10:
            users = Toolbox.retry_client_run(client=client,
                                             command=['rabbitmqctl', 'list_users'],
                                             logger=SetupController._logger).splitlines()
            users = [usr.split('\t')[0] for usr in users if '\t' in usr and '[' in usr and ']' in usr]
            SetupController._logger.debug('Rabbitmq users {0}'.format(users))
            if 'ovs' in users:
                SetupController._logger.debug('User ovs configured in rabbitmq')
                break

            SetupController._logger.debug(Toolbox.retry_client_run(client=client,
                                                                   command=['rabbitmqctl', 'add_user', rabbitmq_login, rabbitmq_password],
                                                                   logger=SetupController._logger))
            SetupController._logger.debug(Toolbox.retry_client_run(client=client,
                                                                   command=['rabbitmqctl', 'set_permissions', rabbitmq_login, '.*', '.*', '.*'],
                                                                   logger=SetupController._logger))
            retry += 1
            time.sleep(1)
        client.run(['rabbitmqctl', 'stop'])
        time.sleep(5)

    @staticmethod
    def _unconfigure_rabbitmq(client):
        rabbitmq_running, same_process = SetupController._is_rabbitmq_running(client)
        if rabbitmq_running is True:
            Toolbox.change_service_state(client, 'rabbitmq-server', 'stop', SetupController._logger)
        client.file_delete('/etc/rabbitmq/rabbitmq.config')

    @staticmethod
    def _check_rabbitmq_and_enable_ha_mode(client):
        if not ServiceManager.has_service('rabbitmq-server', client):
            raise RuntimeError('Service rabbitmq-server has not been added on node {0}'.format(client.ip))
        rabbitmq_running, same_process = SetupController._is_rabbitmq_running(client)
        if rabbitmq_running is False or same_process is False:
            Toolbox.change_service_state(client, 'rabbitmq-server', 'restart', SetupController._logger)

        time.sleep(5)
        client.run(['rabbitmqctl', 'set_policy', 'ha-all', '^(volumerouter|ovs_.*)$', '{"ha-mode":"all"}'])

    @staticmethod
    def _configure_amqp_to_volumedriver():
        SetupController._log(messages='Update existing vPools')
        login = Configuration.get('/ovs/framework/messagequeue|user')
        password = Configuration.get('/ovs/framework/messagequeue|password')
        protocol = Configuration.get('/ovs/framework/messagequeue|protocol')

        uris = []
        for endpoint in Configuration.get('/ovs/framework/messagequeue|endpoints'):
            uris.append({'amqp_uri': '{0}://{1}:{2}@{3}'.format(protocol, login, password, endpoint)})

        if Configuration.dir_exists('/ovs/vpools'):
            for vpool_guid in Configuration.list('/ovs/vpools'):
                for storagedriver_id in Configuration.list('/ovs/vpools/{0}/hosts'.format(vpool_guid)):
                    storagedriver_config = StorageDriverConfiguration('storagedriver', vpool_guid, storagedriver_id)
                    storagedriver_config.load()
                    storagedriver_config.configure_event_publisher(events_amqp_routing_key=Configuration.get('/ovs/framework/messagequeue|queues.storagedriver'),
                                                                   events_amqp_uris=uris)
                    storagedriver_config.save()

    @staticmethod
    def _avahi_installed(client):
        installed = client.run(['which', 'avahi-daemon'], allow_nonzero=True)
        if installed == '':
            SetupController._log(messages='Avahi not installed', loglevel='debug')
            return False
        else:
            SetupController._log(messages='Avahi installed', loglevel='debug')
            return True

    @staticmethod
    def _configure_avahi(client, node_name, node_type):
        cluster_name = Configuration.get('/ovs/framework/cluster_name')
        SetupController._log(messages='Announcing service', title=True)
        client.file_write(SetupController.avahi_filename, """<?xml version="1.0" standalone='no'?>
<!--*-nxml-*-->
<!DOCTYPE service-group SYSTEM "avahi-service.dtd">
<!-- $Id$ -->
<service-group>
    <name replace-wildcards="yes">ovs_cluster_{0}_{1}_{3}</name>
    <service>
        <type>_ovs_{2}_node._tcp</type>
        <port>443</port>
    </service>
</service-group>""".format(cluster_name, node_name, node_type, client.ip.replace('.', '_')))
        Toolbox.change_service_state(client, 'avahi-daemon', 'restart', SetupController._logger)

    @staticmethod
    def _add_services(client, unique_id, node_type):
        SetupController._log(messages='Adding services')
        services = ['workers', 'watcher-framework']
        worker_queue = unique_id
        if node_type == 'master':
            services += ['memcached', 'rabbitmq-server', 'scheduled-tasks', 'webapp-api', 'volumerouter-consumer']
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
        services = ['workers', 'support-agent', 'watcher-framework']
        if node_type == 'master':
            services += ['scheduled-tasks', 'webapp-api', 'volumerouter-consumer']
            if SetupController._is_internally_managed(service='rabbitmq') is True:
                services.append('rabbitmq-server')
            if SetupController._is_internally_managed(service='memcached') is True:
                services.append('memcached')

        for service in services:
            if ServiceManager.has_service(service, client=client):
                SetupController._log(messages='Removing service {0}'.format(service), loglevel='debug')
                ServiceManager.stop_service(service, client=client)
                ServiceManager.remove_service(service, client=client)

    @staticmethod
    def _finalize_setup(client, node_name, node_type, unique_id):
        cluster_ip = client.ip
        client.dir_create('/opt/OpenvStorage/webapps/frontend/logging')
        SetupController._replace_param_in_config(client=client,
                                                 config_file='/opt/OpenvStorage/webapps/frontend/logging/config.js',
                                                 old_value='http://"+window.location.hostname+":9200',
                                                 new_value='http://' + cluster_ip + ':9200')

        # Imports, not earlier than here, as all required config files should be in place.
        from ovs.lib.disk import DiskController
        from ovs.lib.storagerouter import StorageRouterController
        from ovs.dal.hybrids.storagerouter import StorageRouter
        from ovs.dal.lists.storagerouterlist import StorageRouterList

        SetupController._log(messages='Configuring/updating model')
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
            storagerouter.rdma_capable = False
        storagerouter.node_type = node_type
        storagerouter.save()

        StorageRouterController.set_rdma_capability(storagerouter.guid)
        try:
            DiskController.sync_with_reality(storagerouter.guid)
        except Exception as ex:
            SetupController._logger.exception('Error syncing disks: {0}'.format(ex))

        return storagerouter

    @staticmethod
    def _discover_nodes(client):
        nodes = {}
        Toolbox.change_service_state(client, 'dbus', 'start', SetupController._logger)
        Toolbox.change_service_state(client, 'avahi-daemon', 'start', SetupController._logger)
        discover_result = client.run('timeout -k 60 45 avahi-browse -artp 2> /dev/null | grep ovs_cluster || true', allow_insecure=True)
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
        output = client.run(['rabbitmqctl', 'status'], allow_nonzero=True)
        if output:
            match = re.search('\{pid,(?P<pid>\d+?)\}', output)
            if match is not None:
                match_groups = match.groupdict()
                if 'pid' in match_groups:
                    rabbitmq_running = True
                    rabbitmq_pid_ctl = match_groups['pid']

        if ServiceManager.has_service('rabbitmq-server', client) \
                and ServiceManager.get_service_status('rabbitmq-server', client)[0] is True:
            rabbitmq_running = True
            rabbitmq_pid_sm = ServiceManager.get_service_pid('rabbitmq-server', client)

        same_process = rabbitmq_pid_ctl == rabbitmq_pid_sm
        SetupController._logger.debug('Rabbitmq is reported {0}running, pids: {1} and {2}'.format('' if rabbitmq_running else 'not ',
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
            SetupController._log(messages='Executing {0}.{1}'.format(function.__module__, function.__name__))
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
            except UnableToConnectException:
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
        ips = [endpoint.split(':')[0] for endpoint in Configuration.get('/ovs/framework/memcache|endpoints')]
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
        preconfig = '/opt/OpenvStorage/config/preconfig.json'
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
        expected_keys = ['cluster_ip', 'config_mgmt_type', 'enable_heartbeats', 'external_config', 'master_ip', 'master_password', 'node_type', 'rdma']
        for key in actual_keys:
            if key not in expected_keys:
                errors.append('Key {0} is not supported by OpenvStorage to be used in the pre-configuration JSON'.format(key))
        if len(errors) > 0:
            raise ValueError('\nErrors found while verifying pre-configuration:\n - {0}\n\nAllowed keys:\n - {1}'.format('\n - '.join(errors), '\n - '.join(expected_keys)))

        Toolbox.verify_required_params(actual_params=config,
                                       required_params={'cluster_ip': (str, Toolbox.regex_ip, False),
                                                        'enable_heartbeats': (bool, None, False),
                                                        'external_config': (str, None, False),
                                                        'master_ip': (str, Toolbox.regex_ip),
                                                        'master_password': (str, None),
                                                        'node_type': (str, ['master', 'extra'], False),
                                                        'rdma': (bool, None, False),
                                                        'logging_target': (dict, None, False)})
        # Parameters only required for 1st node
        if 'cluster_ip' not in config or config['master_ip'] == config['cluster_ip']:
            Toolbox.verify_required_params(actual_params=config,
                                           required_params={'config_mgmt_type': (str, ['arakoon', 'etcd'])})
        return config

    @staticmethod
    def _retrieve_storagerouters(ip, password):
        """
        Retrieve the storagerouters from model
        """
        storagerouters = {}
        try:
            from ovs.dal.lists.storagerouterlist import StorageRouterList
            with remote(ip_info=ip, modules=[StorageRouterList], username='root', password=password, strict_host_key_checking=False) as rem:
                for sr in rem.StorageRouterList.get_storagerouters():
                    storagerouters[sr.name] = {'ip': sr.ip,
                                               'type': sr.node_type.lower()}
        except Exception as ex:
            SetupController._log('Error loading storagerouters: {0}'.format(ex), loglevel='exception', silent=True)
        return storagerouters

    @staticmethod
    def _is_internally_managed(service):
        """
        Validate whether the service is internally or externally managed
        Etcd has been verified at this point and should be reachable
        :param service: Service to verify (either memcached or rabbitmq)
        :type service: str
        :return: True or False
        :rtype: bool
        """
        if service not in ['memcached', 'rabbitmq']:
            raise ValueError('Can only check memcached or rabbitmq')

        service_name_map = {'memcached': 'memcache',
                            'rabbitmq': 'messagequeue'}[service]
        config_key = '/ovs/framework/{0}'.format(service_name_map)
        if not Configuration.exists(key=config_key):
            return True

        if not Configuration.exists(key='{0}|metadata'.format(config_key)):
            raise ValueError('Not all required keys ({0}) for {1} are present in the configuration management'.format(config_key, service))
        metadata = Configuration.get('{0}|metadata'.format(config_key))
        if 'internal' not in metadata:
            raise ValueError('Internal flag not present in metadata for {0}.\nPlease provide a key: {1} and value "metadata": {{"internal": True/False}}'.format(service, config_key))

        internal = metadata['internal']
        if internal is False:
            if not Configuration.exists(key='{0}|endpoints'.format(config_key)):
                raise ValueError('Externally managed {0} cluster must have "endpoints" information\nPlease provide a key: {1} and value "endpoints": [<ip:port>]'.format(service, config_key))
            endpoints = Configuration.get(key='{0}|endpoints'.format(config_key))
            if not isinstance(endpoints, list) or len(endpoints) == 0:
                raise ValueError('The endpoints for {0} cannot be empty and must be a list'.format(service))
        return internal

    @staticmethod
    def _log(messages, title=False, boxed=False, loglevel='info', silent=False):
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
        if silent is False:
            if boxed is True:
                print Interactive.boxed_message(lines=messages)
            else:
                for message in messages:
                    if title is True:
                        message = '\n+++ {0} +++\n'.format(message)
                    if loglevel in ['error', 'exception']:
                        message = 'ERROR: {0}'.format(message)
                    print message

        for message in messages:
            getattr(SetupController._logger, loglevel)(message)
