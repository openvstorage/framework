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
Module for NodeInstallationController
"""

import os
import sys
import json
import time
import signal
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig, ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration, NotFoundException, ConnectionException
from ovs.extensions.generic.logger import Logger
from ovs_extensions.generic.interactive import Interactive
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System
from ovs.extensions.os.osfactory import OSFactory
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.nodetype import NodeTypeController
from ovs.lib.noderemoval import NodeRemovalController


class NodeInstallationController(object):
    """
    This class contains all logic for setting up an environment, installed with system-native packages
    """
    _logger = Logger('lib')

    nodes = {}
    host_ips = set()

    @staticmethod
    def setup_node(node_type=None, execute_rollback=False):
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
        :param execute_rollback: In case of failure revert the changes made
        :type execute_rollback: bool
        :return: None
        """
        Toolbox.log(logger=NodeInstallationController._logger, messages='Open vStorage Setup', boxed=True)
        Toolbox.verify_required_params(actual_params={'node_type': node_type,
                                                      'execute_rollback': execute_rollback},
                                       required_params={'node_type': (str, ['master', 'extra'], False),
                                                        'execute_rollback': (bool, None)})

        rdma = None
        config = None
        master_ip = None
        cluster_ip = None
        logging_target = None
        external_config = None
        master_password = None
        enable_heartbeats = True

        try:
            preconfig = '/opt/OpenvStorage/config/preconfig.json'
            if os.path.exists(preconfig):
                config = {}
                with open(preconfig) as pre_config:
                    try:
                        config = json.loads(pre_config.read())
                    except Exception as ex:
                        raise ValueError('JSON contents could not be retrieved from file {0}.\nError message: {1}'.format(preconfig, ex))

                if 'setup' not in config or not isinstance(config['setup'], dict):
                    raise ValueError('The OpenvStorage pre-configuration file must contain a "setup" key with a dictionary as value')

                errors = []
                config = config['setup']
                expected_keys = ['cluster_ip', 'enable_heartbeats', 'external_config', 'logging_target', 'master_ip', 'master_password', 'node_type', 'rdma', 'rollback']
                for key in config:
                    if key not in expected_keys:
                        errors.append('Key {0} is not supported by OpenvStorage to be used in the pre-configuration JSON'.format(key))
                if len(errors) > 0:
                    raise ValueError('\nErrors found while verifying pre-configuration:\n - {0}\n\nAllowed keys:\n - {1}'.format('\n - '.join(errors), '\n - '.join(expected_keys)))

                Toolbox.verify_required_params(actual_params=config,
                                               required_params={'cluster_ip': (str, Toolbox.regex_ip, False),
                                                                'enable_heartbeats': (bool, None, False),
                                                                'external_config': (str, None, False),
                                                                'logging_target': (dict, None, False),
                                                                'master_ip': (str, Toolbox.regex_ip),
                                                                'master_password': (str, None),
                                                                'node_type': (str, ['master', 'extra'], False),
                                                                'rdma': (bool, None, False),
                                                                'rollback': (bool, None, False)})

                # Required fields
                master_ip = config['master_ip']
                master_password = config['master_password']

                # Optional fields
                rdma = config.get('rdma', False)
                node_type = config.get('node_type', node_type)
                cluster_ip = config.get('cluster_ip', master_ip)  # If cluster_ip not provided, we assume 1st node installation
                logging_target = config.get('logging_target')
                external_config = config.get('external_config')
                enable_heartbeats = config.get('enable_heartbeats', enable_heartbeats)
                if execute_rollback is False:  # Only overrule cmdline if setting was not passed
                    execute_rollback = config.get('rollback', False)

            # Support resume setup - store entered parameters so when retrying, we have the values
            resume_config = {}
            resume_config_file = '/opt/OpenvStorage/config/openvstorage_resumeconfig.json'
            if os.path.exists(resume_config_file):
                with open(resume_config_file) as resume_cfg:
                    resume_config = json.loads(resume_cfg.read())

            # Create connection to target node
            Toolbox.log(logger=NodeInstallationController._logger, messages='Setting up connections', title=True)

            root_client = SSHClient(endpoint='127.0.0.1', username='root')
            unique_id = System.get_my_machine_id(root_client)

            NodeInstallationController.host_ips = set(OSFactory.get_manager().get_ip_addresses(client=root_client))

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
                Toolbox.log(logger=NodeInstallationController._logger, messages='Collecting cluster information', title=True)

                if root_client.file_exists('/etc/openvstorage_id') is False:
                    raise RuntimeError("The 'openvstorage' package is not installed on this node")

                node_name, fqdn_name = root_client.get_hostname()
                NodeInstallationController._logger.debug('Current host: {0}'.format(node_name))
                node_type = resume_config.get('node_type', node_type)
                master_ip = resume_config.get('master_ip', master_ip)
                cluster_ip = resume_config.get('cluster_ip', cluster_ip)
                external_config = resume_config.get('external_config', external_config)
                execute_rollback = resume_config.get('execute_rollback', execute_rollback)
                enable_heartbeats = resume_config.get('enable_heartbeats', enable_heartbeats)

                if config is None:  # Non-automated install
                    NodeInstallationController._logger.debug('Cluster selection')
                    new_cluster = 'Create a new cluster'
                    discovery_result = {}
                    if NodeTypeController.avahi_installed(client=root_client, logger=NodeInstallationController._logger) is True:
                        ServiceFactory.change_service_state(root_client, 'dbus', 'start', NodeInstallationController._logger)
                        ServiceFactory.change_service_state(root_client, 'avahi-daemon', 'start', NodeInstallationController._logger)
                        for entry in root_client.run('timeout -k 60 45 avahi-browse -artp 2> /dev/null | egrep "ovs_cl_|ovs_cluster_" || true', allow_insecure=True).splitlines():
                            entry_parts = entry.split(';')
                            if entry_parts[0] == '=' and entry_parts[2] == 'IPv4' and entry_parts[7] not in NodeInstallationController.host_ips:
                                # =;eth0;IPv4;ovs_cl_kenneth_ovs100;_ovs_master_node._tcp;local;ovs100.local;172.22.1.10;443;
                                # split(';') -> [3]  = ovs_cl_kenneth_ovs100
                                #               [4]  = _ovs_master_node._tcp -> contains _ovs_<type>_node
                                #               [7]  = 172.22.1.10 (ip)
                                # split('_') -> [-1] = ovs100 (node name)
                                #               [-2] = kenneth (cluster name)
                                avahi_cluster_info = entry_parts[3].split('_')
                                avahi_cluster_name = avahi_cluster_info[2]
                                avahi_node_name = avahi_cluster_info[3]
                                if avahi_cluster_name not in discovery_result:
                                    discovery_result[avahi_cluster_name] = {}
                                if avahi_node_name not in discovery_result[avahi_cluster_name]:
                                    discovery_result[avahi_cluster_name][avahi_node_name] = {'ip': '', 'type': ''}
                                try:
                                    ip = '{0}.{1}.{2}.{3}'.format(avahi_cluster_info[4], avahi_cluster_info[5], avahi_cluster_info[6], avahi_cluster_info[7])
                                except IndexError:
                                    ip = entry_parts[7]
                                discovery_result[avahi_cluster_name][avahi_node_name]['ip'] = ip
                                discovery_result[avahi_cluster_name][avahi_node_name]['type'] = entry_parts[4].split('_')[2]

                    join_manually = 'Join {0} cluster'.format('a' if len(discovery_result) == 0 else 'a different')
                    cluster_options = [new_cluster] + sorted(discovery_result.keys()) + [join_manually]
                    cluster_name = Interactive.ask_choice(choice_options=cluster_options,
                                                          question='Select a cluster to join' if len(discovery_result) > 0 else 'No clusters found',
                                                          sort_choices=False)
                    if cluster_name == new_cluster:  # Create a new OVS cluster
                        first_node = True
                        while True:
                            master_ip = Interactive.ask_choice(NodeInstallationController.host_ips, 'Select the public IP address of {0}'.format(node_name))
                            cluster_name = Interactive.ask_string(message='Please enter the cluster name',
                                                                  regex_info={'regex': '^[0-9a-zA-Z]+(\-[0-9a-zA-Z]+)*$',
                                                                              'message': 'The new cluster name can only contain numbers, letters and dashes.'})
                            if cluster_name in discovery_result:
                                Toolbox.log(logger=NodeInstallationController._logger, messages='The new cluster name should be unique.')
                                continue
                            valid_avahi = NodeTypeController.validate_avahi_cluster_name(ip=master_ip, cluster_name=cluster_name, node_name=node_name)
                            if valid_avahi[0] is False:
                                Toolbox.log(logger=NodeInstallationController._logger, messages=valid_avahi[1])
                                continue
                            break

                        cluster_ip = master_ip
                        NodeInstallationController.nodes = {node_name: {'ip': master_ip,
                                                                        'type': 'master'}}

                    elif cluster_name == join_manually:  # Join an existing cluster manually
                        first_node = False
                        cluster_name = None
                        cluster_ip = Interactive.ask_choice(NodeInstallationController.host_ips, 'Select the public IP address of {0}'.format(node_name))
                        master_ip = Interactive.ask_string(message='Please enter the IP of one of the cluster\'s master nodes',
                                                           regex_info={'regex': SSHClient.IP_REGEX,
                                                                       'message': 'Incorrect IP provided'})
                        if master_ip in root_client.local_ips:
                            raise ValueError("A local IP address was given, please select '{0}' or provide another IP address".format(new_cluster))

                        NodeInstallationController._logger.debug('Trying to manually join cluster on {0}'.format(master_ip))

                        master_password = Toolbox.ask_validate_password(ip=master_ip, logger=NodeInstallationController._logger)
                        NodeInstallationController.nodes = NodeTypeController.retrieve_storagerouter_info_via_host(ip=master_ip, password=master_password)
                        master_ips = [sr_info['ip'] for sr_info in NodeInstallationController.nodes.itervalues() if sr_info['type'] == 'master']
                        if master_ip not in master_ips:
                            if master_ips:
                                raise ValueError('Incorrect master IP provided, please choose from: {0}'.format(', '.join(master_ips)))
                            else:
                                raise ValueError('Could not load master information at {0}. Is that node running correctly?'.format(master_ip))

                        current_sr_message = []
                        for sr_name in sorted(NodeInstallationController.nodes):
                            current_sr_message.append('{0:<15} - {1}'.format(NodeInstallationController.nodes[sr_name]['ip'], sr_name))
                        if Interactive.ask_yesno(message='Following StorageRouters were detected:\n  -  {0}\nIs this correct?'.format('\n  -  '.join(current_sr_message)),
                                                 default_value=True) is False:
                            raise Exception('The cluster on the given master node cannot be joined as not all StorageRouters could be loaded')

                    else:  # Join an existing cluster automatically
                        NodeInstallationController._logger.debug('Cluster {0} selected'.format(cluster_name))
                        first_node = False
                        for host_name, node_info in discovery_result.get(cluster_name, {}).iteritems():
                            if host_name != node_name and node_info.get('type') == 'master':
                                master_ip = node_info['ip']
                                break
                        if master_ip is None:
                            raise RuntimeError('Could not find appropriate master')

                        master_password = Toolbox.ask_validate_password(ip=master_ip, logger=NodeInstallationController._logger)
                        cluster_ip = Interactive.ask_choice(NodeInstallationController.host_ips, 'Select the public IP address of {0}'.format(node_name))
                        NodeInstallationController.nodes = NodeTypeController.retrieve_storagerouter_info_via_host(ip=master_ip, password=master_password)

                else:  # Automated install
                    NodeInstallationController._logger.debug('Automated installation')
                    cluster_ip = master_ip if cluster_ip is None else cluster_ip
                    first_node = master_ip == cluster_ip
                    cluster_name = 'preconfig-{0}'.format(master_ip.replace('.', '-'))
                    NodeInstallationController._logger.info('Detected{0} a 1st node installation'.format('' if first_node is True else ' not'))

                    if first_node is False:
                        NodeInstallationController.nodes = NodeTypeController.retrieve_storagerouter_info_via_host(ip=master_ip, password=master_password)
                    else:
                        NodeInstallationController.nodes[node_name] = {'ip': master_ip,
                                                                       'type': 'master'}

                    # Validation of parameters
                    if master_ip != cluster_ip:
                        master_ips = [sr_info['ip'] for sr_info in NodeInstallationController.nodes.itervalues() if sr_info['type'] == 'master']
                        if master_ip not in master_ips:
                            if master_ips:
                                raise ValueError('Incorrect master IP provided, please choose from: {0}'.format(', '.join(master_ips)))
                            else:
                                raise ValueError('Could not load master information at {0}. Is that node running correctly?'.format(master_ip))
                    else:
                        if node_type == 'extra':
                            raise ValueError('A 1st node can never be installed as an "extra" node')
                    if cluster_ip not in NodeInstallationController.host_ips:
                        raise ValueError('{0} IP provided {1} is not in the list of local IPs: {2}'.format('Master' if master_ip == cluster_ip else 'Cluster',
                                                                                                           cluster_ip,
                                                                                                           ', '.join(NodeInstallationController.host_ips)))

                if len(NodeInstallationController.nodes) == 0:
                    NodeInstallationController._logger.debug('No StorageRouters could be loaded, cannot join the cluster')
                    raise RuntimeError('The cluster on the given master node cannot be joined as no StorageRouters could be loaded')

                if cluster_ip is None or master_ip is None:  # Master IP and cluster IP must be known by now, cluster_ip == master_ip for 1st node
                    raise ValueError('Something must have gone wrong retrieving IP information')
                if first_node is False and cluster_ip in [node['ip'] for node in NodeInstallationController.nodes.itervalues()]:
                    raise ValueError('Node with public IP {0} is already part of cluster {1}'.format(cluster_ip, cluster_name))

                if node_name != fqdn_name:
                    ip_hostname_map = {cluster_ip: [fqdn_name, node_name]}
                else:
                    ip_hostname_map = {cluster_ip: [fqdn_name]}

                for node_host_name, node_info in NodeInstallationController.nodes.iteritems():
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

                if node_name in NodeInstallationController.nodes:
                    NodeInstallationController.nodes[node_name]['client'] = SSHClient(endpoint=cluster_ip, username='root')
                else:
                    NodeInstallationController.nodes[node_name] = {'ip': cluster_ip,
                                                                   'type': 'unknown',
                                                                   'client': SSHClient(endpoint=cluster_ip, username='root')}

                Toolbox.log(logger=NodeInstallationController._logger, messages='Preparing node', title=True)
                Toolbox.log(logger=NodeInstallationController._logger, messages='Setting up and exchanging SSH keys')

                # Fetching clients
                all_ips = NodeInstallationController.host_ips
                local_client = None
                master_client = None
                for node_info in NodeInstallationController.nodes.itervalues():
                    node_ip = node_info['ip']
                    all_ips.add(node_ip)
                    if node_ip == cluster_ip:
                        local_client = node_info['client']
                    if node_ip == master_ip:
                        master_client = node_info['client']
                if local_client is None or master_client is None:
                    raise ValueError('Retrieving client information failed')

                # Templates
                local_home_dirs = {}
                private_key_template = '{0}/.ssh/id_rsa'
                public_key_template = '{0}/.ssh/id_rsa.pub'
                authorized_keys_template = '{0}/.ssh/authorized_keys'
                known_hosts_template = '{0}/.ssh/known_hosts'
                host_name, _ = local_client.get_hostname()
                # Generate SSH keys
                for user in ['root', 'ovs']:
                    home_dir = local_client.run('echo ~{0}'.format(user), allow_insecure=True).strip()
                    local_home_dirs[user] = home_dir
                    ssh_folder = '{0}/.ssh'.format(home_dir)
                    private_key_filename = private_key_template.format(home_dir)
                    public_key_filename = public_key_template.format(home_dir)
                    authorized_keys_filename = authorized_keys_template.format(home_dir)
                    known_hosts_filename = known_hosts_template.format(home_dir)
                    if not local_client.dir_exists(ssh_folder):
                        local_client.dir_create(ssh_folder)
                        local_client.dir_chmod(ssh_folder, 0700)
                        local_client.dir_chown(ssh_folder, user, user)
                    if not local_client.file_exists(private_key_filename):
                        local_client.run(['ssh-keygen',
                                          '-t', 'rsa', '-b', '4096',
                                          '-f', private_key_filename,
                                          '-N', '',
                                          '-C', '{0}@{1}'.format(user, host_name)])
                        local_client.file_chown([private_key_filename, public_key_filename], user, user)
                        local_client.file_chmod(private_key_filename, 0600)
                        local_client.file_chmod(public_key_filename, 0644)
                    for filename in [authorized_keys_filename, known_hosts_filename]:
                        if not local_client.file_exists(filename):
                            local_client.file_create(filename)
                            local_client.file_chown(filename, user, user)
                            local_client.file_chmod(filename, 0600)

                # Exchange SSH keys
                local_pub_key_ovs = local_client.file_read(public_key_template.format(local_home_dirs['ovs']))
                local_pub_key_root = local_client.file_read(public_key_template.format(local_home_dirs['root']))

                # Connect to master and add the ovs and root public SSH key to all other nodes in the cluster
                all_pub_keys = [local_pub_key_ovs, local_pub_key_root]
                if first_node is False:
                    with remote(master_client.ip, [SSHClient], 'root', master_password) as rem:
                        for node_host_name, node in NodeInstallationController.nodes.iteritems():
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
                            ovs_homedir = client.run('echo ~ovs', allow_insecure=True).strip()
                            root_homedir = client.run('echo ~root', allow_insecure=True).strip()
                            for authorized_key in [authorized_keys_template.format(ovs_homedir),
                                                   authorized_keys_template.format(root_homedir)]:
                                if client.file_exists(authorized_key):
                                    master_authorized_keys = client.file_read(authorized_key)
                                    for local_pub_key in [local_pub_key_ovs, local_pub_key_root]:
                                        if local_pub_key not in master_authorized_keys:
                                            master_authorized_keys += '\n{0}'.format(local_pub_key)
                                            client.file_write(authorized_key, master_authorized_keys)
                            all_pub_keys.append(client.file_read(public_key_template.format(ovs_homedir)))
                            all_pub_keys.append(client.file_read(public_key_template.format(root_homedir)))

                # Now add all public keys of all nodes in the cluster to the local node
                for authorized_keys in [authorized_keys_template.format(local_home_dirs['ovs']),
                                        authorized_keys_template.format(local_home_dirs['root'])]:
                    if local_client.file_exists(authorized_keys):
                        keys = local_client.file_read(authorized_keys)
                        for public_key in all_pub_keys:
                            if public_key not in keys:
                                keys += '\n{0}'.format(public_key)
                        local_client.file_write(authorized_keys, keys)

                # Execute ssh-keyscan, required for the "remote" functionality
                def _raise_timeout(*args, **kwargs):
                    _ = args, kwargs
                    raise RuntimeError('Timeout during ssh keyscan, please check node inter-connectivity')
                signal.signal(signal.SIGALRM, _raise_timeout)
                for node_details in NodeInstallationController.nodes.itervalues():
                    signal.alarm(30)
                    for user in ['ovs', 'root']:
                        node_client = SSHClient(endpoint=node_details['ip'], username=user)
                        cmd = 'cp {{0}} {{0}}.tmp; ssh-keyscan -t rsa {0} {1} 2> /dev/null >> {{0}}.tmp; cat {{0}}.tmp | sort -u - > {{0}}'.format(
                            ' '.join(["'{0}'".format(node_client.shell_safe(_ip)) for _ip in all_ips]),
                            ' '.join(["'{0}'".format(node_client.shell_safe(_key)) for _key in NodeInstallationController.nodes.keys()])
                        )
                        home_dir = node_client.run('echo ~{0}'.format(user), allow_insecure=True).strip()
                        node_client.run(cmd.format(known_hosts_template.format(home_dir)), allow_insecure=True)
                    signal.alarm(0)

                Toolbox.log(logger=NodeInstallationController._logger, messages='Updating hosts file')
                for node_details in NodeInstallationController.nodes.itervalues():
                    node_client = node_details.get('client', SSHClient(endpoint=node_details['ip'], username='root'))
                    System.update_hosts_file(ip_hostname_map, node_client)

                # Write resume config
                resume_config['node_type'] = node_type
                resume_config['master_ip'] = master_ip
                resume_config['unique_id'] = unique_id
                resume_config['cluster_ip'] = cluster_ip
                resume_config['cluster_name'] = cluster_name
                resume_config['external_config'] = external_config
                resume_config['execute_rollback'] = execute_rollback
                resume_config['enable_heartbeats'] = enable_heartbeats
                with open(resume_config_file, 'w') as resume_cfg:
                    resume_cfg.write(json.dumps(resume_config))

                ip_client_map = dict((info['ip'], SSHClient(info['ip'], username='root')) for info in NodeInstallationController.nodes.itervalues())
                if first_node is True:
                    try:
                        NodeInstallationController._setup_first_node(target_client=ip_client_map[cluster_ip],
                                                                     cluster_name=cluster_name,
                                                                     node_name=node_name,
                                                                     enable_heartbeats=enable_heartbeats,
                                                                     external_config=external_config,
                                                                     logging_target=logging_target,
                                                                     rdma=rdma)
                    except Exception as ex:
                        Toolbox.log(logger=NodeInstallationController._logger, messages=['Failed to setup first node', ex], loglevel='exception')
                        if execute_rollback is True:
                            NodeInstallationController.rollback_setup(target_client=ip_client_map[cluster_ip])
                        else:
                            root_client.file_write('/tmp/ovs_rollback', 'rollback')
                        raise
                else:
                    # Deciding master/extra
                    try:
                        NodeInstallationController._setup_extra_node(cluster_ip=cluster_ip,
                                                                     master_ip=master_ip,
                                                                     ip_client_map=ip_client_map)
                    except Exception as ex:
                        Toolbox.log(logger=NodeInstallationController._logger, messages=['Failed to setup extra node', ex], loglevel='exception')
                        if execute_rollback is True:
                            NodeInstallationController.rollback_setup(target_client=ip_client_map[cluster_ip])
                        else:
                            root_client.file_write('/tmp/ovs_rollback', 'rollback')
                        raise

                    if promote_completed is False:
                        Toolbox.log(logger=NodeInstallationController._logger, messages='Analyzing cluster layout')
                        framework_cluster_name = str(Configuration.get('/ovs/framework/arakoon_clusters|ovsdb'))
                        arakoon_config = ArakoonClusterConfig(cluster_id=framework_cluster_name)
                        NodeInstallationController._logger.debug('{0} nodes for cluster {1} found'.format(len(arakoon_config.nodes), framework_cluster_name))
                        if (len(arakoon_config.nodes) < 3 or node_type == 'master') and node_type != 'extra':
                            configure_rabbitmq = Toolbox.is_service_internally_managed(service='rabbitmq')
                            configure_memcached = Toolbox.is_service_internally_managed(service='memcached')
                            try:
                                NodeTypeController.promote_node(cluster_ip=cluster_ip,
                                                                master_ip=master_ip,
                                                                ip_client_map=ip_client_map,
                                                                unique_id=unique_id,
                                                                configure_memcached=configure_memcached,
                                                                configure_rabbitmq=configure_rabbitmq)
                            except Exception as ex:
                                if execute_rollback is True:
                                    Toolbox.log(logger=NodeInstallationController._logger, messages=['\nFailed to promote node, rolling back', ex], loglevel='exception')
                                    NodeTypeController.demote_node(cluster_ip=cluster_ip,
                                                                   master_ip=master_ip,
                                                                   ip_client_map=ip_client_map,
                                                                   unique_id=unique_id,
                                                                   unconfigure_memcached=configure_memcached,
                                                                   unconfigure_rabbitmq=configure_rabbitmq)
                                else:
                                    root_client.file_write('/tmp/ovs_rollback', 'demote')
                                raise

            root_client.file_delete(resume_config_file)
            if enable_heartbeats is True:
                Toolbox.log(logger=NodeInstallationController._logger, messages='')
                Toolbox.log(logger=NodeInstallationController._logger,
                            messages=['Open vStorage securely sends a minimal set of error, usage and health',
                                      'information. This information is used to keep the quality and performance',
                                      'of the code at the highest possible levels.',
                                      'Please refer to the documentation for more information.'],
                            boxed=True)

            is_master = [node for node in NodeInstallationController.nodes.itervalues() if node['type'] == 'master' and node['ip'] == cluster_ip]
            Toolbox.log(logger=NodeInstallationController._logger, messages='')
            Toolbox.log(logger=NodeInstallationController._logger,
                        messages=['Setup complete.',
                                  'Point your browser to https://{0} to use Open vStorage'.format(cluster_ip if len(is_master) > 0 else master_ip)],
                        boxed=True)
            NodeInstallationController._logger.info('Setup complete')

            # Try to trigger setups from possibly installed other packages
            if root_client.run(['which', 'asd-manager'], allow_nonzero=True) != '':
                sys.path.append('/opt/asd-manager/')
                from source.asdmanager import setup
                Toolbox.log(logger=NodeInstallationController._logger, messages='\nA local ASD Manager was detected for which the setup will now be launched.\n')
                setup()
        except Exception as exception:
            Toolbox.log(logger=NodeInstallationController._logger, messages='\n')
            Toolbox.log(logger=NodeInstallationController._logger, messages=['An unexpected error occurred:', str(exception).lstrip('\n')], boxed=True, loglevel='exception')
            sys.exit(1)
        except KeyboardInterrupt:
            Toolbox.log(logger=NodeInstallationController._logger, messages='\n')
            Toolbox.log(logger=NodeInstallationController._logger, messages='This setup was aborted. Open vStorage may be in an inconsistent state, make sure to validate the installation.', boxed=True, loglevel='error')
            sys.exit(1)

    @staticmethod
    def rollback_setup(target_client=None):
        """
        Rollback a failed setup
        :param target_client: Client on which to perform the rollback
        :type target_client: ovs_extensions.generic.sshclient.SSHClient
        """
        from ovs.dal.lists.servicetypelist import ServiceTypeList
        from ovs.dal.lists.storagerouterlist import StorageRouterList
        Toolbox.log(logger=NodeInstallationController._logger, messages='Rolling back setup of current node', title=True)
        service_manager = ServiceFactory.get_manager()

        single_node = len(StorageRouterList.get_storagerouters()) == 1
        if target_client is None:
            target_client = SSHClient(endpoint=System.get_my_storagerouter(), username='root')

        if not target_client.file_exists('/tmp/ovs_rollback'):
            Toolbox.log(logger=NodeInstallationController._logger, messages='Cannot rollback on nodes which have been successfully installed. Please use "ovs remove node" instead', boxed=True, loglevel='error')
            sys.exit(1)
        mode = target_client.file_read('/tmp/ovs_rollback').strip()
        if mode != 'rollback':
            Toolbox.log(logger=NodeInstallationController._logger, messages='Rolling back is only supported when installation issues occurred, please execute "ovs setup {0}" first'.format(mode), boxed=True, loglevel='error')
            sys.exit(1)

        cluster_ip = target_client.ip
        machine_id = System.get_my_machine_id(target_client)
        required_info = {'/ovs/framework/memcache': None,
                         '/ovs/framework/paths|ovsdb': '',
                         '/ovs/framework/memcache|endpoints': [],
                         '/ovs/framework/arakoon_clusters|ovsdb': None,
                         '/ovs/framework/messagequeue|endpoints': []}

        for key in required_info:
            try:
                required_info[key] = Configuration.get(key=key)
            except KeyError:
                pass
        unconfigure_rabbitmq = Toolbox.is_service_internally_managed(service='rabbitmq')
        unconfigure_memcached = Toolbox.is_service_internally_managed(service='memcached')

        target_client.dir_delete('/opt/OpenvStorage/webapps/frontend/logging')

        Toolbox.log(logger=NodeInstallationController._logger, messages='Stopping services')
        for service in ['watcher-framework', 'watcher-config', 'workers', 'support-agent']:
            if service_manager.has_service(service, client=target_client):
                ServiceFactory.change_service_state(target_client, service, 'stop', NodeInstallationController._logger)

        endpoints = required_info['/ovs/framework/messagequeue|endpoints']
        if len(endpoints) > 0 and unconfigure_rabbitmq is True:
            Toolbox.log(logger=NodeInstallationController._logger, messages='Un-configuring RabbitMQ')
            try:
                if service_manager.is_rabbitmq_running(client=target_client)[0] is True:
                    ServiceFactory.change_service_state(target_client, 'rabbitmq-server', 'stop', NodeInstallationController._logger)
                target_client.file_delete('/etc/rabbitmq/rabbitmq.config')
            except Exception as ex:
                Toolbox.log(logger=NodeInstallationController._logger, messages=['Failed to un-configure RabbitMQ', ex], loglevel='exception')

            for endpoint in endpoints:
                if endpoint.startswith(target_client.ip):
                    endpoints.remove(endpoint)
                    break
            if len(endpoints) == 0:
                Configuration.delete('/ovs/framework/messagequeue')
            else:
                Configuration.set('/ovs/framework/messagequeue|endpoints', endpoints)

            Toolbox.log(logger=NodeInstallationController._logger, messages='Un-configuring Memcached')
            endpoints = required_info['/ovs/framework/memcache|endpoints']
            if len(endpoints) > 0 and unconfigure_memcached is True:
                service_manager.stop_service('memcached', target_client)
                for endpoint in endpoints:
                    if endpoint.startswith(target_client.ip):
                        endpoints.remove(endpoint)
                        break
                if len(endpoints) == 0:
                    Configuration.delete('/ovs/framework/memcache')
                else:
                    Configuration.set('/ovs/framework/memcache|endpoints', endpoints)

        NodeRemovalController.remove_services(target_client, 'master', logger=NodeInstallationController._logger)
        service = 'watcher-config'
        if service_manager.has_service(service, client=target_client):
            Toolbox.log(logger=NodeInstallationController._logger, messages='Removing service {0}'.format(service))
            service_manager.stop_service(service, client=target_client)
            service_manager.remove_service(service, client=target_client)

        Toolbox.log(logger=NodeInstallationController._logger, messages='Cleaning up model')
        #  Model is completely cleaned up when the arakoon cluster is destroyed
        memcache_configured = required_info['/ovs/framework/memcache']
        storagerouter = None
        if memcache_configured is not None:
            try:
                storagerouter = System.get_my_storagerouter()
            except Exception as ex:
                Toolbox.log(logger=NodeInstallationController._logger, messages='Retrieving StorageRouter information failed with error: {0}'.format(ex), loglevel='error')

            if storagerouter is not None:  # StorageRouter will be None if StorageRouter not yet modeled
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
                    Toolbox.log(logger=NodeInstallationController._logger, messages='Cleaning up model failed with error: {0}'.format(ex), loglevel='error')
            if single_node is True:
                try:
                    for service in ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON).services:  # Externally managed Arakoon services not linked to the StorageRouter
                        service.delete()
                except Exception as ex:
                    Toolbox.log(logger=NodeInstallationController._logger, messages='Cleaning up services failed with error: {0}'.format(ex), loglevel='error')
        if single_node is True:
            for key in Configuration.base_config.keys() + ['install_time', 'plugins']:
                try:
                    Configuration.delete(key='/ovs/framework/{0}'.format(key))
                except KeyError:
                    pass

        try:
            Configuration.delete(key='/ovs/framework/hosts/{0}'.format(machine_id))
        except KeyError:
            pass

        #  Memcached and Arakoon must be the last services to be removed
        services = ['memcached']
        cluster_name = required_info['/ovs/framework/arakoon_clusters|ovsdb']
        try:
            metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)
        except ValueError:
            metadata = None
        if metadata is not None and metadata['internal'] is True:
            services.append(ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name))
        for service in services:
            if service_manager.has_service(service, client=target_client):
                ServiceFactory.change_service_state(target_client, service, 'stop', NodeInstallationController._logger)

        if single_node is True:
            Toolbox.log(logger=NodeInstallationController._logger, messages='Un-configure Arakoon')
            if metadata is not None and metadata['internal'] is True:
                try:
                    arakoon_installer = ArakoonInstaller(cluster_name=cluster_name)
                    arakoon_installer.delete_cluster()
                except Exception as ex:
                    Toolbox.log(logger=NodeInstallationController._logger, messages=['\nFailed to delete cluster', ex], loglevel='exception')
                base_dir = required_info['/ovs/framework/paths|ovsdb']
                #  ArakoonInstall.delete_cluster calls destroy_node which removes these directories already
                directory_info = [ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, cluster_name),
                                  ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, cluster_name)]
                try:
                    ArakoonInstaller.clean_leftover_arakoon_data(ip=cluster_ip,
                                                                 directories=directory_info)
                except Exception as ex:
                    Toolbox.log(logger=NodeInstallationController._logger, messages=['Failed to clean Arakoon data', ex])

        target_client.file_delete('/tmp/ovs_rollback')

    @staticmethod
    def _setup_first_node(target_client, cluster_name, node_name, enable_heartbeats, external_config, logging_target, rdma):
        """
        Sets up the first node services. This node is always a master
        """
        Toolbox.log(logger=NodeInstallationController._logger, messages='Setting up first node', title=True)
        service_manager = ServiceFactory.get_manager()
        cluster_ip = target_client.ip
        machine_id = System.get_my_machine_id(target_client)

        Toolbox.log(logger=NodeInstallationController._logger, messages='Setting up configuration management')
        if external_config is None and not cluster_name.startswith('preconfig-'):
            if Interactive.ask_yesno(message='Use an external cluster?', default_value=False) is True:
                file_location = Configuration.CACC_LOCATION
                while not target_client.file_exists(file_location):
                    Toolbox.log(logger=NodeInstallationController._logger, messages='Please place a copy of the Arakoon\'s client configuration file at: {0}'.format(file_location))
                    Interactive.ask_continue()
                external_config = True

        if not target_client.file_exists(Configuration.CONFIG_STORE_LOCATION):
            target_client.file_create(Configuration.CONFIG_STORE_LOCATION)
        framework_config = {'configuration_store': 'arakoon'}
        target_client.file_write(Configuration.CONFIG_STORE_LOCATION, json.dumps(framework_config, indent=4))

        Toolbox.log(logger=NodeInstallationController._logger, messages='Setting up configuration Arakoon')

        if external_config is None:
            arakoon_config_cluster = 'config'
            arakoon_installer = ArakoonInstaller(cluster_name=arakoon_config_cluster)
            arakoon_installer.create_cluster(cluster_type=ServiceType.ARAKOON_CLUSTER_TYPES.CFG,
                                             ip=cluster_ip,
                                             base_dir='/opt/OpenvStorage/db',
                                             locked=False)
            arakoon_installer.start_cluster()
            contents = target_client.file_read(ArakoonClusterConfig.CONFIG_FILE.format('config'))
            target_client.file_write(Configuration.CACC_LOCATION, contents)
            service_manager.register_service(node_name=machine_id,
                                             service_metadata=arakoon_installer.service_metadata[cluster_ip])
        else:
            arakoon_cacc_cluster = 'cacc'
            arakoon_installer = ArakoonInstaller(cluster_name=arakoon_cacc_cluster)
            arakoon_installer.load(ip=cluster_ip)
            arakoon_installer.claim_cluster()
            arakoon_installer.store_config()

        Configuration.initialize(external_config=external_config, logging_target=logging_target)
        Configuration.initialize_host(machine_id)

        # Write away cluster id to let the support agent read it when Arakoon is down
        framework_config['cluster_id'] = Configuration.get('/ovs/framework/cluster_id')
        target_client.file_write(Configuration.CONFIG_STORE_LOCATION, json.dumps(framework_config, indent=4))

        if rdma is None:
            rdma = Interactive.ask_yesno(message='Enable RDMA?', default_value=False)
        Configuration.set('/ovs/framework/rdma', rdma)
        Configuration.set('/ovs/framework/cluster_name', cluster_name)

        service = 'watcher-config'
        if not service_manager.has_service(service, target_client):
            Toolbox.log(logger=NodeInstallationController._logger, messages='Adding service {0}'.format(service))
            service_manager.add_service(service, params={}, client=target_client)
            ServiceFactory.change_service_state(target_client, service, 'start', NodeInstallationController._logger)

        metadata = ArakoonInstaller.get_unused_arakoon_metadata_and_claim(cluster_type=ServiceType.ARAKOON_CLUSTER_TYPES.FWK)
        arakoon_ports = []
        if metadata is None:  # No externally managed cluster found, we create 1 ourselves
            Toolbox.log(logger=NodeInstallationController._logger, messages='Setting up Arakoon cluster ovsdb')
            internal = True
            arakoon_ovsdb_cluster = 'ovsdb'
            arakoon_installer = ArakoonInstaller(cluster_name=arakoon_ovsdb_cluster)
            arakoon_installer.create_cluster(cluster_type=ServiceType.ARAKOON_CLUSTER_TYPES.FWK,
                                             ip=cluster_ip,
                                             base_dir=Configuration.get('/ovs/framework/paths|ovsdb'),
                                             locked=False)
            arakoon_installer.start_cluster()
            metadata = arakoon_installer.metadata
            arakoon_ports = arakoon_installer.ports[cluster_ip]
        else:
            Toolbox.log(logger=NodeInstallationController._logger, messages='Externally managed Arakoon cluster of type {0} found with name {1}'.format(ServiceType.ARAKOON_CLUSTER_TYPES.FWK, metadata['cluster_name']))
            internal = False

        Configuration.set('/ovs/framework/arakoon_clusters|ovsdb', metadata['cluster_name'])
        NodeTypeController.add_services(client=target_client, node_type='master', logger=NodeInstallationController._logger)
        Toolbox.log(logger=NodeInstallationController._logger, messages='Build configuration files')

        configure_rabbitmq = Toolbox.is_service_internally_managed(service='rabbitmq')
        configure_memcached = Toolbox.is_service_internally_managed(service='memcached')
        if configure_rabbitmq is True:
            Configuration.set('/ovs/framework/messagequeue|endpoints', ['{0}:5672'.format(cluster_ip)])
            NodeTypeController.configure_rabbitmq(client=target_client, logger=NodeInstallationController._logger)
        if configure_memcached is True:
            Configuration.set('/ovs/framework/memcache|endpoints', ['{0}:11211'.format(cluster_ip)])
            NodeTypeController.configure_memcached(client=target_client, logger=NodeInstallationController._logger)
        VolatileFactory.store = None

        Toolbox.log(logger=NodeInstallationController._logger, messages='Starting model services')
        model_services = ['memcached', 'arakoon-ovsdb'] if internal is True else ['memcached']
        for service in model_services:
            if service_manager.has_service(service, client=target_client):
                ServiceFactory.change_service_state(target_client, service, 'restart', NodeInstallationController._logger)

        Toolbox.log(logger=NodeInstallationController._logger, messages='Start model migration')
        from ovs.dal.helpers import Migration
        Migration.migrate()

        Toolbox.log(logger=NodeInstallationController._logger, messages='Finalizing setup', title=True)
        storagerouter = NodeInstallationController._finalize_setup(target_client, node_name, 'MASTER')

        from ovs.dal.lists.servicelist import ServiceList
        arakoon_service_name = ArakoonInstaller.get_service_name_for_cluster(cluster_name=metadata['cluster_name'])
        if arakoon_service_name not in [s.name for s in ServiceList.get_services()]:
            from ovs.dal.lists.servicetypelist import ServiceTypeList
            from ovs.dal.hybrids.service import Service
            service = Service()
            service.name = arakoon_service_name
            service.type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.ARAKOON)
            service.ports = arakoon_ports
            service.storagerouter = storagerouter if internal is True else None
            service.save()

        Toolbox.log(logger=NodeInstallationController._logger, messages='Updating configuration files')
        Configuration.set('/ovs/framework/hosts/{0}/ip'.format(machine_id), cluster_ip)

        Toolbox.log(logger=NodeInstallationController._logger, messages='Starting services on 1st node')
        for service in model_services + ['rabbitmq-server']:
            if service_manager.has_service(service, client=target_client):
                ServiceFactory.change_service_state(target_client, service, 'start', NodeInstallationController._logger)
        # Enable HA for the rabbitMQ queues
        NodeTypeController.check_rabbitmq_and_enable_ha_mode(client=target_client, logger=NodeInstallationController._logger)

        for service in ['watcher-framework', 'watcher-config']:
            ServiceFactory.change_service_state(target_client, service, 'start', NodeInstallationController._logger)

        Toolbox.log(logger=NodeInstallationController._logger, messages='Check ovs-workers')
        # Workers are started by ovs-watcher-framework, but for a short time they are in pre-start
        ServiceFactory.wait_for_service(client=target_client, name='workers', status='active', logger=NodeInstallationController._logger)

        Toolbox.run_hooks(component='nodeinstallation',
                          sub_component='firstnode',
                          logger=NodeInstallationController._logger,
                          cluster_ip=cluster_ip)

        if enable_heartbeats is False:
            Configuration.set('/ovs/framework/support|support_agent', False)
        else:
            service = 'support-agent'
            if not service_manager.has_service(service, target_client):
                service_manager.add_service(service, client=target_client)
                ServiceFactory.change_service_state(target_client, service, 'start', NodeInstallationController._logger)

        if NodeTypeController.avahi_installed(client=target_client, logger=NodeInstallationController._logger) is True:
            NodeTypeController.configure_avahi(client=target_client, node_name=node_name, node_type='master', logger=NodeInstallationController._logger)
        Configuration.set('/ovs/framework/hosts/{0}/setupcompleted'.format(machine_id), True)
        Configuration.set('/ovs/framework/hosts/{0}/promotecompleted'.format(machine_id), True)
        Configuration.set('/ovs/framework/hosts/{0}/type'.format(machine_id), 'MASTER')
        Configuration.set('/ovs/framework/install_time', time.time())
        target_client.run(['chown', '-R', 'ovs:ovs', '/opt/OpenvStorage/config'])
        Toolbox.log(logger=NodeInstallationController._logger, messages='First node complete')

    @staticmethod
    def _setup_extra_node(cluster_ip, master_ip, ip_client_map):
        """
        Sets up an additional node
        """
        Toolbox.log(logger=NodeInstallationController._logger, messages='Adding extra node', title=True)
        service_manager = ServiceFactory.get_manager()
        target_client = ip_client_map[cluster_ip]
        master_client = ip_client_map[master_ip]
        machine_id = System.get_my_machine_id(target_client)

        target_client.file_write(Configuration.CONFIG_STORE_LOCATION,
                                 master_client.file_read(Configuration.CONFIG_STORE_LOCATION))
        target_client.file_write(Configuration.CACC_LOCATION,
                                 master_client.file_read(Configuration.CACC_LOCATION))
        Configuration.initialize_host(machine_id)

        service = 'watcher-config'
        if not service_manager.has_service(service, target_client):
            Toolbox.log(logger=NodeInstallationController._logger, messages='Adding service {0}'.format(service))
            service_manager.add_service(service, params={}, client=target_client)
            ServiceFactory.change_service_state(target_client, service, 'start', NodeInstallationController._logger)
        NodeTypeController.add_services(client=target_client, node_type='extra', logger=NodeInstallationController._logger)

        enabled = Configuration.get('/ovs/framework/support|support_agent')
        if enabled is True:
            service = 'support-agent'
            if not service_manager.has_service(service, target_client):
                service_manager.add_service(service, client=target_client)
                ServiceFactory.change_service_state(target_client, service, 'start', NodeInstallationController._logger)

        node_name, _ = target_client.get_hostname()
        NodeInstallationController._finalize_setup(target_client, node_name, 'EXTRA')

        Configuration.set('/ovs/framework/hosts/{0}/ip'.format(machine_id), cluster_ip)

        Toolbox.log(logger=NodeInstallationController._logger, messages='Starting services')
        for service in ['watcher-framework', 'watcher-config']:
            if service_manager.get_service_status(service, target_client) != 'active':
                ServiceFactory.change_service_state(target_client, service, 'start', NodeInstallationController._logger)

        Toolbox.log(logger=NodeInstallationController._logger, messages='Check ovs-workers')
        # Workers are started by ovs-watcher-framework, but for a short time they are in pre-start
        ServiceFactory.wait_for_service(client=target_client, name='workers', status='active', logger=NodeInstallationController._logger)

        Toolbox.log(logger=NodeInstallationController._logger, messages='Restarting workers')
        for node_client in ip_client_map.itervalues():
            ServiceFactory.change_service_state(node_client, 'workers', 'restart', NodeInstallationController._logger)

        Toolbox.run_hooks(component='nodeinstallation',
                          sub_component='extranode',
                          logger=NodeInstallationController._logger,
                          cluster_ip=cluster_ip,
                          master_ip=master_ip)

        if NodeTypeController.avahi_installed(client=target_client, logger=NodeInstallationController._logger) is True:
            NodeTypeController.configure_avahi(client=target_client, node_name=node_name, node_type='extra', logger=NodeInstallationController._logger)
        Configuration.set('/ovs/framework/hosts/{0}/setupcompleted'.format(machine_id), True)
        Configuration.set('/ovs/framework/hosts/{0}/type'.format(machine_id), 'EXTRA')
        target_client.run(['chown', '-R', 'ovs:ovs', '/opt/OpenvStorage/config'])
        Toolbox.log(logger=NodeInstallationController._logger, messages='Extra node complete')

    @staticmethod
    def _finalize_setup(client, node_name, node_type):
        # Configure Redis
        cluster_ip = client.ip

        Toolbox.log(logger=NodeInstallationController._logger, messages='Setting up Redis')
        client.run(['sed', '-i', 's/^# maxmemory <bytes>.*/maxmemory 128mb/g', '/etc/redis/redis.conf'])
        client.run(['sed', '-i', 's/^# maxmemory-policy .*/maxmemory-policy allkeys-lru/g', '/etc/redis/redis.conf'])
        client.run(['sed', '-i', 's/^bind 127.0.0.1.*/bind {0}/g'.format(cluster_ip), '/etc/redis/redis.conf'])
        ServiceFactory.change_service_state(client, 'redis-server', 'restart', NodeInstallationController._logger)

        client.dir_create('/opt/OpenvStorage/webapps/frontend/logging')
        config_file = '/opt/OpenvStorage/webapps/frontend/logging/config.js'
        old_value = 'http://"+window.location.hostname+":9200'
        new_value = 'http://' + cluster_ip + ':9200'

        if client.file_exists(config_file):
            contents = client.file_read(config_file)
            if new_value in contents and new_value.find(old_value) > 0:
                pass
            elif old_value in contents:
                contents = contents.replace(old_value, new_value)
            client.file_write(config_file, contents)

        # Imports, not earlier than here, as all required config files should be in place.
        from ovs.lib.disk import DiskController
        from ovs.lib.storagerouter import StorageRouterController
        from ovs.dal.hybrids.storagerouter import StorageRouter
        from ovs.dal.lists.storagerouterlist import StorageRouterList

        Toolbox.log(logger=NodeInstallationController._logger, messages='Configuring/updating model')
        unique_id = System.get_my_machine_id(client=client)
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
        try:
            if not Configuration.exists('/ovs/framework/edition'):
                val = storagerouter.features['alba']['edition']
                Configuration.set(key='/ovs/framework/edition', value=val)
        except Exception:
            NodeInstallationController._logger.exception('Error loading edition for StorageRouter {0}'.format(node_name))

        StorageRouterController.set_rdma_capability(storagerouter.guid)
        try:
            DiskController.sync_with_reality(storagerouter.guid)
        except Exception as ex:
            NodeInstallationController._logger.exception('Error syncing disks: {0}'.format(ex))
        return storagerouter
