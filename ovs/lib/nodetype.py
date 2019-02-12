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
Module for NodeTypeController
"""

import os
import re
import sys
import json
import time
from ovs.dal.hybrids.servicetype import ServiceType
from ovs_extensions.constants.vpools import HOSTS_BASE_PATH
from ovs.extensions.db.arakooninstaller import ArakoonClusterConfig, ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
from ovs.lib.helpers.toolbox import Toolbox


class NodeTypeController(object):
    """
    This class contains all logic for promoting and demoting nodes in the cluster
    """
    _logger = Logger('lib')

    avahi_filename = '/etc/avahi/services/ovs_cluster.service'

    @staticmethod
    def promote_or_demote_node(node_action, cluster_ip=None, execute_rollback=False):
        """
        Promotes or demotes the local node
        :param node_action: Demote or promote
        :type node_action: str
        :param cluster_ip: IP of node to promote or demote
        :type cluster_ip: str
        :param execute_rollback: In case of failure revert the changes made
        :type execute_rollback: bool
        :return: None
        """

        if node_action not in ('promote', 'demote'):
            raise ValueError('Nodes can only be promoted or demoted')

        Toolbox.log(logger=NodeTypeController._logger, messages='Open vStorage Setup - {0}'.format(node_action.capitalize()), boxed=True)
        try:
            Toolbox.log(logger=NodeTypeController._logger, messages='Collecting information', title=True)

            machine_id = System.get_my_machine_id()
            if Configuration.get('/ovs/framework/hosts/{0}/setupcompleted'.format(machine_id)) is False:
                raise RuntimeError('No local OVS setup found.')

            if cluster_ip and not re.match(Toolbox.regex_ip, cluster_ip):
                raise RuntimeError('Incorrect IP provided ({0})'.format(cluster_ip))

            if cluster_ip:
                client = SSHClient(endpoint=cluster_ip)
                machine_id = System.get_my_machine_id(client)

            node_type = Configuration.get('/ovs/framework/hosts/{0}/type'.format(machine_id))
            if node_action == 'promote' and node_type == 'MASTER':
                raise RuntimeError('This node is already master.')
            elif node_action == 'demote' and node_type == 'EXTRA':
                raise RuntimeError('This node should be a master.')
            elif node_type not in ['MASTER', 'EXTRA']:
                raise RuntimeError('This node is not correctly configured.')

            master_ip = None
            offline_nodes = []

            online = True
            target_client = None
            if node_action == 'demote' and cluster_ip:  # Demote an offline node
                from ovs.dal.lists.storagerouterlist import StorageRouterList
                from ovs.lib.storagedriver import StorageDriverController

                ip = cluster_ip
                unique_id = None
                ip_client_map = {}
                for storage_router in StorageRouterList.get_storagerouters():
                    try:
                        client = SSHClient(storage_router.ip, username='root')
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
                target_password = Toolbox.ask_validate_password(ip='127.0.0.1', logger=NodeTypeController._logger)
                target_client = SSHClient('127.0.0.1', username='root', password=target_password)

                unique_id = System.get_my_machine_id(target_client)
                ip = Configuration.get('/ovs/framework/hosts/{0}/ip'.format(unique_id))

                storagerouter_info = NodeTypeController.retrieve_storagerouter_info_via_host(ip=target_client.ip, password=target_password)
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
                    config = ArakoonClusterConfig(cluster_id=cluster_name)
                    arakoon_client = ArakoonInstaller.build_client(config)
                    metadata = json.loads(arakoon_client.get(ArakoonInstaller.METADATA_KEY))
                    if len(config.nodes) == 1 and config.nodes[0].ip == ip and metadata.get('internal') is True:
                        raise RuntimeError('Demote is not supported when single node Arakoon cluster(s) are present on the node to be demoted.')

            configure_rabbitmq = Toolbox.is_service_internally_managed(service='rabbitmq')
            configure_memcached = Toolbox.is_service_internally_managed(service='memcached')
            if node_action == 'promote':
                try:
                    NodeTypeController.promote_node(cluster_ip=ip,
                                                    master_ip=master_ip,
                                                    ip_client_map=ip_client_map,
                                                    unique_id=unique_id,
                                                    configure_memcached=configure_memcached,
                                                    configure_rabbitmq=configure_rabbitmq)
                except Exception:
                    if execute_rollback is True:
                        NodeTypeController.demote_node(cluster_ip=ip,
                                                       master_ip=master_ip,
                                                       ip_client_map=ip_client_map,
                                                       unique_id=unique_id,
                                                       unconfigure_memcached=configure_memcached,
                                                       unconfigure_rabbitmq=configure_rabbitmq,
                                                       offline_nodes=offline_nodes)
                    elif target_client is not None:
                        target_client.file_write('/tmp/ovs_rollback', 'demote')
                    raise
            else:
                try:
                    NodeTypeController.demote_node(cluster_ip=ip,
                                                   master_ip=master_ip,
                                                   ip_client_map=ip_client_map,
                                                   unique_id=unique_id,
                                                   unconfigure_memcached=configure_memcached,
                                                   unconfigure_rabbitmq=configure_rabbitmq,
                                                   offline_nodes=offline_nodes)
                except Exception:
                    if execute_rollback is True:
                        NodeTypeController.promote_node(cluster_ip=ip,
                                                        master_ip=master_ip,
                                                        ip_client_map=ip_client_map,
                                                        unique_id=unique_id,
                                                        configure_memcached=configure_memcached,
                                                        configure_rabbitmq=configure_rabbitmq)
                    elif target_client is not None:
                        target_client.file_write('/tmp/ovs_rollback', 'promote')
                    raise

            Toolbox.log(logger=NodeTypeController._logger, messages='\n')
            Toolbox.log(logger=NodeTypeController._logger, messages='{0} complete.'.format(node_action.capitalize()), boxed=True)
        except Exception as exception:
            Toolbox.log(logger=NodeTypeController._logger, messages='\n')
            Toolbox.log(logger=NodeTypeController._logger, messages=['An unexpected error occurred:', str(exception)], boxed=True, loglevel='exception')
            sys.exit(1)
        except KeyboardInterrupt:
            Toolbox.log(logger=NodeTypeController._logger, messages='\n')
            Toolbox.log(logger=NodeTypeController._logger,
                        messages='This setup was aborted. Open vStorage may be in an inconsistent state, make sure to validate the installation.',
                        boxed=True,
                        loglevel='error')
            sys.exit(1)

    @staticmethod
    def promote_node(cluster_ip, master_ip, ip_client_map, unique_id, configure_memcached, configure_rabbitmq):
        """
        Promotes a given node
        """
        from ovs.dal.lists.storagerouterlist import StorageRouterList
        from ovs.dal.lists.servicetypelist import ServiceTypeList
        from ovs.dal.lists.servicelist import ServiceList
        from ovs.dal.hybrids.service import Service

        Toolbox.log(logger=NodeTypeController._logger, messages='Promoting node', title=True)
        service_manager = ServiceFactory.get_manager()
        if configure_memcached is True:
            if NodeTypeController._validate_local_memcache_servers(ip_client_map) is False:
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
            Toolbox.log(logger=NodeTypeController._logger, messages='Joining Arakoon configuration cluster')
            arakoon_installer = ArakoonInstaller(cluster_name='config')
            arakoon_installer.load(ip=master_ip)
            arakoon_installer.extend_cluster(new_ip=cluster_ip,
                                             base_dir=Configuration.get('/ovs/framework/paths|ovsdb'))
            arakoon_installer.restart_cluster_after_extending(new_ip=cluster_ip)
            service_manager.register_service(node_name=machine_id,
                                             service_metadata=arakoon_installer.service_metadata[cluster_ip])

        # Find other (arakoon) master nodes
        arakoon_cluster_name = str(Configuration.get('/ovs/framework/arakoon_clusters|ovsdb'))
        arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=arakoon_cluster_name)
        config = ArakoonClusterConfig(cluster_id=arakoon_cluster_name)
        master_node_ips = [node.ip for node in config.nodes]
        if cluster_ip in master_node_ips:
            master_node_ips.remove(cluster_ip)
        if len(master_node_ips) == 0:
            raise RuntimeError('There should be at least one other master node')

        arakoon_ports = []
        if arakoon_metadata['internal'] is True:
            Toolbox.log(logger=NodeTypeController._logger, messages='Joining Arakoon OVS DB cluster')
            arakoon_installer = ArakoonInstaller(cluster_name=arakoon_cluster_name)
            arakoon_installer.load()
            arakoon_installer.extend_cluster(new_ip=cluster_ip,
                                             base_dir=Configuration.get('/ovs/framework/paths|ovsdb'))
            arakoon_installer.restart_cluster_after_extending(new_ip=cluster_ip)
            arakoon_ports = arakoon_installer.ports[cluster_ip]

        if configure_memcached is True:
            NodeTypeController.configure_memcached(client=target_client, logger=NodeTypeController._logger)
        NodeTypeController.add_services(client=target_client, node_type='master', logger=NodeTypeController._logger)

        Toolbox.log(logger=NodeTypeController._logger, messages='Update configurations')
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
            Toolbox.log(logger=NodeTypeController._logger, messages='Restarting master node services')
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
            NodeTypeController.configure_rabbitmq(client=target_client, logger=NodeTypeController._logger)
            # Copy rabbitmq cookie
            rabbitmq_cookie_file = '/var/lib/rabbitmq/.erlang.cookie'

            Toolbox.log(logger=NodeTypeController._logger, messages='Copying RabbitMQ cookie')
            contents = master_client.file_read(rabbitmq_cookie_file)
            master_hostname, _ = master_client.get_hostname()
            target_client.dir_create(os.path.dirname(rabbitmq_cookie_file))
            target_client.file_write(rabbitmq_cookie_file, contents)
            target_client.file_chmod(rabbitmq_cookie_file, mode=0400)
            target_client.run(['rabbitmq-server', '-detached'])
            time.sleep(5)
            target_client.run(['rabbitmqctl', 'stop_app'])
            time.sleep(5)
            target_client.run(['rabbitmqctl', 'join_cluster', 'rabbit@{0}'.format(master_hostname)])
            time.sleep(5)
            target_client.run(['rabbitmqctl', 'stop'])
            time.sleep(5)

            # Enable HA for the rabbitMQ queues
            ServiceFactory.change_service_state(target_client, 'rabbitmq-server', 'start', NodeTypeController._logger)
            NodeTypeController.check_rabbitmq_and_enable_ha_mode(client=target_client, logger=NodeTypeController._logger)

        NodeTypeController._configure_amqp_to_volumedriver()

        Toolbox.log(logger=NodeTypeController._logger, messages='Starting services')
        services = ['memcached', 'arakoon-ovsdb', 'rabbitmq-server']
        if arakoon_metadata['internal'] is True:
            services.remove('arakoon-ovsdb')
        for service in services:
            if service_manager.has_service(service, client=target_client):
                ServiceFactory.change_service_state(target_client, service, 'start', NodeTypeController._logger)

        Toolbox.log(logger=NodeTypeController._logger, messages='Restarting services')
        NodeTypeController.restart_framework_and_memcache_services(clients=ip_client_map, logger=NodeTypeController._logger)

        if Toolbox.run_hooks(component='nodetype',
                             sub_component='promote',
                             logger=NodeTypeController._logger,
                             cluster_ip=cluster_ip,
                             master_ip=master_ip):
            Toolbox.log(logger=NodeTypeController._logger, messages='Restarting services')
            NodeTypeController.restart_framework_and_memcache_services(clients=ip_client_map, logger=NodeTypeController._logger)

        if NodeTypeController.avahi_installed(client=target_client, logger=NodeTypeController._logger) is True:
            NodeTypeController.configure_avahi(client=target_client, node_name=node_name, node_type='master', logger=NodeTypeController._logger)
        Configuration.set('/ovs/framework/hosts/{0}/type'.format(machine_id), 'MASTER')
        target_client.run(['chown', '-R', 'ovs:ovs', '/opt/OpenvStorage/config'])
        Configuration.set('/ovs/framework/hosts/{0}/promotecompleted'.format(machine_id), True)

        if target_client.file_exists('/tmp/ovs_rollback'):
            target_client.file_delete('/tmp/ovs_rollback')

        Toolbox.log(logger=NodeTypeController._logger, messages='Promote complete')

    @staticmethod
    def demote_node(cluster_ip, master_ip, ip_client_map, unique_id, unconfigure_memcached, unconfigure_rabbitmq, offline_nodes=None):
        """
        Demotes a given node
        """
        from ovs.dal.lists.storagerouterlist import StorageRouterList

        Toolbox.log(logger=NodeTypeController._logger, messages='Demoting node', title=True)
        service_manager = ServiceFactory.get_manager()
        if offline_nodes is None:
            offline_nodes = []

        if unconfigure_memcached is True and len(offline_nodes) == 0:
            if NodeTypeController._validate_local_memcache_servers(ip_client_map) is False:
                raise RuntimeError('Not all memcache nodes can be reached which is required for demoting a node.')

        # Find other (arakoon) master nodes
        arakoon_cluster_name = str(Configuration.get('/ovs/framework/arakoon_clusters|ovsdb'))
        arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=arakoon_cluster_name)
        config = ArakoonClusterConfig(cluster_id=arakoon_cluster_name)
        master_node_ips = [node.ip for node in config.nodes]
        shrink = False
        if cluster_ip in master_node_ips:
            shrink = True
            master_node_ips.remove(cluster_ip)
        if len(master_node_ips) == 0:
            raise RuntimeError('There should be at least one other master node')

        storagerouter = StorageRouterList.get_by_machine_id(unique_id)
        storagerouter.node_type = 'EXTRA'
        storagerouter.save()

        offline_node_ips = [node.ip for node in offline_nodes]
        if arakoon_metadata['internal'] is True and shrink is True:
            Toolbox.log(logger=NodeTypeController._logger, messages='Leaving Arakoon {0} cluster'.format(arakoon_cluster_name))
            arakoon_installer = ArakoonInstaller(cluster_name=arakoon_cluster_name)
            arakoon_installer.load()
            arakoon_installer.shrink_cluster(removal_ip=cluster_ip,
                                             offline_nodes=offline_node_ips)
            arakoon_installer.restart_cluster_after_shrinking()
        try:
            external_config = Configuration.get('/ovs/framework/external_config')
            if external_config is None and shrink is True:
                Toolbox.log(logger=NodeTypeController._logger, messages='Leaving Arakoon config cluster')
                arakoon_installer = ArakoonInstaller(cluster_name='config')
                arakoon_installer.load(ip=master_node_ips[0])
                arakoon_installer.shrink_cluster(removal_ip=cluster_ip,
                                                 offline_nodes=offline_node_ips)
                arakoon_installer.restart_cluster_after_shrinking()
        except Exception as ex:
            Toolbox.log(logger=NodeTypeController._logger, messages=['\nFailed to leave configuration cluster', ex], loglevel='exception')

        Toolbox.log(logger=NodeTypeController._logger, messages='Update configurations')
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
            Toolbox.log(logger=NodeTypeController._logger, messages=['\nFailed to update configurations', ex], loglevel='exception')

        if arakoon_metadata['internal'] is True:
            Toolbox.log(logger=NodeTypeController._logger, messages='Restarting master node services')
            remaining_nodes = ip_client_map.keys()[:]
            if cluster_ip in remaining_nodes:
                remaining_nodes.remove(cluster_ip)

            PersistentFactory.store = None
            VolatileFactory.store = None

            for service in storagerouter.services:
                if service.name == 'arakoon-ovsdb':
                    service.delete()

        target_client = None
        if storagerouter in offline_nodes:
            if unconfigure_rabbitmq is True:
                Toolbox.log(logger=NodeTypeController._logger, messages='Removing/unconfiguring offline RabbitMQ node')
                client = ip_client_map[master_ip]
                try:
                    client.run(['rabbitmqctl', 'forget_cluster_node', 'rabbit@{0}'.format(storagerouter.name)])
                except Exception as ex:
                    Toolbox.log(logger=NodeTypeController._logger, messages=['\nFailed to forget RabbitMQ cluster node', ex], loglevel='exception')
        else:
            target_client = ip_client_map[cluster_ip]
            if unconfigure_rabbitmq is True:
                Toolbox.log(logger=NodeTypeController._logger, messages='Removing/unconfiguring RabbitMQ')
                try:
                    if service_manager.has_service('rabbitmq-server', client=target_client):
                        ServiceFactory.change_service_state(target_client, 'rabbitmq-server', 'stop', NodeTypeController._logger)
                        target_client.run(['rabbitmq-server', '-detached'])
                        time.sleep(5)
                        target_client.run(['rabbitmqctl', 'stop_app'])
                        time.sleep(5)
                        target_client.run(['rabbitmqctl', 'reset'])
                        time.sleep(5)
                        target_client.run(['rabbitmqctl', 'stop'])
                        time.sleep(5)
                        target_client.file_unlink("/var/lib/rabbitmq/.erlang.cookie")
                        ServiceFactory.change_service_state(target_client, 'rabbitmq-server', 'stop', NodeTypeController._logger)  # To be sure
                except Exception as ex:
                    Toolbox.log(logger=NodeTypeController._logger, messages=['\nFailed to remove/unconfigure RabbitMQ', ex], loglevel='exception')

            Toolbox.log(logger=NodeTypeController._logger, messages='Stopping services')
            services = ['memcached', 'rabbitmq-server']
            if unconfigure_rabbitmq is False:
                services.remove('rabbitmq-server')
            if unconfigure_memcached is False:
                services.remove('memcached')
            for service in services:
                if service_manager.has_service(service, client=target_client):
                    Toolbox.log(logger=NodeTypeController._logger, messages='Stopping service {0}'.format(service))
                    try:
                        ServiceFactory.change_service_state(target_client, service, 'stop', NodeTypeController._logger)
                    except Exception as ex:
                        Toolbox.log(logger=NodeTypeController._logger, messages=['\nFailed to stop service'.format(service), ex], loglevel='exception')

            Toolbox.log(logger=NodeTypeController._logger, messages='Removing services')
            services = ['scheduled-tasks', 'webapp-api', 'volumerouter-consumer']
            for service in services:
                if service_manager.has_service(service, client=target_client):
                    Toolbox.log(logger=NodeTypeController._logger, messages='Removing service {0}'.format(service))
                    try:
                        ServiceFactory.change_service_state(target_client, service, 'stop', NodeTypeController._logger)
                        service_manager.remove_service(service, client=target_client)
                    except Exception as ex:
                        Toolbox.log(logger=NodeTypeController._logger, messages=['\nFailed to remove service'.format(service), ex], loglevel='exception')

            if service_manager.has_service('workers', client=target_client):
                service_manager.add_service(name='workers',
                                            client=target_client,
                                            params={'WORKER_QUEUE': '{0}'.format(unique_id)})
        try:
            NodeTypeController._configure_amqp_to_volumedriver()
        except Exception as ex:
            Toolbox.log(logger=NodeTypeController._logger, messages=['\nFailed to configure AMQP to Storage Driver', ex], loglevel='exception')

        Toolbox.log(logger=NodeTypeController._logger, messages='Restarting services')
        NodeTypeController.restart_framework_and_memcache_services(clients=ip_client_map, logger=NodeTypeController._logger, offline_node_ips=offline_node_ips)

        if Toolbox.run_hooks(component='nodetype',
                             sub_component='demote',
                             logger=NodeTypeController._logger,
                             cluster_ip=cluster_ip,
                             master_ip=master_ip,
                             offline_node_ips=offline_node_ips):
            Toolbox.log(logger=NodeTypeController._logger, messages='Restarting services')
            NodeTypeController.restart_framework_and_memcache_services(clients=ip_client_map, logger=NodeTypeController._logger, offline_node_ips=offline_node_ips)

        if storagerouter not in offline_nodes:
            target_client = ip_client_map[cluster_ip]
            node_name, _ = target_client.get_hostname()
            if NodeTypeController.avahi_installed(client=target_client, logger=NodeTypeController._logger) is True:
                NodeTypeController.configure_avahi(client=target_client, node_name=node_name, node_type='extra', logger=NodeTypeController._logger)
        Configuration.set('/ovs/framework/hosts/{0}/type'.format(storagerouter.machine_id), 'EXTRA')

        if target_client is not None and target_client.file_exists('/tmp/ovs_rollback'):
            target_client.file_write('/tmp/ovs_rollback', 'rollback')

        Toolbox.log(logger=NodeTypeController._logger, messages='Demote complete', title=True)

    @staticmethod
    def restart_framework_and_memcache_services(clients, logger, offline_node_ips=None):
        """
        Restart framework and Memcached services
        :param clients: Clients on which to restart these services
        :type clients: dict
        :param logger: Logger object used for logging
        :type logger: ovs.extensions.generic.logger.Logger
        :param offline_node_ips: IP addresses of offline nodes in the cluster
        :type offline_node_ips: list
        :return: None
        """
        from ovs.dal.lists.storagerouterlist import StorageRouterList

        service_manager = ServiceFactory.get_manager()
        master_ips = [sr.ip for sr in StorageRouterList.get_masters()]
        slave_ips = [sr.ip for sr in StorageRouterList.get_slaves()]
        if offline_node_ips is None:
            offline_node_ips = []
        memcached = 'memcached'
        watcher = 'watcher-framework'
        support_agent = 'support-agent'
        for ip in master_ips + slave_ips:
            if ip not in offline_node_ips:
                if service_manager.has_service(watcher, clients[ip]):
                    ServiceFactory.change_service_state(clients[ip], watcher, 'stop', logger)
        for ip in master_ips:
            if ip not in offline_node_ips:
                ServiceFactory.change_service_state(clients[ip], memcached, 'restart', logger)
        for ip in master_ips + slave_ips:
            if ip not in offline_node_ips:
                if service_manager.has_service(watcher, clients[ip]):
                    ServiceFactory.change_service_state(clients[ip], watcher, 'start', logger)
                if service_manager.has_service(support_agent, clients[ip]):
                    ServiceFactory.change_service_state(clients[ip], support_agent, 'restart', logger)
        VolatileFactory.store = None

    @staticmethod
    def configure_memcached(client, logger):
        """
        Configure Memcached
        :param client: Client on which to configure Memcached
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param logger: Logger object used for logging
        :type logger: ovs.extensions.generic.logger.Logger
        :return: None
        """
        Toolbox.log(logger=logger, messages='Setting up Memcached')
        client.run(['sed', '-i', 's/^-l.*/-l 0.0.0.0/g', '/etc/memcached.conf'])
        client.run(['sed', '-i', 's/^-m.*/-m 1024/g', '/etc/memcached.conf'])
        client.run(['sed', '-i', '-E', 's/^-v(.*)/# -v\1/g', '/etc/memcached.conf'])  # Put all -v, -vv, ... back in comment
        client.run(['sed', '-i', 's/^# -v[^v]*$/-v/g', '/etc/memcached.conf'])     # Uncomment only -v

    @staticmethod
    def configure_rabbitmq(client, logger):
        """
        Configure RabbitMQ
        :param client: Client on which to configure RabbitMQ
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param logger: Logger object used for logging
        :type logger: ovs.extensions.generic.logger.Logger
        :return: None
        """
        Toolbox.log(logger=logger, messages='Setting up RabbitMQ')
        service_manager = ServiceFactory.get_manager()
        rabbitmq_port = Configuration.get('/ovs/framework/messagequeue|endpoints')[0].split(':')[1]
        rabbitmq_login = Configuration.get('/ovs/framework/messagequeue|user')
        rabbitmq_password = Configuration.get('/ovs/framework/messagequeue|password')
        client.file_write('/etc/rabbitmq/rabbitmq.config', """[
   {{rabbit, [{{tcp_listeners, [{0}]}},
              {{default_user, <<"{1}">>}},
              {{default_pass, <<"{2}">>}},
              {{cluster_partition_handling, autoheal}},
              {{log_levels, [{{connection, warning}}]}},
              {{vm_memory_high_watermark, 0.2}}]}}
].""".format(rabbitmq_port, rabbitmq_login, rabbitmq_password))

        rabbitmq_running, same_process = service_manager.is_rabbitmq_running(client=client)
        if rabbitmq_running is True:
            # Example output of 'list_users' command
            # Listing users ...
            # guest   [administrator]
            # ovs     []
            # ... done.
            users = [user.split('\t')[0] for user in client.run(['rabbitmqctl', 'list_users']).splitlines() if '\t' in user and '[' in user and ']' in user]
            if 'ovs' in users:
                Toolbox.log(logger=logger, messages='Already configured RabbitMQ')
                return
            ServiceFactory.change_service_state(client, 'rabbitmq-server', 'stop', logger)

        client.run(['rabbitmq-server', '-detached'])
        time.sleep(5)

        # Sometimes/At random the rabbitmq server takes longer than 5 seconds to start,
        #  and the next command fails so the best solution is to retry several times
        # Also retry the add_user/set_permissions, and validate the result
        retry = 0
        while retry < 10:
            users = Toolbox.retry_client_run(client=client,
                                             command=['rabbitmqctl', 'list_users'],
                                             logger=logger).splitlines()
            users = [usr.split('\t')[0] for usr in users if '\t' in usr and '[' in usr and ']' in usr]
            logger.debug('Rabbitmq users {0}'.format(users))
            if 'ovs' in users:
                logger.debug('User ovs configured in rabbitmq')
                break

            logger.debug(Toolbox.retry_client_run(client=client,
                                                  command=['rabbitmqctl', 'add_user', rabbitmq_login, rabbitmq_password],
                                                  logger=logger))
            logger.debug(Toolbox.retry_client_run(client=client,
                                                  command=['rabbitmqctl', 'set_permissions', rabbitmq_login, '.*', '.*', '.*'],
                                                  logger=logger))
            retry += 1
            time.sleep(1)
        client.run(['rabbitmqctl', 'stop'])
        time.sleep(5)

    @staticmethod
    def check_rabbitmq_and_enable_ha_mode(client, logger):
        """
        Verify RabbitMQ is running properly and enable HA mode
        :param client: Client on which to check RabbitMQ
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param logger: Logger object used for logging
        :type logger: ovs.extensions.generic.logger.Logger
        :return: None
        """
        service_manager = ServiceFactory.get_manager()
        if not service_manager.has_service('rabbitmq-server', client):
            raise RuntimeError('Service rabbitmq-server has not been added on node {0}'.format(client.ip))
        rabbitmq_running, same_process = service_manager.is_rabbitmq_running(client=client)
        if rabbitmq_running is False or same_process is False:
            ServiceFactory.change_service_state(client, 'rabbitmq-server', 'restart', logger)

        time.sleep(5)
        client.run(['rabbitmqctl', 'set_policy', 'ha-all', '^(volumerouter|ovs_.*)$', '{"ha-mode":"all"}'])

    @staticmethod
    def avahi_installed(client, logger):
        """
        Verify whether Avahi is installed
        :param client: Client on which to check for Avahi
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param logger: Logger object used for logging
        :type logger: ovs.extensions.generic.logger.Logger
        :return: True if Avahi is installed, False otherwise
        :rtype: bool
        """
        installed = client.run(['which', 'avahi-daemon'], allow_nonzero=True)
        if installed == '':
            Toolbox.log(logger=logger, messages='Avahi not installed')
            return False
        else:
            Toolbox.log(logger=logger, messages='Avahi installed')
            return True

    @staticmethod
    def validate_avahi_cluster_name(ip, cluster_name, node_name):
        """
        Validate whether the provided cluster_name is good enough for Avahi
        :param ip: IP address on which to configure Avahi
        :type ip: str
        :param cluster_name: Name of the cluster to configure
        :type cluster_name: str
        :param node_name: Name of the node to set in Avahi
        :type node_name: str
        :return: A boolean indicating a valid name and the actual name or an error message
        :rtype: tuple
        """
        # 'ovs_cl_' is 7 characters, '_<ip>' is max 16 characters
        # Avahi allows 63 characters ==> 63 - 7 - 16 = 40 for '<node_name>_<cluster_name>'
        # So if we allow a node_name of maximum 24 characters, we can always allow a cluster_name of at least 15 characters
        avahi_name = 'ovs_cl_{0}_{1}_{2}'.format(cluster_name, node_name[-24:], ip.replace('.', '_'))
        if len(avahi_name) > 63:
            return False, 'The Avahi cluster name should be limited to {0} characters'.format(63 - (len(avahi_name) - len(cluster_name)))
        return True, avahi_name

    @staticmethod
    def configure_avahi(client, node_name, node_type, logger):
        """
        Configure Avahi
        :param client: Client on which to configure avahi
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param node_name: Name of the node to set in Avahi
        :type node_name: str
        :param node_type: Type of the node ('master' or 'extra')
        :type node_type: str
        :param logger: Logger object used for logging
        :type logger: ovs.extensions.generic.logger.Logger
        :return: None
        """
        valid_avahi = NodeTypeController.validate_avahi_cluster_name(ip=client.ip,
                                                                     cluster_name=Configuration.get('/ovs/framework/cluster_name'),
                                                                     node_name=node_name)
        if valid_avahi[0] is False:
            raise RuntimeError(valid_avahi[1])
        Toolbox.log(logger=logger, messages='Announcing service')
        client.file_write(NodeTypeController.avahi_filename, """<?xml version="1.0" standalone='no'?>
<!--*-nxml-*-->
<!DOCTYPE service-group SYSTEM "avahi-service.dtd">
<!-- $Id$ -->
<service-group>
    <name replace-wildcards="yes">{0}</name>
    <service>
        <type>_ovs_{1}_node._tcp</type>
        <port>443</port>
    </service>
</service-group>""".format(valid_avahi[1], node_type))
        ServiceFactory.change_service_state(client, 'avahi-daemon', 'restart', NodeTypeController._logger)

    @staticmethod
    def add_services(client, node_type, logger):
        """
        Add the services required by the OVS cluster
        :param client: Client on which to add the services
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param node_type: Type of node ('master' or 'extra')
        :type node_type: str
        :param logger: Logger object used for logging
        :type logger: ovs.extensions.generic.logger.Logger
        :return: None
        """
        Toolbox.log(logger=logger, messages='Adding services')
        service_manager = ServiceFactory.get_manager()
        services = {}
        worker_queue = System.get_my_machine_id(client=client)
        if node_type == 'master':
            worker_queue += ',ovs_masters'
            services.update({'memcached': {'MEMCACHE_NODE_IP': client.ip, 'WORKER_QUEUE': worker_queue},
                             'rabbitmq-server': {'MEMCACHE_NODE_IP': client.ip, 'WORKER_QUEUE': worker_queue},
                             'scheduled-tasks': {},
                             'webapp-api': {},
                             'volumerouter-consumer': {}})
        services.update({'workers': {'WORKER_QUEUE': worker_queue},
                         'watcher-framework': {}})

        for service_name, params in services.iteritems():
            if not service_manager.has_service(service_name, client):
                Toolbox.log(logger=logger, messages='Adding service {0}'.format(service_name))
                service_manager.add_service(name=service_name, params=params, client=client)

    @staticmethod
    def retrieve_storagerouter_info_via_host(ip, password):
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
            Toolbox.log(logger=NodeTypeController._logger, messages='Error loading storagerouters: {0}'.format(ex), loglevel='exception', silent=True)
        return storagerouters

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
    def _configure_amqp_to_volumedriver():
        Toolbox.log(logger=NodeTypeController._logger, messages='Update existing vPools')
        login = Configuration.get('/ovs/framework/messagequeue|user')
        password = Configuration.get('/ovs/framework/messagequeue|password')
        protocol = Configuration.get('/ovs/framework/messagequeue|protocol')

        uris = []
        for endpoint in Configuration.get('/ovs/framework/messagequeue|endpoints'):
            uris.append({'amqp_uri': '{0}://{1}:{2}@{3}'.format(protocol, login, password, endpoint)})

        if Configuration.dir_exists('/ovs/vpools'):
            for vpool_guid in Configuration.list('/ovs/vpools'):
                for storagedriver_id in Configuration.list(HOSTS_BASE_PATH.format(vpool_guid)):
                    storagedriver_config = StorageDriverConfiguration(vpool_guid, storagedriver_id)
                    storagedriver_config.event_publisher.events_amqp_routing_key = Configuration.get('/ovs/framework/messagequeue|queues.storagedriver')
                    storagedriver_config.event_publisher.events_amqp_uris = uris
                    storagedriver_config.save()
