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
Module for NodeRemovalController
"""

import os
import re
import sys
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.generic.interactive import Interactive
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import NotAuthenticatedException, SSHClient, TimeOutException, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.nodetype import NodeTypeController
from ovs.log.log_handler import LogHandler


class NodeRemovalController(object):
    """
    This class contains all logic for removing a node from the cluster
    """
    LogHandler.get('extensions', name='ovs_extensions')  # Initiate extensions logger
    _logger = LogHandler.get('lib', name='node-removal')
    _logger.logger.propagate = False

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
        LogHandler.get('extensions', name='ovs_extensions')  # Initiate extensions logger
        from ovs.lib.storagedriver import StorageDriverController
        from ovs.lib.storagerouter import StorageRouterController
        from ovs.dal.lists.storagerouterlist import StorageRouterList

        Toolbox.log(logger=NodeRemovalController._logger, messages='Remove node', boxed=True)
        Toolbox.log(logger=NodeRemovalController._logger, messages='WARNING: Some of these steps may take a very long time, please check the logs for more information\n\n')
        service_manager = ServiceFactory.get_manager()

        ###############
        # VALIDATIONS #
        ###############
        try:
            node_ip = node_ip.strip()
            if not isinstance(node_ip, str):
                raise ValueError('Node IP must be a string')
            if not re.match(SSHClient.IP_REGEX, node_ip):
                raise ValueError('Invalid IP {0} specified'.format(node_ip))

            storage_router_all = sorted(StorageRouterList.get_storagerouters(), key=lambda k: k.name)
            storage_router_masters = StorageRouterList.get_masters()
            storage_router_all_ips = set([storage_router.ip for storage_router in storage_router_all])
            storage_router_master_ips = set([storage_router.ip for storage_router in storage_router_masters])
            storage_router_to_remove = StorageRouterList.get_by_ip(node_ip)
            offline_reasons = {}
            if node_ip not in storage_router_all_ips:
                raise ValueError('Unknown IP specified\nKnown in model:\n - {0}\nSpecified for removal:\n - {1}'.format('\n - '.join(storage_router_all_ips), node_ip))

            if len(storage_router_all_ips) == 1:
                raise RuntimeError("Removing the only node is not possible")

            if node_ip in storage_router_master_ips and len(storage_router_master_ips) == 1:
                raise RuntimeError("Removing the only master node is not possible")

            if System.get_my_storagerouter() == storage_router_to_remove:
                raise RuntimeError('The node to be removed cannot be identical to the node on which the removal is initiated')

            Toolbox.log(logger=NodeRemovalController._logger, messages='Creating SSH connections to remaining master nodes')
            master_ip = None
            ip_client_map = {}
            storage_routers_offline = []
            storage_router_to_remove_online = True
            for storage_router in storage_router_all:
                try:
                    client = SSHClient(storage_router, username='root', timeout=10)
                except (UnableToConnectException, NotAuthenticatedException, TimeOutException) as ex:
                    if isinstance(ex, UnableToConnectException):
                        msg = 'Unable to connect'
                    elif isinstance(ex, NotAuthenticatedException):
                        msg = 'Could not authenticate'
                    elif isinstance(ex, TimeOutException):
                        msg = 'Connection timed out'
                    Toolbox.log(logger=NodeRemovalController._logger, messages='  * Node with IP {0:<15}- {1}'.format(storage_router.ip, msg))
                    offline_reasons[storage_router.ip] = msg
                    storage_routers_offline.append(storage_router)
                    if storage_router == storage_router_to_remove:
                        storage_router_to_remove_online = False
                    continue

                Toolbox.log(logger=NodeRemovalController._logger, messages='  * Node with IP {0:<15}- Successfully connected'.format(storage_router.ip))
                ip_client_map[storage_router.ip] = client
                if storage_router != storage_router_to_remove and storage_router.node_type == 'MASTER':
                    master_ip = storage_router.ip

            if len(ip_client_map) == 0 or master_ip is None:
                raise RuntimeError('Could not connect to any master node in the cluster')

            storage_router_to_remove.invalidate_dynamics('vdisks_guids')
            if len(storage_router_to_remove.vdisks_guids) > 0:  # vDisks are supposed to be moved away manually before removing a node
                raise RuntimeError("Still vDisks attached to Storage Router {0}".format(storage_router_to_remove.name))

            internal_memcached = Toolbox.is_service_internally_managed(service='memcached')
            internal_rabbit_mq = Toolbox.is_service_internally_managed(service='rabbitmq')
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

            Toolbox.run_hooks(component='noderemoval',
                              sub_component='validate_removal',
                              logger=NodeRemovalController._logger,
                              cluster_ip=storage_router_to_remove.ip)
        except KeyboardInterrupt:
            Toolbox.log(logger=NodeRemovalController._logger, messages='\n')
            Toolbox.log(logger=NodeRemovalController._logger,
                        messages='Removal has been aborted during the validation step. No changes have been applied.',
                        boxed=True,
                        loglevel='warning')
            sys.exit(1)
        except Exception as exception:
            Toolbox.log(logger=NodeRemovalController._logger, messages=[str(exception)], boxed=True, loglevel='exception')
            sys.exit(1)

        #################
        # CONFIRMATIONS #
        #################
        try:
            interactive = silent != '--force-yes'
            remove_asd_manager = not interactive  # Remove ASD manager if non-interactive else ask
            if interactive is True:
                if len(storage_routers_offline) > 0:
                    Toolbox.log(logger=NodeRemovalController._logger, messages='Certain nodes appear to be offline. These will not fully removed and will cause issues if they are not really offline.')
                    Toolbox.log(logger=NodeRemovalController._logger, messages='Offline nodes: {0}'.format(''.join(('\n  * {0:<15}- {1}.'.format(ip, message) for ip, message in offline_reasons.iteritems()))))
                    valid_node_info = Interactive.ask_yesno(message='Continue the removal with these being presumably offline?', default_value=False)
                    if valid_node_info is False:
                        Toolbox.log(logger=NodeRemovalController._logger, messages='Please validate the state of the nodes before removing.', title=True)
                        sys.exit(1)
                proceed = Interactive.ask_yesno(message='Are you sure you want to remove node {0}?'.format(storage_router_to_remove.name), default_value=False)
                if proceed is False:
                    Toolbox.log(logger=NodeRemovalController._logger, messages='Abort removal', title=True)
                    sys.exit(1)

                remove_asd_manager = True
                if storage_router_to_remove_online is True:
                    client = SSHClient(endpoint=storage_router_to_remove, username='root')
                    if service_manager.has_service(name='asd-manager', client=client):
                        remove_asd_manager = Interactive.ask_yesno(message='Do you also want to remove the ASD manager and related ASDs?', default_value=False)

                if remove_asd_manager is True or storage_router_to_remove_online is False:
                    for fct in Toolbox.fetch_hooks('noderemoval', 'validate_asd_removal'):
                        validation_output = fct(storage_router_to_remove.ip)
                        if validation_output['confirm'] is True:
                            if Interactive.ask_yesno(message=validation_output['question'], default_value=False) is False:
                                remove_asd_manager = False
                                break
        except KeyboardInterrupt:
            Toolbox.log(logger=NodeRemovalController._logger, messages='\n')
            Toolbox.log(logger=NodeRemovalController._logger,
                        messages='Removal has been aborted during the confirmation step. No changes have been applied.',
                        boxed=True,
                        loglevel='warning')
            sys.exit(1)
        except Exception as exception:
            Toolbox.log(logger=NodeRemovalController._logger, messages=[str(exception)], boxed=True,
                        loglevel='exception')
            sys.exit(1)
        ###########
        # REMOVAL #
        ###########
        try:
            Toolbox.log(logger=NodeRemovalController._logger, messages='Starting removal of node {0} - {1}'.format(storage_router_to_remove.name, storage_router_to_remove.ip))
            if storage_router_to_remove_online is False:
                Toolbox.log(logger=NodeRemovalController._logger, messages='  Marking all Storage Drivers served by Storage Router {0} as offline'.format(storage_router_to_remove.ip))
                StorageDriverController.mark_offline(storagerouter_guid=storage_router_to_remove.guid)

            # Remove vPools
            Toolbox.log(logger=NodeRemovalController._logger, messages='  Removing vPools from node'.format(storage_router_to_remove.ip))
            storage_routers_offline_guids = [sr.guid for sr in storage_routers_offline if sr.guid != storage_router_to_remove.guid]
            for storage_driver in storage_router_to_remove.storagedrivers:
                Toolbox.log(logger=NodeRemovalController._logger, messages='    Removing vPool {0} from node'.format(storage_driver.vpool.name))
                StorageRouterController.remove_storagedriver(storagedriver_guid=storage_driver.guid,
                                                             offline_storage_router_guids=storage_routers_offline_guids)

            # Demote if MASTER
            if storage_router_to_remove.node_type == 'MASTER':
                NodeTypeController.demote_node(cluster_ip=storage_router_to_remove.ip,
                                               master_ip=master_ip,
                                               ip_client_map=ip_client_map,
                                               unique_id=storage_router_to_remove.machine_id,
                                               unconfigure_memcached=internal_memcached,
                                               unconfigure_rabbitmq=internal_rabbit_mq,
                                               offline_nodes=storage_routers_offline)

            # Stop / remove services
            Toolbox.log(logger=NodeRemovalController._logger, messages='Stopping and removing services')
            if storage_router_to_remove_online is True:
                client = SSHClient(endpoint=storage_router_to_remove, username='root')
                NodeRemovalController.remove_services(client=client, node_type=storage_router_to_remove.node_type.lower(), logger=NodeRemovalController._logger)
                service = 'watcher-config'
                if service_manager.has_service(service, client=client):
                    Toolbox.log(logger=NodeRemovalController._logger, messages='Removing service {0}'.format(service))
                    service_manager.stop_service(service, client=client)
                    service_manager.remove_service(service, client=client)

            Toolbox.run_hooks(component='noderemoval',
                              sub_component='remove',
                              logger=NodeRemovalController._logger,
                              cluster_ip=storage_router_to_remove.ip,
                              complete_removal=remove_asd_manager)

            # Clean up model
            Toolbox.log(logger=NodeRemovalController._logger, messages='Removing node from model')
            for service in storage_router_to_remove.services:
                service.delete()
            for disk in storage_router_to_remove.disks:
                for partition in disk.partitions:
                    partition.delete()
                disk.delete()
            for j_domain in storage_router_to_remove.domains:
                j_domain.delete()
            Configuration.delete('/ovs/framework/hosts/{0}'.format(storage_router_to_remove.machine_id))

            NodeTypeController.restart_framework_and_memcache_services(clients=ip_client_map,
                                                                       offline_node_ips=[node.ip for node in storage_routers_offline],
                                                                       logger=NodeRemovalController._logger)

            if storage_router_to_remove_online is True:
                client = SSHClient(endpoint=storage_router_to_remove, username='root')
                client.file_delete(filenames=[Configuration.CACC_LOCATION])
                client.file_delete(filenames=[Configuration.BOOTSTRAP_CONFIG_LOCATION])
            storage_router_to_remove.delete()
            Toolbox.log(logger=NodeRemovalController._logger, messages='Successfully removed node\n')
        except Exception as exception:
            Toolbox.log(logger=NodeRemovalController._logger, messages='\n')
            Toolbox.log(logger=NodeRemovalController._logger, messages=['An unexpected error occurred:', str(exception)], boxed=True, loglevel='exception')
            sys.exit(1)
        except KeyboardInterrupt:
            Toolbox.log(logger=NodeRemovalController._logger, messages='\n')
            Toolbox.log(logger=NodeRemovalController._logger,
                        messages='This setup was aborted. Open vStorage may be in an inconsistent state, make sure to validate the installation.',
                        boxed=True,
                        loglevel='error')
            sys.exit(1)

        if remove_asd_manager is True and storage_router_to_remove_online is True:
            Toolbox.log(logger=NodeRemovalController._logger, messages='\nRemoving ASD Manager')
            with remote(storage_router_to_remove.ip, [os]) as rem:
                rem.os.system('asd-manager remove --force-yes')
        Toolbox.log(logger=NodeRemovalController._logger, messages='Remove nodes finished', title=True)

    @staticmethod
    def remove_services(client, node_type, logger):
        """
        Remove all services managed by OVS
        :param client: Client on which to remove the services
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param node_type: Type of node, can be 'master' or 'extra'
        :type node_type: str
        :param logger: Logger object used for logging
        :type logger: ovs.log.log_handler.LogHandler
        :return: None
        """
        Toolbox.log(logger=logger, messages='Removing services')
        service_manager = ServiceFactory.get_manager()
        stop_only = ['rabbitmq-server', 'memcached']
        services = ['workers', 'support-agent', 'watcher-framework']
        if node_type == 'master':
            services += ['scheduled-tasks', 'webapp-api', 'volumerouter-consumer']
            if Toolbox.is_service_internally_managed(service='rabbitmq') is True:
                services.append('rabbitmq-server')
            if Toolbox.is_service_internally_managed(service='memcached') is True:
                services.append('memcached')

        for service in services:
            if service_manager.has_service(service, client=client):
                Toolbox.log(logger=logger,
                            messages='{0} service {1}'.format('Removing' if service not in stop_only else 'Stopping', service))
                service_manager.stop_service(service, client=client)
                if service not in stop_only:
                    service_manager.remove_service(service, client=client)
