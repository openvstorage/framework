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
Module for UpdateController
"""

import subprocess
from ovs.celery_run import celery
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.filemutex import file_mutex
from ovs.extensions.generic.filemutex import NoLockAvailableException
from ovs.extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.migration.migrator import Migrator
from ovs.extensions.packages.package import PackageManager
from ovs.extensions.services.service import ServiceManager
from ovs.lib.helpers.decorators import add_hooks
from ovs.lib.helpers.toolbox import Toolbox
from ovs.log.log_handler import LogHandler


class UpdateController(object):
    """
    This class contains all logic for updating an environment
    """
    _logger = LogHandler.get('lib', name='update')
    _logger.logger.propagate = False
    model_services = ['memcached', 'arakoon-ovsdb']

    @staticmethod
    @add_hooks('update', 'package_info')
    def get_package_information(client, package_info):
        """
        Retrieve and store the package information for the StorageRouter represented by the client provided
        :param client: Client on which to collect package information
        :type client: SSHClient
        :param package_info: Dictionary passed in by the thread calling this function
        :type package_info: dict
        :return: Package information
        :rtype: dict
        """
        relevant_packages = ['openvstorage-core', 'openvstorage-webapps']
        installed = PackageManager.get_installed_versions(client=client, package_names=relevant_packages)
        candidate = PackageManager.get_candidate_versions(client=client, package_names=relevant_packages)
        if set(installed.keys()) != set(relevant_packages) or set(candidate.keys()) != set(relevant_packages):
            raise RuntimeError('Failed to retrieve the installed and candidate versions for packages: {0}'.format(', '.join(relevant_packages)))

        package_info[client.ip]['framework'].update(dict((package_name, {'installed': installed[package_name], 'candidate': candidate[package_name]}) for package_name in relevant_packages))

    # @staticmethod
    # @add_hooks('update', 'metadata')
    # def get_metadata_framework(client):
    #     """
    #     Retrieve packages and services on which the framework depends
    #     :param client: SSHClient on which to retrieve the metadata
    #     :type client: SSHClient
    #     :return: List of dictionaries which contain services to restart,
    #                                                 packages to update,
    #                                                 information about potential downtime
    #                                                 information about unmet prerequisites
    #     :rtype: list
    #     """
    #     this_sr = StorageRouterList.get_by_ip(client.ip)
    #     srs = StorageRouterList.get_storagerouters()
    #     downtime = []
    #     fwk_cluster_name = Configuration.get('/ovs/framework/arakoon_clusters|ovsdb')
    #     metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=fwk_cluster_name)
    #     if metadata is None:
    #         raise ValueError('Expected exactly 1 arakoon cluster of type {0}, found None'.format(ServiceType.ARAKOON_CLUSTER_TYPES.FWK))
    #
    #     if metadata['internal'] is True:
    #         ovsdb_cluster = [ser.storagerouter_guid for sr in srs for ser in sr.services if ser.type.name == ServiceType.SERVICE_TYPES.ARAKOON and ser.name == 'arakoon-ovsdb']
    #         downtime = [('ovs', 'ovsdb', None)] if len(ovsdb_cluster) < 3 and this_sr.guid in ovsdb_cluster else []
    #
    #     ovs_info = PackageManager.verify_update_required(packages=['openvstorage-core', 'openvstorage-webapps', 'openvstorage-cinder-plugin'],
    #                                                      services=['watcher-framework', 'memcached'],
    #                                                      client=client)
    #     arakoon_info = PackageManager.verify_update_required(packages=['arakoon'],
    #                                                          services=['arakoon-ovsdb'],
    #                                                          client=client)
    #
    #     return {'framework': [{'name': 'ovs',
    #                            'version': ovs_info['version'],
    #                            'services': ovs_info['services'],
    #                            'packages': ovs_info['packages'],
    #                            'downtime': [],
    #                            'namespace': 'ovs',
    #                            'prerequisites': []},
    #                           {'name': 'arakoon',
    #                            'version': arakoon_info['version'],
    #                            'services': arakoon_info['services'],
    #                            'packages': arakoon_info['packages'],
    #                            'downtime': downtime,
    #                            'namespace': 'ovs',
    #                            'prerequisites': []}]}
    #
    # @staticmethod
    # @add_hooks('update', 'metadata')
    # def get_metadata_volumedriver(client):
    #     """
    #     Retrieve packages and services on which the volumedriver depends
    #     :param client: SSHClient on which to retrieve the metadata
    #     :type client: SSHClient
    #     :return: List of dictionaries which contain services to restart,
    #                                                 packages to update,
    #                                                 information about potential downtime
    #                                                 information about unmet prerequisites
    #     :rtype: list
    #     """
    #     srs = StorageRouterList.get_storagerouters()
    #     this_sr = StorageRouterList.get_by_ip(client.ip)
    #     downtime = []
    #     key = '/ovs/framework/arakoon_clusters|voldrv'
    #     if Configuration.exists(key):
    #         sd_cluster_name = Configuration.get(key)
    #         metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=sd_cluster_name)
    #         if metadata is None:
    #             raise ValueError('Expected exactly 1 arakoon cluster of type {0}, found None'.format(ServiceType.ARAKOON_CLUSTER_TYPES.SD))
    #
    #         if metadata['internal'] is True:
    #             voldrv_cluster = [ser.storagerouter_guid for sr in srs for ser in sr.services if ser.type.name == ServiceType.SERVICE_TYPES.ARAKOON and ser.name == 'arakoon-voldrv']
    #             downtime = [('ovs', 'voldrv', None)] if len(voldrv_cluster) < 3 and this_sr.guid in voldrv_cluster else []
    #
    #     alba_proxies = []
    #     alba_downtime = []
    #     for sr in srs:
    #         for service in sr.services:
    #             if service.type.name == ServiceType.SERVICE_TYPES.ALBA_PROXY and service.storagerouter_guid == this_sr.guid:
    #                 alba_proxies.append(service.alba_proxy)
    #                 alba_downtime.append(('ovs', 'proxy', service.alba_proxy.storagedriver.vpool.name))
    #
    #     prerequisites = []
    #     volumedriver_services = ['ovs-volumedriver_{0}'.format(sd.vpool.name)
    #                              for sd in this_sr.storagedrivers]
    #     volumedriver_services.extend(['ovs-dtl_{0}'.format(sd.vpool.name)
    #                                   for sd in this_sr.storagedrivers])
    #     voldrv_info = PackageManager.verify_update_required(packages=['volumedriver-base', 'volumedriver-server',
    #                                                                   'volumedriver-no-dedup-base', 'volumedriver-no-dedup-server'],
    #                                                         services=volumedriver_services,
    #                                                         client=client)
    #     alba_info = PackageManager.verify_update_required(packages=['alba'],
    #                                                       services=[service.service.name for service in alba_proxies],
    #                                                       client=client)
    #     arakoon_info = PackageManager.verify_update_required(packages=['arakoon'],
    #                                                          services=['arakoon-voldrv'],
    #                                                          client=client)
    #
    #     return {'volumedriver': [{'name': 'volumedriver',
    #                               'version': voldrv_info['version'],
    #                               'services': voldrv_info['services'],
    #                               'packages': voldrv_info['packages'],
    #                               'downtime': alba_downtime,
    #                               'namespace': 'ovs',
    #                               'prerequisites': prerequisites},
    #                              {'name': 'alba',
    #                               'version': alba_info['version'],
    #                               'services': alba_info['services'],
    #                               'packages': alba_info['packages'],
    #                               'downtime': alba_downtime,
    #                               'namespace': 'ovs',
    #                               'prerequisites': prerequisites},
    #                              {'name': 'arakoon',
    #                               'version': arakoon_info['version'],
    #                               'services': arakoon_info['services'],
    #                               'packages': arakoon_info['packages'],
    #                               'downtime': downtime,
    #                               'namespace': 'ovs',
    #                               'prerequisites': []}]}
    #
    # @staticmethod
    # @celery.task(name='ovs.storagerouter.update_framework')
    # def execute_update_framework(storagerouter_ip):
    #     """
    #     Launch the update_framework method in update.py
    #     :param storagerouter_ip: IP of the Storage Router to update the framework packages on
    #     :type storagerouter_ip: str
    #     :return: None
    #     """
    #     root_client = SSHClient(storagerouter_ip,
    #                             username='root')
    #     root_client.run('ovs update framework')
    #
    # @staticmethod
    # @celery.task(name='ovs.storagerouter.update_volumedriver')
    # def execute_update_volumedriver(storagerouter_ip):
    #     """
    #     Launch the update_volumedriver method in setup.py
    #     :param storagerouter_ip: IP of the Storage Router to update the volumedriver packages on
    #     :type storagerouter_ip: str
    #     :return: None
    #     """
    #     root_client = SSHClient(storagerouter_ip,
    #                             username='root')
    #     root_client.run('ovs update volumedriver')
    #
    # @staticmethod
    # @celery.task(name='ovs.storagerouter.get_update_status')
    # def get_update_status(storagerouter_ip):
    #     """
    #     Checks for new updates
    #     :param storagerouter_ip: IP of the Storage Router to check for updates
    #     :type storagerouter_ip: str
    #     :return: Update status for specified storage router
    #     :rtype: dict
    #     """
    #     # Check plugin requirements
    #     root_client = SSHClient(storagerouter_ip,
    #                             username='root')
    #     required_plugin_params = {'name': (str, None),             # Name of a subpart of the plugin and is used for translation in html. Eg: alba:packages.SDM
    #                               'version': (str, None),          # Available version to be installed
    #                               'namespace': (str, None),        # Name of the plugin and is used for translation in html. Eg: ALBA:packages.sdm
    #                               'services': (list, str),         # Services which the plugin depends upon and should be stopped during update
    #                               'packages': (list, str),         # Packages which contain the plugin code and should be updated
    #                               'downtime': (list, tuple),       # Information about crucial services which will go down during the update
    #                               'prerequisites': (list, tuple)}  # Information about prerequisites which are unmet (eg running vms for storage driver update)
    #     package_map = {}
    #     plugin_functions = Toolbox.fetch_hooks('update', 'metadata')
    #     for function in plugin_functions:
    #         output = function(root_client)
    #         if not isinstance(output, dict):
    #             raise ValueError('Update cannot continue. Failed to retrieve correct plugin information ({0})'.format(function.func_name))
    #
    #         for key, value in output.iteritems():
    #             for out in value:
    #                 Toolbox.verify_required_params(required_plugin_params, out)
    #             if key not in package_map:
    #                 package_map[key] = []
    #             package_map[key] += value
    #
    #     # Update apt (only our ovs apt repo)
    #     PackageManager.update(client=root_client)
    #
    #     # Compare installed and candidate versions
    #     return_value = {'upgrade_ongoing': os.path.exists('/etc/upgrade_ongoing')}
    #     for gui_name, package_information in package_map.iteritems():
    #         return_value[gui_name] = []
    #         for package_info in package_information:
    #             version = package_info['version']
    #             if version:
    #                 gui_down = 'watcher-framework' in package_info['services'] or 'nginx' in package_info['services']
    #                 info_added = False
    #                 for index, item in enumerate(return_value[gui_name]):
    #                     if item['name'] == package_info['name']:
    #                         return_value[gui_name][index]['downtime'].extend(package_info['downtime'])
    #                         info_added = True
    #                         if gui_down is True and return_value[gui_name][index]['gui_down'] is False:
    #                             return_value[gui_name][index]['gui_down'] = True
    #                 if info_added is False:  # Some plugins can have same package dependencies as core and we only want to show each package once in GUI (Eg: Arakoon for core and ALBA)
    #                     return_value[gui_name].append({'to': version,
    #                                                    'name': package_info['name'],
    #                                                    'gui_down': gui_down,
    #                                                    'downtime': package_info['downtime'],
    #                                                    'namespace': package_info['namespace'],
    #                                                    'prerequisites': package_info['prerequisites']})
    #     return return_value
    #
    # @staticmethod
    # def update_framework():
    #     """
    #     Update the framework
    #     :return: None
    #     """
    #     filemutex = file_mutex('system_update', wait=2)
    #     upgrade_file = '/etc/ready_for_upgrade'
    #     upgrade_ongoing_check_file = '/etc/upgrade_ongoing'
    #     ssh_clients = []
    #     try:
    #         filemutex.acquire()
    #         UpdateController._log_message('+++ Starting framework update +++')
    #
    #         from ovs.dal.lists.storagerouterlist import StorageRouterList
    #
    #         UpdateController._log_message('Generating SSH client connections for each storage router')
    #         upgrade_file = '/etc/ready_for_upgrade'
    #         upgrade_ongoing_check_file = '/etc/upgrade_ongoing'
    #         storage_routers = StorageRouterList.get_storagerouters()
    #         ssh_clients = []
    #         master_ips = []
    #         extra_ips = []
    #         for sr in storage_routers:
    #             ssh_clients.append(SSHClient(sr.ip, username='root'))
    #             if sr.node_type == 'MASTER':
    #                 master_ips.append(sr.ip)
    #             elif sr.node_type == 'EXTRA':
    #                 extra_ips.append(sr.ip)
    #         this_client = [client for client in ssh_clients if client.is_local is True][0]
    #
    #         # Create locks
    #         UpdateController._log_message('Creating lock files', client_ip=this_client.ip)
    #         for client in ssh_clients:
    #             client.run('touch {0}'.format(upgrade_file))  # Prevents manual install or upgrade individual packages
    #             client.run('touch {0}'.format(upgrade_ongoing_check_file))  # Prevents clicking x times on 'Update' btn
    #
    #         # Check requirements
    #         packages_to_update = set()
    #         all_services_to_restart = []
    #         for client in ssh_clients:
    #             for function in Toolbox.fetch_hooks('update', 'metadata'):
    #                 UpdateController._log_message('Executing function {0}'.format(function.__name__),
    #                                               client_ip=client.ip)
    #                 output = function(client)
    #                 for key, value in output.iteritems():
    #                     if key != 'framework':
    #                         continue
    #                     for package_info in value:
    #                         packages_to_update.update(package_info['packages'])
    #                         all_services_to_restart += package_info['services']
    #
    #         services_to_restart = []
    #         for service in all_services_to_restart:
    #             if service not in services_to_restart:
    #                 services_to_restart.append(service)  # Filter out duplicates maintaining the order of services (eg: watcher-framework before memcached)
    #
    #         UpdateController._log_message('Services which will be restarted --> {0}'.format(', '.join(services_to_restart)))
    #         UpdateController._log_message('Packages which will be installed --> {0}'.format(', '.join(packages_to_update)))
    #
    #         # Stop services
    #         if UpdateController._change_services_state(services=services_to_restart,
    #                                                    ssh_clients=ssh_clients,
    #                                                    action='stop') is False:
    #             UpdateController._log_message('Stopping all services on every node failed, cannot continue',
    #                                           client_ip=this_client.ip, severity='warning')
    #             UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
    #
    #             # Start services again if a service could not be stopped
    #             UpdateController._log_message('Attempting to start the services again', client_ip=this_client.ip)
    #             UpdateController._change_services_state(services=services_to_restart,
    #                                                     ssh_clients=ssh_clients,
    #                                                     action='start')
    #
    #             UpdateController._log_message('Failed to stop all required services, aborting update',
    #                                           client_ip=this_client.ip, severity='error')
    #             return
    #
    #         # Update packages
    #         failed_clients = []
    #         for client in ssh_clients:
    #             PackageManager.update(client=client)
    #             try:
    #                 UpdateController._log_message('Installing latest packages', client.ip)
    #                 for package in packages_to_update:
    #                     UpdateController._log_message('Installing {0}'.format(package), client.ip)
    #                     PackageManager.install(package_name=package,
    #                                            client=client,
    #                                            force=True)
    #                     UpdateController._log_message('Installed {0}'.format(package), client.ip)
    #                 client.file_delete(upgrade_file)
    #             except subprocess.CalledProcessError as cpe:
    #                 UpdateController._log_message('Upgrade failed with error: {0}'.format(cpe.output), client.ip,
    #                                               'error')
    #                 failed_clients.append(client)
    #                 break
    #
    #         if failed_clients:
    #             UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
    #             UpdateController._log_message('Error occurred. Attempting to start all services again',
    #                                           client_ip=this_client.ip, severity='error')
    #             UpdateController._change_services_state(services=services_to_restart,
    #                                                     ssh_clients=ssh_clients,
    #                                                     action='start')
    #             UpdateController._log_message('Failed to upgrade following nodes:\n - {0}\nPlease check the logs on {1} for more information'.format('\n - '.join([client.ip for client in failed_clients]), this_client.ip),
    #                                           this_client.ip,
    #                                           'error')
    #             return
    #
    #         # Migrate code
    #         for client in ssh_clients:
    #             try:
    #                 UpdateController._log_message('Started code migration', client.ip)
    #                 try:
    #                     with remote(client.ip, [Migrator]) as rem:
    #                         rem.Migrator.migrate(master_ips, extra_ips)
    #                 except EOFError as eof:
    #                     UpdateController._log_message('EOFError during code migration, retrying {0}'.format(eof), client.ip, 'warning')
    #                     with remote(client.ip, [Migrator]) as rem:
    #                         rem.Migrator.migrate(master_ips, extra_ips)
    #                 UpdateController._log_message('Finished code migration', client.ip)
    #             except Exception as ex:
    #                 UpdateController._remove_lock_files([upgrade_ongoing_check_file], ssh_clients)
    #                 UpdateController._log_message('Code migration failed with error: {0}'.format(ex), client.ip, 'error')
    #                 return
    #
    #         # Start services
    #         UpdateController._log_message('Starting services', client_ip=this_client.ip)
    #         model_services = []
    #         if 'arakoon-ovsdb' in services_to_restart:
    #             model_services.append('arakoon-ovsdb')
    #             services_to_restart.remove('arakoon-ovsdb')
    #         if 'memcached' in services_to_restart:
    #             model_services.append('memcached')
    #             services_to_restart.remove('memcached')
    #         UpdateController._change_services_state(services=model_services,
    #                                                 ssh_clients=ssh_clients,
    #                                                 action='start')
    #
    #         # Migrate model
    #         UpdateController._log_message('Started model migration', client_ip=this_client.ip)
    #         try:
    #             from ovs.dal.helpers import Migration
    #             with remote(ssh_clients[0].ip, [Migration]) as rem:
    #                 rem.Migration.migrate()
    #             UpdateController._log_message('Finished model migration', client_ip=this_client.ip)
    #         except Exception as ex:
    #             UpdateController._remove_lock_files([upgrade_ongoing_check_file], ssh_clients)
    #             UpdateController._log_message('An unexpected error occurred: {0}'.format(ex), client_ip=this_client.ip,
    #                                           severity='error')
    #             return
    #
    #         # Post upgrade actions
    #         UpdateController._log_message('Executing post upgrade actions', client_ip=this_client.ip)
    #         for client in ssh_clients:
    #             with remote(client.ip, [Toolbox, SSHClient]) as rem:
    #                 for function in rem.Toolbox.fetch_hooks('update', 'postupgrade'):
    #                     UpdateController._log_message('Executing action {0}'.format(function.__name__),
    #                                                   client_ip=client.ip)
    #                     try:
    #                         function(rem.SSHClient(client.ip, username='root'))
    #                         UpdateController._log_message('Executing action {0} completed'.format(function.__name__),
    #                                                       client_ip=client.ip)
    #                     except Exception as ex:
    #                         UpdateController._log_message('Post upgrade action failed with error: {0}'.format(ex),
    #                                                       client.ip, 'error')
    #
    #         # Start watcher and restart support-agent
    #         UpdateController._change_services_state(services=services_to_restart,
    #                                                 ssh_clients=ssh_clients,
    #                                                 action='start')
    #         UpdateController._change_services_state(services=['support-agent'],
    #                                                 ssh_clients=ssh_clients,
    #                                                 action='restart')
    #
    #         UpdateController._remove_lock_files([upgrade_ongoing_check_file], ssh_clients)
    #         UpdateController._log_message('+++ Finished updating +++')
    #     except RuntimeError as rte:
    #         UpdateController._log_message('Error during framework update: {0}'.format(rte), severity='error')
    #         UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
    #     except NoLockAvailableException:
    #         UpdateController._log_message('Another framework update is currently in progress!')
    #     except Exception as ex:
    #         UpdateController._log_message('Error during framework update: {0}'.format(ex), severity='error')
    #         UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
    #     finally:
    #         filemutex.release()
    #
    # @staticmethod
    # def update_volumedriver():
    #     """
    #     Update the volumedriver
    #     :return: None
    #     """
    #     filemutex = file_mutex('system_update', wait=2)
    #     upgrade_file = '/etc/ready_for_upgrade'
    #     upgrade_ongoing_check_file = '/etc/upgrade_ongoing'
    #     ssh_clients = []
    #     try:
    #         filemutex.acquire()
    #         UpdateController._log_message('+++ Starting volumedriver update +++')
    #
    #         from ovs.dal.lists.storagerouterlist import StorageRouterList
    #
    #         UpdateController._log_message('Generating SSH client connections for each storage router')
    #         storage_routers = StorageRouterList.get_storagerouters()
    #         ssh_clients = [SSHClient(storage_router.ip, 'root') for storage_router in storage_routers]
    #         this_client = [client for client in ssh_clients if client.is_local is True][0]
    #
    #         # Commence update !!!!!!!
    #         # 0. Create locks
    #         UpdateController._log_message('Creating lock files', client_ip=this_client.ip)
    #         for client in ssh_clients:
    #             client.run('touch {0}'.format(upgrade_file))  # Prevents manual install or upgrade individual packages
    #             client.run('touch {0}'.format(upgrade_ongoing_check_file))  # Prevents clicking x times on 'Update' btn
    #
    #         # 1. Check requirements
    #         packages_to_update = set()
    #         all_services_to_restart = []
    #         for client in ssh_clients:
    #             for function in Toolbox.fetch_hooks('update', 'metadata'):
    #                 UpdateController._log_message('Executing function {0}'.format(function.__name__),
    #                                               client_ip=client.ip)
    #                 output = function(client)
    #                 for key, value in output.iteritems():
    #                     if key != 'volumedriver':
    #                         continue
    #                     for package_info in value:
    #                         packages_to_update.update(package_info['packages'])
    #                         all_services_to_restart += package_info['services']
    #
    #         services_to_restart = []
    #         for service in all_services_to_restart:
    #             if service not in services_to_restart:
    #                 services_to_restart.append(service)  # Filter out duplicates keeping the order of services (eg: watcher-framework before memcached)
    #
    #         UpdateController._log_message('Services which will be restarted --> {0}'.format(', '.join(services_to_restart)))
    #         UpdateController._log_message('Packages which will be installed --> {0}'.format(', '.join(packages_to_update)))
    #
    #         # 1. Stop services
    #         if UpdateController._change_services_state(services=services_to_restart,
    #                                                    ssh_clients=ssh_clients,
    #                                                    action='stop') is False:
    #             UpdateController._log_message('Stopping all services on every node failed, cannot continue',
    #                                           client_ip=this_client.ip, severity='warning')
    #             UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
    #
    #             UpdateController._log_message('Attempting to start the services again', client_ip=this_client.ip)
    #             UpdateController._change_services_state(services=services_to_restart,
    #                                                     ssh_clients=ssh_clients,
    #                                                     action='start')
    #             UpdateController._log_message('Failed to stop all required services, update aborted',
    #                                           client_ip=this_client.ip, severity='error')
    #             return
    #
    #         # 2. Update packages
    #         failed_clients = []
    #         for client in ssh_clients:
    #             PackageManager.update(client=client)
    #             try:
    #                 for package_name in packages_to_update:
    #                     UpdateController._log_message('Installing {0}'.format(package_name), client.ip)
    #                     PackageManager.install(package_name=package_name,
    #                                            client=client,
    #                                            force=True)
    #                     UpdateController._log_message('Installed {0}'.format(package_name), client.ip)
    #                 client.file_delete(upgrade_file)
    #             except subprocess.CalledProcessError as cpe:
    #                 UpdateController._log_message('Upgrade failed with error: {0}'.format(cpe.output), client.ip,
    #                                               'error')
    #                 failed_clients.append(client)
    #                 break
    #
    #         if failed_clients:
    #             UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
    #             UpdateController._log_message('Error occurred. Attempting to start all services again',
    #                                           client_ip=this_client.ip, severity='error')
    #             UpdateController._change_services_state(services=services_to_restart,
    #                                                     ssh_clients=ssh_clients,
    #                                                     action='start')
    #             UpdateController._log_message('Failed to upgrade following nodes:\n - {0}\nPlease check /var/log/ovs/lib.log on {1} for more information'.format('\n - '.join([client.ip for client in failed_clients]), this_client.ip),
    #                                           this_client.ip,
    #                                           'error')
    #             return
    #
    #         # 3. Post upgrade actions
    #         UpdateController._log_message('Executing post upgrade actions', client_ip=this_client.ip)
    #         for client in ssh_clients:
    #             for function in Toolbox.fetch_hooks('update', 'postupgrade'):
    #                 UpdateController._log_message('Executing action: {0}'.format(function.__name__), client_ip=client.ip)
    #                 try:
    #                     function(client)
    #                 except Exception as ex:
    #                     UpdateController._log_message('Post upgrade action failed with error: {0}'.format(ex),
    #                                                   client.ip, 'error')
    #
    #         # 4. Start services
    #         UpdateController._log_message('Starting services', client_ip=this_client.ip)
    #         UpdateController._change_services_state(services=services_to_restart,
    #                                                 ssh_clients=ssh_clients,
    #                                                 action='start')
    #
    #         UpdateController._remove_lock_files([upgrade_ongoing_check_file], ssh_clients)
    #         UpdateController._log_message('+++ Finished updating +++')
    #     except RuntimeError as rte:
    #         UpdateController._log_message('Error during volumedriver update: {0}'.format(rte), severity='error')
    #         UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
    #     except NoLockAvailableException:
    #         UpdateController._log_message('Another volumedriver update is currently in progress!')
    #     except Exception as ex:
    #         UpdateController._log_message('Error during volumedriver update: {0}'.format(ex), severity='error')
    #         UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
    #     finally:
    #         filemutex.release()
    #
    # @staticmethod
    # def _log_message(message, client_ip=None, severity='info'):
    #     if client_ip is not None:
    #         message = '{0:<15}: {1}'.format(client_ip, message)
    #     if severity == 'info':
    #         UpdateController._logger.info(message, print_msg=True)
    #     elif severity == 'warning':
    #         UpdateController._logger.warning(message, print_msg=True)
    #     elif severity == 'error':
    #         UpdateController._logger.error(message, print_msg=True)
    #
    # @staticmethod
    # def _remove_lock_files(files, ssh_clients):
    #     for ssh_client in ssh_clients:
    #         for file_name in files:
    #             if ssh_client.file_exists(file_name):
    #                 ssh_client.file_delete(file_name)
    #
    # @staticmethod
    # def _change_services_state(services, ssh_clients, action):
    #     """
    #     Stop/start services on SSH clients
    #     If action is start, we ignore errors and try to start other services on other nodes
    #     """
    #     if action == 'start':
    #         services.reverse()  # Start services again in reverse order of stopping
    #     for service_name in services:
    #         for ssh_client in ssh_clients:
    #             description = 'stopping' if action == 'stop' else 'starting' if action == 'start' else 'restarting'
    #             try:
    #                 if ServiceManager.has_service(service_name, client=ssh_client):
    #                     UpdateController._log_message('{0} service {1}'.format(description.capitalize(), service_name),
    #                                                   ssh_client.ip)
    #                     Toolbox.change_service_state(client=ssh_client,
    #                                                  name=service_name,
    #                                                  state=action,
    #                                                  logger=UpdateController._logger)
    #                     UpdateController._log_message('{0} service {1}'.format('Stopped' if action == 'stop' else 'Started' if action == 'start' else 'Restarted', service_name), ssh_client.ip)
    #             except Exception as exc:
    #                 UpdateController._log_message('Something went wrong {0} service {1}: {2}'.format(description, service_name, exc), ssh_client.ip, severity='warning')
    #                 if action == 'stop':
    #                     return False
    #     return True
