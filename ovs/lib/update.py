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

import time
from subprocess import CalledProcessError
from ovs.celery_run import celery
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonClusterConfig, ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.filemutex import file_mutex
from ovs.extensions.generic.filemutex import NoLockAvailableException
from ovs.extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.toolbox import Toolbox as ExtensionToolbox
from ovs.extensions.generic.system import System
from ovs.extensions.migration.migrator import Migrator
from ovs.extensions.packages.package import PackageManager
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.lib.helpers.decorators import add_hooks
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.scheduledtask import ScheduledTaskController
from ovs.log.log_handler import LogHandler


class UpdateController(object):
    """
    This class contains all logic for updating an environment
    """
    _logger = LogHandler.get('lib', name='update')
    _logger.logger.propagate = False
    framework_packages = {'arakoon', 'openvstorage'}
    volumedriver_packages = {'alba', 'arakoon', 'volumedriver-no-dedup-base', 'volumedriver-no-dedup-server'}
    all_core_packages = framework_packages.union(volumedriver_packages)

    #########
    # HOOKS #
    #########
    @staticmethod
    @add_hooks('update', 'get_package_info_multi')
    def get_package_information_core(client, package_info):
        """
        Retrieve information about the currently installed versions of the core packages
        Retrieve information about the versions to which each package can potentially be upgraded
        If installed version is different from candidate version --> store this information in model

        Additionally if installed version is identical to candidate version, check the services with a 'run' file
        Verify whether the running version is identical to the candidate version
        If different --> store this information in the model

        Result: Every package with updates or which requires services to be restarted is stored in the model

        :param client: Client on which to collect the version information
        :type client: SSHClient
        :param package_info: Dictionary passed in by the thread calling this function
        :type package_info: dict
        :return: Package information
        :rtype: dict
        """
        try:
            if client.username != 'root':
                raise RuntimeError('Only the "root" user can retrieve the package information')

            installed = PackageManager.get_installed_versions(client=client, package_names=UpdateController.all_core_packages)
            candidate = PackageManager.get_candidate_versions(client=client, package_names=UpdateController.all_core_packages)
            if set(installed.keys()) != set(UpdateController.all_core_packages) or set(candidate.keys()) != set(UpdateController.all_core_packages):
                raise RuntimeError('Failed to retrieve the installed and candidate versions for packages: {0}'.format(', '.join(UpdateController.all_core_packages)))

            storagerouter = StorageRouterList.get_by_ip(client.ip)
            alba_proxies = []
            for service in storagerouter.services:
                if service.type.name == ServiceType.SERVICE_TYPES.ALBA_PROXY:
                    alba_proxies.append(service.name)

            #                       component:   package_name: services_with_run_file
            for component, info in {'framework': {'arakoon': ['arakoon-ovsdb'],
                                                  'openvstorage': []},
                                    'storagedriver': {'alba': alba_proxies,
                                                      'arakoon': ['arakoon-voldrv'],
                                                      'volumedriver-no-dedup-base': [],
                                                      'volumedriver-no-dedup-server': []}}.iteritems():
                packages = []
                for package_name, services in info.iteritems():
                    old = installed[package_name]
                    new = candidate[package_name]
                    if old != new:
                        packages.append({'name': package_name,
                                         'installed': old,
                                         'candidate': new,
                                         'namespace': 'ovs',  # Namespace refers to json translation file: ovs.json
                                         'services_to_restart': []})
                    else:
                        services_to_restart = UpdateController.get_running_service_info(client=client,
                                                                                        services=dict((service_name, new) for service_name in services))
                        if len(services_to_restart) > 0:
                            packages.append({'name': package_name,
                                             'installed': services_to_restart.values()[0],
                                             'candidate': new,
                                             'namespace': 'ovs',
                                             'services_to_restart': services_to_restart})
                package_info[client.ip][component].extend(packages)
        except Exception as ex:
            if 'errors' not in package_info[client.ip]:
                package_info[client.ip]['errors'] = []
            package_info[client.ip]['errors'].append(ex)
        return package_info

    @staticmethod
    @add_hooks('update', 'information')
    def get_update_information_core(information):
        """
        Retrieve the update information for all StorageRouters for the core packages
        """
        # Verify arakoon downtime
        arakoon_ovs_down = False
        arakoon_voldrv_down = False
        for cluster in ['ovsdb', 'voldrv']:
            cluster_name = ArakoonClusterConfig.get_cluster_name(cluster)
            if cluster_name is None:
                continue

            arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)
            if arakoon_metadata['internal'] is True:
                config = ArakoonClusterConfig(cluster_id=cluster_name, filesystem=False)
                config.load_config()
                if cluster == 'ovsdb':
                    arakoon_ovs_down = len(config.nodes) < 3
                else:
                    arakoon_voldrv_down = len(config.nodes) < 3

        # Verify StorageRouter downtime
        prerequisites = []
        all_storagerouters = StorageRouterList.get_storagerouters()
        for storagerouter in all_storagerouters:
            try:
                SSHClient(endpoint=storagerouter, username='root')
            except UnableToConnectException:
                prerequisites.append(['node_down', storagerouter.name])

        for key in ['framework', 'storagedriver']:
            if key not in information:
                information[key] = {'packages': [],
                                    'downtime': [],
                                    'prerequisites': prerequisites,
                                    'services_stop_start': set(),
                                    'services_post_update': set()}

            for storagerouter in all_storagerouters:
                if key not in storagerouter.package_information:
                    continue

                # Retrieve ALBA proxy issues
                alba_services = []
                alba_downtime = []
                for service in storagerouter.services:
                    if service.type.name != ServiceType.SERVICE_TYPES.ALBA_PROXY or service.alba_proxy is None:
                        continue
                    alba_services.append(service.name)
                    alba_downtime.append(['proxy', service.alba_proxy.storagedriver.vpool.name])

                # Retrieve StorageDriver issues
                storagedriver_downtime = []
                storagedriver_services = []
                for sd in storagerouter.storagedrivers:
                    # Order of services is important, first we want to stop all volume-drivers, then DTLs
                    storagedriver_services.append('ovs-volumedriver_{0}'.format(sd.vpool.name))
                for sd in storagerouter.storagedrivers:
                    storagedriver_services.append('ovs-dtl_{0}'.format(sd.vpool.name))
                    if len(sd.vdisks_guids) > 0:
                        storagedriver_downtime.append(['voldrv', sd.vpool.name])

                for package_info in storagerouter.package_information[key]:
                    package_name = package_info['name']
                    covered_packages = [pkg['name'] for pkg in information[key]['packages']]
                    if package_name not in UpdateController.all_core_packages:
                        continue  # Only gather information for the core packages
                    if package_name != 'arakoon' and package_name in covered_packages:
                        continue  # Current package is already required for update by another StorageRouter

                    services_to_restart = package_info.pop('services_to_restart')
                    information[key]['services_post_update'].update(services_to_restart)
                    if package_name not in covered_packages and len(services_to_restart) == 0:  # Services to restart is only populated when installed version == candidate version, but some services require a restart
                        information[key]['packages'].append(package_info)

                    if package_name == 'openvstorage':
                        information[key]['downtime'].append(['gui', None])
                        information[key]['services_stop_start'].update({'watcher-framework', 'memcached'})
                    elif package_name == 'alba':
                        information[key]['downtime'].extend(alba_downtime)
                        information[key]['services_post_update'].update(alba_services)
                    elif package_name == 'volumedriver-no-dedup-base':
                        information[key]['downtime'].extend(storagedriver_downtime)
                        information[key]['services_stop_start'].update(storagedriver_services)
                    elif package_name == 'volumedriver-no-dedup-server':
                        information[key]['downtime'].extend(storagedriver_downtime)
                        information[key]['services_stop_start'].update(storagedriver_services)
                    elif package_name == 'arakoon':
                        if key == 'framework':
                            information[key]['services_post_update'].update({'ovs-arakoon-{0}'.format(ArakoonClusterConfig.get_cluster_name('ovsdb'))})
                            if arakoon_ovs_down is True:
                                information[key]['downtime'].append(['ovsdb', None])
                        else:
                            cluster_name = ArakoonClusterConfig.get_cluster_name('voldrv')
                            if cluster_name is not None:
                                information[key]['services_post_update'].update({'ovs-arakoon-{0}'.format(cluster_name)})
                                if arakoon_voldrv_down is True:
                                    information[key]['downtime'].append(['voldrv', None])
        return information

    @staticmethod
    @add_hooks('update', 'package_install_multi')
    def package_install_core(client, package_names):
        """
        Update the core packages
        :param client: Client on which to execute update the packages
        :type client: SSHClient
        :param package_names: Packages to install
        :type package_names: list
        :return: None
        """
        for package_name in package_names:
            if package_name in UpdateController.all_core_packages:
                PackageManager.install(package_name=package_name, client=client)

    @staticmethod
    @add_hooks('update', 'post_update_multi')
    def post_update_core(client, components):
        """
        Execute functionality after the openvstorage core packages have been updated
        For framework:
            * Restart support-agent on every client
            * Restart arakoon-ovsdb on every client (if present and required)
        For storagedriver:
            * ALBA proxies on every client
            * Restart arakoon-voldrv on every client (if present and required)
        :param client: Client on which to execute this post update functionality
        :type client: SSHClient
        :param components: Update components which have been executed
        :type components: list
        :return: None
        """
        update_information = UpdateController.get_update_information_core({})
        for component in components:
            UpdateController._log_message('Executing post update code for component: "{0}"'.format(component.capitalize()), client_ip=client.ip)
            if component == 'framework':
                UpdateController.change_services_state(services=['support-agent'], ssh_clients=[client], action='restart')

            services_to_restart = update_information.get(component, {}).get('services_post_update', set())
            for service_name in services_to_restart:
                if not service_name.startswith('ovs-arakoon-'):
                    UpdateController.change_services_state(services=[service_name], ssh_clients=[client], action='restart')
                else:
                    cluster_name = ArakoonClusterConfig.get_cluster_name(ExtensionToolbox.remove_prefix(service_name, 'ovs-arakoon-'))
                    arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)
                    if arakoon_metadata['internal'] is True:
                        UpdateController._log_message('Restarting arakoon node {0}'.format(cluster_name), client_ip=client.ip)
                        ArakoonInstaller.restart_node(cluster_name=cluster_name,
                                                      client=client)

    ################
    # CELERY TASKS #
    ################
    @staticmethod
    @celery.task(name='ovs.update.merge_package_information')
    def merge_package_information():
        """
        Retrieve the package information from the model for both StorageRouters and ALBA Nodes and merge it
        :return: Package information for all StorageRouters and ALBA nodes
        :rtype: dict
        """
        package_info = {}
        for storagerouter in StorageRouterList.get_storagerouters():
            package_info[storagerouter.ip] = storagerouter.package_information
        for function in Toolbox.fetch_hooks('update', 'merge_package_info'):
            output = function()
            for ip in output:
                if ip in package_info:
                    package_info[ip].update(output[ip])
                else:
                    package_info[ip] = output[ip]
        return package_info

    @staticmethod
    @celery.task(name='ovs.update.get_update_metadata')
    def get_update_metadata(storagerouter_ip):
        """
        Returns metadata required for updating
          - Checks if 'at' is installed properly
          - Checks if ongoing updates are busy
          - Check if StorageRouter is reachable
        :param storagerouter_ip: IP of the Storage Router to check the metadata for
        :type storagerouter_ip: str
        :return: Update status for specified storage router
        :rtype: dict
        """
        at_ok = True
        reachable = True
        root_client = None
        update_ongoing = False
        try:
            root_client = SSHClient(endpoint=storagerouter_ip, username='root')
            update_ongoing = root_client.file_exists(filename='/etc/update_ongoing')
            root_client.run(['which',  'at'])
            root_client.run('echo "echo test > /tmp/test_at_2" > /tmp/test_at_1', allow_insecure=True)
            root_client.run(['at', '-f', '/tmp/test_at_1', 'now'])
            counter = 0
            while counter < 10:
                if root_client.file_exists('/tmp/test_at_2'):
                    at_ok = True
                    if root_client.file_read('/tmp/test_at_2').strip() != 'test':
                        at_ok = False
                    break
                at_ok = False
                time.sleep(0.1)
                counter += 1
        except UnableToConnectException:
            UpdateController._logger.warning('StorageRouter with IP {0} could not be checked'.format(storagerouter_ip))
            reachable = False
        except CalledProcessError:
            UpdateController._logger.exception('Verifying "at" dependency on StorageRouter with IP {0} failed'.format(storagerouter_ip))
            at_ok = False
        finally:
            if root_client is not None:
                root_client.file_delete(['/tmp/test_at_2', '/tmp/test_at_1'])

        return {'at_ok': at_ok,
                'reachable': reachable,
                'update_ongoing': update_ongoing}

    @staticmethod
    @celery.task(name='ovs.update.get_update_information')
    def get_update_information_all():
        """
        Retrieve the update information for all StorageRouters
        This contains information about
            - downtime of model, GUI, vPools, proxies, ...
            - services that will be restarted
            - packages that will be updated
            - prerequisites that have not been met
        :return: Information about the update
        :rtype: dict
        """
        information = {}
        for function in Toolbox.fetch_hooks('update', 'information'):
            function(information=information)
        return information

    @staticmethod
    @celery.task(name='ovs.update.update_all')
    def update_all(components):
        """
        Initiate the update through commandline for all StorageRouters
        This is called upon by the API
        :return: None
        """
        components = [component.strip() for component in components]
        root_client = SSHClient(endpoint=System.get_my_storagerouter(),
                                username='root')
        root_client.run(['ovs', 'update', ','.join(components)])

    #############
    # FUNCTIONS #
    #############
    @staticmethod
    def execute_update(components):
        """
        Update the specified components on all StorageRouters
        This is called upon by 'at'
        :return: None
        """
        filemutex = file_mutex('system_update', wait=2)
        update_file = '/etc/ready_for_update'
        # @TODO: Remove me
        upgrade_file = '/etc/ready_for_upgrade'
        update_ongoing_file = '/etc/update_ongoing'
        ssh_clients = []
        try:
            filemutex.acquire()
            UpdateController._log_message('+++ Starting update +++')

            from ovs.dal.lists.storagerouterlist import StorageRouterList

            # Create SSHClients to all nodes
            UpdateController._log_message('Generating SSH client connections for each storage router')
            storage_routers = StorageRouterList.get_storagerouters()
            ssh_clients = []
            master_ips = []
            extra_ips = []
            for sr in storage_routers:
                try:
                    ssh_clients.append(SSHClient(sr.ip, username='root'))
                    if sr.node_type == 'MASTER':
                        master_ips.append(sr.ip)
                    elif sr.node_type == 'EXTRA':
                        extra_ips.append(sr.ip)
                except UnableToConnectException:
                    raise Exception('Update is only allowed on systems where all nodes are online and fully functional')

            # Check requirements
            packages_to_update = set()
            services_stop_start = set()
            services_post_update = set()
            update_information = UpdateController.get_update_information_all()
            for component, component_info in update_information.iteritems():
                if component in components:
                    UpdateController._log_message('Verifying update information for component: {0}'.format(component.upper()))
                    Toolbox.verify_required_params(actual_params=component_info,
                                                   required_params={'downtime': (list, None),
                                                                    'packages': (list, {'name': (str, None),
                                                                                        'candidate': (str, None),
                                                                                        'installed': (str, None),
                                                                                        'namespace': (str, None)}),
                                                                    'prerequisites': (list, None),
                                                                    'services_stop_start': (set, None),
                                                                    'services_post_update': (set, None)})
                    if len(component_info['prerequisites']) > 0:
                        raise Exception('Update is only allowed when all prerequisites have been met')

                    packages_to_update.update([pkg['name'] for pkg in component_info['packages']])
                    services_stop_start.update(component_info['services_stop_start'])
                    services_post_update.update(component_info['services_post_update'])
            if len(packages_to_update) > 0:
                UpdateController._log_message('Packages to be updated: {0}'.format(', '.join(sorted(packages_to_update))))
            if len(services_stop_start) > 0:
                UpdateController._log_message('Services to stop before package update: {0}'.format(', '.join(sorted(services_stop_start))))
            if len(services_post_update) > 0:
                UpdateController._log_message('Services which will be restarted after update: {0}'.format(', '.join(sorted(services_post_update))))

            # Create locks
            for client in ssh_clients:
                UpdateController._log_message('Creating lock files', client_ip=client.ip)
                client.run(['touch', update_file])  # Prevents manual install or update individual packages
                client.run(['touch', upgrade_file])
                client.run(['touch', update_ongoing_file])  # Prevents clicking x times on 'Update' btn

            # Stop services
            if UpdateController.change_services_state(services=services_stop_start,
                                                      ssh_clients=ssh_clients,
                                                      action='stop') is False:
                UpdateController._log_message('Stopping all services on every node failed, cannot continue', severity='warning')
                UpdateController._remove_lock_files([update_file, update_ongoing_file, upgrade_file], ssh_clients)

                # Start services again if a service could not be stopped
                UpdateController._log_message('Attempting to start the services again')
                UpdateController.change_services_state(services=services_stop_start,
                                                       ssh_clients=ssh_clients,
                                                       action='start')

                UpdateController._log_message('Failed to stop all required services, aborting update', severity='error')
                return

            # Install packages
            # First install packages on all StorageRouters individually
            if packages_to_update:
                failures = False
                for client in ssh_clients:
                    UpdateController._log_message('Installing packages', client_ip=client.ip)
                    for function in Toolbox.fetch_hooks('update', 'package_install_multi'):
                        try:
                            UpdateController._log_message('Executing hook {0}'.format(function.__name__), client_ip=client.ip)
                            function(client=client, package_names=packages_to_update)
                            UpdateController._log_message('Executed hook {0}'.format(function.__name__), client_ip=client.ip)
                        except Exception as ex:
                            UpdateController._log_message('Package installation hook {0} failed with error: {1}'.format(function.__name__, ex), client.ip, 'error')
                            failures = True

                # Second install packages on all ALBA nodes
                for function in Toolbox.fetch_hooks('update', 'package_install_single'):
                    try:
                        UpdateController._log_message('Executing hook {0}'.format(function.__name__))
                        function(package_names=packages_to_update)
                        UpdateController._log_message('Executed hook {0}'.format(function.__name__))
                    except Exception as ex:
                        UpdateController._log_message('Package installation hook {0} failed with error: {1}'.format(function.__name__, ex), severity='error')
                        failures = True

                if failures is True:
                    UpdateController._remove_lock_files([update_file, update_ongoing_file, upgrade_file], ssh_clients)
                    UpdateController._log_message('Error occurred. Attempting to start all services again', severity='error')
                    UpdateController.change_services_state(services=services_stop_start,
                                                           ssh_clients=ssh_clients,
                                                           action='start')
                    UpdateController._log_message('Failed to update. Please check all the logs for more information', severity='error')
                    return

            # Remove update file
            for client in ssh_clients:
                client.file_delete([update_file, upgrade_file])

            # Migrate code
            if 'framework' in components:
                for client in ssh_clients:
                    UpdateController._log_message('Verifying extensions code migration is required', client.ip)
                    try:
                        key = '/ovs/framework/hosts/{0}/versions'.format(System.get_my_machine_id(client=client))
                        old_versions = Configuration.get(key) if Configuration.exists(key) else {}
                        try:
                            with remote(client.ip, [Migrator]) as rem:
                                rem.Migrator.migrate(master_ips, extra_ips)
                        except EOFError as eof:
                            UpdateController._log_message('EOFError during code migration, retrying {0}'.format(eof), client.ip, 'warning')
                            with remote(client.ip, [Migrator]) as rem:
                                rem.Migrator.migrate(master_ips, extra_ips)
                        new_versions = Configuration.get(key) if Configuration.exists(key) else {}
                        if old_versions != new_versions:
                            UpdateController._log_message('Finished extensions code migration. Old versions: {0} --> New versions: {1}'.format(old_versions, new_versions), client.ip)
                    except Exception as ex:
                        UpdateController._remove_lock_files([update_ongoing_file], ssh_clients)
                        UpdateController._log_message('Code migration failed with error: {0}'.format(ex), client.ip, 'error')
                        UpdateController.change_services_state(services=services_stop_start,
                                                               ssh_clients=ssh_clients,
                                                               action='start')
                        UpdateController._log_message('Failed to update. Please check all the logs for more information', severity='error')
                        return

            # Start memcached
            if 'memcached' in services_stop_start:
                services_stop_start.remove('memcached')
                UpdateController._log_message('Starting memcached')
                UpdateController.change_services_state(services=['memcached'],
                                                       ssh_clients=ssh_clients,
                                                       action='start')

            # Migrate model
            if 'framework' in components:
                UpdateController._log_message('Verifying DAL code migration is required')
                try:
                    old_versions = PersistentFactory.get_client().get('ovs_model_version') if PersistentFactory.get_client().exists('ovs_model_version') else {}

                    from ovs.dal.helpers import Migration
                    with remote(ssh_clients[0].ip, [Migration]) as rem:
                        rem.Migration.migrate()
                except Exception as ex:
                    UpdateController._remove_lock_files([update_ongoing_file], ssh_clients)
                    UpdateController._log_message('An unexpected error occurred: {0}'.format(ex), severity='error')
                    UpdateController.change_services_state(services=services_stop_start,
                                                           ssh_clients=ssh_clients,
                                                           action='start')
                    UpdateController._log_message('Failed to update. Please check all the logs for more information', severity='error')
                    return
                new_versions = PersistentFactory.get_client().get('ovs_model_version') if PersistentFactory.get_client().exists('ovs_model_version') else {}
                if old_versions != new_versions:
                    UpdateController._log_message('Finished DAL code migration. Old versions: {0} --> New versions: {1}'.format(old_versions, new_versions))

            # Post update actions
            for client in ssh_clients:
                UpdateController._log_message('Executing post-update actions', client_ip=client.ip)
                for function in Toolbox.fetch_hooks('update', 'post_update_multi'):
                    try:
                        UpdateController._log_message('Executing hook {0}'.format(function.__name__), client_ip=client.ip)
                        function(client=client, components=components)
                        UpdateController._log_message('Executed hook {0}'.format(function.__name__), client_ip=client.ip)
                    except Exception as ex:
                        UpdateController._log_message('Post update hook {0} failed with error: {1}'.format(function.__name__, ex), client.ip, 'error')

            for function in Toolbox.fetch_hooks('update', 'post_update_single'):
                try:
                    UpdateController._log_message('Executing hook {0}'.format(function.__name__))
                    function(components=components)
                    UpdateController._log_message('Executed hook {0}'.format(function.__name__))
                except Exception as ex:
                    UpdateController._log_message('Post update hook {0} failed with error: {1}'.format(function.__name__, ex), severity='error')

            # Start services
            UpdateController.change_services_state(services=services_stop_start,
                                                   ssh_clients=ssh_clients,
                                                   action='start')

            # Refresh updates
            UpdateController._log_message('Refreshing package information')
            counter = 1
            while counter < 6:
                try:
                    ScheduledTaskController.refresh_package_information()
                    break
                except NoLockAvailableException:
                    UpdateController._log_message('Attempt {0}: Could not refresh the update information, trying again'.format(counter))
                    time.sleep(6)  # Wait 30 seconds max in total
                counter += 1
                if counter == 6:
                    raise Exception('Could not refresh the update information')
            UpdateController._remove_lock_files([update_ongoing_file], ssh_clients)
            UpdateController._log_message('+++ Finished updating +++')
        except NoLockAvailableException:
            UpdateController._log_message('Another update is currently in progress!')
        except Exception as ex:
            UpdateController._log_message('Error during update: {0}'.format(ex), severity='error')
            UpdateController._remove_lock_files([update_file, update_ongoing_file, upgrade_file], ssh_clients)
        finally:
            filemutex.release()

    @staticmethod
    def get_running_service_info(client, services, component='OpenvStorage'):
        """
        Compare the running version of the specified services with the version provided, return the service name if versions are different
        :param client: Client on which to check the running version of the specified services
        :type client: SSHClient
        :param services: Information about the service name and the expected version
        :type services: dict
        :param component: Component to check
        :type component: str
        :return: Information about current running version for services which are running an unexpected version
        :rtype: dict
        """
        services_to_restart = {}
        for service, expected_version in services.iteritems():
            version_file = '/opt/{0}/run/{1}.version'.format(component, service)
            if client.file_exists(version_file):
                running_version = client.file_read(version_file).strip()
                if running_version != expected_version:
                    services_to_restart[service] = running_version
        return services_to_restart

    @staticmethod
    def change_services_state(services, ssh_clients, action):
        """
        Stop/start services on SSH clients
        If action is start, we ignore errors and try to start other services on other nodes
        """
        services = list(services)
        if action == 'start':
            services.reverse()  # Start services again in reverse order of stopping
        for service_name in services:
            for ssh_client in ssh_clients:
                description = 'stopping' if action == 'stop' else 'starting' if action == 'start' else 'restarting'
                try:
                    if ServiceManager.has_service(service_name, client=ssh_client):
                        Toolbox.change_service_state(client=ssh_client,
                                                     name=service_name,
                                                     state=action,
                                                     logger=UpdateController._logger)
                except Exception as exc:
                    UpdateController._log_message('Something went wrong {0} service {1}: {2}'.format(description, service_name, exc), ssh_client.ip, severity='warning')
                    if action == 'stop':
                        return False
        return True

    ###########
    # HELPERS #
    ###########
    @staticmethod
    def _log_message(message, client_ip=None, severity='debug'):
        if client_ip is not None:
            message = '{0}: {1}'.format(client_ip, message)
        if severity == 'info':
            UpdateController._logger.info(message, print_msg=True)
        elif severity == 'warning':
            UpdateController._logger.warning(message, print_msg=True)
        elif severity == 'error':
            UpdateController._logger.error(message, print_msg=True)
        elif severity == 'debug':
            UpdateController._logger.debug(message, print_msg=True)

    @staticmethod
    def _remove_lock_files(files, ssh_clients):
        for ssh_client in ssh_clients:
            for file_name in files:
                if ssh_client.file_exists(file_name):
                    ssh_client.file_delete(file_name)
