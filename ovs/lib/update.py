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
import copy
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
    _logger = LogHandler.get('update', name='core')
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
        Called by ScheduledTaskController.refresh_package_information() every hour

        Retrieve information about the currently installed versions of the core packages
        Retrieve information about the versions to which each package can potentially be upgraded
        If installed version is different from candidate version --> store this information in model

        Additionally check the services with a 'run' file
        Verify whether the running version is up-to-date with the candidate version
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

            # Retrieve Arakoon information
            cacc_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name='cacc', filesystem=True, ip=client.ip)
            voldrv_cluster = ArakoonClusterConfig.get_cluster_name('voldrv')
            framework_arakoons = ['arakoon-{0}'.format(cacc_metadata['cluster_name']),
                                  'arakoon-{0}'.format(ArakoonClusterConfig.get_cluster_name('ovsdb'))]
            storagedriver_arakoons = [] if voldrv_cluster is None else ['arakoon-{0}'.format(voldrv_cluster)]

            storagerouter = StorageRouterList.get_by_ip(client.ip)
            alba_proxies = []
            for service in storagerouter.services:
                if service.type.name == ServiceType.SERVICE_TYPES.ALBA_PROXY:
                    alba_proxies.append(service.name)

            default_entry = {'candidate': None,
                             'installed': None,
                             'services_to_restart': []}
            #                       component:   package_name: services_with_run_file
            for component, info in {'framework': {'arakoon': framework_arakoons,
                                                  'openvstorage': []},
                                    'storagedriver': {'alba': alba_proxies,
                                                      'arakoon': storagedriver_arakoons,
                                                      'volumedriver-no-dedup-base': [],
                                                      'volumedriver-no-dedup-server': []}}.iteritems():
                component_info = {}
                for package_name, services in info.iteritems():
                    for service in services:
                        service = ExtensionToolbox.remove_prefix(service, 'ovs-')
                        version_file = '/opt/OpenvStorage/run/{0}.version'.format(service)
                        if not client.file_exists(version_file):
                            UpdateController._log_message('Failed to find a version file in /opt/asd-manager/run for service {0}'.format(service), client_ip=client.ip, severity='warning')
                            continue
                        running_versions = client.file_read(version_file).strip()
                        for version in running_versions.split(';'):
                            version = version.strip()
                            running_version = None
                            if '=' in version:
                                package_name = version.split('=')[0]
                                running_version = version.split('=')[1]
                                if package_name not in UpdateController.all_core_packages:
                                    raise ValueError('Unknown package dependency found in {0}'.format(version_file))
                            elif version:
                                running_version = version

                            if running_version is not None and running_version != candidate[package_name]:
                                if package_name not in component_info:
                                    component_info[package_name] = copy.deepcopy(default_entry)
                                component_info[package_name]['installed'] = running_version
                                component_info[package_name]['candidate'] = candidate[package_name]
                                component_info[package_name]['services_to_restart'].append('ovs-{0}'.format(service))

                    if installed[package_name] != candidate[package_name] and package_name not in component_info:
                        component_info[package_name] = copy.deepcopy(default_entry)
                        component_info[package_name]['installed'] = installed[package_name]
                        component_info[package_name]['candidate'] = candidate[package_name]
                if component_info:
                    if component not in package_info[client.ip]:
                        package_info[client.ip][component] = {}
                    package_info[client.ip][component].update(component_info)
        except Exception as ex:
            if 'errors' not in package_info[client.ip]:
                package_info[client.ip]['errors'] = []
            package_info[client.ip]['errors'].append(ex)
        return package_info

    @staticmethod
    @add_hooks('update', 'information')
    def get_update_information_core(information):
        """
        Called when the 'Update' button in the GUI is pressed
        This call collects additional information about the packages which can be updated
        Eg:
            * Downtime for Arakoons
            * Downtime for StorageDrivers
            * Prerequisites that haven't been met
            * Services which will be stopped during update
            * Services which will be restarted after update
        """
        # Verify arakoon info
        arakoon_ovs_info = {'down': False,
                            'name': None,
                            'internal': False}
        arakoon_cacc_info = {'down': False,
                             'name': None,
                             'internal': False}
        arakoon_voldrv_info = {'down': False,
                               'name': None,
                               'internal': False}
        for cluster in ['cacc', 'ovsdb', 'voldrv']:
            cluster_name = ArakoonClusterConfig.get_cluster_name(cluster)
            if cluster_name is None:
                continue

            if cluster == 'cacc':
                arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name, filesystem=True, ip=System.get_my_storagerouter().ip)
            else:
                arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)

            if arakoon_metadata['internal'] is True:
                config = ArakoonClusterConfig(cluster_id=cluster_name, filesystem=(cluster == 'cacc'))
                config.load_config(System.get_my_storagerouter().ip if cluster == 'cacc' else None)
                if cluster == 'ovsdb':
                    arakoon_ovs_info['down'] = len(config.nodes) < 3
                    arakoon_ovs_info['name'] = arakoon_metadata['cluster_name']
                    arakoon_ovs_info['internal'] = True
                elif cluster == 'voldrv':
                    arakoon_voldrv_info['down'] = len(config.nodes) < 3
                    arakoon_voldrv_info['name'] = arakoon_metadata['cluster_name']
                    arakoon_voldrv_info['internal'] = True
                else:
                    arakoon_cacc_info['name'] = arakoon_metadata['cluster_name']
                    arakoon_cacc_info['internal'] = True

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
                information[key] = {'packages': {},
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

                # Retrieve the actual update information
                for package_name, package_info in storagerouter.package_information[key].iteritems():
                    if package_name not in UpdateController.all_core_packages:
                        continue  # Only gather information for the core packages

                    information[key]['services_post_update'].update(package_info.pop('services_to_restart'))
                    if package_name not in information[key]['packages']:
                        information[key]['packages'][package_name] = {}
                    information[key]['packages'][package_name].update(package_info)

                    if package_name == 'openvstorage':
                        if ['gui', None] not in information[key]['downtime']:
                            information[key]['downtime'].append(['gui', None])
                        if ['api', None] not in information[key]['downtime']:
                            information[key]['downtime'].append(['api', None])
                        information[key]['services_stop_start'].update({'watcher-framework', 'memcached'})
                    elif package_name == 'alba':
                        for down in alba_downtime:
                            if down not in information[key]['downtime']:
                                information[key]['downtime'].append(down)
                        information[key]['services_post_update'].update(alba_services)
                    elif package_name == 'volumedriver-no-dedup-base':
                        for down in storagedriver_downtime:
                            if down not in information[key]['downtime']:
                                information[key]['downtime'].append(down)
                        information[key]['services_stop_start'].update(storagedriver_services)
                    elif package_name == 'volumedriver-no-dedup-server':
                        for down in storagedriver_downtime:
                            if down not in information[key]['downtime']:
                                information[key]['downtime'].append(down)
                        information[key]['services_stop_start'].update(storagedriver_services)
                    elif package_name == 'arakoon':
                        if key == 'framework':
                            framework_arakoons = set()
                            if arakoon_ovs_info['internal'] is True:
                                framework_arakoons.add('ovs-arakoon-{0}'.format(arakoon_ovs_info['name']))
                            if arakoon_cacc_info['internal'] is True:
                                framework_arakoons.add('ovs-arakoon-{0}'.format(arakoon_cacc_info['name']))

                            information[key]['services_post_update'].update(framework_arakoons)
                            if arakoon_ovs_info['down'] is True and ['ovsdb', None] not in information[key]['downtime']:
                                information[key]['downtime'].append(['ovsdb', None])
                        elif arakoon_voldrv_info['internal'] is True:
                            information[key]['services_post_update'].update({'ovs-arakoon-{0}'.format(arakoon_voldrv_info['name'])})
                            if arakoon_voldrv_info['down'] is True and ['voldrv', None] not in information[key]['downtime']:
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
        if 'framework' not in components and 'storagedriver' not in components:
            return

        if 'framework' in components:
            UpdateController.change_services_state(services=['support-agent'], ssh_clients=[client], action='restart')

        update_information = UpdateController.get_update_information_core({})
        services_to_restart = set()
        if 'storagedriver' in components:
            services_to_restart.update(update_information.get('storagedriver', {}).get('services_post_update', set()))
        if 'framework' in components:
            services_to_restart.update(update_information.get('framework', {}).get('services_post_update', set()))

        for service_name in services_to_restart:
            if not service_name.startswith('ovs-arakoon-'):
                UpdateController.change_services_state(services=[service_name], ssh_clients=[client], action='restart')
            else:
                cluster_name = ArakoonClusterConfig.get_cluster_name(ExtensionToolbox.remove_prefix(service_name, 'ovs-arakoon-'))
                if cluster_name == 'cacc':
                    arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name, filesystem=True, ip=System.get_my_storagerouter().ip)
                else:
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
        package_info = dict((storagerouter.ip, storagerouter.package_information) for storagerouter in StorageRouterList.get_storagerouters())
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

        for component, info in copy.deepcopy(information).iteritems():
            if len(info['packages']) == 0:
                information.pop(component)
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
        update_ongoing_file = '/etc/update_ongoing'
        ssh_clients = []
        this_sr = System.get_my_storagerouter()
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
            local_ip = None
            for sr in storage_routers:
                if sr == this_sr:
                    local_ip = sr.ip
                try:
                    ssh_clients.append(SSHClient(sr.ip, username='root'))
                    if sr.node_type == 'MASTER':
                        master_ips.append(sr.ip)
                    elif sr.node_type == 'EXTRA':
                        extra_ips.append(sr.ip)
                except UnableToConnectException:
                    raise Exception('Update is only allowed on systems where all nodes are online and fully functional')

            # Create locks
            for client in ssh_clients:
                UpdateController._log_message('Creating lock files', client_ip=client.ip)
                client.run(['touch', update_file])  # Prevents manual install or update individual packages
                client.run(['touch', update_ongoing_file])

            # Check requirements
            packages_to_update = set()
            services_stop_start = set()
            services_post_update = set()
            update_information = UpdateController.get_update_information_all()
            for component, component_info in update_information.iteritems():
                if component in components:
                    UpdateController._log_message('Verifying update information for component: {0}'.format(component.upper()), client_ip=local_ip)
                    try:
                        Toolbox.verify_required_params(actual_params=component_info,
                                                       required_params={'downtime': (list, None),
                                                                        'packages': (dict, None),
                                                                        'prerequisites': (list, None),
                                                                        'services_stop_start': (set, None),
                                                                        'services_post_update': (set, None)})
                    except Exception:
                        UpdateController._remove_lock_files([update_file, update_ongoing_file], ssh_clients)
                        raise
                    if len(component_info['prerequisites']) > 0:
                        raise Exception('Update is only allowed when all prerequisites have been met')

                    packages_to_update.update(component_info['packages'].keys())
                    services_stop_start.update(component_info['services_stop_start'])
                    services_post_update.update(component_info['services_post_update'])
            if len(packages_to_update) > 0:
                UpdateController._log_message('Packages to be updated: {0}'.format(', '.join(sorted(packages_to_update))), client_ip=local_ip)
            if len(services_stop_start) > 0:
                UpdateController._log_message('Services to stop before package update: {0}'.format(', '.join(sorted(services_stop_start))), client_ip=local_ip)
            if len(services_post_update) > 0:
                UpdateController._log_message('Services which will be restarted after update: {0}'.format(', '.join(sorted(services_post_update))), client_ip=local_ip)

            # Stop services
            if UpdateController.change_services_state(services=services_stop_start,
                                                      ssh_clients=ssh_clients,
                                                      action='stop') is False:
                UpdateController._log_message('Stopping all services on every node failed, cannot continue', client_ip=local_ip, severity='warning')
                UpdateController._remove_lock_files([update_file, update_ongoing_file], ssh_clients)

                # Start services again if a service could not be stopped
                UpdateController._log_message('Attempting to start the services again', client_ip=local_ip)
                UpdateController.change_services_state(services=services_stop_start,
                                                       ssh_clients=ssh_clients,
                                                       action='start')

                UpdateController._log_message('Failed to stop all required services, aborting update', client_ip=local_ip, severity='error')
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

                if set(components).difference({'framework', 'storagedriver'}):
                    # Second install packages on all ALBA nodes
                    for function in Toolbox.fetch_hooks('update', 'package_install_single'):
                        try:
                            UpdateController._log_message('Executing hook {0}'.format(function.__name__), client_ip=local_ip)
                            function(package_names=packages_to_update)
                            UpdateController._log_message('Executed hook {0}'.format(function.__name__), client_ip=local_ip)
                        except Exception as ex:
                            UpdateController._log_message('Package installation hook {0} failed with error: {1}'.format(function.__name__, ex), client_ip=local_ip, severity='error')
                            failures = True

                if failures is True:
                    UpdateController._remove_lock_files([update_file, update_ongoing_file], ssh_clients)
                    UpdateController._log_message('Error occurred. Attempting to start all services again', client_ip=local_ip, severity='error')
                    UpdateController.change_services_state(services=services_stop_start,
                                                           ssh_clients=ssh_clients,
                                                           action='start')
                    UpdateController._log_message('Failed to update. Please check all the logs for more information', client_ip=local_ip, severity='error')
                    return

            # Remove update file
            for client in ssh_clients:
                client.file_delete([update_file])

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
                        UpdateController._log_message('Failed to update. Please check all the logs for more information', client.ip, severity='error')
                        return

            # Start memcached
            if 'memcached' in services_stop_start:
                services_stop_start.remove('memcached')
                UpdateController._log_message('Starting memcached', client_ip=local_ip)
                UpdateController.change_services_state(services=['memcached'],
                                                       ssh_clients=ssh_clients,
                                                       action='start')

            # Migrate model
            if 'framework' in components:
                UpdateController._log_message('Verifying DAL code migration is required', client_ip=local_ip)
                try:
                    old_versions = PersistentFactory.get_client().get('ovs_model_version') if PersistentFactory.get_client().exists('ovs_model_version') else {}

                    from ovs.dal.helpers import Migration
                    with remote(ssh_clients[0].ip, [Migration]) as rem:
                        rem.Migration.migrate()
                except Exception as ex:
                    UpdateController._remove_lock_files([update_ongoing_file], ssh_clients)
                    UpdateController._log_message('An unexpected error occurred: {0}'.format(ex), client_ip=local_ip, severity='error')
                    UpdateController.change_services_state(services=services_stop_start,
                                                           ssh_clients=ssh_clients,
                                                           action='start')
                    UpdateController._log_message('Failed to update. Please check all the logs for more information', client_ip=local_ip, severity='error')
                    return
                new_versions = PersistentFactory.get_client().get('ovs_model_version') if PersistentFactory.get_client().exists('ovs_model_version') else {}
                if old_versions != new_versions:
                    UpdateController._log_message('Finished DAL code migration. Old versions: {0} --> New versions: {1}'.format(old_versions, new_versions), client_ip=local_ip)

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
                    UpdateController._log_message('Executing hook {0}'.format(function.__name__), client_ip=local_ip)
                    function(components=components)
                    UpdateController._log_message('Executed hook {0}'.format(function.__name__), client_ip=local_ip)
                except Exception as ex:
                    UpdateController._log_message('Post update hook {0} failed with error: {1}'.format(function.__name__, ex), client_ip=local_ip, severity='error')

            # Start services
            UpdateController.change_services_state(services=services_stop_start,
                                                   ssh_clients=ssh_clients,
                                                   action='start')

            # Refresh updates
            UpdateController._log_message('Refreshing package information', client_ip=local_ip)
            counter = 1
            while counter < 6:
                try:
                    ScheduledTaskController.refresh_package_information()
                    break
                except NoLockAvailableException:
                    UpdateController._log_message('Attempt {0}: Could not refresh the update information, trying again'.format(counter), client_ip=local_ip)
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
            UpdateController._remove_lock_files([update_file, update_ongoing_file], ssh_clients)
        finally:
            filemutex.release()

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
