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
import inspect
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
from ovs.extensions.generic.system import System
from ovs.extensions.generic.toolbox import Toolbox as ExtensionToolbox
from ovs.extensions.migration.migrator import Migrator
from ovs.extensions.packages.package import PackageManager
from ovs.extensions.services.service import ServiceManager
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.lib.helpers.decorators import add_hooks
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.generic import GenericController
from ovs.log.log_handler import LogHandler


class UpdateController(object):
    """
    This class contains all logic for updating an environment
    """
    _logger = LogHandler.get('update', name='core')
    _logger.logger.propagate = False
    _update_file = '/etc/ready_for_upgrade'
    _update_ongoing_file = '/etc/update_ongoing'

    framework_packages = {'arakoon', 'openvstorage'}
    volumedriver_packages = {'alba', 'arakoon', 'volumedriver-no-dedup-base', 'volumedriver-no-dedup-server'}
    all_core_packages = framework_packages.union(volumedriver_packages)
    core_packages_with_binaries = {'alba', 'arakoon', 'volumedriver-no-dedup-server'}

    #########
    # HOOKS #
    #########
    @staticmethod
    @add_hooks('update', 'get_package_info_multi')
    def get_package_information_core(client, package_info):
        """
        Called by GenericController.refresh_package_information() every hour

        Retrieve information about the currently installed versions of the core packages
        Retrieve information about the versions to which each package can potentially be updated
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

            binaries = PackageManager.get_binary_versions(client=client, package_names=UpdateController.core_packages_with_binaries)
            installed = PackageManager.get_installed_versions(client=client, package_names=UpdateController.all_core_packages)
            candidate = PackageManager.get_candidate_versions(client=client, package_names=UpdateController.all_core_packages)
            if set(installed.keys()) != set(UpdateController.all_core_packages) or set(candidate.keys()) != set(UpdateController.all_core_packages):
                raise RuntimeError('Failed to retrieve the installed and candidate versions for packages: {0}'.format(', '.join(UpdateController.all_core_packages)))

            # Retrieve Arakoon information
            framework_arakoons = []
            storagedriver_arakoons = []
            for cluster, arakoon_list in {'cacc': framework_arakoons,
                                          'ovsdb': framework_arakoons,
                                          'voldrv': storagedriver_arakoons}.iteritems():
                cluster_name = ArakoonClusterConfig.get_cluster_name(cluster)
                if cluster_name is None:
                    continue

                if cluster == 'cacc':
                    arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name, filesystem=True, ip=client.ip)
                else:
                    arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)

                if arakoon_metadata['internal'] is True:
                    arakoon_list.append(ArakoonInstaller.get_service_name_for_cluster(cluster_name=arakoon_metadata['cluster_name']))

            storagerouter = StorageRouterList.get_by_ip(client.ip)
            alba_proxies = []
            for service in storagerouter.services:
                if service.type.name == ServiceType.SERVICE_TYPES.ALBA_PROXY:
                    alba_proxies.append(service.name)

            storagedriver_services = []
            for sd in storagerouter.storagedrivers:
                storagedriver_services.append('dtl_{0}'.format(sd.vpool.name))
                storagedriver_services.append('volumedriver_{0}'.format(sd.vpool.name))

            default_entry = {'candidate': None,
                             'installed': None,
                             'services_to_restart': []}

            #                       component:   package_name: services_with_run_file
            for component, info in {'framework': {'arakoon': framework_arakoons,
                                                  'openvstorage': []},
                                    'storagedriver': {'alba': alba_proxies,
                                                      'arakoon': storagedriver_arakoons,
                                                      'volumedriver-no-dedup-base': [],
                                                      'volumedriver-no-dedup-server': storagedriver_services}}.iteritems():
                component_info = {}
                for package, services in info.iteritems():
                    for service in services:
                        version_file = '/opt/OpenvStorage/run/{0}.version'.format(service)
                        if not client.file_exists(version_file):
                            UpdateController._logger.warning('{0}: Failed to find a version file in /opt/OpenvStorage/run for service {1}'.format(client.ip, service))
                            continue
                        package_name = package
                        running_versions = client.file_read(version_file).strip()
                        for version in running_versions.split(';'):
                            version = version.strip()
                            running_version = None
                            if '=' in version:
                                package_name = version.split('=')[0]
                                running_version = version.split('=')[1]
                            elif version:
                                running_version = version

                            if package_name not in UpdateController.all_core_packages:
                                raise ValueError('Unknown package dependency found in {0}'.format(version_file))
                            if package_name not in binaries:
                                raise RuntimeError('Binary version for package {0} was not retrieved'.format(package_name))

                            if running_version is not None and running_version != binaries[package_name]:
                                if package_name not in component_info:
                                    component_info[package_name] = copy.deepcopy(default_entry)
                                component_info[package_name]['installed'] = running_version
                                component_info[package_name]['candidate'] = binaries[package_name]
                                component_info[package_name]['services_to_restart'].append(service)

                    if installed[package] != candidate[package] and package not in component_info:
                        component_info[package] = copy.deepcopy(default_entry)
                        component_info[package]['installed'] = installed[package]
                        component_info[package]['candidate'] = candidate[package]
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
                    storagedriver_services.append('volumedriver_{0}'.format(sd.vpool.name))
                for sd in storagerouter.storagedrivers:
                    storagedriver_services.append('dtl_{0}'.format(sd.vpool.name))
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
                        information[key]['services_post_update'].update(storagedriver_services)
                    elif package_name == 'volumedriver-no-dedup-server':
                        for down in storagedriver_downtime:
                            if down not in information[key]['downtime']:
                                information[key]['downtime'].append(down)
                        information[key]['services_post_update'].update(storagedriver_services)
                    elif package_name == 'arakoon':
                        if key == 'framework':
                            framework_arakoons = set()
                            if arakoon_ovs_info['internal'] is True:
                                framework_arakoons.add(ArakoonInstaller.get_service_name_for_cluster(cluster_name=arakoon_ovs_info['name']))
                            if arakoon_cacc_info['internal'] is True:
                                framework_arakoons.add(ArakoonInstaller.get_service_name_for_cluster(cluster_name=arakoon_cacc_info['name']))

                            information[key]['services_post_update'].update(framework_arakoons)
                            if arakoon_ovs_info['down'] is True and ['ovsdb', None] not in information[key]['downtime']:
                                information[key]['downtime'].append(['ovsdb', None])
                        elif arakoon_voldrv_info['internal'] is True:
                            information[key]['services_post_update'].update({ArakoonInstaller.get_service_name_for_cluster(cluster_name=arakoon_voldrv_info['name'])})
                            if arakoon_voldrv_info['down'] is True and ['voldrv', None] not in information[key]['downtime']:
                                information[key]['downtime'].append(['voldrv', None])
        return information

    @staticmethod
    @add_hooks('update', 'package_install_multi')
    def package_install_core(client, package_info, components):
        """
        Update the core packages
        :param client: Client on which to execute update the packages
        :type client: SSHClient
        :param package_info: Information about the packages (installed, candidate)
        :type package_info: dict
        :param components: Components which have been selected for update
        :type components: list
        :return: None
        """
        if 'framework' not in components and 'storagedriver' not in components:
            return

        packages_to_install = {}
        for pkg_name, pkg_info in package_info.iteritems():
            if pkg_name in UpdateController.all_core_packages:
                packages_to_install[pkg_name] = pkg_info
        if not packages_to_install:
            return

        UpdateController._logger.debug('{0}: Executing hook {1}'.format(client.ip, inspect.currentframe().f_code.co_name))
        for pkg_name, pkg_info in packages_to_install.iteritems():
            UpdateController._logger.debug('{0}: Updating core package {1} ({2} --> {3})'.format(client.ip, pkg_name, pkg_info['installed'], pkg_info['candidate']))
            PackageManager.install(package_name=pkg_name, client=client)
            UpdateController._logger.debug('{0}: Updated core package {1}'.format(client.ip, pkg_name))
        UpdateController._logger.debug('{0}: Executed hook {1}'.format(client.ip, inspect.currentframe().f_code.co_name))

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

        # Remove services which have been renamed in the migration code
        local_sr = System.get_my_storagerouter()
        local_ip = local_sr.ip
        local_client = SSHClient(endpoint=local_sr, username='root')
        for version_file in local_client.file_list(directory='/opt/OpenvStorage/run'):
            if not version_file.endswith('.remove'):
                continue
            packages = set()
            contents = local_client.file_read(filename='/opt/OpenvStorage/run/{0}'.format(version_file))
            for part in contents.split(';'):
                packages.add(part.split('=')[0])
            if packages.issubset(UpdateController.volumedriver_packages) and 'storagedriver' in components:
                service_name = version_file.replace('.remove', '').replace('.version', '')
                UpdateController._logger.debug('{0}: Removing service {1}'.format(client.ip, service_name))
                ServiceManager.stop_service(name=service_name, client=local_client)
                ServiceManager.remove_service(name=service_name, client=local_client)
                local_client.file_delete(filenames=['/opt/OpenvStorage/run/{0}'.format(version_file)])

        # Verify whether certain services need to be restarted
        update_information = UpdateController.get_update_information_core({})
        services_to_restart = set()
        if 'storagedriver' in components:
            services_to_restart.update(update_information.get('storagedriver', {}).get('services_post_update', set()))
        if 'framework' in components:
            services_to_restart.update(update_information.get('framework', {}).get('services_post_update', set()))
            services_to_restart.add('support-agent')

        # Restart the services
        if services_to_restart:
            UpdateController._logger.debug('{0}: Executing hook {1}'.format(client.ip, inspect.currentframe().f_code.co_name))
            for service_name in sorted(services_to_restart):
                if not service_name.startswith('arakoon-'):
                    UpdateController.change_services_state(services=[service_name], ssh_clients=[client], action='restart')
                else:
                    cluster_name = ArakoonClusterConfig.get_cluster_name(ExtensionToolbox.remove_prefix(service_name, 'arakoon-'))
                    if cluster_name == 'config':
                        filesystem = True
                        arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name='cacc', filesystem=True, ip=local_ip)
                    else:
                        filesystem = True
                        arakoon_metadata = ArakoonInstaller.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)
                    if arakoon_metadata['internal'] is True:
                        master_ip = StorageRouterList.get_masters()[0].ip  # Any master node should be part of the internal 'cacc' cluster
                        config = ArakoonClusterConfig(cluster_id=cluster_name, filesystem=filesystem)
                        config.load_config(ip=master_ip)
                        if local_ip in [node.ip for node in config.nodes]:
                            UpdateController._logger.debug('{0}: Restarting arakoon node {1}'.format(client.ip, cluster_name))
                            ArakoonInstaller.restart_node(cluster_name=cluster_name,
                                                          client=client)
            UpdateController._logger.debug('{0}: Executed hook {1}'.format(client.ip, inspect.currentframe().f_code.co_name))

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
    @celery.task(name='ovs.update.update_components')
    def update_components(components):
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
        ssh_clients = []
        services_stop_start = set()
        try:
            filemutex.acquire()
            UpdateController._logger.debug('+++ Starting update +++')

            from ovs.dal.lists.storagerouterlist import StorageRouterList

            # Create SSHClients to all nodes
            UpdateController._logger.debug('Generating SSH client connections for each storage router')
            storage_routers = StorageRouterList.get_storagerouters()
            master_ips = []
            extra_ips = []
            local_ip = None
            for sr in storage_routers:
                try:
                    ssh_clients.append(SSHClient(sr, username='root'))
                    if sr == System.get_my_storagerouter():
                        local_ip = sr.ip
                    if sr.node_type == 'MASTER':
                        master_ips.append(sr.ip)
                    elif sr.node_type == 'EXTRA':
                        extra_ips.append(sr.ip)
                except UnableToConnectException:
                    raise Exception('Update is only allowed on systems where all nodes are online and fully functional')

            # Create locks
            for client in ssh_clients:
                UpdateController._logger.debug('{0}: Creating lock files'.format(client.ip))
                client.run(['touch', UpdateController._update_file])  # Prevents manual install or update individual packages
                client.run(['touch', UpdateController._update_ongoing_file])

            # Check requirements
            packages_to_update = {}
            services_post_update = set()
            update_information = UpdateController.get_update_information_all()
            for component, component_info in update_information.iteritems():
                if component in components:
                    UpdateController._logger.debug('Verifying update information for component: {0}'.format(component.upper()))
                    Toolbox.verify_required_params(actual_params=component_info,
                                                   required_params={'downtime': (list, None),
                                                                    'packages': (dict, None),
                                                                    'prerequisites': (list, None),
                                                                    'services_stop_start': (set, None),
                                                                    'services_post_update': (set, None)})
                    if len(component_info['prerequisites']) > 0:
                        raise Exception('Update is only allowed when all prerequisites have been met')

                    packages_to_update.update(component_info['packages'])
                    services_stop_start.update(component_info['services_stop_start'])
                    services_post_update.update(component_info['services_post_update'])
            if len(packages_to_update) > 0:
                UpdateController._logger.debug('Packages to be updated: {0}'.format(', '.join(sorted(packages_to_update.keys()))))
            if len(services_stop_start) > 0:
                UpdateController._logger.debug('Services to stop before package update: {0}'.format(', '.join(sorted(services_stop_start))))
            if len(services_post_update) > 0:
                UpdateController._logger.debug('Services which will be restarted after update: {0}'.format(', '.join(sorted(services_post_update))))

            # Stop services
            if UpdateController.change_services_state(services=services_stop_start,
                                                      ssh_clients=ssh_clients,
                                                      action='stop') is False:
                raise Exception('Stopping all services on every node failed, cannot continue')

            # Install packages
            # First install packages on all StorageRouters individually
            package_install_multi_hooks = Toolbox.fetch_hooks('update', 'package_install_multi')
            package_install_single_hooks = Toolbox.fetch_hooks('update', 'package_install_single')
            if packages_to_update:
                failures = False
                for client in ssh_clients:
                    UpdateController._logger.debug('{0}: Installing packages'.format(client.ip))
                    for function in package_install_multi_hooks:
                        try:
                            function(client=client, package_info=packages_to_update, components=components)
                        except Exception as ex:
                            UpdateController._logger.error('{0}: Package installation hook {1} failed with error: {2}'.format(client.ip, function.__name__, ex))
                            failures = True

                if set(components).difference({'framework', 'storagedriver'}):
                    # Second install packages on all ALBA nodes
                    for function in package_install_single_hooks:
                        try:
                            function(package_info=packages_to_update, components=components)
                        except Exception as ex:
                            UpdateController._logger.exception('Package installation hook {0} failed with error: {1}'.format(function.__name__, ex))
                            failures = True

                if failures is True:
                    raise Exception('Installing the packages failed on 1 or more nodes')

            # Remove update file
            for client in ssh_clients:
                client.file_delete(UpdateController._update_file)

            # Migrate code
            if 'framework' in components:
                failures = []
                for client in ssh_clients:
                    UpdateController._logger.debug('{0}: Verifying extensions code migration is required'.format(client.ip))
                    try:
                        key = '/ovs/framework/hosts/{0}/versions'.format(System.get_my_machine_id(client=client))
                        old_versions = Configuration.get(key) if Configuration.exists(key) else {}
                        try:
                            with remote(client.ip, [Migrator]) as rem:
                                rem.Migrator.migrate(master_ips, extra_ips)
                        except EOFError as eof:
                            UpdateController._logger.warning('{0}: EOFError during code migration, retrying {1}'.format(client.ip, eof))
                            with remote(client.ip, [Migrator]) as rem:
                                rem.Migrator.migrate(master_ips, extra_ips)
                        new_versions = Configuration.get(key) if Configuration.exists(key) else {}
                        if old_versions != new_versions:
                            UpdateController._logger.debug('{0}: Finished extensions code migration. Old versions: {1} --> New versions: {2}'.format(client.ip, old_versions, new_versions))
                    except Exception as ex:
                        failures.append('{0}: {1}'.format(client.ip, str(ex)))
                if len(failures) > 0:
                    raise Exception('Failed to run the extensions migrate code on all nodes. Errors found:\n\n{0}'.format('\n\n'.join(failures)))

            # Start memcached
            if 'memcached' in services_stop_start:
                services_stop_start.remove('memcached')
                UpdateController.change_services_state(services=['memcached'],
                                                       ssh_clients=ssh_clients,
                                                       action='start')

            # Migrate model
            if 'framework' in components:
                UpdateController._logger.debug('Verifying DAL code migration is required')
                old_versions = PersistentFactory.get_client().get('ovs_model_version') if PersistentFactory.get_client().exists('ovs_model_version') else {}

                from ovs.dal.helpers import Migration
                with remote(ssh_clients[0].ip, [Migration]) as rem:
                    rem.Migration.migrate()

                new_versions = PersistentFactory.get_client().get('ovs_model_version') if PersistentFactory.get_client().exists('ovs_model_version') else {}
                if old_versions != new_versions:
                    UpdateController._logger.debug('Finished DAL code migration. Old versions: {0} --> New versions: {1}'.format(old_versions, new_versions))

            # Post update actions
            for client in ssh_clients:
                UpdateController._logger.debug('{0}: Executing post-update actions'.format(client.ip))
                with remote(client.ip, [Toolbox]) as rem:
                    for function in rem.Toolbox.fetch_hooks('update', 'post_update_multi'):
                        try:
                            function(client=client, components=components)
                        except Exception as ex:
                            UpdateController._logger.exception('{0}: Post update hook {1} failed with error: {2}'.format(client.ip, function.__name__, ex))

            with remote(local_ip, [Toolbox]) as rem:
                for function in rem.Toolbox.fetch_hooks('update', 'post_update_single'):
                    try:
                        function(components=components)
                    except Exception as ex:
                        UpdateController._logger.exception('Post update hook {0} failed with error: {1}'.format(function.__name__, ex))

            # Start services
            UpdateController.change_services_state(services=services_stop_start,
                                                   ssh_clients=ssh_clients,
                                                   action='start')

            UpdateController._refresh_package_information()
            UpdateController._logger.debug('+++ Finished updating +++')
        except NoLockAvailableException:
            UpdateController._logger.debug('Another update is currently in progress!')
        except Exception as ex:
            UpdateController._logger.exception('Error during update: {0}'.format(ex))
            if len(ssh_clients) > 0:
                UpdateController.change_services_state(services=services_stop_start,
                                                       ssh_clients=ssh_clients,
                                                       action='start')
                UpdateController._refresh_package_information()
                UpdateController._logger.error('Failed to update. Please check all the logs for more information')
        finally:
            filemutex.release()
            for ssh_client in ssh_clients:
                for file_name in [UpdateController._update_file, UpdateController._update_ongoing_file]:
                    try:
                        if ssh_client.file_exists(file_name):
                            ssh_client.file_delete(file_name)
                    except:
                        UpdateController._logger.warning('[0}: Failed to remove lock file {1}'.format(ssh_client.ip, file_name))

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
                    UpdateController._logger.warning('{0}: Something went wrong {1} service {2}: {3}'.format(ssh_client.ip, description, service_name, exc))
                    if action == 'stop':
                        return False
        return True

    ###########
    # HELPERS #
    ###########
    @staticmethod
    def _refresh_package_information():
        # Refresh updates
        UpdateController._logger.debug('Refreshing package information')
        counter = 1
        while counter < 6:
            try:
                GenericController.refresh_package_information()
                return
            except Exception:
                UpdateController._logger.debug('Attempt {0}: Could not refresh the update information, trying again'.format(counter))
                time.sleep(6)  # Wait 30 seconds max in total
            counter += 1
            if counter == 6:
                raise Exception('Could not refresh the update information')
