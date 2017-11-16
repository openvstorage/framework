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
import inspect
from subprocess import CalledProcessError
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.db.arakooninstaller import ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.generic.filemutex import file_mutex, NoLockAvailableException
from ovs.extensions.generic.logger import Logger
from ovs_extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.generic.system import System
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs.extensions.migration.migrator import Migrator
from ovs.extensions.packages.packagefactory import PackageFactory
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.lib.generic import GenericController
from ovs.lib.helpers.decorators import add_hooks, ovs_task
from ovs.lib.helpers.toolbox import Toolbox


class UpdateController(object):
    """
    This class contains all logic for updating an environment
    """
    _logger = Logger(name='update', forced_target_type='file')
    _update_file = '/etc/ready_for_upgrade'
    _update_ongoing_file = '/etc/update_ongoing'
    _package_manager = PackageFactory.get_manager()
    _service_manager = ServiceFactory.get_manager()

    #########
    # HOOKS #
    #########
    @classmethod
    @add_hooks('update', 'get_package_info_multi')
    def _get_package_information_core(cls, client, package_info):
        """
        Called by GenericController.refresh_package_information() every hour

        Retrieve information about the currently installed versions of the core packages
        Retrieve information about the versions to which each package can potentially be updated
        If installed version is different from candidate version --> store this information in model

        Additionally if installed version is identical to candidate version, check the services with a 'run' file
        Verify whether the running version is identical to the candidate version
        If different --> store this information in the model

        Result: Every package with updates or which requires services to be restarted is stored in the model

        :param client: Client on which to collect the version information
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param package_info: Dictionary passed in by the thread calling this function
        :type package_info: dict
        :return: Package information
        :rtype: dict
        """
        try:
            if client.username != 'root':
                raise RuntimeError('Only the "root" user can retrieve the package information')

            binaries = cls._package_manager.get_binary_versions(client=client)
            service_info = ServiceFactory.get_services_with_version_files(storagerouter=StorageRouterList.get_by_ip(ip=client.ip))
            packages_to_update = PackageFactory.get_packages_to_update(client=client)
            services_to_update = ServiceFactory.get_services_to_update(client=client,
                                                                       binaries=binaries,
                                                                       service_info=service_info)

            # First we merge in the services
            ExtensionsToolbox.merge_dicts(dict1=package_info[client.ip],
                                          dict2=services_to_update)
            # Then the packages merge can potentially overrule the installed/candidate version, because these versions need priority over the service versions
            ExtensionsToolbox.merge_dicts(dict1=package_info[client.ip],
                                          dict2=packages_to_update)
        except Exception as ex:
            if 'errors' not in package_info[client.ip]:
                package_info[client.ip]['errors'] = []
            package_info[client.ip]['errors'].append(ex)
        return package_info

    @classmethod
    @add_hooks('update', 'information')
    def get_update_information_core(cls, information):
        """
        Called when the 'Update' button in the GUI is pressed
        This call collects additional information about the packages which can be updated
        Eg:
            * Downtime for Arakoons
            * Downtime for StorageDrivers
            * Prerequisites that haven't been met
            * Services which will be stopped during update
            * Services which will be restarted after update
        :param information: Information about all components for the entire cluster. This is passed in by the calling thread and thus also (pre-)populated by other threads
        :type information: dict
        :return: All the information collected
        :rtype: dict
        """
        # Verify StorageRouter downtime
        prerequisites = []
        all_storagerouters = StorageRouterList.get_storagerouters()
        for storagerouter in all_storagerouters:
            try:
                SSHClient(endpoint=storagerouter, username='root')
            except UnableToConnectException:
                prerequisites.append(['node_down', storagerouter.name])

        arakoon_ovs_info = None
        arakoon_voldrv_info = None

        # Combine all information
        for storagerouter in all_storagerouters:
            for component, package_names in PackageFactory.get_package_info()['names'].iteritems():
                if component not in storagerouter.package_information:
                    continue

                if component not in information:
                    information[component] = {'packages': {},
                                              'downtime': [],
                                              'prerequisites': [],
                                              'services_stop_start': {10: set(), 20: set()},  # Lowest get stopped first and started last
                                              'services_post_update': {10: set(), 20: set()}}  # Lowest get restarted first
                component_info = information[component]
                if component == PackageFactory.COMP_FWK:
                    component_info['prerequisites'].extend(prerequisites)

                # Loop the actual update information
                for package_name, package_info in storagerouter.package_information[component].iteritems():
                    if package_name not in package_names:
                        continue  # Only gather the information for the packages related to the current component

                    # Add the services which require a restart to the post_update services
                    for importance, services in package_info.pop('services_to_restart', {}).iteritems():
                        if importance not in component_info['services_post_update']:
                            component_info['services_post_update'][importance] = set()
                        component_info['services_post_update'][importance].update(set(services))
                    # Add the version information for current package
                    if package_name not in component_info['packages']:
                        component_info['packages'][package_name] = package_info

                    # Add downtime and additional services for each package
                    if package_name == PackageFactory.PKG_OVS:
                        if ['gui', None] not in component_info['downtime']:
                            component_info['downtime'].append(['gui', None])
                        if ['api', None] not in component_info['downtime']:
                            component_info['downtime'].append(['api', None])
                        component_info['services_stop_start'][10].add('watcher-framework')
                        component_info['services_stop_start'][20].add('memcached')
                        component_info['services_post_update'][20].add('support-agent')
                    elif package_name in [PackageFactory.PKG_VOLDRV_BASE, PackageFactory.PKG_VOLDRV_BASE_EE, PackageFactory.PKG_VOLDRV_SERVER, PackageFactory.PKG_VOLDRV_SERVER_EE]:
                        for storagedriver in storagerouter.storagedrivers:
                            vpool_name = storagedriver.vpool.name
                            if len(storagedriver.vdisks_guids) > 0 and ['voldrv', vpool_name] not in component_info['downtime']:
                                component_info['downtime'].append(['voldrv', vpool_name])
                    elif package_name == PackageFactory.PKG_ARAKOON:
                        if component == PackageFactory.COMP_SD:
                            if arakoon_voldrv_info is None:
                                arakoon_voldrv_info = ArakoonInstaller.get_arakoon_update_info(internal_cluster_name='voldrv')
                            if arakoon_voldrv_info['internal'] is True and arakoon_voldrv_info['downtime'] is True and ['voldrv', None] not in component_info['downtime']:
                                component_info['downtime'].append(['voldrv', None])
                        elif component == PackageFactory.COMP_FWK:
                            if arakoon_ovs_info is None:
                                arakoon_ovs_info = ArakoonInstaller.get_arakoon_update_info(internal_cluster_name='ovsdb')
                            if arakoon_ovs_info['internal'] is True and arakoon_ovs_info['downtime'] is True and ['ovsdb', None] not in component_info['downtime']:
                                component_info['downtime'].append(['ovsdb', None])
        return information

    @classmethod
    @add_hooks('update', 'package_install_multi')
    def _package_install_core(cls, client, package_info, components):
        """
        Update the core packages
        :param client: Client on which to execute update the packages
        :type client: SSHClient
        :param package_info: Information about the packages (installed, candidate)
        :type package_info: dict
        :param components: Components which have been selected for update
        :type components: list
        :return: Boolean indicating whether to continue with the update or not
        :rtype: bool
        """
        return PackageFactory.update_packages(client=client, packages=package_info, components=components)

    @classmethod
    @add_hooks('update', 'post_update_single')
    def _post_update_async_migrator(cls, components=None):
        _ = components
        try:
            # noinspection PyUnresolvedReferences
            from ovs.lib.migration import MigrationController
            MigrationController.migrate.s().apply_async(countdown=30)
        except ImportError:
            cls._logger.error('Could not import MigrationController.')

    @classmethod
    @add_hooks('update', 'post_update_multi')
    def _post_update_core(cls, client, components, update_information):
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
        :param update_information: Information required for an update
        :type update_information: dict
        :return: None
        :rtype: NoneType
        """
        method_name = inspect.currentframe().f_code.co_name
        cls._logger.info('{0}: Executing hook {1}'.format(client.ip, method_name))
        pkg_names_to_check = set()
        for component, package_names in PackageFactory.get_package_info()['names'].iteritems():
            if component in components:
                pkg_names_to_check.update(package_names)

        try:
            ServiceFactory.remove_services_marked_for_removal(client=client,
                                                              package_names=pkg_names_to_check)
        except Exception:
            cls._logger.exception('{0}: Removing the services marked for removal failed'.format(client.ip))

        other_services = set()
        arakoon_services = set()
        for component, update_info in update_information.iteritems():
            if component not in PackageFactory.SUPPORTED_COMPONENTS:
                continue
            for restart_order in sorted(update_info['services_post_update']):
                for service_name in update_info['services_post_update'][restart_order]:
                    if service_name.startswith('arakoon-'):
                        arakoon_services.add(service_name)
                    else:
                        other_services.add(service_name)

        UpdateController.change_services_state(services=sorted(other_services), ssh_clients=[client], action='restart')
        for service_name in sorted(arakoon_services):
            try:
                cluster_name = ArakoonInstaller.get_cluster_name(ExtensionsToolbox.remove_prefix(service_name, 'arakoon-'))
                ip = System.get_my_storagerouter().ip if cluster_name == 'config' else None
                arakoon_metadata = ArakoonInstaller.get_arakoon_update_info(actual_cluster_name=cluster_name, ip=ip)
                if arakoon_metadata['internal'] is True:
                    arakoon_installer = ArakoonInstaller(cluster_name=cluster_name)
                    arakoon_installer.load(ip=ip)
                    if client.ip in [node.ip for node in arakoon_installer.config.nodes]:
                        cls._logger.warning('{0}: Restarting arakoon node {1}'.format(client.ip, cluster_name))
                        arakoon_installer.restart_node(client=client)
            except Exception:
                cls._logger.exception('{0}: Restarting service {1} failed'.format(client.ip, service_name))

        cls._logger.info('{0}: Executed hook {1}'.format(client.ip, method_name))

    ################
    # CELERY TASKS #
    ################
    @staticmethod
    @ovs_task(name='ovs.update.merge_package_information')
    def merge_package_information():
        """
        Retrieve the package information from the model for both StorageRouters and ALBA Nodes and merge it
        :return: Package information for all StorageRouters and ALBA nodes
        :rtype: dict
        """
        package_info = dict((storagerouter.ip, storagerouter.package_information) for storagerouter in StorageRouterList.get_storagerouters())
        for _function in Toolbox.fetch_hooks('update', 'merge_package_info'):
            package_info = ExtensionsToolbox.merge_dicts(dict1=package_info,
                                                         dict2=_function())
        return package_info

    @staticmethod
    @ovs_task(name='ovs.update.get_update_metadata')
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
    @ovs_task(name='ovs.update.get_update_information')
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
        for _function in Toolbox.fetch_hooks('update', 'information'):
            _function(information=information)
        return information

    @staticmethod
    @ovs_task(name='ovs.update.update_components')
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
        :rtype: NoneType
        """
        abort = False
        filemutex = file_mutex('system_update', wait=2)
        ssh_clients = []
        services_stop_start = list()
        errors_during_update = False
        try:
            filemutex.acquire()
            UpdateController._logger.info('+++ Starting update +++')

            # Create SSHClients to all nodes
            UpdateController._logger.info('Generating SSH client connections for each storage router')
            storage_routers = StorageRouterList.get_storagerouters()
            master_ips = []
            extra_ips = []
            local_ip = None
            for sr in storage_routers:
                try:
                    ssh_clients.append(SSHClient(endpoint=sr, username='root'))
                    if sr == System.get_my_storagerouter():
                        local_ip = sr.ip
                    if sr.node_type == 'MASTER':
                        master_ips.append(sr.ip)
                    elif sr.node_type == 'EXTRA':
                        extra_ips.append(sr.ip)
                except UnableToConnectException:
                    raise Exception('Update is only allowed on systems where all nodes are online and fully functional')

            ssh_clients.sort(key=lambda cl: ExtensionsToolbox.advanced_sort(element=cl.ip, separator='.'))

            # Create locks
            for client in ssh_clients:
                UpdateController._logger.info('{0}: Creating lock files'.format(client.ip))
                client.run(['touch', UpdateController._update_file])  # Prevents manual install or update individual packages
                client.run(['touch', UpdateController._update_ongoing_file])

            # Check requirements
            packages_to_update = {}
            services_post_update = list()
            update_information = UpdateController.get_update_information_all()
            for component, component_info in update_information.iteritems():
                if component in components:
                    UpdateController._logger.info('Verifying update information for component: {0}'.format(component.upper()))
                    Toolbox.verify_required_params(actual_params=component_info,
                                                   required_params={'downtime': (list, None),
                                                                    'packages': (dict, None),
                                                                    'prerequisites': (list, None),
                                                                    'services_stop_start': (dict, None),
                                                                    'services_post_update': (dict, None)})
                    if len(component_info['prerequisites']) > 0:
                        raise Exception('Update is only allowed when all prerequisites have been met')

                    packages_to_update.update(component_info['packages'])
                    for order in sorted(component_info['services_stop_start']):
                        services_stop_start.extend(list(component_info['services_stop_start'][order]))
                    for order in sorted(component_info['services_post_update']):
                        services_post_update.extend(list(component_info['services_post_update'][order]))
            if len(packages_to_update) > 0:
                UpdateController._logger.info('Packages to update')
                for package_to_update in sorted(packages_to_update):
                    UpdateController._logger.info('    * {0}'.format(package_to_update))
            if len(services_stop_start) > 0:
                UpdateController._logger.info('Services to stop BEFORE packages will be updated')
                for service_to_stop in sorted(services_stop_start):
                    UpdateController._logger.info('    * {0}'.format(service_to_stop))
            if len(services_post_update) > 0:
                UpdateController._logger.info('Services to restart AFTER packages have been updated')
                for service_to_restart in sorted(services_post_update):
                    UpdateController._logger.info('    * {0}'.format(service_to_restart))

            # Stop services
            if UpdateController.change_services_state(services=services_stop_start,
                                                      ssh_clients=ssh_clients,
                                                      action='stop') is False:
                raise Exception('Stopping all services on every node failed, cannot continue')

            # Collect the functions to be executed before they get overwritten by updated packages, so on each the same functionality is executed
            package_install_multi_hooks = Toolbox.fetch_hooks('update', 'package_install_multi')
            package_install_single_hooks = Toolbox.fetch_hooks('update', 'package_install_single')

            # Install packages (cluster nodes)
            if packages_to_update:
                for client in ssh_clients:
                    for _function in package_install_multi_hooks:
                        abort |= _function(client=client, package_info=packages_to_update, components=components)

            # Install packages (storage nodes, eg: SDM, iSCSI, ...)
            for _function in package_install_single_hooks:
                try:
                    abort |= _function(package_info=None, components=components)
                except Exception:
                    UpdateController._logger.exception('Package installation hook {0} failed'.format(_function.__name__))

            if abort is True:
                raise Exception('Installing the packages failed on 1 or more nodes')

            # Remove update file
            for client in ssh_clients:
                client.file_delete(UpdateController._update_file)

            # Migrate extensions
            if PackageFactory.COMP_FWK in components:
                failures = []
                for client in ssh_clients:
                    UpdateController._logger.info('{0}: Starting extensions code migration'.format(client.ip))
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
                            UpdateController._logger.info('{0}: Finished extensions code migration. Old versions: {1} --> New versions: {2}'.format(client.ip, old_versions, new_versions))
                    except Exception as ex:
                        abort = True
                        failures.append('{0}: {1}'.format(client.ip, str(ex)))
                if len(failures) > 0:
                    raise Exception('Failed to run the extensions migrate code on all nodes. Errors found:\n\n{0}'.format('\n\n'.join(failures)))

            # Start memcached
            if 'memcached' in services_stop_start:
                services_stop_start.remove('memcached')
                UpdateController.change_services_state(services=['memcached'],
                                                       ssh_clients=ssh_clients,
                                                       action='start')
                VolatileFactory.store = None

            # Migrate DAL
            if PackageFactory.COMP_FWK in components:
                UpdateController._logger.info('Starting DAL code migration')
                try:
                    old_versions = PersistentFactory.get_client().get('ovs_model_version') if PersistentFactory.get_client().exists('ovs_model_version') else {}
                    from ovs.dal.helpers import Migration
                    with remote(ssh_clients[0].ip, [Migration]) as rem:
                        rem.Migration.migrate()

                    new_versions = PersistentFactory.get_client().get('ovs_model_version') if PersistentFactory.get_client().exists('ovs_model_version') else {}
                    if old_versions != new_versions:
                        UpdateController._logger.info('Finished DAL code migration. Old versions: {0} --> New versions: {1}'.format(old_versions, new_versions))
                except Exception:
                    abort = True
                    raise

            # Post update actions
            for client in ssh_clients:
                UpdateController._logger.info('{0}: Executing post-update actions'.format(client.ip))
                with remote(client.ip, [Toolbox]) as rem:
                    for _function in rem.Toolbox.fetch_hooks('update', 'post_update_multi'):
                        try:
                            _function(client=client, components=components, update_information=update_information)
                        except Exception as ex:
                            UpdateController._logger.exception('{0}: Post update hook {1} failed with error: {2}'.format(client.ip, _function.__name__, ex))

            with remote(local_ip, [Toolbox]) as rem:
                for _function in rem.Toolbox.fetch_hooks('update', 'post_update_single'):
                    try:
                        _function(components=components)
                    except Exception as ex:
                        UpdateController._logger.exception('Post update hook {0} failed with error: {1}'.format(_function.__name__, ex))

            # Start services
            UpdateController.change_services_state(services=services_stop_start,
                                                   ssh_clients=ssh_clients,
                                                   action='start')
        except NoLockAvailableException:
            UpdateController._logger.error('Another update is currently in progress!')
        except Exception as ex:
            errors_during_update = True
            UpdateController._logger.exception('Error during update: {0}'.format(ex))
            if len(ssh_clients) > 0 and abort is False:
                UpdateController.change_services_state(services=services_stop_start,
                                                       ssh_clients=ssh_clients,
                                                       action='start')
        finally:
            UpdateController._refresh_package_information(ssh_clients[0])
            filemutex.release()
            for ssh_client in ssh_clients:
                for file_name in [UpdateController._update_file, UpdateController._update_ongoing_file]:
                    try:
                        if ssh_client.file_exists(file_name):
                            ssh_client.file_delete(file_name)
                    except:
                        UpdateController._logger.warning('[0}: Failed to remove lock file {1}'.format(ssh_client.ip, file_name))
            if errors_during_update is True:
                UpdateController._logger.error('Failed to update. Please check all the logs for more information')
            else:
                UpdateController._logger.info('+++ Finished updating +++')

    @classmethod
    def change_services_state(cls, services, ssh_clients, action):
        """
        Stop/start services on SSH clients
        If action is start, we ignore errors and try to start other services on other nodes
        """
        if action == 'start':
            services.reverse()  # Start services again in reverse order of stopping
        for service_name in services:
            for ssh_client in ssh_clients:
                description = 'stopping' if action == 'stop' else 'starting' if action == 'start' else 'restarting'
                try:
                    if cls._service_manager.has_service(service_name, client=ssh_client):
                        ServiceFactory.change_service_state(client=ssh_client,
                                                            name=service_name,
                                                            state=action,
                                                            logger=cls._logger)
                except Exception as exc:
                    cls._logger.warning('{0}: Something went wrong {1} service {2}: {3}'.format(ssh_client.ip, description, service_name, exc))
                    if action == 'stop':
                        return False
        return True

    ###########
    # HELPERS #
    ###########
    @staticmethod
    def _refresh_package_information(client):
        # Refresh updates
        UpdateController._logger.info('Refreshing update information')
        counter = 1
        while counter < 6:
            try:
                with remote(client.ip, [GenericController]) as rem:
                    rem.GenericController.refresh_package_information()
                return
            except Exception:
                UpdateController._logger.error('Attempt {0}: Could not refresh the update information, trying again'.format(counter))
                time.sleep(6)  # Wait 30 seconds max in total
                counter += 1
        UpdateController._logger.exception('Failed to refresh the update information')
