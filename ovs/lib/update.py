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
from ovs.dal.hybrids.diskpartition import DiskPartition
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
    @add_hooks('update', 'get_package_info_cluster')
    def _get_package_information_cluster_fwk(cls, client, package_info):
        """
        Retrieve information about the currently installed versions of the core packages
        Retrieve information about the versions to which each package can potentially be updated
        This information is combined for all plugins and further used in the GenericController.refresh_package_information call

        :param client: Client on which to collect the version information
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :param package_info: Dictionary passed in by the thread calling this function
        :type package_info: dict
        :return: None
        :rtype: NoneType
        """
        cls._logger.info('StorageRouter {0}: Refreshing framework package information'.format(client.ip))
        try:
            if client.username != 'root':
                raise RuntimeError('Only the "root" user can retrieve the package information')

            # This also validates whether the required packages have been installed and unexpected packages have not been installed
            packages_to_update = PackageFactory.get_packages_to_update(client=client)
            cls._logger.debug('StorageRouter {0}: Framework packages with updates: {1}'.format(client.ip, packages_to_update))
            for component, pkg_info in packages_to_update.iteritems():
                if component not in package_info[client.ip]:
                    package_info[client.ip][component] = pkg_info
                else:
                    for package_name, package_versions in pkg_info.iteritems():
                        package_info[client.ip][component][package_name] = package_versions
            cls._logger.info('StorageRouter {0}: Refreshed framework package information'.format(client.ip))
        except Exception as ex:
            cls._logger.exception('StorageRouter {0}: Refreshing framework package information failed'.format(client.ip))
            if 'errors' not in package_info[client.ip]:
                package_info[client.ip]['errors'] = []
            package_info[client.ip]['errors'].append(ex)

    @classmethod
    @add_hooks('update', 'get_update_info_cluster')
    def _get_update_information_cluster_fwk(cls, client, update_info, package_info):
        """
        In this function the services for each component / package combination are defined
        This service information consists out of:
            * Services to stop (before update) and start (after update of packages) -> 'services_stop_start'
            * Services to restart after update (post-update logic)                  -> 'services_post_update'
            * Down-times which will be caused due to service restarts               -> 'downtime'
            * Prerequisites that have not been met                                  -> 'prerequisites'

        Verify whether all relevant services have the correct binary active
        Whether a service has the correct binary version in use, we use the ServiceFactory.verify_restart_required functionality
        When a service has an older binary version running, we add this information to the 'update_info'

        This combined information is then stored in the 'package_information' of the StorageRouter DAL object

        :param client: SSHClient on which to retrieve the service information required for an update
        :type client: ovs.extensions.generic.sshclient.SSHClient
        :param update_info: Dictionary passed in by the thread calling this function used to store all update information
        :type update_info: dict
        :param package_info: Dictionary containing the components and packages which have an update available for current SSHClient
        :type package_info: dict
        :return: None
        :rtype: NoneType
        """
        cls._logger.info('StorageRouter {0}: Refreshing update information'.format(client.ip))
        try:
            binaries = cls._package_manager.get_binary_versions(client=client)
            storagerouter = StorageRouterList.get_by_ip(ip=client.ip)
            cls._logger.debug('StorageRouter {0}: Binary versions: {1}'.format(client.ip, binaries))

            for component, package_names in PackageFactory.get_package_info()['names'].iteritems():
                package_names = sorted(package_names)
                cls._logger.debug('StorageRouter {0}: Validating component {1} and related packages: {2}'.format(client.ip, component, package_names))

                if component not in update_info[client.ip]:
                    update_info[client.ip][component] = copy.deepcopy(ServiceFactory.DEFAULT_UPDATE_ENTRY)
                svc_component_info = update_info[client.ip][component]
                pkg_component_info = package_info.get(component, {})

                for package_name in package_names:
                    cls._logger.debug('StorageRouter {0}: Validating package {1}'.format(client.ip, package_name))
                    if package_name == PackageFactory.PKG_OVS and package_name in pkg_component_info:
                        if ['gui', None] not in svc_component_info['downtime']:
                            svc_component_info['downtime'].append(['gui', None])
                        if ['api', None] not in svc_component_info['downtime']:
                            svc_component_info['downtime'].append(['api', None])
                        svc_component_info['services_stop_start'][10].append('ovs-watcher-framework')
                        svc_component_info['services_stop_start'][20].append('memcached')
                        svc_component_info['services_post_update'][20].append('ovs-support-agent')
                        cls._logger.debug('StorageRouter {0}: Added services "ovs-watcher-framework" and "memcached" to stop-start services'.format(client.ip))
                        cls._logger.debug('StorageRouter {0}: Added ovs-support-agent service to post-update services'.format(client.ip))
                        cls._logger.debug('StorageRouter {0}: Added GUI and API to downtime'.format(client.ip))

                    elif package_name == PackageFactory.PKG_ARAKOON:
                        if storagerouter.node_type != 'MASTER' or DiskPartition.ROLES.DB not in storagerouter.partition_config or len(storagerouter.partition_config) == 0:
                            # Arakoon only needs to be checked for master nodes with a DB role
                            cls._logger.debug('StorageRouter {0}: This StorageRouter is no MASTER or does not have a {1} role'.format(client.ip, DiskPartition.ROLES.DB))
                            continue

                        # For Arakoon we retrieve the clusters which have been deployed and verify whether they need a restart
                        if component == PackageFactory.COMP_FWK:
                            cluster_names = ['ovsdb', 'config']
                        elif component == PackageFactory.COMP_SD:
                            cluster_names = ['voldrv']
                        else:
                            continue

                        for internal_cluster_name in cluster_names:
                            cls._logger.debug('StorageRouter {0}: Validating Arakoon cluster {1}'.format(client.ip, internal_cluster_name))
                            actual_cluster_name = ArakoonInstaller.get_cluster_name(internal_name=internal_cluster_name)
                            arakoon_service_name = ArakoonInstaller.get_service_name_for_cluster(cluster_name=actual_cluster_name)
                            arakoon_service_version = ServiceFactory.verify_restart_required(client=client, service_name=arakoon_service_name, binary_versions=binaries)
                            cls._logger.debug('StorageRouter {0}: Arakoon service information for cluster {1}: {2}'.format(client.ip, internal_cluster_name, arakoon_service_version))

                            if package_name in pkg_component_info or arakoon_service_version is not None:
                                arakoon_update_info = ArakoonInstaller.get_arakoon_update_info(cluster_name=actual_cluster_name,
                                                                                               ip=StorageRouterList.get_masters()[0].ip if internal_cluster_name == 'config' else None)
                                if arakoon_update_info['internal'] is False:
                                    cls._logger.debug('StorageRouter {0}: Arakoon cluster {1} is externally managed'.format(client.ip, internal_cluster_name))
                                    continue

                                svc_component_info['services_post_update'][10].append('ovs-{0}'.format(arakoon_service_name))
                                cls._logger.debug('StorageRouter {0}: Added service {1} to post-update services'.format(client.ip, 'ovs-{0}'.format(arakoon_service_name)))
                                if arakoon_service_version is not None and PackageFactory.PKG_ARAKOON not in svc_component_info['packages']:
                                    svc_component_info['packages'][PackageFactory.PKG_ARAKOON] = arakoon_service_version
                                if arakoon_update_info['downtime'] is True and [internal_cluster_name, None] not in svc_component_info['downtime']:
                                    svc_component_info['downtime'].append([internal_cluster_name, None])
                                    cls._logger.debug('StorageRouter {0}: Added Arakoon cluster {1} to downtime'.format(client.ip, internal_cluster_name))

                    elif package_name in [PackageFactory.PKG_VOLDRV_BASE, PackageFactory.PKG_VOLDRV_BASE_EE, PackageFactory.PKG_VOLDRV_SERVER, PackageFactory.PKG_VOLDRV_SERVER_EE]:
                        # For VolumeDriver Server we must check the version files too
                        for storagedriver in storagerouter.storagedrivers:
                            vpool_name = storagedriver.vpool.name
                            cls._logger.debug('StorageRouter {0}: Validating StorageDriver {1} for vPool {2}'.format(client.ip, storagedriver.storagedriver_id, vpool_name))
                            if package_name in [PackageFactory.PKG_VOLDRV_SERVER, PackageFactory.PKG_VOLDRV_SERVER_EE]:
                                for prefix, importance in {'dtl': 20, 'volumedriver': 10}.iteritems():
                                    sd_service_name = '{0}_{1}'.format(prefix, vpool_name)
                                    sd_service_version = ServiceFactory.verify_restart_required(client=client, service_name=sd_service_name, binary_versions=binaries)
                                    cls._logger.debug('StorageRouter {0}: Service {1} is running version {2}'.format(client.ip, sd_service_name, sd_service_version))
                                    if package_name in pkg_component_info or sd_service_version is not None:
                                        cls._logger.debug('StorageRouter {0}: Added service {1} to post-update services'.format(client.ip, sd_service_name))
                                        svc_component_info['services_post_update'][importance].append(sd_service_name)
                            # For VolumeDriver base we must add the downtime
                            elif package_name in [PackageFactory.PKG_VOLDRV_BASE, PackageFactory.PKG_VOLDRV_BASE_EE] and package_name in pkg_component_info:
                                if len(storagedriver.vdisks_guids) > 0 and ['voldrv', vpool_name] not in svc_component_info['downtime']:
                                    svc_component_info['downtime'].append(['voldrv', vpool_name])
                                    cls._logger.debug('StorageRouter {0}: Added vPool {1} to downtime'.format(client.ip, vpool_name))

                    # Extend the service information with the package information related to this repository for current StorageRouter
                    if package_name in pkg_component_info and package_name not in svc_component_info['packages']:
                        cls._logger.debug('StorageRouter {0}: Adding package {1} because it has an update available'.format(client.ip, package_name))
                        svc_component_info['packages'][package_name] = pkg_component_info[package_name]
            cls._logger.info('StorageRouter {0}: Refreshed update information'.format(client.ip))
        except Exception as ex:
            cls._logger.exception('StorageRouter {0}: Refreshing update information failed'.format(client.ip))
            if 'errors' not in update_info[client.ip]:
                update_info[client.ip]['errors'] = []
            update_info[client.ip]['errors'].append(ex)

    @classmethod
    @add_hooks('update', 'merge_package_info')
    def _merge_package_information_fwk(cls):
        """
        Retrieve the information stored in the 'package_information' property on the StorageRouter DAL object
        This actually returns all information stored in the 'package_information' property including downtime info, prerequisites, services, ...
        The caller of this function will strip out and merge the relevant package information
        :return: Update information for all StorageRouters
        :rtype: dict
        """
        cls._logger.debug('Retrieving package information for framework')
        update_info = {}
        for storagerouter in StorageRouterList.get_storagerouters():
            update_info[storagerouter.ip] = storagerouter.package_information
        cls._logger.debug('Retrieved package information for framework')
        return update_info

    @classmethod
    @add_hooks('update', 'merge_downtime_info')
    def _merge_downtime_information_fwk(cls):
        """
        Called when the 'Update' button in the GUI is pressed
        This call merges the downtime and prerequisite information present in the 'package_information' property for each StorageRouter DAL object
        :return: Information about prerequisites not met and downtime issues
        :rtype: dict
        """
        cls._logger.debug('Retrieving downtime and prerequisite information for framework')
        merged_update_info = {}
        for storagerouter in StorageRouterList.get_storagerouters():
            for component_name, component_info in storagerouter.package_information.iteritems():
                if component_name not in merged_update_info:
                    merged_update_info[component_name] = {'downtime': [],
                                                          'prerequisites': []}
                for downtime in component_info['downtime']:
                    if downtime not in merged_update_info[component_name]['downtime']:
                        merged_update_info[component_name]['downtime'].append(downtime)
                for prerequisite in component_info['prerequisites']:
                    if prerequisite not in merged_update_info[component_name]['prerequisites']:
                        merged_update_info[component_name]['prerequisites'].append(prerequisite)
        cls._logger.debug('Retrieved downtime and prerequisite information for framework: {0}'.format(merged_update_info))
        return merged_update_info

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
    def _post_update_core(cls, client, components):
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
        :rtype: NoneType
        """
        method_name = inspect.currentframe().f_code.co_name
        cls._logger.info('{0}: Executing hook {1}'.format(client.ip, method_name))

        storagerouter = StorageRouterList.get_by_ip(ip=client.ip)
        for component in components:
            if component not in storagerouter.package_information:
                continue

            component_info = storagerouter.package_information[component]
            if 'packages' not in component_info:
                # Package_information still has the old format, so refresh update information
                # This can occur when updating from earlier than 2.11.0 to 2.11.0 and older
                GenericController.refresh_package_information()
                storagerouter.discard()
                component_info = storagerouter.package_information.get(component, {})

            try:
                ServiceFactory.remove_services_marked_for_removal(client=client,
                                                                  package_names=component_info.get('packages', {}).keys())
            except Exception:
                cls._logger.exception('{0}: Removing the services marked for removal failed'.format(client.ip))

            other_services = set()
            arakoon_services = set()
            for restart_order in sorted(component_info.get('services_post_update', {})):
                for service_name in component_info['services_post_update'][restart_order]:
                    if service_name.startswith('ovs-arakoon-'):
                        arakoon_services.add(service_name)
                    else:
                        other_services.add(service_name)

            UpdateController.change_services_state(services=sorted(other_services), ssh_clients=[client], action='restart')
            for service_name in sorted(arakoon_services):
                try:
                    cluster_name = ArakoonInstaller.get_cluster_name(ExtensionsToolbox.remove_prefix(service_name, 'ovs-arakoon-'))
                    ip = System.get_my_storagerouter().ip if cluster_name == 'config' else None
                    arakoon_metadata = ArakoonInstaller.get_arakoon_update_info(cluster_name=cluster_name, ip=ip)
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
        This is called upon by the Update overview page, to show all updates grouped by IP, being either a StorageRouter, ALBA Node, iSCSI Node, ...
        Merge the package information of all StorageRouters and plugins (ALBA, iSCSI, ...) per IP
        :return: Package information for all StorageRouters and ALBA nodes
        :rtype: dict
        """
        UpdateController._logger.debug('Merging package information')
        merged_package_info = {}
        for fct in Toolbox.fetch_hooks(component='update', sub_component='merge_package_info'):
            package_info = fct()
            merged_package_info = ExtensionsToolbox.merge_dicts(dict1=merged_package_info,
                                                                dict2=package_info)
            UpdateController._logger.debug('Package information for {0} to merge in: {1}'.format(fct.__name__, package_info))
            UpdateController._logger.debug('Package information after {0} merge: {1}'.format(fct.__name__, merged_package_info))

        # The 'package_information' property actually contains all information required for the update
        # This includes services to restart, downtime issues, prerequisites, ...
        # The 'merge_package_information' is called upon when opening the Updates page in the GUI and thus only requires the 'packages' information
        merged_info = {}
        for ip, update_info in merged_package_info.iteritems():
            merged_info[ip] = {}
            for component_name, component_info in update_info.iteritems():
                merged_info[ip][component_name] = component_info['packages']
        UpdateController._logger.debug('Merged package information: {0}'.format(merged_info))
        return merged_info

    @staticmethod
    @ovs_task(name='ovs.update.merge_downtime_information')
    def merge_downtime_information():
        """
        This is called upon by the Update overview page when clicking the 'Update' button to show the prerequisites which have not been met and downtime issues
        Merge the downtime information and prerequisite information of all StorageRouters and plugins (ALBA, iSCSI, ...) per component
        This contains information about
            - downtime of model, GUI, vPools, proxies, Arakoon clusters, ...
            - prerequisites that have not been met
        :return: Information about the update
        :rtype: dict
        """
        UpdateController._logger.debug('Merging downtime and prerequisite information')
        merged_downtime_info = {}
        for fct in Toolbox.fetch_hooks(component='update', sub_component='merge_downtime_info'):
            downtime_info = fct()
            merged_downtime_info = ExtensionsToolbox.merge_dicts(dict1=merged_downtime_info,
                                                                 dict2=downtime_info)
            UpdateController._logger.debug('Downtime and prerequisite information for {0} to merge in: {1}'.format(fct.__name__, downtime_info))
            UpdateController._logger.debug('Downtime and prerequisite information after {0} merge: {1}'.format(fct.__name__, merged_downtime_info))

        # Since every plugin can potentially have common downtime and/or prerequisites, we will filter out the duplicate ones
        merged_info = {}
        for component_name, component_info in merged_downtime_info.iteritems():
            merged_info[component_name] = {'downtime': [],
                                           'prerequisites': []}
            for downtime in component_info['downtime']:
                if downtime not in merged_info[component_name]['downtime']:
                    merged_info[component_name]['downtime'].append(downtime)
            for prerequisite in component_info['prerequisites']:
                if prerequisite not in merged_info[component_name]['prerequisites']:
                    merged_info[component_name]['prerequisites'].append(prerequisite)
        UpdateController._logger.debug('Merged downtime and prerequisite information: {0}'.format(merged_info))
        return merged_info

    @classmethod
    def merge_services_stop_start_information(cls):
        """
        This is called upon by the update logic to retrieve all services to stop and start during update itself
        :return: Service names to stop before update and start after update, stored in dict with keys being the importance of stop order
        :rtype: dict
        """
        UpdateController._logger.debug('Merging services to stop-start information')
        merged_service_info = {}
        for fct in Toolbox.fetch_hooks(component='update', sub_component='merge_package_info'):
            for ip, info in fct().iteritems():
                for component_info in info.itervalues():
                    for importance, service_names in component_info['services_stop_start'].iteritems():
                        importance = int(importance)
                        if importance not in merged_service_info:
                            merged_service_info[importance] = []
                        for service_name in service_names:
                            if service_name not in merged_service_info[importance]:
                                merged_service_info[importance].append(service_name)
        return merged_service_info

    @staticmethod
    @ovs_task(name='ovs.update.get_update_metadata')
    def get_update_metadata(storagerouter_ip):
        """
        Returns metadata required for updating
          - Checks if 'at' is installed properly
          - Checks if ongoing updates are busy
          - Check if StorageRouter is reachable
        :param storagerouter_ip: IP of the StorageRouter to check the metadata for
        :type storagerouter_ip: str
        :return: Update status for specified StorageRouter
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
    @classmethod
    def _package_install_cluster(cls, components):
        """
        Update all the packages related to the specified components which have been stored in the 'package_information' property on the StorageRouter DAL objects
        :param components: Components which have been selected for update
        :type components: list
        :return: Boolean indicating whether to continue with the update or not
        :rtype: bool
        """
        cls._logger.info('Updating packages')
        abort = False
        for storagerouter in StorageRouterList.get_storagerouters():
            cls._logger.debug('StorageRouter {0}: Updating packages'.format(storagerouter.ip))
            try:
                client = SSHClient(endpoint=storagerouter, username='root')
            except UnableToConnectException:
                cls._logger.exception('StorageRouter {0}: Updating packages failed'.format(storagerouter.ip))
                abort = True
                continue

            for component in components:
                packages = storagerouter.package_information.get(component, {}).get('packages', {})
                if len(packages) > 0:
                    cls._logger.debug('StorageRouter {0}: Updating packages for component {1}'.format(storagerouter.ip, component))
                    abort |= PackageFactory.update_packages(client=client, packages=packages)
        cls._logger.info('Updated packages')
        return abort

    @staticmethod
    def execute_update(components):
        """
        Update the specified components on all StorageRouters
        This is called upon by 'at'
        :return: None
        :rtype: NoneType
        """
        UpdateController._logger.info('+++ Starting update +++')
        GenericController.refresh_package_information()  # Not in try - except, because we don't want to start updating if we can't even refresh the update information

        # These prerequisites are refreshed by 'refresh_package_information' and also check whether all StorageRouters are online
        for component, info in UpdateController.merge_downtime_information().iteritems():
            if component in components and len(info['prerequisites']) > 0:
                raise Exception('Not all prerequisites have been met to update component {0}'.format(component))

        # Order the services to stop before update and start after update according to their importance
        service_info = UpdateController.merge_services_stop_start_information()
        services_stop_start = []
        for importance in sorted(service_info):
            services_stop_start.extend(service_info[importance])

        abort = False
        filemutex = file_mutex('system_update', wait=2)
        ssh_clients = []
        errors_during_update = False
        try:
            filemutex.acquire()

            # Create SSHClients to all nodes
            UpdateController._logger.info('Generating SSH client connections for each StorageRouter')
            local_ip = None
            extra_ips = []
            master_ips = []
            for sr in StorageRouterList.get_storagerouters():
                ssh_clients.append(SSHClient(endpoint=sr, username='root'))
                if sr == System.get_my_storagerouter():
                    local_ip = sr.ip
                if sr.node_type == 'MASTER':
                    master_ips.append(sr.ip)
                elif sr.node_type == 'EXTRA':
                    extra_ips.append(sr.ip)

            ssh_clients.sort(key=lambda cl: ExtensionsToolbox.advanced_sort(element=cl.ip, separator='.'))

            # Create locks
            for client in ssh_clients:
                UpdateController._logger.info('{0}: Creating lock files'.format(client.ip))
                client.run(['touch', UpdateController._update_file])  # Prevents manual install or update individual packages
                client.run(['touch', UpdateController._update_ongoing_file])

            # Stop services
            if UpdateController.change_services_state(services=services_stop_start,
                                                      ssh_clients=ssh_clients,
                                                      action='stop') is False:
                raise Exception('Stopping all services on every node failed, cannot continue')

            # Collect the functions to be executed before they get overwritten by updated packages, so on each the same functionality is executed
            package_install_plugins = Toolbox.fetch_hooks(component='update', sub_component='package_install_plugin')
            abort |= UpdateController._package_install_cluster(components=components)  # Install packages on StorageRouters

            # Install packages on plugins (ALBA, iSCSI, ...)
            for _function in package_install_plugins:
                try:
                    abort |= _function(components=components)
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
                            _function(client=client, components=components)
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
