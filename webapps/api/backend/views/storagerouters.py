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
StorageRouter module
"""

from celery.task.control import revoke
from rest_framework import viewsets
from rest_framework.decorators import action, link
from rest_framework.permissions import IsAuthenticated
from api.backend.decorators import required_roles, return_list, return_object, return_task, return_simple, load, log
from api.backend.serializers.serializers import FullSerializer
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.j_storagerouterdomain import StorageRouterDomain
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs_extensions.api.exceptions import HttpNotAcceptableException
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.lib.disk import DiskController
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.generic import GenericController
from ovs.lib.helpers.toolbox import Toolbox
from ovs.lib.storagedriver import StorageDriverController
from ovs.lib.storagerouter import StorageRouterController
from ovs.lib.update import UpdateController
from ovs.lib.vdisk import VDiskController
from ovs.lib.vpool import VPoolController


class StorageRouterViewSet(viewsets.ViewSet):
    """
    Information about StorageRouters
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'storagerouters'
    base_name = 'storagerouters'

    DOMAIN_CHANGE_KEY = 'ovs_dedupe_domain_change'

    @log()
    @required_roles(['read', 'manage'])
    @return_list(StorageRouter, 'name')
    @load()
    def list(self):
        """
        Overview of all StorageRouters
        :return: List of StorageRouters
        :rtype: list[ovs.dal.hybrids.storagerouter.StorageRouter]
        """
        return StorageRouterList.get_storagerouters()

    @log()
    @required_roles(['read', 'manage'])
    @return_object(StorageRouter)
    @load(StorageRouter)
    def retrieve(self, storagerouter):
        """
        Load information about a given StorageRouter
        :param storagerouter: StorageRouter to return
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :return: The StorageRouter requested
        :rtype: ovs.dal.hybrids.storagerouter.StorageRouter
        """
        return storagerouter

    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_object(StorageRouter, mode='accepted')
    @load(StorageRouter)
    def partial_update(self, storagerouter, request, contents=None):
        """
        Update a StorageRouter
        :param storagerouter: StorageRouter to update
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :param request: The raw Request
        :type request: Request
        :param contents: Contents to be updated/returned
        :type contents: str
        :return: The StorageRouter updated
        :rtype: ovs.dal.hybrids.storagerouter.StorageRouter
        """
        contents = None if contents is None else contents.split(',')
        serializer = FullSerializer(StorageRouter, contents=contents, instance=storagerouter, data=request.DATA)
        storagerouter = serializer.deserialize()
        storagerouter.save()
        return storagerouter

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def mark_offline(self, storagerouter):
        """
        Marks all StorageDrivers of a given node offline. DO NOT USE ON RUNNING STORAGEROUTERS!
        :param storagerouter: StorageRouter to mark offline
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return StorageDriverController.mark_offline.delay(storagerouter.guid)

    @action()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def get_metadata(self, storagerouter):
        """
        Returns a list of mount points on the given StorageRouter
        :param storagerouter: StorageRouter to get the metadata from
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return StorageRouterController.get_metadata.delay(storagerouter.guid)

    @link()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def get_version_info(self, storagerouter):
        """
        DEPRECATED API CALL
        Gets version information of a given StorageRouter
        :param storagerouter: StorageRouter to get the versions from
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return StorageRouterController.get_version_info.delay(storagerouter.guid)

    @link()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def get_support_info(self):
        """
        Returns support information for the entire cluster
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return StorageRouterController.get_support_info.delay()

    @link()
    @log()
    @required_roles(['read', 'manage'])
    @return_task()
    @load(StorageRouter)
    def get_proxy_config(self, storagerouter, vpool_guid):
        """
        Gets the ALBA proxy for a given StorageRouter and vPool
        :param storagerouter: StorageRouter on which the ALBA proxy is configured
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :param vpool_guid: Guid of the vPool for which the proxy is configured
        :type vpool_guid: str
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return StorageRouterController.get_proxy_config.delay(vpool_guid=vpool_guid,
                                                              storagerouter_guid=storagerouter.guid)

    @link()
    @log()
    @required_roles(['read', 'manage'])
    @return_task()
    @load(StorageRouter)
    def create_hprm_config_files(self, local_storagerouter, storagerouter, parameters):
        """
        DEPRECATED API CALL - USE /vpool/vpool_guid/create_hprm_config_files instead
        Create the required configuration files to be able to make use of HPRM (aka PRACC)
        These configuration will be zipped and made available for download
        :param local_storagerouter: StorageRouter this call is executed on
        :type local_storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :param storagerouter: The StorageRouter for which a HPRM manager needs to be deployed
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :param parameters: Additional information required for the HPRM configuration files
        :type parameters: dict
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        _ = storagerouter
        Toolbox.verify_required_params(actual_params=parameters,
                                       required_params={'vpool_guid': (str, Toolbox.regex_guid)})
        return VPoolController.create_hprm_config_files.delay(parameters=parameters,
                                                              vpool_guid=parameters['vpool_guid'],
                                                              local_storagerouter_guid=local_storagerouter.guid)

    @link()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def get_support_metadata(self, storagerouter):
        """
        Gets support metadata of a given StorageRouter
        :param storagerouter: StorageRouter to get the support metadata from
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return StorageRouterController.get_support_metadata.apply_async(
            routing_key='sr.{0}'.format(storagerouter.machine_id)
        )

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def configure_support(self, support_info):
        """
        Configures support on all StorageRouters
        :param support_info: Information about which components should be configured
            {'stats_monkey': True,  # Enable/disable the stats monkey scheduled task
             'support_agent': True,  # Responsible for enabling the ovs-support-agent service, which collects heart beat data
             'remote_access': False,  # Cannot be True when support agent is False. Is responsible for opening an OpenVPN tunnel to allow for remote access
             'stats_monkey_config': {}}  # Dict with information on how to configure the stats monkey (Only required when enabling the stats monkey
        :type support_info: dict
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return StorageRouterController.configure_support.delay(support_info=support_info)

    @link()
    @log()
    @required_roles(['read', 'manage'])
    @return_task()
    @load(StorageRouter)
    def get_logfiles(self, local_storagerouter, storagerouter):
        """
        Collects logs, moves them to a web-accessible location and returns log TGZs filename
        :param local_storagerouter: StorageRouter this call is executed on (to store the log files on)
        :type local_storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :param storagerouter: The StorageRouter to collect the logs from
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return StorageRouterController.get_logfiles.s(local_storagerouter.guid).apply_async(
            routing_key='sr.{0}'.format(storagerouter.machine_id)
        )

    @action()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def check_mtpt(self, storagerouter, name):
        """
        Validates whether the mount point for a vPool is available
        :param storagerouter: The StorageRouter to validate the mount point on
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :param name: The name of the mount point to validate (vPool name)
        :type name: str
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return StorageRouterController.mountpoint_exists.delay(name=str(name), storagerouter_guid=storagerouter.guid)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def add_vpool(self, call_parameters, local_storagerouter, request):
        """
        Adds a vPool to a given StorageRouter
        :param call_parameters: A complex (JSON encoded) dictionary containing all various parameters to create the vPool
        :type call_parameters: dict
        :param local_storagerouter: StorageRouter on which the call is executed
        :type local_storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :param request: The raw request
        :type request: Request
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        # API backwards compatibility
        if 'backend_connection_info' in call_parameters:
            raise HttpNotAcceptableException(error='invalid_data',
                                             error_description='Invalid data passed: "backend_connection_info" is deprecated')

        # API client translation (cover "local backend" selection in GUI)
        if 'backend_info' not in call_parameters or 'connection_info' not in call_parameters or 'config_params' not in call_parameters:
            raise HttpNotAcceptableException(error='invalid_data',
                                             error_description='Invalid call_parameters passed')
        connection_info = call_parameters['connection_info']
        if 'backend_info_aa' in call_parameters:
            # Backwards compatibility
            call_parameters['backend_info_fc'] = call_parameters['backend_info_aa']
            del call_parameters['backend_info_aa']
        if 'connection_info_aa' in call_parameters:
            # Backwards compatibility
            call_parameters['connection_info_fc'] = call_parameters['connection_info_aa']
            del call_parameters['connection_info_aa']
        connection_info_fc = call_parameters.get('connection_info_fc')
        connection_info_bc = call_parameters.get('connection_info_bc')
        if connection_info['host'] == '' or (connection_info_fc is not None and connection_info_fc['host'] == '') or \
                                            (connection_info_bc is not None and connection_info_bc['host'] == ''):
            client = None
            for _client in request.client.user.clients:
                if _client.ovs_type == 'INTERNAL' and _client.grant_type == 'CLIENT_CREDENTIALS':
                    client = _client
            if client is None:
                raise HttpNotAcceptableException(error='invalid_data',
                                                 error_description='Invalid call_parameters passed')
            if connection_info['host'] == '':
                connection_info['client_id'] = client.client_id
                connection_info['client_secret'] = client.client_secret
                connection_info['host'] = local_storagerouter.ip
                connection_info['port'] = 443
                connection_info['local'] = True
            if connection_info_fc is not None and connection_info_fc['host'] == '':
                connection_info_fc['client_id'] = client.client_id
                connection_info_fc['client_secret'] = client.client_secret
                connection_info_fc['host'] = local_storagerouter.ip
                connection_info_fc['port'] = 443
                connection_info_fc['local'] = True
            if connection_info_bc is not None and connection_info_bc['host'] == '':
                connection_info_bc['client_id'] = client.client_id
                connection_info_bc['client_secret'] = client.client_secret
                connection_info_bc['host'] = local_storagerouter.ip
                connection_info_bc['port'] = 443
                connection_info_bc['local'] = True

        if 'block_cache_on_read' not in call_parameters:
            call_parameters['block_cache_on_read'] = False
        if 'block_cache_on_write' not in call_parameters:
            call_parameters['block_cache_on_write'] = False

        call_parameters.pop('type', None)
        call_parameters.pop('readcache_size', None)
        call_parameters['config_params'].pop('dedupe_mode', None)
        call_parameters['config_params'].pop('cache_strategy', None)

        # Finally, launching the add_vpool task
        return StorageRouterController.add_vpool.delay(StorageRouterController, call_parameters)

    @link()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter, max_version=6)
    def get_update_status(self, storagerouter):
        """
        Return available updates for framework, volumedriver, ...
        DEPRECATED API call
        :param storagerouter: StorageRouter to get the update information from
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        update_info = UpdateController.get_update_information_core({})
        framework_info = update_info.pop('framework', None)
        storagedriver_info = update_info.pop('storagedriver', None)

        return_value = {'upgrade_ongoing': UpdateController.get_update_metadata(storagerouter_ip=storagerouter.ip)['update_ongoing']}
        if framework_info is not None and framework_info['packages']:
            return_value['framework'] = []
            for pkg_name, pkg_info in framework_info['packages'].iteritems():
                return_value['framework'].append({'to': pkg_info['candidate'],
                                                  'name': pkg_name,
                                                  'gui_down': True,
                                                  'downtime': framework_info['downtime'],
                                                  'namespace': 'ovs',
                                                  'prerequisites': framework_info['prerequisites']})
        if storagedriver_info is not None and storagedriver_info['packages']:
            return_value['storagedriver'] = []
            for pkg_name, pkg_info in storagedriver_info['packages'].iteritems():
                return_value['storagedriver'].append({'to': pkg_info['candidate'],
                                                      'name': pkg_name,
                                                      'gui_down': False,
                                                      'downtime': storagedriver_info['downtime'],
                                                      'namespace': 'ovs',
                                                      'prerequisites': storagedriver_info['prerequisites']})

        for plugin_name, info in update_info.iteritems():
            if info['packages']:
                return_value[plugin_name] = []
                for pkg_name, pkg_info in info['packages'].iteritems():
                    return_value[plugin_name].append({'to': pkg_info['candidate'],
                                                      'name': pkg_name,
                                                      'gui_down': False,
                                                      'downtime': info['downtime'],
                                                      'namespace': plugin_name,
                                                      'prerequisites': info['prerequisites']})
        return return_value

    @link()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def get_update_metadata(self, storagerouter):
        """
        Returns metadata required for updating
          - Checks if 'at' can be used properly
          - Checks if ongoing updates are busy
        :param storagerouter: StorageRouter to get the update metadata from
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return UpdateController.get_update_metadata.delay(storagerouter.ip)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter, max_version=6)
    def update_framework(self, storagerouter):
        """
        Initiate a task on the given StorageRouter to update the framework on ALL StorageRouters
        DEPRECATED API call - use update_components in the future
        :param storagerouter: StorageRouter to start the update on
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        _ = storagerouter
        return UpdateController.update_components.delay(components=['framework'])

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter, max_version=6)
    def update_volumedriver(self, storagerouter):
        """
        Initiate a task on the given StorageRouter to update the volumedriver on ALL StorageRouters
        DEPRECATED API call - use update_components in the future
        :param storagerouter: StorageRouter to start the update on
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        _ = storagerouter
        return UpdateController.update_components.delay(components=['storagedriver'])

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def update_components(self, components):
        """
        Initiate a task on a StorageRouter to update the specified components on ALL StorageRouters
        :param components: Components to update
        :type components: list
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return UpdateController.update_components.delay(components=components)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def configure_disk(self, storagerouter, disk_guid, offset, size, roles, partition_guid=None):
        """
        Configures a disk on a StorageRouter
        :param storagerouter: StorageRouter on which to configure the disk
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :param disk_guid: The GUID of the Disk to configure
        :type disk_guid: str
        :param offset: The offset of the partition to configure
        :type offset: int
        :param size: The size of the partition to configure
        :type size: int
        :param roles: A list of all roles to be assigned
        :type roles: list
        :param partition_guid: The guid of the partition if applicable
        :type partition_guid: str
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return StorageRouterController.configure_disk.delay(storagerouter.guid, disk_guid, partition_guid, offset, size, roles)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def rescan_disks(self, storagerouter):
        """
        Triggers a disk sync on the given StorageRouter
        :param storagerouter: StorageRouter on which to rescan all disks
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return DiskController.sync_with_reality.delay(storagerouter.guid)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def refresh_hardware(self, storagerouter):
        """
        Refreshes all hardware parameters
        :param storagerouter: StorageRouter on which to refresh all hardware capabilities
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return StorageRouterController.refresh_hardware.delay(storagerouter.guid)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_simple()
    @load(StorageRouter)
    def set_domains(self, storagerouter, domain_guids, recovery_domain_guids):
        """
        Configures the given domains to the StorageRouter.
        :param storagerouter: The StorageRouter to update
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :param domain_guids: A list of Domain guids
        :type domain_guids: list
        :param recovery_domain_guids: A list of Domain guids to set as recovery Domain
        :type recovery_domain_guids: list
        :return: None
        :rtype: None
        """
        change = False
        for junction in storagerouter.domains:
            if junction.backup is False:
                if junction.domain_guid not in domain_guids:
                    junction.delete()
                    change = True
                else:
                    domain_guids.remove(junction.domain_guid)
            else:
                if junction.domain_guid not in recovery_domain_guids:
                    junction.delete()
                    change = True
                else:
                    recovery_domain_guids.remove(junction.domain_guid)
        for domain_guid in domain_guids + recovery_domain_guids:
            junction = StorageRouterDomain()
            junction.domain = Domain(domain_guid)
            junction.backup = domain_guid in recovery_domain_guids
            junction.storagerouter = storagerouter
            junction.save()
            change = True

        # Schedule a task to run after 60 seconds, re-schedule task if another identical task gets triggered
        if change is True:
            cache = VolatileFactory.get_client()
            task_ids = cache.get(StorageRouterViewSet.DOMAIN_CHANGE_KEY)
            if task_ids:
                for task_id in task_ids:
                    revoke(task_id)
            task_ids = [MDSServiceController.mds_checkup.s().apply_async(countdown=60).id,
                        VDiskController.dtl_checkup.s().apply_async(countdown=60).id,
                        StorageDriverController.cluster_registry_checkup.s().apply_async(countdown=60).id]
            cache.set(StorageRouterViewSet.DOMAIN_CHANGE_KEY, task_ids, 600)  # Store the task ids
            storagerouter.invalidate_dynamics(['regular_domains', 'recovery_domains'])

    @link()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def merge_package_information(self):
        """
        Retrieve the package information from the model for both StorageRouters and ALBA Nodes and merge it
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return UpdateController.merge_package_information.delay()

    @link()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def refresh_package_information(self):
        """
        Refresh the updates for all StorageRouters
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return GenericController.refresh_package_information.delay()

    @link()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def get_update_information(self):
        """
        Retrieve the update information for all StorageRouters
        This contains information about
            - downtime of model, GUI, vPools, proxies, ...
            - services that will be restarted
            - packages that will be updated
            - prerequisites that have not been met
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return UpdateController.merge_downtime_information.delay()
