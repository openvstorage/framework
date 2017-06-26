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
from api.backend.exceptions import HttpNotAcceptableException
from api.backend.serializers.serializers import FullSerializer
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.j_storagerouterdomain import StorageRouterDomain
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.lib.disk import DiskController
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.generic import GenericController
from ovs.lib.storagedriver import StorageDriverController
from ovs.lib.storagerouter import StorageRouterController
from ovs.lib.update import UpdateController
from ovs.lib.vdisk import VDiskController


class StorageRouterViewSet(viewsets.ViewSet):
    """
    Information about Storage Routers
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'storagerouters'
    base_name = 'storagerouters'

    DOMAIN_CHANGE_KEY = 'ovs_dedupe_domain_change'

    @log()
    @required_roles(['read', 'manage'])
    @return_list(StorageRouter, 'name')
    @load()
    def list(self, query=None):
        """
        Overview of all Storage Routers
        :param query: A query to filter the StorageRouters
        :type query: DataQuery
        """
        if query is None:
            return StorageRouterList.get_storagerouters()
        else:
            return DataList(StorageRouter, query)

    @log()
    @required_roles(['read', 'manage'])
    @return_object(StorageRouter)
    @load(StorageRouter)
    def retrieve(self, storagerouter):
        """
        Load information about a given storage router
        :param storagerouter: StorageRouter to return
        :type storagerouter: StorageRouter
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
        :type storagerouter: StorageRouter
        :param request: The raw Request
        :type request: Request
        :param contents: Contents to be updated/returned
        :type contents: str
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
        :type storagerouter: StorageRouter
        """
        return StorageDriverController.mark_offline.delay(storagerouter.guid)

    @action()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def get_metadata(self, storagerouter):
        """
        Returns a list of mountpoints on the given Storage Router
        :param storagerouter: StorageRouter to get the metadata from
        :type storagerouter: StorageRouter
        """
        return StorageRouterController.get_metadata.delay(storagerouter.guid)

    @link()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def get_version_info(self, storagerouter):
        """
        Gets version information of a given Storage Router
        :param storagerouter: StorageRouter to get the versions from
        :type storagerouter: StorageRouter
        """
        return StorageRouterController.get_version_info.delay(storagerouter.guid)

    @link()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def get_support_info(self, storagerouter):
        """
        Gets support information of a given Storage Router
        :param storagerouter: StorageRouter to get the support info from
        :type storagerouter: StorageRouter
        """
        return StorageRouterController.get_support_info.s(storagerouter.guid).apply_async(
            routing_key='sr.{0}'.format(storagerouter.machine_id)
        )

    @link()
    @log()
    @required_roles(['read', 'manage'])
    @return_task()
    @load(StorageRouter)
    def get_proxy_config(self, storagerouter, vpool_guid):
        """
        Gets the ALBA proxy for a given Storage Router and vPool
        :param storagerouter: StorageRouter on which the ALBA proxy is configured
        :type storagerouter: StorageRouter
        :param vpool_guid: Guid of the vPool for which the proxy is configured
        :type vpool_guid: str
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
        Create the required configuration files to be able to make use of HPRM (aka PRACC)
        These configuration will be zipped and made available for download
        :param local_storagerouter: StorageRouter this call is executed on
        :type local_storagerouter: StorageRouter
        :param storagerouter: The StorageRouter for which a HPRM manager needs to be deployed
        :type storagerouter: StorageRouter
        :param parameters: Additional information required for the HPRM configuration files
        :type parameters: dict
        :return: Asynchronous result of a CeleryTask
        :rtype: celery.result.AsyncResult
        """
        return StorageRouterController.create_hprm_config_files.delay(parameters=parameters,
                                                                      storagerouter_guid=storagerouter.guid,
                                                                      local_storagerouter_guid=local_storagerouter.guid)

    @link()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def get_support_metadata(self, storagerouter):
        """
        Gets support metadata of a given Storage Router
        :param storagerouter: StorageRouter to get the support metadata from
        :type storagerouter: StorageRouter
        """
        return StorageRouterController.get_support_metadata.apply_async(
            routing_key='sr.{0}'.format(storagerouter.machine_id)
        )

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def configure_support(self, enable, enable_support):
        """
        Configures support
        :param enable: Indicates whether to enable heartbeats
        :type enable: bool
        :param enable_support: Indicates whether to enable remote support
        :type enable_support: bool
        """
        return StorageRouterController.configure_support.delay(enable, enable_support)

    @link()
    @log()
    @required_roles(['read', 'manage'])
    @return_task()
    @load(StorageRouter)
    def get_logfiles(self, local_storagerouter, storagerouter):
        """
        Collects logs, moves them to a web-accessible location and returns log tgz's filename
        :param local_storagerouter: StorageRouter this call is executed on (to store the logfiles on)
        :type local_storagerouter: StorageRouter
        :param storagerouter: The StorageRouter to collect the logs from
        :type storagerouter: StorageRouter
        """
        return StorageRouterController.get_logfiles.s(local_storagerouter.guid).apply_async(
            routing_key='sr.{0}'.format(storagerouter.machine_id)
        )

    @action()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter, max_version=5)
    def check_s3(self, host, port, accesskey, secretkey):
        """
        Validates whether connection to a given S3 backend can be made
        :param host: The host of an S3 endpoint
        :type host: str
        :param port: The port of an S3 endpoint
        :type port: int
        :param accesskey: The accesskey to be used when validating the S3 endpoint
        :type accesskey: str
        :param secretkey: The secretkey to be used when validating the S3 endpoint
        :type secretkey: str
        """
        parameters = {'host': host,
                      'port': port,
                      'accesskey': accesskey,
                      'secretkey': secretkey}
        for field in parameters:
            if not isinstance(parameters[field], int):
                parameters[field] = str(parameters[field])
        return StorageRouterController.check_s3.delay(**parameters)

    @action()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def check_mtpt(self, storagerouter, name):
        """
        Validates whether the mountpoint for a vPool is available
        :param storagerouter: The StorageRouter to validate the mountpoint on
        :type storagerouter: StorageRouter
        :param name: The name of the mountpoint to validate (vPool name)
        :type name: str
        """
        return StorageRouterController.mountpoint_exists.delay(name=str(name), storagerouter_guid=storagerouter.guid)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def add_vpool(self, storagerouter, call_parameters, version, local_storagerouter, request):
        """
        Adds a vPool to a given Storage Router
        :param storagerouter: StorageRouter to add the vPool to
        :type storagerouter: StorageRouter
        :param call_parameters: A complex (JSON encoded) dictionary containing all various parameters to create the vPool
        :type call_parameters: dict
        :param version: Client version
        :type version: int
        :param local_storagerouter: StorageRouter on which the call is executed
        :type local_storagerouter: StorageRouter
        :param request: The raw request
        :type request: Request
        """
        def _validate_required_keys(section):
            for required_key in ['host', 'backend']:
                if required_key not in call_parameters[section]:
                    raise HttpNotAcceptableException(error_description='Invalid data passed: "{0}" misses information about {1}'.format(section, required_key),
                                                     error='invalid_data')
            for sub_required_key in ['backend', 'metadata']:
                if sub_required_key not in call_parameters[section]['backend']:
                    raise HttpNotAcceptableException(error_description='Invalid data passed: "{0}" missing information about {1}'.format(section, sub_required_key),
                                                     error='invalid_data')

        # API backwards compatibility
        if version <= 2:
            call_parameters['storagerouter_ip'] = storagerouter.ip
            call_parameters['fragment_cache_on_read'] = True
            call_parameters['fragment_cache_on_write'] = False
            call_parameters['backend_connection_info'] = {'host': call_parameters.pop('connection_host'),
                                                          'port': call_parameters.pop('connection_port'),
                                                          'username': call_parameters.pop('connection_username'),
                                                          'password': call_parameters.pop('connection_password')}
            if 'connection_backend' in call_parameters:
                connection_backend = call_parameters.pop('connection_backend')
                call_parameters['backend_connection_info']['backend'] = {'backend': connection_backend.pop('backend') if 'backend' in connection_backend else None,
                                                                         'metadata': connection_backend.pop('metadata') if 'metadata' in connection_backend else None}
        if version < 6:
            if 'backend_connection_info' not in call_parameters:
                raise HttpNotAcceptableException(error_description='Invalid data passed: "backend_connection_info" should be passed',
                                                 error='invalid_data')
            _validate_required_keys(section='backend_connection_info')
            if 'backend_info' not in call_parameters:
                call_parameters['backend_info'] = {}
            if 'connection_info' not in call_parameters:
                call_parameters['connection_info'] = {}
            call_parameters['backend_info']['preset'] = call_parameters['backend_connection_info']['backend']['metadata']
            call_parameters['backend_info']['alba_backend_guid'] = call_parameters['backend_connection_info']['backend']['backend']
            call_parameters['connection_info']['host'] = call_parameters['backend_connection_info']['host']
            call_parameters['connection_info']['port'] = call_parameters['backend_connection_info'].get('port', '')
            call_parameters['connection_info']['client_id'] = call_parameters['backend_connection_info'].get('username', '')
            call_parameters['connection_info']['client_secret'] = call_parameters['backend_connection_info'].get('password', '')
            del call_parameters['backend_connection_info']

            if 'backend_connection_info_aa' in call_parameters:
                if 'backend_info_fc' not in call_parameters:
                    call_parameters['backend_info_fc'] = {}
                if 'connection_info_fc' not in call_parameters:
                    call_parameters['connection_info_fc'] = {}
                _validate_required_keys(section='backend_connection_info_aa')
                call_parameters['backend_info_fc']['preset'] = call_parameters['backend_connection_info_aa']['backend']['metadata']
                call_parameters['backend_info_fc']['alba_backend_guid'] = call_parameters['backend_connection_info_aa']['backend']['backend']
                call_parameters['connection_info_fc']['host'] = call_parameters['backend_connection_info_aa']['host']
                call_parameters['connection_info_fc']['port'] = call_parameters['backend_connection_info_aa'].get('port', '')
                call_parameters['connection_info_fc']['client_id'] = call_parameters['backend_connection_info_aa'].get('username', '')
                call_parameters['connection_info_fc']['client_secret'] = call_parameters['backend_connection_info_aa'].get('password', '')
                del call_parameters['backend_connection_info_aa']

        if version >= 6 and 'backend_connection_info' in call_parameters:
            raise HttpNotAcceptableException(error_description='Invalid data passed: "backend_connection_info" is deprecated',
                                             error='invalid_data')

        # API client translation (cover "local backend" selection in GUI)
        if 'backend_info' not in call_parameters or 'connection_info' not in call_parameters or 'config_params' not in call_parameters:
            raise HttpNotAcceptableException(error_description='Invalid call_parameters passed',
                                             error='invalid_data')
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
                raise HttpNotAcceptableException(error_description='Invalid call_parameters passed',
                                                 error='invalid_data')
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
        return StorageRouterController.add_vpool.delay(call_parameters)

    @link()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter, max_version=6)
    def get_update_status(self, storagerouter):
        """
        Return available updates for framework, volumedriver, ...
        :param storagerouter: StorageRouter to get the update information from
        :type storagerouter: StorageRouter
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
        :type storagerouter: StorageRouter
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
        :param storagerouter: StorageRouter to start the update on
        :type storagerouter: StorageRouter
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
        :param storagerouter: StorageRouter to start the update on
        :type storagerouter: StorageRouter
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
        :type storagerouter: StorageRouter
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
        """
        return StorageRouterController.configure_disk.delay(storagerouter.guid, disk_guid, partition_guid, offset, size, roles)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def rescan_disks(self, storagerouter):
        """
        Triggers a disk sync on the given storagerouter
        :param storagerouter: StorageRouter on which to rescan all disks
        :type storagerouter: StorageRouter
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
        :type storagerouter: StorageRouter
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
        :type storagerouter: StorageRouter
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
        """
        return UpdateController.get_update_information_all.delay()
