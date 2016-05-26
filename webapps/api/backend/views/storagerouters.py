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

import json
from celery.task.control import revoke
from backend.decorators import required_roles, return_list, return_object, return_task, return_plain, load, log
from backend.serializers.serializers import FullSerializer
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.j_storagerouterdomain import StorageRouterDomain
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.lib.disk import DiskController
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.storagedriver import StorageDriverController
from ovs.lib.storagerouter import StorageRouterController
from ovs.lib.vdisk import VDiskController
from rest_framework import viewsets, status
from rest_framework.decorators import action, link
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response


class StorageRouterViewSet(viewsets.ViewSet):
    """
    Information about Storage Routers
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'storagerouters'
    base_name = 'storagerouters'

    DOMAIN_CHANGE_KEY = 'ovs_dedupe_domain_change'
    RECOVERY_DOMAIN_CHANGE_KEY = 'ovs_dedupe_recovery_domain_change'

    @log()
    @required_roles(['read'])
    @return_list(StorageRouter, 'name')
    @load()
    def list(self, query=None):
        """
        Overview of all Storage Routers
        """
        if query is None:
            return StorageRouterList.get_storagerouters()
        else:
            query = json.loads(query)
            return DataList(StorageRouter, query)

    @log()
    @required_roles(['read'])
    @return_object(StorageRouter)
    @load(StorageRouter)
    def retrieve(self, storagerouter):
        """
        Load information about a given storage router
        """
        return storagerouter

    @log()
    @required_roles(['read', 'write', 'manage'])
    @load(StorageRouter)
    def partial_update(self, storagerouter, request, contents=None):
        """
        Update a StorageRouter
        """
        contents = None if contents is None else contents.split(',')
        serializer = FullSerializer(StorageRouter, contents=contents, instance=storagerouter, data=request.DATA)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_202_ACCEPTED)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def move_away(self, storagerouter):
        """
        Moves away all vDisks from all Storage Drivers this Storage Router is serving
        """
        return StorageDriverController.move_away.delay(storagerouter.guid)

    @link()
    @log()
    @required_roles(['read'])
    @return_plain()
    @load(StorageRouter)
    def get_available_actions(self):
        """
        Gets a list of all available actions
        """
        actions = []
        storagerouters = StorageRouterList.get_storagerouters()
        if len(storagerouters) > 1:
            actions.append('MOVE_AWAY')
        return actions

    @action()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def get_metadata(self, storagerouter):
        """
        Returns a list of mountpoints on the given Storage Router
        """
        return StorageRouterController.get_metadata.s(storagerouter.guid).apply_async(
            routing_key='sr.{0}'.format(storagerouter.machine_id)
        )

    @link()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def get_version_info(self, storagerouter):
        """
        Gets version information of a given Storage Router
        """
        return StorageRouterController.get_version_info.s(storagerouter.guid).apply_async(
            routing_key='sr.{0}'.format(storagerouter.machine_id)
        )

    @link()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def get_support_info(self, storagerouter):
        """
        Gets support information of a given Storage Router
        """
        return StorageRouterController.get_support_info.s(storagerouter.guid).apply_async(
            routing_key='sr.{0}'.format(storagerouter.machine_id)
        )

    @link()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def get_support_metadata(self, storagerouter):
        """
        Gets support metadata of a given Storage Router
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
        """
        return StorageRouterController.get_logfiles.s(local_storagerouter.guid).apply_async(
            routing_key='sr.{0}'.format(storagerouter.machine_id)
        )

    @action()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def check_s3(self, host, port, accesskey, secretkey):
        """
        Validates whether connection to a given S3 backend can be made
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
        """
        name = str(name)
        return StorageRouterController.check_mtpt.s(name).apply_async(
            routing_key='sr.{0}'.format(storagerouter.machine_id)
        )

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def add_vpool(self, storagerouter, call_parameters, version):
        """
        Adds a vPool to a given Storage Router
        """
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
        return StorageRouterController.add_vpool.delay(call_parameters)

    @link()
    @log()
    @required_roles(['read'])
    @return_plain()
    @load(StorageRouter)
    def get_mgmtcenter_info(self, storagerouter):
        """
        Return mgmtcenter info (ip, username, name, type)
        """
        data = {}
        mgmtcenter = storagerouter.pmachine.mgmtcenter
        if mgmtcenter:
            data = {'ip': mgmtcenter.ip,
                    'username': mgmtcenter.username,
                    'name': mgmtcenter.name,
                    'type': mgmtcenter.type}
        return data

    @link()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def get_update_status(self, storagerouter):
        """
        Return available updates for framework, volumedriver, ...
        """
        return StorageRouterController.get_update_status.delay(storagerouter.ip)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def update_framework(self, storagerouter):
        """
        Initiate a task on 1 storagerouter to update the framework on ALL storagerouters
        """
        return StorageRouterController.update_framework.delay(storagerouter.ip)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def update_volumedriver(self, storagerouter):
        """
        Initiate a task on 1 storagerouter to update the volumedriver on ALL storagerouters
        """
        return StorageRouterController.update_volumedriver.delay(storagerouter.ip)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def configure_disk(self, storagerouter, disk_guid, offset, size, roles, partition_guid=None):
        """
        Configures a disk on a StorageRouter
        """
        return StorageRouterController.configure_disk.s(
            storagerouter.guid, disk_guid, partition_guid, offset, size, roles
        ).apply_async(routing_key='sr.{0}'.format(storagerouter.machine_id))

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_task()
    @load(StorageRouter)
    def rescan_disks(self, storagerouter):
        """
        Triggers a disk sync on the given storagerouter
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
        """
        return StorageRouterController.refresh_hardware.delay(storagerouter.guid)

    @action()
    @log()
    @required_roles(['read', 'write', 'manage'])
    @return_plain()
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
            task_id_domain = cache.get(StorageRouterViewSet.DOMAIN_CHANGE_KEY)
            task_id_backup = cache.get(StorageRouterViewSet.RECOVERY_DOMAIN_CHANGE_KEY)
            if task_id_domain:
                revoke(task_id_domain)  # If key exists, task was already scheduled. If task is already running, the revoke message will be ignored
            if task_id_backup:
                revoke(task_id_backup)
            async_mds_result = MDSServiceController.mds_checkup.s().apply_async(countdown=60)
            async_dtl_result = VDiskController.dtl_checkup.s().apply_async(countdown=60)
            cache.set(StorageRouterViewSet.DOMAIN_CHANGE_KEY, async_mds_result.id, 600)  # Store the task id
            cache.set(StorageRouterViewSet.RECOVERY_DOMAIN_CHANGE_KEY, async_dtl_result.id, 600)  # Store the task id
            storagerouter.invalidate_dynamics(['regular_domains', 'recovery_domains'])
