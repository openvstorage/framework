# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
StorageRouter module
"""

import json
from celery.task.control import revoke
from rest_framework import viewsets, status
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import action, link
from rest_framework.exceptions import NotAcceptable
from rest_framework.response import Response
from backend.serializers.serializers import FullSerializer
from ovs.extensions.storage.volatilefactory import VolatileFactory
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.lib.storagerouter import StorageRouterController
from ovs.lib.storagedriver import StorageDriverController
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.disk import DiskController
from backend.decorators import required_roles, return_list, return_object, return_task, return_plain, load, log


class StorageRouterViewSet(viewsets.ViewSet):
    """
    Information about Storage Routers
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'storagerouters'
    base_name = 'storagerouters'

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
            query_result = DataList({'object': StorageRouter,
                                     'data': DataList.select.GUIDS,
                                     'query': query}).data
            return DataObjectList(query_result, StorageRouter)

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
        previous_primary = storagerouter.primary_failure_domain
        previous_secondary = storagerouter.secondary_failure_domain
        serializer = FullSerializer(StorageRouter, contents=contents, instance=storagerouter, data=request.DATA)
        if serializer.is_valid():
            primary = storagerouter.primary_failure_domain
            secondary = storagerouter.secondary_failure_domain
            if primary is None:
                raise NotAcceptable('A StorageRouter must have a primary FD configured')
            if secondary is not None:
                if primary.guid == secondary.guid:
                    raise NotAcceptable('A StorageRouter cannot have the same FD for both primary and secondary')
                if len(secondary.primary_storagerouters) == 0:
                    raise NotAcceptable('The secondary FD should be set as primary FD by at least one StorageRouter')
            if len(previous_primary.secondary_storagerouters) > 0 and len(previous_primary.primary_storagerouters) == 1 and \
                    previous_primary.primary_storagerouters[0].guid == storagerouter.guid and previous_primary.guid != primary.guid:
                raise NotAcceptable('Cannot change the primary FD as this StorageRouter is the only one serving it while it is used as secondary FD')
            serializer.save()
            if previous_primary != primary or previous_secondary != secondary:
                cache = VolatileFactory.get_client()
                key = 'ovs_dedupe_fdchange_{0}'.format(storagerouter.guid)
                task_id = cache.get(key)
                if task_id:
                    # Key exists, task was already scheduled
                    # If task is already running, the revoke message will
                    # be ignored
                    revoke(task_id)
                async_result = MDSServiceController.mds_checkup.s().apply_async(countdown=60)
                cache.set(key, async_result.id, 600)  # Store the task id

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
    def add_vpool(self, storagerouter, call_parameters):
        """
        Adds a vPool to a given Storage Router
        """
        call_parameters['storagerouter_ip'] = storagerouter.ip
        return StorageRouterController.add_vpool.s(call_parameters).apply_async(routing_key='sr.{0}'.format(storagerouter.machine_id))

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
