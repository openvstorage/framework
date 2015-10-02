# Copyright 2014 Open vStorage NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
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
from rest_framework import status, viewsets
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import action, link
from rest_framework.exceptions import NotAcceptable
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.lib.storagerouter import StorageRouterController
from ovs.lib.storagedriver import StorageDriverController
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
        Load information about a given vMachine
        """
        return storagerouter

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
