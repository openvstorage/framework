# Copyright 2014 CloudFounders NV
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
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import action, link
from rest_framework.exceptions import NotAcceptable
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.lib.storagerouter import StorageRouterController
from ovs.lib.storagedriver import StorageDriverController
from backend.decorators import required_roles, return_list, return_object, return_task, load, log


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
    @load(StorageRouter)
    def get_available_actions(self):
        """
        Gets a list of all available actions
        """
        actions = []
        storagerouters = StorageRouterList.get_storagerouters()
        if len(storagerouters) > 1:
            actions.append('MOVE_AWAY')
        return Response(actions, status=status.HTTP_200_OK)

    @action()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def get_physical_metadata(self, storagerouter, files=None):
        """
        Returns a list of mountpoints on the given Storage Router
        """
        files = [] if files is None else files.strip().split(',')
        return StorageRouterController.get_physical_metadata.s(files, storagerouter.guid).apply_async(
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
        fields = ['vpool_name', 'type', 'connection_host', 'connection_port', 'connection_timeout', 'connection_backend',
                  'connection_username', 'connection_password', 'mountpoint_temp', 'mountpoint_bfs', 'mountpoint_md',
                  'mountpoint_readcache1', 'mountpoint_readcache2', 'mountpoint_writecache', 'mountpoint_foc',
                  'storage_ip', 'config_cinder', 'cinder_controller', 'cinder_user', 'cinder_pass', 'cinder_tenant']
        parameters = {'storagerouter_ip': storagerouter.ip }
        for field in fields:
            if field not in call_parameters:
                if field in ['mountpoint_readcache2', 'connection_backend']:
                    parameters[field] = ''
                    continue
                else:
                    raise NotAcceptable('Invalid data passed: {0} is missing'.format(field))
            parameters[field] = call_parameters[field]
            if not isinstance(parameters[field], int):
                parameters[field] = str(parameters[field])

        return StorageRouterController.add_vpool.s(parameters).apply_async(routing_key='sr.{0}'.format(storagerouter.machine_id))

    @action()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def check_cinder(self, storagerouter):
        """
        Checks whether cinder process is running on the specified machine
        """
        return StorageRouterController.check_cinder.s().apply_async(routing_key='sr.{0}'.format(storagerouter.machine_id))

    @action()
    @log()
    @required_roles(['read'])
    @return_task()
    @load(StorageRouter)
    def valid_cinder_credentials(self, storagerouter, cinder_password, cinder_user, tenant_name, controller_ip):
        """
        Checks whether cinder process is running on the specified machine
        """
        return StorageRouterController.valid_cinder_credentials.s(cinder_password, cinder_user, tenant_name, controller_ip).apply_async(
            routing_key='sr.{0}'.format(storagerouter.machine_id))
