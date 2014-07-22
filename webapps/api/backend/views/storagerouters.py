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
from backend.decorators import required_roles, expose, return_list, return_object, celery_task, discover


class StorageRouterViewSet(viewsets.ViewSet):
    """
    Information about Storage Routers
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'storagerouters'
    base_name = 'storagerouters'

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @return_list(StorageRouter, 'name')
    @discover()
    def list(self):
        """
        Overview of all Storage Routers
        """
        return StorageRouterList.get_storagerouters()

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @return_object(StorageRouter)
    @discover(StorageRouter)
    def retrieve(self, storagerouter):
        """
        Load information about a given vMachine
        """
        return storagerouter

    @expose(internal=True)
    @required_roles(['view'])
    @return_list(StorageRouter)
    @discover()
    def filter(self, query):
        """
        Filters vMachines based on a filter object
        """
        query_result = DataList({'object': StorageRouter,
                                 'data': DataList.select.DESCRIPTOR,
                                 'query': query}).data
        return DataObjectList(query_result, StorageRouter)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'system'])
    @celery_task()
    @discover(StorageRouter)
    def move_away(self, storagerouter):
        """
        Moves away all vDisks from all Storage Drivers this Storage Router is serving
        """
        return StorageDriverController.move_away.delay(storagerouter.guid)

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @discover(StorageRouter)
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
    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @celery_task()
    @discover(StorageRouter)
    def get_physical_metadata(self, storagerouter, files=None):
        """
        Returns a list of mountpoints on the given Storage Router
        """
        files = [] if files is None else files.strip().split(',')
        return StorageRouterController.get_physical_metadata.s(files, storagerouter.guid).apply_async(
            routing_key='sr.{0}'.format(storagerouter.machineid)
        )

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @celery_task()
    @discover(StorageRouter)
    def get_version_info(self, storagerouter):
        """
        Gets version information of a given Storage Router
        """
        return StorageRouterController.get_version_info.s(storagerouter.guid).apply_async(
            routing_key='sr.{0}'.format(storagerouter.machineid)
        )

    @action()
    @expose(internal=True)
    @required_roles(['view'])
    @celery_task()
    @discover(StorageRouter)
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
    @expose(internal=True)
    @required_roles(['view'])
    @celery_task()
    @discover(StorageRouter)
    def check_mtpt(self, storagerouter, name):
        """
        Validates whether the mountpoint for a vPool is available
        """
        name = str(name)
        return StorageRouterController.check_mtpt.s(name).apply_async(
            routing_key='sr.{0}'.format(storagerouter.machineid)
        )

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @celery_task()
    @discover(StorageRouter)
    def add_vpool(self, storagerouter, call_parameters):
        """
        Adds a vPool to a given Storage Router
        """
        fields = ['vpool_name', 'backend_type', 'connection_host', 'connection_port', 'connection_timeout',
                  'connection_username', 'connection_password', 'mountpoint_temp', 'mountpoint_bfs', 'mountpoint_md',
                  'mountpoint_cache', 'storage_ip', 'vrouter_port']
        parameters = {'storagerouter_ip': storagerouter.ip}
        for field in fields:
            if field not in call_parameters:
                raise NotAcceptable('Invalid data passed: {0} is missing'.format(field))
            parameters[field] = call_parameters[field]
            if not isinstance(parameters[field], int):
                parameters[field] = str(parameters[field])

        return StorageRouterController.add_vpool.s(parameters).apply_async(routing_key='sr.{0}'.format(storagerouter.machineid))
