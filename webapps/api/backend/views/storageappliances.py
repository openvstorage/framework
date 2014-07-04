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
StorageAppliance module
"""

from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import action, link
from rest_framework.exceptions import NotAcceptable
from ovs.dal.lists.storageappliancelist import StorageApplianceList
from ovs.dal.hybrids.storageappliance import StorageAppliance
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.lib.storageappliance import StorageApplianceController
from ovs.lib.storagerouter import StorageRouterController
from backend.decorators import required_roles, expose, validate, get_list, get_object, celery_task


class StorageApplianceViewSet(viewsets.ViewSet):
    """
    Information about Storage Appliances
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'storageappliances'
    base_name = 'storageappliances'

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @get_list(StorageAppliance, 'name,vpool_guid')
    def list(self, request, hints):
        """
        Overview of all Storage Appliances
        """
        _ = hints, request
        return StorageApplianceList.get_storageappliances()

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @validate(StorageAppliance)
    @get_object(StorageAppliance)
    def retrieve(self, request, obj):
        """
        Load information about a given vMachine
        """
        _ = request
        return obj

    @expose(internal=True)
    @required_roles(['view'])
    @get_list(StorageAppliance)
    def filter(self, request, pk=None, format=None, hints=None):
        """
        Filters vMachines based on a filter object
        """
        _ = pk, format, hints
        query_result = DataList({'object': StorageAppliance,
                                 'data': DataList.select.DESCRIPTOR,
                                 'query': request.DATA['query']}).data
        return DataObjectList(query_result, StorageAppliance)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'system'])
    @validate(StorageAppliance)
    @celery_task()
    def move_away(self, request, obj):
        """
        Moves away all vDisks from all Storage Routers this Storage Appliance is serving
        """
        _ = request
        return StorageRouterController.move_away.delay(obj.guid)

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(StorageAppliance)
    def get_available_actions(self, request, obj):
        """
        Gets a list of all available actions
        """
        _ = request, obj
        actions = []
        storageappliances = StorageApplianceList.get_storageappliances()
        if len(storageappliances) > 1:
            actions.append('MOVE_AWAY')
        return Response(actions, status=status.HTTP_200_OK)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @validate(StorageAppliance)
    @celery_task()
    def get_physical_metadata(self, request, obj):
        """
        Returns a list of mountpoints on the given Storage Appliance
        """
        files = []
        if 'files' in request.DATA:
            files = request.DATA['files'].strip().split(',')
        return StorageApplianceController.get_physical_metadata.s(files, obj.guid).apply_async(
            routing_key='sa.{0}'.format(obj.machineid)
        )

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(StorageAppliance)
    @celery_task()
    def get_version_info(self, request, obj):
        """
        Gets version information of a given Storage Appliance
        """
        _ = request
        return StorageApplianceController.get_version_info.s(obj.guid).apply_async(
            routing_key='sa.{0}'.format(obj.machineid)
        )

    @action()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(StorageAppliance)
    @celery_task()
    def check_s3(self, request, obj):
        """
        Validates whether connection to a given S3 backend can be made
        """
        _ = obj
        fields = ['host', 'port', 'accesskey', 'secretkey']
        parameters = {}
        for field in fields:
            if field not in request.DATA:
                raise NotAcceptable('Invalid data passed: {0} is missing'.format(field))
            parameters[field] = request.DATA[field]
            if not isinstance(parameters[field], int):
                parameters[field] = str(parameters[field])
        return StorageApplianceController.check_s3.delay(**parameters)

    @action()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(StorageAppliance)
    @celery_task()
    def check_mtpt(self, request, obj):
        """
        Validates whether the mountpoint for a vPool is available
        """
        fields = ['name']
        parameters = {}
        for field in fields:
            if field not in request.DATA:
                raise NotAcceptable('Invalid data passed: {0} is missing'.format(field))
            parameters[field] = request.DATA[field]
            if not isinstance(parameters[field], int):
                parameters[field] = str(parameters[field])
        return StorageApplianceController.check_mtpt.s(parameters['name']).apply_async(
            routing_key='sa.{0}'.format(obj.machineid)
        )

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(StorageAppliance)
    @celery_task()
    def add_vpool(self, request, obj):
        """
        Adds a vPool to a given Storage Appliance
        """
        fields = ['vpool_name', 'backend_type', 'connection_host', 'connection_port', 'connection_timeout',
                  'connection_username', 'connection_password', 'mountpoint_temp', 'mountpoint_bfs', 'mountpoint_md',
                  'mountpoint_cache', 'storage_ip', 'vrouter_port']
        parameters = {'storageappliance_ip': obj.ip}
        for field in fields:
            if field not in request.DATA:
                raise NotAcceptable('Invalid data passed: {0} is missing'.format(field))
            parameters[field] = request.DATA[field]
            if not isinstance(parameters[field], int):
                parameters[field] = str(parameters[field])

        return StorageApplianceController.add_vpool.s(parameters).apply_async(routing_key='sa.{0}'.format(obj.machineid))
