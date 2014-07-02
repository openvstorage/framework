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
VMachine module
"""

from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import action, link
from rest_framework.exceptions import NotAcceptable
from django.http import Http404
from ovs.dal.lists.vmachinelist import VMachineList
from ovs.dal.hybrids.vmachine import VMachine
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.lib.vmachine import VMachineController
from ovs.lib.volumestoragerouter import VolumeStorageRouterController
from ovs.dal.exceptions import ObjectNotFoundException
from backend.decorators import required_roles, expose, validate, get_list, get_object, celery_task


class VMachineViewSet(viewsets.ViewSet):
    """
    Information about machines
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'vmachines'
    base_name = 'vmachines'

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @get_list(VMachine, 'name,vpool_guid')
    def list(self, request, hints):
        """
        Overview of all machines
        """
        _ = hints
        vpoolguid = request.QUERY_PARAMS.get('vpoolguid', None)
        if vpoolguid is not None:
            vpool = VPool(vpoolguid)
            vmachine_guids = []
            vmachines = []
            for vdisk in vpool.vdisks:
                if vdisk.vmachine_guid is not None and vdisk.vmachine_guid not in vmachine_guids:
                    vmachine_guids.append(vdisk.vmachine.guid)
                    if vdisk.vmachine.is_internal is False and vdisk.vmachine.is_vtemplate is False:
                        vmachines.append(vdisk.vmachine)
        else:
            vmachines = VMachineList.get_vmachines()
        return vmachines

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @validate(VMachine)
    @get_object(VMachine)
    def retrieve(self, request, obj):
        """
        Load information about a given vMachine
        """
        _ = request
        return obj

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['delete'])
    @validate(VMachine)
    @celery_task()
    def destroy(self, request, obj):
        """
        Deletes a machine
        """
        _ = request
        if not obj.is_vtemplate:
            raise NotAcceptable('vMachine should be a vTemplate')
        return VMachineController.delete.delay(machineguid=obj.guid)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VMachine)
    @celery_task()
    def rollback(self, request, obj):
        """
        Clones a machine
        """
        if obj.is_internal or obj.is_vtemplate:
            raise NotAcceptable('vMachine should not be a VSA or vTemplate')
        return VMachineController.rollback.delay(machineguid=obj.guid,
                                                 timestamp=request.DATA['timestamp'])

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VMachine)
    @celery_task()
    def snapshot(self, request, obj):
        """
        Snapshots a given machine
        """
        if obj.is_internal or obj.is_vtemplate:
            raise NotAcceptable('vMachine should not be a VSA or vTemplate')
        label = str(request.DATA['name'])
        is_consistent = True if request.DATA['consistent'] else False  # Assure boolean type
        return VMachineController.snapshot.delay(machineguid=obj.guid,
                                                 label=label,
                                                 is_consistent=is_consistent,
                                                 is_automatic=False)

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(VMachine)
    @get_list(VMachine)
    def get_served_children(self, request, obj, hints):
        """
        Returns set of served vpool guids and (indirectly) served vmachine guids
        """
        _ = request
        vmachine_guids = []
        vmachines = []
        if obj.is_internal is False:
            raise NotAcceptable('vMachine is not a VSA')
        for vsr in obj.served_vsrs:
            for vdisk in vsr.vpool.vdisks:
                if vdisk.vsrid == vsr.vsrid and \
                        vdisk.vmachine_guid is not None and \
                        vdisk.vmachine_guid not in vmachine_guids:
                    vmachine_guids.append(vdisk.vmachine_guid)
                    if hints['full'] is True:
                        # Only load full object if required
                        vmachines.append(vdisk.vmachine)
        return vmachines if hints['full'] is True else vmachine_guids

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(VMachine)
    @get_list(VMachine)
    def get_children(self, request, obj, hints):
        """
        Returns a list of vMachines guid(s) of children of a given vMachine
        """
        _ = request
        children_vmachine_guids = []
        children_vmachines = []
        if obj.is_vtemplate is False:
            raise NotAcceptable('vMachine is not a vTemplate')
        for vdisk in obj.vdisks:
            for cdisk in vdisk.child_vdisks:
                if cdisk.vmachine_guid not in children_vmachine_guids:
                    children_vmachine_guids.append(cdisk.vmachine_guid)
                    if hints['full'] is True:
                        # Only load full object is required
                        children_vmachines.append(cdisk.vmachine)
        return children_vmachines if hints['full'] is True else children_vmachine_guids

    @expose(internal=True)
    @required_roles(['view'])
    @get_list(VMachine)
    def filter(self, request, pk=None, format=None, hints=None):
        """
        Filters vMachines based on a filter object
        """
        _ = pk, format, hints
        query_result = DataList({'object': VMachine,
                                 'data': DataList.select.DESCRIPTOR,
                                 'query': request.DATA['query']}).data
        return DataObjectList(query_result, VMachine)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VMachine)
    @celery_task()
    def set_as_template(self, request, obj):
        """
        Sets a given machine as template
        """
        _ = request
        return VMachineController.set_as_template.delay(machineguid=obj.guid)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VMachine)
    @celery_task()
    def create_from_template(self, request, obj):
        """
        Creates a vMachine based on a vTemplate
        """
        try:
            pmachine = PMachine(request.DATA['pmachineguid'])
        except ObjectNotFoundException:
            raise Http404('pMachine could not be found')
        if obj.is_vtemplate is False:
            raise NotAcceptable('vMachine is not a vTemplate')
        return VMachineController.create_from_template.delay(machineguid=obj.guid,
                                                             pmachineguid=pmachine.guid,
                                                             name=str(request.DATA['name']),
                                                             description=str(request.DATA['description']))

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VMachine)
    @celery_task()
    def create_multiple_from_template(self, request, obj):
        """
        Creates a certain amount of vMachines based on a vTemplate
        """
        pmachineguids = request.DATA['pmachineguids']
        if len(pmachineguids) == 0:
            raise NotAcceptable
        try:
            for pmachienguid in pmachineguids:
                _ = PMachine(pmachienguid)
        except ObjectNotFoundException:
            raise Http404('pMachine could not be found')
        if obj.is_vtemplate is False:
            raise NotAcceptable('vMachine is not a vTemplate')
        amount = request.DATA['amount']
        start = request.DATA['start']
        if not isinstance(amount, int) or not isinstance(start, int):
            raise NotAcceptable('Fields amount and start should be numeric')
        amount = max(1, amount)
        start = max(0, start)
        return VMachineController.create_multiple_from_template.delay(machineguid=obj.guid,
                                                                      pmachineguids=pmachineguids,
                                                                      amount=amount,
                                                                      start=start,
                                                                      name=str(request.DATA['name']),
                                                                      description=str(request.DATA['description']))

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'system'])
    @validate(VMachine)
    @celery_task()
    def move_away(self, request, obj):
        """
        Moves away all vDisks from all VSRs this VSA is serving
        """
        _ = request
        if not obj.is_internal:
            raise NotAcceptable('vMachine is not a VSA')
        return VolumeStorageRouterController.move_away.delay(obj.guid)

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(VMachine)
    @get_list(PMachine)
    def get_target_pmachines(self, request, obj, hints):
        """
        Gets all possible target pMachines for a given vMachine
        """
        _ = request
        if not obj.is_vtemplate:
            raise NotAcceptable('vMachine is not a vTemplate')
        # Collect all vPools used by the given template
        vpool_guids = []
        vpools = []
        if obj.vpool is not None:
            if obj.vpool_guid not in vpool_guids:
                vpools.append(obj.vpool)
                vpool_guids.append(obj.vpool_guid)
        for vdisk in obj.vdisks:
            if vdisk.vpool_guid not in vpool_guids:
                vpools.append(vdisk.vpool)
                vpool_guids.append(vdisk.vpool_guid)
        # Find pMachines which have all above vPools available.
        pmachine_guids = None
        pmachines = {}
        for vpool in vpools:
            this_pmachine_guids = set()
            for vsr in vpool.vsrs:
                this_pmachine_guids.add(vsr.serving_vmachine.pmachine_guid)
                if hints['full'] is True:
                    pmachines[vsr.serving_vmachine.pmachine_guid] = vsr.serving_vmachine.pmachine
            if pmachine_guids is None:
                pmachine_guids = list(this_pmachine_guids)
            else:
                pmachine_guids = list(this_pmachine_guids & set(pmachine_guids))
        return pmachine_guids if hints['full'] is False else [pmachines[guid] for guid in pmachine_guids]

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(VMachine)
    def get_available_actions(self, request, obj):
        """
        Gets a list of all available actions
        """
        _ = request
        actions = []
        if obj.is_internal and not obj.is_vtemplate:
            vsas = VMachineList.get_vsas()
            if len(vsas) > 1:
                actions.append('MOVE_AWAY')
        return Response(actions, status=status.HTTP_200_OK)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @validate(VMachine)
    @celery_task()
    def get_physical_metadata(self, request, obj):
        """
        Returns a list of mountpoints on the given VSA
        """
        if not obj.is_internal:
            raise NotAcceptable('vMachine is not a VSA')

        files = []
        if 'files' in request.DATA:
            files = request.DATA['files'].strip().split(',')

        return VMachineController.get_physical_metadata.s(files, obj.guid).apply_async(
            routing_key='vsa.{0}'.format(obj.machineid)
        )

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(VMachine)
    @celery_task()
    def get_version_info(self, request, obj):
        """
        Gets version information of a given VSA
        """
        _ = request
        if not obj.is_internal:
            raise NotAcceptable('vMachine is not a VSA')

        return VMachineController.get_version_info.s(obj.guid).apply_async(
            routing_key='vsa.{0}'.format(obj.machineid)
        )

    @action()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(VMachine)
    @celery_task()
    def check_s3(self, request, obj):
        """
        Validates whether connection to a given S3 backend can be made
        """
        if not obj.is_internal:
            raise NotAcceptable('vMachine is not a VSA')

        fields = ['host', 'port', 'accesskey', 'secretkey']
        parameters = {}
        for field in fields:
            if field not in request.DATA:
                raise NotAcceptable('Invalid data passed: {0} is missing'.format(field))
            parameters[field] = request.DATA[field]
            if not isinstance(parameters[field], int):
                parameters[field] = str(parameters[field])

        return VMachineController.check_s3.delay(**parameters)

    @action()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(VMachine)
    @celery_task()
    def check_mtpt(self, request, obj):
        """
        Validates whether the mountpoint for a vPool is available
        """
        if not obj.is_internal:
            raise NotAcceptable('vMachine is not a VSA')

        fields = ['name']
        parameters = {}
        for field in fields:
            if field not in request.DATA:
                raise NotAcceptable('Invalid data passed: {0} is missing'.format(field))
            parameters[field] = request.DATA[field]
            if not isinstance(parameters[field], int):
                parameters[field] = str(parameters[field])

        return VMachineController.check_mtpt.s(parameters['name']).apply_async(
            routing_key='vsa.{0}'.format(obj.machineid)
        )

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VMachine)
    @celery_task()
    def add_vpool(self, request, obj):
        """
        Adds a vPool to a given VSA
        """
        if not obj.is_internal:
            raise NotAcceptable('vMachine is not a VSA')

        fields = ['vpool_name', 'backend_type', 'connection_host', 'connection_port', 'connection_timeout',
                  'connection_username', 'connection_password', 'mountpoint_temp', 'mountpoint_dfs', 'mountpoint_md',
                  'mountpoint_cache', 'storage_ip', 'vrouter_port']
        parameters = {'vsa_ip': obj.ip}
        for field in fields:
            if field not in request.DATA:
                raise NotAcceptable('Invalid data passed: {0} is missing'.format(field))
            parameters[field] = request.DATA[field]
            if not isinstance(parameters[field], int):
                parameters[field] = str(parameters[field])

        return VMachineController.add_vpool.s(parameters).apply_async(routing_key='vsa.{0}'.format(obj.machineid))
