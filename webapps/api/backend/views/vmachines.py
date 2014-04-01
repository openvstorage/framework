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
from backend.serializers.serializers import FullSerializer
from backend.decorators import required_roles, expose, validate
from backend.toolbox import Toolbox


class VMachineViewSet(viewsets.ViewSet):
    """
    Information about machines
    """
    permission_classes = (IsAuthenticated,)

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    def list(self, request, format=None):
        """
        Overview of all machines
        """
        _ = format
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
        vmachines, serializer, contents = Toolbox.handle_list(vmachines, request, default_sort='name,vpool_guid')
        serialized = serializer(VMachine, contents=contents, instance=vmachines, many=True)
        return Response(serialized.data, status=status.HTTP_200_OK)

    @expose(internal=True, customer=True)
    @required_roles(['view'])
    @validate(VMachine)
    def retrieve(self, request, obj):
        """
        Load information about a given vMachine
        """
        contents = Toolbox.handle_retrieve(request)
        return Response(FullSerializer(VMachine, contents=contents, instance=obj).data, status=status.HTTP_200_OK)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VMachine)
    def rollback(self, request, obj):
        """
        Clones a machine
        """
        if obj.is_internal or obj.is_vtemplate:
            raise NotAcceptable('vMachine should not be a VSA or vTemplate')
        task = VMachineController.rollback.delay(machineguid=obj.guid,
                                                 timestamp=request.DATA['timestamp'])
        return Response(task.id, status=status.HTTP_200_OK)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VMachine)
    def snapshot(self, request, obj):
        """
        Snapshots a given machine
        """
        if obj.is_internal or obj.is_vtemplate:
            raise NotAcceptable('vMachine should not be a VSA or vTemplate')
        label = str(request.DATA['name'])
        is_consistent = True if request.DATA['consistent'] else False  # Assure boolean type
        task = VMachineController.snapshot.delay(machineguid=obj.guid,
                                                 label=label,
                                                 is_consistent=is_consistent,
                                                 is_automatic=False)
        return Response(task.id, status=status.HTTP_200_OK)

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(VMachine)
    def get_served_children(self, request, obj):
        """
        Returns set of served vpool guids and (indirectly) served vmachine guids
        """
        _ = request
        vpool_guids = set()
        vmachine_guids = set()
        if obj.is_internal is False:
            raise NotAcceptable('vMachine is not a VSA')
        for vsr in obj.served_vsrs:
            vpool_guids.add(vsr.vpool_guid)
            for vdisk in vsr.vpool.vdisks:
                if vdisk.vsrid == vsr.vsrid and vdisk.vmachine_guid is not None:
                    vmachine_guids.add(vdisk.vmachine_guid)
        return Response({'vpool_guids': list(vpool_guids),
                         'vmachine_guids': list(vmachine_guids)}, status=status.HTTP_200_OK)

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(VMachine)
    def get_children(self, request, obj):
        """
        Returns a list of vMachines guid(s) of children of a given vMachine
        """
        _ = request
        children_vmachine_guids = set()
        if obj.is_vtemplate is False:
            raise NotAcceptable('vMachine is not a vTemplate')
        for vdisk in obj.vdisks:
            for cdisk in vdisk.child_vdisks:
                children_vmachine_guids.add(cdisk.vmachine_guid)
        return Response(children_vmachine_guids, status=status.HTTP_200_OK)

    @expose(internal=True)
    @required_roles(['view'])
    def filter(self, request, pk=None, format=None):
        """
        Filters vMachines based on a filter object
        """
        _ = pk, format
        query_result = DataList({'object': VMachine,
                                 'data': DataList.select.DESCRIPTOR,
                                 'query': request.DATA['query']}).data
        vmachines = DataObjectList(query_result, VMachine)
        vmachines, serializer, contents = Toolbox.handle_list(vmachines, request)
        serialized = serializer(VMachine, contents=contents, instance=vmachines, many=True)
        return Response(serialized.data, status=status.HTTP_200_OK)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VMachine)
    def set_as_template(self, request, obj):
        """
        Sets a given machine as template
        """
        _ = request
        task = VMachineController.set_as_template.delay(machineguid=obj.guid)
        return Response(task.id, status=status.HTTP_200_OK)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VMachine)
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
        task = VMachineController.create_from_template.delay(machineguid=obj.guid,
                                                             pmachineguid=pmachine.guid,
                                                             name=str(request.DATA['name']),
                                                             description=str(request.DATA['description']))
        return Response(task.id, status=status.HTTP_200_OK)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'create'])
    @validate(VMachine)
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
        task = VMachineController.create_multiple_from_template.delay(machineguid=obj.guid,
                                                                      pmachineguids=pmachineguids,
                                                                      amount=amount,
                                                                      start=start,
                                                                      name=str(request.DATA['name']),
                                                                      description=str(request.DATA['description']))
        return Response(task.id, status=status.HTTP_200_OK)

    @action()
    @expose(internal=True, customer=True)
    @required_roles(['view', 'system'])
    @validate(VMachine)
    def move_away(self, request, obj):
        """
        Moves away all vDisks from all VSRs this VSA is serving
        """
        _ = request
        if not obj.is_internal:
            raise NotAcceptable('vMachine is not a VSA')
        task = VolumeStorageRouterController.move_away.delay(obj.guid)
        return Response(task.id, status=status.HTTP_200_OK)

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @validate(VMachine)
    def get_target_pmachines(self, request, obj):
        """
        Gets all possible target pMachines for a given vMachine
        """
        _ = request
        if not obj.is_vtemplate:
            raise NotAcceptable('vMachine is not a vTemplate')
        pmachine_guids = set()
        for vsr in obj.vpool.vsrs:
            pmachine_guids.add(vsr.serving_vmachine.pmachine_guid)
        guids = [{'guid': guid} for guid in pmachine_guids]
        return Response(guids, status=status.HTTP_200_OK)

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
